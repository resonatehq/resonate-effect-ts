import { Effect, Fiber, Layer } from "effect";
import { describe, expect, it } from "vitest";
import * as Codec from "../src/codec.js";
import type { Context } from "../src/context.js";
import * as Pass from "../src/pass.js";
import * as Registry from "../src/registry.js";
import { root, server, task } from "./server.js";

const run = (workflows: Registry.Registration[], name: string, opts: { latency?: (id: string) => number } = {}) => {
  const s = server({ latency: opts.latency });
  const effect = Pass.executeUntilBlocked(
    { task: task(), promise: root(name), preload: [] },
    { target: "poll://worker" },
  ).pipe(Effect.provide(Layer.mergeAll(s.layer, Codec.layerJson, Registry.layer(workflows))));
  return { server: s, effect };
};

describe("creation is ordered, even under concurrency", () => {
  it("issues one create at a time, each waiting for the server", async () => {
    const fan = Registry.register("fan", (ctx: Context) =>
      Effect.all([ctx.rpc("a"), ctx.rpc("b"), ctx.rpc("c")], { concurrency: "unbounded" }),
    );
    // Reverse latency: if creates raced, foo:3 would land first.
    const { effect, server: s } = run([fan], "fan", {
      latency: (id) => ({ "foo:1": 40, "foo:2": 20, "foo:3": 1 })[id] ?? 0,
    });

    const out = await Effect.runPromise(effect);

    // Never interleaved: request, response, request, response, ...
    expect(s.journal).toEqual([
      ">create foo:1",
      "create foo:1",
      ">create foo:2",
      "create foo:2",
      ">create foo:3",
      "create foo:3",
    ]);
    // Ids follow source order, not completion order.
    expect(out).toEqual({ _tag: "Suspended", awaited: ["foo:1", "foo:2", "foo:3"] });
  });

  it("orders ids by construction, not by which fiber the scheduler starts", async () => {
    const forks = Registry.register("forks", (ctx: Context) =>
      Effect.gen(function* () {
        const a = yield* Effect.fork(ctx.rpc("a"));
        const b = yield* Effect.fork(ctx.rpc("b"));
        yield* Effect.sleep("40 millis");
        return yield* Fiber.joinAll([a, b]);
      }),
    );
    const { effect, server: s } = run([forks], "forks", { latency: (id) => (id === "foo:1" ? 20 : 1) });

    await Effect.runPromise(effect);

    expect(s.journal).toEqual([">create foo:1", "create foo:1", ">create foo:2", "create foo:2"]);
  });
});

describe("one pass discovers every edge", () => {
  const leafRpc = Registry.register("leaf", (ctx: Context) => ctx.rpc("remote"));

  const flat = Registry.register("flat", (ctx: Context) =>
    Effect.all([ctx.rpc("a"), ctx.rpc("b"), ctx.rpc("c")], { concurrency: "unbounded" }),
  );

  it("collects all three under a flat fan-out", async () => {
    // Any real server takes some time to answer, so every sibling fiber has
    // started by the time the first one reaches its edge. Started is what the
    // uninterruptible frame protects: told to stop, a sibling runs on to its
    // own edge, records it, and only then goes down.
    const { effect } = run([flat], "flat", { latency: () => 5 });

    expect(await Effect.runPromise(effect)).toEqual({
      _tag: "Suspended",
      awaited: ["foo:1", "foo:2", "foo:3"],
    });
  });

  it("stays consistent when a sibling never starts at all", async () => {
    // An instant server: the first branch can finish before the others are
    // scheduled, and a fiber that has not started cannot be inside the region
    // that would protect it. The pass then suspends on fewer edges and costs an
    // extra round trip -- but never on an edge it did not create, which is the
    // invariant that matters. The unstarted branches minted no promise, so the
    // next pass creates them under the very same ids.
    const { effect, server: s } = run([flat], "flat", { latency: () => 0 });

    const out = await Effect.runPromise(effect);

    const awaited = (out as { awaited: ReadonlyArray<string> }).awaited;
    expect(awaited.length).toBeGreaterThanOrEqual(1);
    const created = [...s.promises.keys()];
    expect([...awaited].sort()).toEqual(created.sort());
  });

  it("collects all three through nested local children, however skewed", async () => {
    const nested = Registry.register("nested", (ctx: Context) =>
      Effect.all([ctx.run(leafRpc), ctx.run(leafRpc), ctx.run(leafRpc)], { concurrency: "unbounded" }),
    );
    // The first branch reaches its edge long before the others reach theirs.
    const { effect } = run([nested, leafRpc], "nested", {
      latency: (id) => (id.startsWith("foo:1") ? 1 : id.startsWith("foo:2") ? 30 : 60),
    });

    const out = await Effect.runPromise(effect);

    expect(out._tag).toBe("Suspended");
    expect([...(out as { awaited: ReadonlyArray<string> }).awaited].sort()).toEqual(["foo:1.1", "foo:2.1", "foo:3.1"]);
  });
});

describe("the hop upward", () => {
  it("stops the whole onion and settles nothing", async () => {
    const leaf = Registry.register("leaf", (ctx: Context) => ctx.rpc("remote"));
    const mid = Registry.register("mid", (ctx: Context) => ctx.run(leaf));
    const top = Registry.register("top", (ctx: Context) => ctx.run(mid));
    const { effect, server: s } = run([top, mid, leaf], "top");

    const out = await Effect.runPromise(effect);

    expect(out).toEqual({ _tag: "Suspended", awaited: ["foo:1.1.1"] });
    // Three promises created down the lineage, not one of them settled.
    expect(s.journal.filter((e) => e.startsWith("settle"))).toEqual([]);
    expect([...s.promises.keys()]).toEqual(["foo:1", "foo:1.1", "foo:1.1.1"]);
    expect([...s.promises.values()].every((p) => p.state === "pending")).toBe(true);
  });
});

describe("the guards", () => {
  const blocked = Registry.register("blocked", (ctx: Context) => ctx.rpc("remote"));

  it("does not settle a child whose subtree reached an edge, even if the block was caught", async () => {
    const swallow = Registry.register("swallow", (ctx: Context) =>
      ctx.run(blocked).pipe(Effect.catchAllCause(() => Effect.succeed("substituted"))),
    );
    const { effect, server: s } = run([swallow, blocked], "swallow");

    const out = await Effect.runPromise(effect);

    expect(out).toEqual({ _tag: "Suspended", awaited: ["foo:1.1"] });
    expect(s.journal.filter((e) => e.startsWith("settle"))).toEqual([]);
  });

  it.each([
    ["Effect.exit", (e: Effect.Effect<any, any>) => e.pipe(Effect.exit, Effect.as("substituted"))],
    [
      "catchAllCause",
      (e: Effect.Effect<any, any>) => e.pipe(Effect.catchAllCause(() => Effect.succeed("substituted"))),
    ],
    [
      "catchAllDefect",
      (e: Effect.Effect<any, any>) => e.pipe(Effect.catchAllDefect(() => Effect.succeed("substituted"))),
    ],
    [
      "sandbox",
      (e: Effect.Effect<any, any>) =>
        e.pipe(
          Effect.sandbox,
          Effect.catchAll(() => Effect.succeed("substituted")),
        ),
    ],
  ])("survives a caught block via %s", async (_label, wrap) => {
    const swallow = Registry.register("swallow", (ctx: Context) => wrap(ctx.rpc("remote")));
    const { effect, server: s } = run([swallow], "swallow");

    expect(await Effect.runPromise(effect)).toEqual({ _tag: "Suspended", awaited: ["foo:1"] });
    expect(s.journal.filter((e) => e.startsWith("settle"))).toEqual([]);
  });

  it("refuses a durable operation on a path that already reached an edge", async () => {
    const diverge = Registry.register("diverge", (ctx: Context) =>
      Effect.gen(function* () {
        yield* ctx.rpc("a").pipe(Effect.catchAllCause(() => Effect.succeed("substituted")));
        return yield* ctx.rpc("b"); // would mint an id the next pass will not
      }),
    );
    const { effect, server: s } = run([diverge], "diverge");

    const out = await Effect.runPromise(effect);

    expect(out).toEqual({ _tag: "Suspended", awaited: ["foo:1"] });
    // foo:2 was never minted, so the next pass will not find a stale record there.
    expect([...s.promises.keys()]).toEqual(["foo:1"]);
  });

  it("lets the error channel through untouched", async () => {
    const handled = Registry.register("handled", (ctx: Context) =>
      Effect.gen(function* () {
        const failing = Registry.register("failing", (_c: Context) => Effect.fail("expected"));
        return yield* ctx.run(failing).pipe(Effect.catchAll((e) => Effect.succeed(`caught ${e}`)));
      }),
    );
    const failing = Registry.register("failing", (_c: Context) => Effect.fail("expected"));
    const { effect, server: s } = run([handled, failing], "handled");

    const out = await Effect.runPromise(effect);

    expect(out).toMatchObject({ _tag: "Completed", state: "resolved" });
    // The child was really rejected on the way through.
    expect(s.journal).toContain("settle foo:1 rejected");
  });
});

describe("sleep and detached", () => {
  it("sleeps by creating a timer promise and blocking on it", async () => {
    const napper = Registry.register("napper", (ctx: Context) => ctx.sleep(60_000));
    const { effect, server: s } = run([napper], "napper");

    const out = await Effect.runPromise(effect);

    expect(out).toEqual({ _tag: "Suspended", awaited: ["foo:1"] });
    expect(s.promises.get("foo:1")?.tags["resonate:timer"]).toBe("true");
    expect(s.promises.get("foo:1")?.tags["resonate:target"]).toBe("poll://worker");
  });

  it("spawns a detached workflow without suspending on it", async () => {
    const spawner = Registry.register("spawner", (ctx: Context) =>
      Effect.map(ctx.detached("background"), (h) => `spawned ${h.id.startsWith("foo:d")}`),
    );
    const { effect, server: s } = run([spawner], "spawner");

    const out = await Effect.runPromise(effect);

    expect(out).toMatchObject({ _tag: "Completed", state: "resolved" });
    const spawned = [...s.promises.values()].find((p) => p.id !== "foo");
    expect(spawned?.tags["resonate:parent"]).toBe("foo");
    expect(spawned?.tags["resonate:branch"]).toBe(spawned?.id);
  });
});
