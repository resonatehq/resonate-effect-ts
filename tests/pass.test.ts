import { Effect, Layer } from "effect";
import { describe, expect, it } from "vitest";
import * as Codec from "../src/codec.js";
import type { Context } from "../src/context.js";
import type { Execution } from "../src/executor.js";
import * as Pass from "../src/pass.js";
import type { PromiseRecord } from "../src/protocol.js";
import * as Registry from "../src/registry.js";
import { decode, root, server, task } from "./server.js";

const run = (
  workflows: Registry.Registration[],
  rootPromise: PromiseRecord,
  opts: { preload?: PromiseRecord[]; latency?: (id: string) => number; seed?: PromiseRecord[] } = {},
) => {
  const s = server({ latency: opts.latency });
  for (const p of opts.seed ?? []) s.seed(p);
  const effect = Pass.executeUntilBlocked(
    { task: task(), promise: rootPromise, preload: opts.preload ?? [] },
    { target: "poll://worker" },
  ).pipe(Effect.provide(Layer.mergeAll(s.layer, Codec.layerJson, Registry.layer(workflows))));
  return { server: s, effect };
};

const value = (e: Execution) => (e._tag === "Completed" ? decode(e.value) : undefined);

describe("a pass that completes", () => {
  it("runs a workflow and reports its value, encoded", async () => {
    const greet = Registry.register("greet", (_ctx: Context, name: string) => Effect.succeed(`hi ${name}`));
    const { effect } = run([greet], root("greet", ["ada"]));

    const out = await Effect.runPromise(effect);

    expect(out._tag).toBe("Completed");
    expect(out).toMatchObject({ id: "foo", state: "resolved" });
    expect(value(out)).toBe("hi ada");
  });

  it("reports a typed failure as a rejection", async () => {
    const boom = Registry.register("boom", (_ctx: Context) => Effect.fail({ code: "nope" }));
    const { effect } = run([boom], root("boom"));

    const out = await Effect.runPromise(effect);

    expect(out).toMatchObject({ _tag: "Completed", state: "rejected" });
    expect(value(out)).toEqual({ code: "nope" });
  });

  it("dedups a root that is already settled without running anything", async () => {
    let ran = false;
    const once = Registry.register("once", (_ctx: Context) => Effect.sync(() => (ran = true)));
    const settled = root("once", [], {
      state: "resolved",
      value: { data: Buffer.from('"cached"', "utf8").toString("base64") },
    });
    const { effect } = run([once], settled);

    const out = await Effect.runPromise(effect);

    expect(out).toMatchObject({ _tag: "Completed", state: "resolved" });
    expect(value(out)).toBe("cached");
    expect(ran).toBe(false);
  });

  it("drops a timer that is not yet due", async () => {
    const { effect, server: s } = run([], root("unused", [], { tags: { "resonate:timer": "true" } }));

    const out = await Effect.runPromise(effect);

    expect(out).toEqual({ _tag: "Suspended", awaited: [] });
    expect(s.journal).toEqual([]);
  });
});

describe("ctx.run — a local child", () => {
  const child = Registry.register("child", (_ctx: Context, n: number) => Effect.succeed(n * 2));
  const parent = Registry.register("parent", (ctx: Context) => Effect.map(ctx.run(child, 21), (v) => `got ${v}`));

  it("creates the child's promise, runs it, and settles it", async () => {
    const { effect, server: s } = run([parent, child], root("parent"));

    const out = await Effect.runPromise(effect);

    expect(value(out)).toBe("got 42");
    expect(s.journal).toEqual([">create foo:1", "create foo:1", ">settle foo:1", "settle foo:1 resolved"]);
    expect(decode(s.promises.get("foo:1")?.value)).toBe(42);
    // A local child stays on the parent's branch, which is what the preload groups.
    expect(s.promises.get("foo:1")?.tags["resonate:branch"]).toBe("foo");
    expect(s.promises.get("foo:1")?.tags["resonate:parent"]).toBe("foo");
  });

  it("replays a settled child from the preload without touching the server", async () => {
    let ran = 0;
    const counted = Registry.register("child", (_ctx: Context, n: number) => Effect.sync(() => ++ran && n * 2));
    const done: PromiseRecord = {
      id: "foo:1",
      state: "resolved",
      param: {},
      value: { data: Buffer.from("99", "utf8").toString("base64") },
      tags: { "resonate:branch": "foo" },
      timeoutAt: 10_000_000,
      createdAt: 0,
    };
    const { effect, server: s } = run([parent, counted], root("parent"), { preload: [done] });

    const out = await Effect.runPromise(effect);

    expect(value(out)).toBe("got 99");
    expect(ran).toBe(0);
    expect(s.journal).toEqual([]);
  });

  it("nests: ids follow the Dewey path", async () => {
    const leaf = Registry.register("leaf", (_ctx: Context) => Effect.succeed("leaf"));
    const mid = Registry.register("mid", (ctx: Context) => ctx.run(leaf));
    const top = Registry.register("top", (ctx: Context) => ctx.run(mid));
    const { effect, server: s } = run([top, mid, leaf], root("top"));

    await Effect.runPromise(effect);

    expect([...s.promises.keys()]).toEqual(["foo:1", "foo:1.1"]);
  });
});

describe("ctx.rpc — the edge", () => {
  const caller = Registry.register("caller", (ctx: Context) => ctx.rpc("remote", 1));

  it("suspends on the promise it cannot settle, and settles nothing", async () => {
    const { effect, server: s } = run([caller], root("caller"));

    const out = await Effect.runPromise(effect);

    expect(out).toEqual({ _tag: "Suspended", awaited: ["foo:1"] });
    expect(s.journal).toEqual([">create foo:1", "create foo:1"]);
    // A remote child roots its own branch and carries a target.
    expect(s.promises.get("foo:1")?.tags["resonate:branch"]).toBe("foo:1");
    expect(s.promises.get("foo:1")?.tags["resonate:target"]).toBe("poll://worker");
  });

  it("replays once the awaited promise has settled", async () => {
    const settled: PromiseRecord = {
      id: "foo:1",
      state: "resolved",
      param: {},
      value: { data: Buffer.from('"remote said hi"', "utf8").toString("base64") },
      tags: { "resonate:branch": "foo:1" },
      timeoutAt: 10_000_000,
      createdAt: 0,
    };
    const { effect } = run([caller], root("caller"), { seed: [settled] });

    const out = await Effect.runPromise(effect);

    expect(out._tag).toBe("Completed");
    expect(value(out)).toBe("remote said hi");
  });

  it("suspends even when the body returned, if a remote child is outstanding", async () => {
    const forgetful = Registry.register("forgetful", (ctx: Context) =>
      Effect.gen(function* () {
        yield* Effect.forkChild(ctx.rpc("remote"));
        yield* Effect.sleep("30 millis"); // let the fork reach its edge
        return "returned anyway";
      }),
    );
    const { effect } = run([forgetful], root("forgetful"));

    const out = await Effect.runPromise(effect);

    expect(out).toEqual({ _tag: "Suspended", awaited: ["foo:1"] });
  });
});
