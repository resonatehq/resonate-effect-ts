import { Effect, Layer } from "effect";
import { describe, expect, it } from "vitest";
import * as Codec from "../src/codec.js";
import type { Context } from "../src/context.js";
import * as Engine from "../src/engine.js";
import * as Pass from "../src/pass.js";
import * as Registry from "../src/registry.js";
import { decode, root, server } from "./server.js";

// The whole loop: claim, execute, suspend or fulfill, continue. The engine and
// the pass together, over a server that really stores promises.

const options: Engine.EngineOptions = { pid: "worker-1", ttl: 30_000 };

const world = (workflows: Registry.Registration[], name: string, args: unknown[] = []) => {
  const s = server();
  s.seed(root(name, args));
  const base = Layer.mergeAll(s.layer, Codec.layerJson, Registry.layer(workflows));
  const layer = Layer.merge(base, Pass.layer({ target: "poll://worker-1" }).pipe(Layer.provide(base)));
  const process = (task: Engine.Task) => Engine.process(options)(task).pipe(Effect.provide(layer));
  return { server: s, process };
};

describe("end to end", () => {
  const remote = Registry.register("remote", (_ctx: Context, n: number) => Effect.succeed(n));
  const workflow = Registry.register("workflow", (ctx: Context, n: number) =>
    Effect.gen(function* () {
      const doubled = yield* ctx.run(
        Registry.register("double", (_c: Context, x: number) => Effect.succeed(x * 2)),
        n,
      );
      const fromRemote = yield* ctx.rpc(remote, doubled);
      return `${doubled}/${fromRemote}`;
    }),
  );
  const double = Registry.register("double", (_c: Context, x: number) => Effect.succeed(x * 2));

  it("claims, runs to the edge, suspends, then resumes and fulfills", async () => {
    const { server: s, process } = world([workflow, double, remote], "workflow", [5]);

    // Pass one: the local child settles, the remote call is the edge.
    const first = await Effect.runPromise(process(Engine.unclaimed("foo", 1)));

    expect(first).toEqual({ _tag: "Suspended", id: "foo", awaited: ["foo:2"] });
    expect(s.kinds).toEqual(["task.acquire", "task.fence", "task.fence", "task.fence", "task.suspend"]);
    expect(decode(s.promises.get("foo:1")?.value)).toBe(10);
    expect(s.promises.get("foo:2")?.state).toBe("pending");
    expect(s.callbacks).toEqual([{ awaiter: "foo", awaited: "foo:2" }]);
    expect(s.promises.get("foo")?.state).toBe("pending");

    // Someone else runs the remote call.
    s.settleExternally("foo:2", 99);

    // Pass two: the local child replays from the preload, the remote replays
    // from its record, and the root is fulfilled.
    s.kinds.length = 0;
    const second = await Effect.runPromise(process(Engine.unclaimed("foo", 2)));

    expect(second).toMatchObject({ _tag: "Completed", id: "foo", state: "resolved" });
    expect(decode((second as { value: { data?: string } }).value)).toBe("10/99");
    expect(s.promises.get("foo")?.state).toBe("resolved");
    // The local child replayed out of the preload for free — it shares the
    // root's branch. The remote child does not, so it costs exactly one
    // idempotent create, which comes back already settled.
    expect(s.kinds).toEqual(["task.acquire", "task.fence", "task.fulfill"]);
    expect(s.journal).toContain("dedup foo:2");
  });

  it("continues on the same lease when the awaited promise settled underneath it", async () => {
    const { server: s, process } = world([workflow, double, remote], "workflow", [5]);

    // Settle the remote call before the pass ever asks — so task.suspend
    // answers 300 rather than parking the task.
    s.seed({
      id: "foo:2",
      state: "resolved",
      param: {},
      value: { data: Buffer.from("7", "utf8").toString("base64") },
      tags: { "resonate:branch": "foo:2" },
      timeoutAt: 10_000_000,
      createdAt: 0,
    });

    const out = await Effect.runPromise(process(Engine.unclaimed("foo", 1)));

    expect(out).toMatchObject({ _tag: "Completed", state: "resolved" });
    expect(decode((out as { value: { data?: string } }).value)).toBe("10/7");
    // Never suspended: the create came straight back settled.
    expect(s.kinds).not.toContain("task.suspend");
    expect(s.kinds).toContain("task.fulfill");
  });

  it("drives a claimed task without a second round trip", async () => {
    const plain = Registry.register("plain", (_ctx: Context) => Effect.succeed("done"));
    const { server: s, process } = world([plain], "plain");

    const out = await Effect.runPromise(
      process(Engine.claimed({ id: "foo", state: "acquired", version: 1, resumes: [] }, s.promises.get("foo")!)),
    );

    expect(out).toMatchObject({ _tag: "Completed", state: "resolved" });
    expect(s.kinds).toEqual(["task.fulfill"]);
  });

  it("suspends on every edge a fan-out reaches, in one pass", async () => {
    const fan = Registry.register("fan", (ctx: Context) =>
      Effect.all([ctx.rpc(remote, 1), ctx.rpc(remote, 2), ctx.rpc(remote, 3)], { concurrency: "unbounded" }),
    );
    const { server: s, process } = world([fan, remote], "fan");

    const out = await Effect.runPromise(process(Engine.unclaimed("foo", 1)));

    expect(out).toEqual({ _tag: "Suspended", id: "foo", awaited: ["foo:1", "foo:2", "foo:3"] });
    // One task.suspend registering all three callbacks — one resume wakes it.
    expect(s.kinds.filter((k) => k === "task.suspend")).toHaveLength(1);
    expect(s.callbacks.map((c) => c.awaited)).toEqual(["foo:1", "foo:2", "foo:3"]);
  });
});
