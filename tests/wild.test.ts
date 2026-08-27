import { Effect, Fiber, Layer } from "effect";
import { describe, expect, it } from "vitest";
import * as Codec from "../src/codec.js";
import type { Context } from "../src/context.js";
import * as Pass from "../src/pass.js";
import type { PromiseRecord } from "../src/protocol.js";
import * as Registry from "../src/registry.js";
import { decode, root, server, task } from "./server.js";

// Real timers, stashed everywhere a workflow could stash them: nested children,
// forked-and-joined fibers, catch handlers, finalizers, forEach, a race, and
// plain code before and after durable calls. One rpc blocks the whole thing.
// Everything that is not transitively blocked on it has to finish anyway.

const wait = (ms: number) => Effect.promise(() => new Promise((r) => setTimeout(r, ms)));

let calls: string[] = [];

const query = Registry.register("query", (_c: Context, name: string, ms: number) =>
  Effect.gen(function* () {
    yield* wait(ms);
    calls.push(name);
    return `${name}@${ms}`;
  }),
);

const failing = Registry.register("failing", (_c: Context) =>
  Effect.gen(function* () {
    yield* wait(250);
    calls.push("failing");
    return yield* Effect.fail("boom");
  }),
);

// Three deep, with non-durable work above, between and below the durable calls.
const level3 = Registry.register("level3", (c: Context) => c.run(query, "deep", 1000));
const level2 = Registry.register("level2", (c: Context) =>
  Effect.gen(function* () {
    yield* wait(200);
    const v = yield* c.run(level3);
    yield* wait(150);
    return `l2(${v})`;
  }),
);
const level1 = Registry.register("level1", (c: Context) =>
  Effect.gen(function* () {
    const v = yield* c.run(level2);
    yield* wait(300);
    return `l1(${v})`;
  }),
);

const finalized = Registry.register("finalized", (c: Context) =>
  c.run(query, "in-finalizer-scope", 600).pipe(Effect.ensuring(wait(120))),
);

const wild = Registry.register("wild", (ctx: Context) =>
  Effect.all(
    [
      // a flat one-second query
      ctx.run(query, "flat", 1000),
      // three levels deep, work at every level
      ctx.run(level1),
      // forked, non-durable work in between, joined after
      Effect.gen(function* () {
        const f = yield* Effect.fork(ctx.run(query, "forked", 800));
        yield* wait(200);
        return yield* Fiber.join(f);
      }),
      // a child that fails, recovered by another child started from the handler
      // Built inside a catch handler, so *when* it is constructed depends on
      // timing. Pinned, or it would take whatever number the counter is at.
      ctx.run(failing).pipe(Effect.catchAll(() => ctx.run(query, ["recovered", 400], { id: "wild-recovered" }))),
      // a batch, each element its own durable child
      // Same: built inside a forEach body, so pinned too.
      Effect.forEach([300, 500, 700], (ms) => ctx.run(query, [`each${ms}`, ms], { id: `wild-each${ms}` }), {
        concurrency: "unbounded",
      }),
      // a race whose winner is real work and whose loser is a remote call
      ctx.race([(c) => c.run(query, "raced", 700), (c) => c.rpc("never-answers")]),
      // a finalizer that does work on the way out
      ctx.run(finalized),
      // and the one thing that actually blocks
      ctx.rpc("remote"),
    ],
    { concurrency: "unbounded" },
  ),
);

const registrations = [wild, query, failing, level1, level2, level3, finalized];

const runPass = (preload: PromiseRecord[] = [], seed: PromiseRecord[] = []) => {
  const s = server({ latency: () => 1 });
  for (const p of seed) s.seed(p);
  const effect = Pass.executeUntilBlocked(
    { task: task(), promise: root("wild"), preload },
    { target: "poll://worker" },
  ).pipe(Effect.provide(Layer.mergeAll(s.layer, Codec.layerJson, Registry.layer(registrations))));
  return { server: s, effect };
};

/** A local promise pending with nothing blocked under it is abandoned work. */
const abandoned = (s: ReturnType<typeof server>, awaited: ReadonlyArray<string>) =>
  [...s.promises.values()]
    .filter(
      (p) =>
        p.tags["resonate:scope"] === "local" &&
        p.state === "pending" &&
        !awaited.some((a) => a === p.id || a.startsWith(`${p.id}.`) || a.startsWith(`${p.id}:`)),
    )
    .map((p) => p.id);

describe("a pass full of real work, blocked on one call", () => {
  it("finishes every unblocked branch and suspends only on the rpc", async () => {
    calls = [];
    const { effect, server: s } = runPass();

    const started = Date.now();
    const out = await Effect.runPromise(effect);
    const elapsed = Date.now() - started;

    // Only the top-level rpc. The race's losing rpc was cancelled, not awaited.
    expect(out._tag).toBe("Suspended");
    const awaited = (out as { awaited: ReadonlyArray<string> }).awaited;
    expect(awaited).toHaveLength(1);
    expect(s.promises.get(awaited[0]!)?.tags["resonate:target"]).toBe("poll://worker");

    // Nothing was thrown away mid-flight.
    expect(abandoned(s, awaited)).toEqual([]);

    // Every query ran, exactly once each.
    expect(calls.sort()).toEqual([
      "deep",
      "each300",
      "each500",
      "each700",
      "failing",
      "flat",
      "forked",
      "in-finalizer-scope",
      "raced",
      "recovered",
    ]);

    // The branches really ran concurrently: the longest chain is level1 —
    // 200 + 1000 + 150 + 300 = 1650ms — not the 6.2s sum of every timer.
    expect(elapsed).toBeGreaterThan(1600);
    expect(elapsed).toBeLessThan(3500);

    // The race recorded its winner and cancelled its loser.
    const raceLoser = [...s.promises.values()].find((p) => p.state === "rejected_canceled");
    expect(raceLoser).toBeDefined();
  }, 30_000);

  it("replays all of it on the next pass without re-running a single query", async () => {
    calls = [];
    const first = runPass();
    const out = await Effect.runPromise(first.effect);
    const awaited = (out as { awaited: ReadonlyArray<string> }).awaited;
    const ranFirst = [...calls].sort();

    // Resume: the branch comes back in the preload, everything else is on the
    // server, and the awaited call has been answered by someone else.
    calls = [];
    first.server.settleExternally(awaited[0]!, "answered");
    const preload = [...first.server.promises.values()].filter((p) => p.tags["resonate:branch"] === "foo");
    const second = runPass(preload, [...first.server.promises.values()]);

    const started = Date.now();
    const out2 = await Effect.runPromise(second.effect);
    const elapsed = Date.now() - started;

    expect(out2).toMatchObject({ _tag: "Completed", state: "resolved" });
    // Not one query ran again — every result came from the record.
    expect(calls).toEqual([]);
    expect(ranFirst).toHaveLength(10);
    // And with no work to redo it is fast.
    expect(elapsed).toBeLessThan(500);

    // Every branch got back its own result, in branch order.
    const value = decode((out2 as { value: { data?: string } }).value) as unknown[];
    expect(value).toEqual([
      "flat@1000",
      "l1(l2(deep@1000))",
      "forked@800",
      "recovered@400",
      ["each300@300", "each500@500", "each700@700"],
      "raced@700",
      "in-finalizer-scope@600",
      "answered",
    ]);
  }, 30_000);
});

describe("an id taken twice is caught, not silently obeyed", () => {
  // Two calls that differ only in when their callbacks fire. On the first pass
  // the catch handler runs after a 200ms failure, so it takes a later number
  // than the forEach body. On replay the failure comes back from the record at
  // once, the handler fires first, and the two swap — each then reading a
  // promise that belongs to the other.
  const drifting = Registry.register("drifting", (ctx: Context) =>
    Effect.all(
      [
        ctx.run(failing).pipe(Effect.catchAll(() => ctx.run(query, "recovered", 10))),
        Effect.forEach([20, 30], (ms) => ctx.run(query, `each${ms}`, ms), { concurrency: "unbounded" }),
      ],
      { concurrency: "unbounded" },
    ),
  );
  const regs = [drifting, query, failing];

  const pass = (preload: PromiseRecord[], seed: PromiseRecord[]) => {
    const s = server({ latency: () => 1 });
    for (const p of seed) s.seed(p);
    return {
      server: s,
      effect: Pass.executeUntilBlocked(
        { task: task(), promise: root("drifting"), preload },
        { target: "poll://worker" },
      ).pipe(Effect.provide(Layer.mergeAll(s.layer, Codec.layerJson, Registry.layer(regs)))),
    };
  };

  it("rejects with a diagnostic rather than returning another branch's value", async () => {
    calls = [];
    const first = pass([], []);
    await Effect.runPromise(first.effect);

    const stored = [...first.server.promises.values()];
    const second = pass(
      stored.filter((p) => p.tags["resonate:branch"] === "foo"),
      stored,
    );
    const out = await Effect.runPromise(second.effect);

    expect(out).toMatchObject({ _tag: "Completed", state: "rejected" });
    const message = String((decode((out as { value: { data?: string } }).value) as Error).message);
    expect(message).toContain("already exists and holds a different call");
    expect(message).toContain("options({ id })");
  }, 30_000);
});
