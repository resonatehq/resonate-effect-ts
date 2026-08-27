import { Effect, Layer } from "effect";
import { describe, expect, it } from "vitest";
import * as Codec from "../src/codec.js";
import type { Context } from "../src/context.js";
import * as Pass from "../src/pass.js";
import type { PromiseRecord } from "../src/protocol.js";
import * as Registry from "../src/registry.js";
import { decode, root, server, task } from "./server.js";

const settled = (id: string, v: unknown): PromiseRecord => ({
  id,
  state: "resolved",
  param: {},
  value: { data: Buffer.from(JSON.stringify(v), "utf8").toString("base64") },
  tags: { "resonate:branch": id },
  timeoutAt: 10_000_000,
  createdAt: 0,
});

const go = (
  wf: Registry.Registration,
  seed: PromiseRecord[] = [],
  latency?: (id: string) => number,
  preload: PromiseRecord[] = [],
) => {
  const s = server({ latency });
  for (const p of seed) s.seed(p);
  const effect = Pass.executeUntilBlocked(
    { task: task(), promise: root(wf.name), preload },
    { target: "poll://worker" },
  ).pipe(Effect.provide(Layer.mergeAll(s.layer, Codec.layerJson, Registry.layer([wf]))));
  return { server: s, effect };
};

// The race promise is foo:1; its branches are foo:1.1 and foo:1.2.
const racer = Registry.register("racer", (ctx: Context) => ctx.race([(c) => c.rpc("a"), (c) => c.rpc("b")]));

describe("ctx.race", () => {
  it("suspends on every branch while none has settled", async () => {
    const { effect, server: s } = go(racer);

    const out = await Effect.runPromise(effect);

    expect(out).toEqual({ _tag: "Suspended", awaited: ["foo:1.1", "foo:1.2"] });
    // Undecided, so nothing is recorded: one resume on either branch wakes it.
    expect(s.promises.get("foo:1")?.state).toBe("pending");
  });

  it("records the winner and cancels the loser", async () => {
    const { effect, server: s } = go(racer, [settled("foo:1.1", "A")]);

    const out = await Effect.runPromise(effect);

    expect(out).toMatchObject({ _tag: "Completed", state: "resolved" });
    expect(decode((out as { value: { data?: string } }).value)).toBe("A");
    // The decision is durable...
    expect(s.promises.get("foo:1")?.state).toBe("resolved");
    expect(decode(s.promises.get("foo:1")?.value)).toBe("A");
    // ...and the loser is no longer outstanding, so the pass could complete.
    expect(s.promises.get("foo:1.2")?.state).toBe("rejected_canceled");
  });

  it("lets a later branch win when the earlier one has not settled", async () => {
    const { effect, server: s } = go(racer, [settled("foo:1.2", "B")]);

    const out = await Effect.runPromise(effect);

    expect(decode((out as { value: { data?: string } }).value)).toBe("B");
    expect(s.promises.get("foo:1.1")?.state).toBe("rejected_canceled");
  });

  it("breaks a tie by declaration order, not by latency", async () => {
    // The second branch's create answers far sooner than the first's.
    const { effect } = go(racer, [settled("foo:1.1", "A"), settled("foo:1.2", "B")], (id) =>
      id === "foo:1.1" ? 25 : 1,
    );

    const out = await Effect.runPromise(effect);

    expect(decode((out as { value: { data?: string } }).value)).toBe("A");
  });

  it("replays the decision instead of re-racing", async () => {
    const decided = { ...settled("foo:1", "already decided"), tags: { "resonate:branch": "foo" } };
    const { effect, server: s } = go(racer, [decided, settled("foo:1.2", "B")]);

    const out = await Effect.runPromise(effect);

    expect(decode((out as { value: { data?: string } }).value)).toBe("already decided");
    // One idempotent create discovers the decision; the branches are never
    // built, so no branch promise is touched and no second race can happen.
    expect(s.journal).toEqual([">create foo:1", "dedup foo:1"]);
    expect(s.promises.has("foo:1.1")).toBe(false);
  });

  it("replays the decision for free when it is in the preload", async () => {
    // The race promise shares the root's branch, so a resuming pass gets it
    // back with the claim and pays nothing at all to replay the decision.
    const decided = { ...settled("foo:1", "already decided"), tags: { "resonate:branch": "foo" } };
    const { effect, server: s } = go(racer, [decided], undefined, [decided]);

    const out = await Effect.runPromise(effect);

    expect(decode((out as { value: { data?: string } }).value)).toBe("already decided");
    expect(s.journal).toEqual([]);
  });

  it("races a call against a durable timeout", async () => {
    const timed = Registry.register("timed", (ctx: Context) =>
      ctx.race([(c) => c.rpc("slow"), (c) => Effect.as(c.sleep(30_000), "timed out")]),
    );
    // The timer has fired; the call has not come back.
    const timer: PromiseRecord = { ...settled("foo:1.2", null), tags: { "resonate:branch": "foo:1.2" } };
    const { effect, server: s } = go(timed, [timer]);

    const out = await Effect.runPromise(effect);

    expect(decode((out as { value: { data?: string } }).value)).toBe("timed out");
    expect(s.promises.get("foo:1.1")?.state).toBe("rejected_canceled");
  });

  it("rejects when every branch failed", async () => {
    const allFail = Registry.register("allFail", (ctx: Context) =>
      ctx.race([() => Effect.fail("first"), () => Effect.fail("second")]),
    );
    const { effect, server: s } = go(allFail);

    const out = await Effect.runPromise(effect);

    expect(out).toMatchObject({ _tag: "Completed", state: "rejected" });
    expect(decode((out as { value: { data?: string } }).value)).toBe("first");
    expect(s.promises.get("foo:1")?.state).toBe("rejected");
  });

  it("keeps a failing branch from beating a blocked one to the record", async () => {
    const mixed = Registry.register("mixed", (ctx: Context) =>
      ctx.race([() => Effect.fail("nope"), (c) => c.rpc("b")]),
    );
    const { effect, server: s } = go(mixed);

    // The second branch might still win once it settles, so nothing is decided.
    expect(await Effect.runPromise(effect)).toEqual({ _tag: "Suspended", awaited: ["foo:1.1"] });
    expect(s.promises.get("foo:1")?.state).toBe("pending");
  });
});
