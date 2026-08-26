import { Effect, Layer } from "effect";
import { describe, expect, it } from "vitest";
import * as Engine from "../src/engine.js";
import { ProtocolError, ServerError } from "../src/errors.js";
import * as Executor from "../src/executor.js";
import type { Request, TaskAcquireReq, TaskFulfillReq, TaskSuspendReq } from "../src/protocol.js";
import { promise, respond, scripted, task } from "./helpers.js";

const options: Engine.EngineOptions = { pid: "pid-1", ttl: 5_000 };

/** Run `process` against a scripted server and a scripted executor. */
const run = (
  input: Engine.Task,
  script: (req: Request, nth: number) => ReturnType<typeof respond>,
  passes: ReadonlyArray<Executor.Execution>,
) => {
  const server = scripted(script);
  let pass = 0;
  const seen: Executor.Invocation[] = [];
  const executor = Executor.layer((invocation) => {
    seen.push(invocation);
    const next = passes[pass++];
    return next
      ? Effect.succeed(next)
      : Effect.fail(new Executor.ExecutionError({ id: invocation.promise.id, message: "no pass scripted" }));
  });

  return {
    sent: server.sent,
    seen,
    outcome: Engine.process(options)(input).pipe(Effect.provide(Layer.merge(server.layer, executor))),
  };
};

describe("Engine.process", () => {
  it("claims an unclaimed task, runs it, and fulfills it", async () => {
    const { sent, seen, outcome } = run(
      Engine.unclaimed("foo", 1),
      (req) =>
        req.kind === "task.acquire"
          ? respond(req, 200, { task: task(), promise: promise(), preload: [promise()] })
          : respond(req, 200, { promise: promise({ state: "resolved" }) }),
      [Executor.completed("foo", "resolved", { data: '"done"' })],
    );

    const result = await Effect.runPromise(outcome);

    expect(result).toEqual({ _tag: "Completed", id: "foo", state: "resolved", value: { data: '"done"' } });

    // The lease is taken with this process's pid and ttl.
    const acquire = sent[0] as TaskAcquireReq;
    expect(acquire.kind).toBe("task.acquire");
    expect(acquire.data).toEqual({ id: "foo", version: 1, pid: "pid-1", ttl: 5_000 });
    expect(acquire.head.version).toBe("2026-04-01");

    // The pass sees the promise and branch the claim returned.
    expect(seen).toHaveLength(1);
    expect(seen[0]?.preload).toHaveLength(1);

    // Fulfill carries the settle as a nested request, each with its own corrId.
    const fulfill = sent[1] as TaskFulfillReq;
    expect(fulfill.kind).toBe("task.fulfill");
    expect(fulfill.data.id).toBe("foo");
    expect(fulfill.data.version).toBe(1);
    expect(fulfill.data.action.kind).toBe("promise.settle");
    expect(fulfill.data.action.data).toEqual({ id: "foo", state: "resolved", value: { data: '"done"' } });
    expect(fulfill.data.action.head.corrId).not.toBe(fulfill.head.corrId);
    expect(sent).toHaveLength(2);
  });

  it("skips the claim when the task is already claimed", async () => {
    const { sent, outcome } = run(
      Engine.claimed(task(), promise()),
      (req) => respond(req, 200, { promise: promise({ state: "resolved" }) }),
      [Executor.completed("foo", "resolved", { data: "1" })],
    );

    await Effect.runPromise(outcome);

    expect(sent.map((r) => r.kind)).toEqual(["task.fulfill"]);
  });

  it("suspends a blocked task against every promise it awaits", async () => {
    const { sent, outcome } = run(Engine.claimed(task(), promise()), (req) => respond(req, 200, {}), [
      Executor.suspended(["foo:1", "foo:2"]),
    ]);

    const result = await Effect.runPromise(outcome);

    expect(result).toEqual({ _tag: "Suspended", id: "foo", awaited: ["foo:1", "foo:2"] });

    const suspend = sent[0] as TaskSuspendReq;
    expect(suspend.kind).toBe("task.suspend");
    expect(suspend.data.id).toBe("foo");
    expect(suspend.data.version).toBe(1);
    expect(suspend.data.actions.map((a) => a.data)).toEqual([
      { awaiter: "foo", awaited: "foo:1" },
      { awaiter: "foo", awaited: "foo:2" },
    ]);
    expect(suspend.data.actions.every((a) => a.kind === "promise.register_callback")).toBe(true);
  });

  it("keeps executing on the same lease when suspend answers 300", async () => {
    const resumed = promise({ id: "foo:1", state: "resolved", value: { data: "42" } });
    const { sent, seen, outcome } = run(
      Engine.claimed(task(), promise()),
      (req, nth) => {
        if (req.kind === "task.suspend") {
          // First blocked pass races a settlement: the server says continue.
          return nth === 1 ? respond(req, 300, { preload: [resumed] }) : respond(req, 200, {});
        }
        return respond(req, 200, { promise: promise({ state: "resolved" }) });
      },
      [Executor.suspended(["foo:1"]), Executor.completed("foo", "resolved", { data: "42" })],
    );

    const result = await Effect.runPromise(outcome);

    expect(result._tag).toBe("Completed");
    // Two passes, the second replaying from the branch the 300 handed back.
    expect(seen).toHaveLength(2);
    expect(seen[0]?.preload).toEqual([]);
    expect(seen[1]?.preload).toEqual([resumed]);
    expect(sent.map((r) => r.kind)).toEqual(["task.suspend", "task.fulfill"]);
  });

  it("drops a task it loses the race to claim", async () => {
    for (const status of [404, 409]) {
      const { sent, seen, outcome } = run(Engine.unclaimed("foo", 1), (req) => respond(req, status, "taken"), []);

      const result = await Effect.runPromise(outcome);

      expect(result).toEqual({ _tag: "Dropped", id: "foo", reason: "unclaimable", message: "taken" });
      expect(sent.map((r) => r.kind)).toEqual(["task.acquire"]);
      expect(seen).toHaveLength(0);
    }
  });

  it("drops a task whose lease went stale mid-pass", async () => {
    const { outcome } = run(Engine.claimed(task(), promise()), (req) => respond(req, 409, "not acquired"), [
      Executor.completed("foo", "resolved", {}),
    ]);

    const result = await Effect.runPromise(outcome);

    expect(result).toEqual({ _tag: "Dropped", id: "foo", reason: "lease-lost", message: "not acquired" });
  });

  it("drops a pass that blocked on nothing, without touching the lease", async () => {
    const { sent, outcome } = run(Engine.claimed(task(), promise()), (req) => respond(req, 200, {}), [
      Executor.suspended([]),
    ]);

    const result = await Effect.runPromise(outcome);

    expect(result).toEqual({
      _tag: "Dropped",
      id: "foo",
      reason: "blocked-on-nothing",
      message: "the pass blocked without naming a promise to wait on",
    });
    // No suspend (the protocol rejects an empty action list) and no release
    // (a released task is re-dispatched at once, which would spin).
    expect(sent).toEqual([]);
  });

  it("releases the lease when a pass fails", async () => {
    const { sent, outcome } = run(Engine.claimed(task(), promise()), (req) => respond(req, 200, {}), []);

    const error = await Effect.runPromise(Effect.flip(outcome));

    expect(error).toBeInstanceOf(Executor.ExecutionError);
    expect(sent.map((r) => r.kind)).toEqual(["task.release"]);
    expect(sent[0]?.data).toEqual({ id: "foo", version: 1 });
  });

  it("fails on a status it cannot act on, and still releases", async () => {
    const { sent, outcome } = run(
      Engine.claimed(task(), promise()),
      (req) => (req.kind === "task.suspend" ? respond(req, 422, "awaited promise not found") : respond(req, 200, {})),
      [Executor.suspended(["foo:1"])],
    );

    const error = await Effect.runPromise(Effect.flip(outcome));

    expect(error).toBeInstanceOf(ServerError);
    expect((error as ServerError).status).toBe(422);
    expect(sent.map((r) => r.kind)).toEqual(["task.suspend", "task.release"]);
  });

  it("rejects a claimed task that is not actually acquired", async () => {
    const { sent, outcome } = run(
      Engine.claimed(task({ state: "pending" }), promise()),
      (req) => respond(req, 200, {}),
      [Executor.completed("foo", "resolved", {})],
    );

    const error = await Effect.runPromise(Effect.flip(outcome));

    expect(error).toBeInstanceOf(ProtocolError);
    expect(sent).toEqual([]);
  });

  it("reads the task out of an execute message", () => {
    expect(Engine.fromExecuteMsg({ kind: "execute", head: {}, data: { task: { id: "foo", version: 2 } } })).toEqual({
      _tag: "Unclaimed",
      id: "foo",
      version: 2,
    });
  });
});
