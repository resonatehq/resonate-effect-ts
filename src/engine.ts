import { Context, Effect, Layer } from "effect";
import * as Connection from "./connection.js";
import type * as Correlation from "./correlation.js";
import { ProtocolError, type ServerError, serverError, type TransportError } from "./errors.js";
import * as Executor from "./executor.js";
import { type ExecuteMsg, isRedirect, isSuccess, type PromiseRecord, type TaskRecord, type Value } from "./protocol.js";
import * as Requests from "./requests.js";

// =============================================================================
// TASK
// =============================================================================
//
// A task reaches the engine in one of two conditions.
//
// Unclaimed is what an `execute` message carries: an id and a version, and no
// right to run anything. The engine has to acquire the lease first, and may
// lose that race.
//
// Claimed is what `task.acquire` and `task.create` return: the lease is already
// held by this process, and the root promise and its branch came back with it.
// A worker that creates a task for itself is handed one of these and can go
// straight to executing — no second round trip to claim what it already holds.

export interface Unclaimed {
  readonly _tag: "Unclaimed";
  readonly id: string;
  readonly version: number;
}

export interface Claimed {
  readonly _tag: "Claimed";
  readonly task: TaskRecord;
  readonly promise: PromiseRecord;
  readonly preload: ReadonlyArray<PromiseRecord>;
}

export type Task = Unclaimed | Claimed;

export const unclaimed = (id: string, version: number): Unclaimed => ({ _tag: "Unclaimed", id, version });

export const claimed = (
  task: TaskRecord,
  promise: PromiseRecord,
  preload: ReadonlyArray<PromiseRecord> = [],
): Claimed => ({ _tag: "Claimed", task, promise, preload });

/** The task an `execute` message names, which always arrives unclaimed. */
export const fromExecuteMsg = (msg: ExecuteMsg): Unclaimed => unclaimed(msg.data.task.id, msg.data.task.version);

// =============================================================================
// OUTCOME
// =============================================================================

/** The computation ran to a settled value and the task was fulfilled. */
export interface Completed {
  readonly _tag: "Completed";
  readonly id: string;
  readonly state: "resolved" | "rejected" | "rejected_canceled";
  readonly value: Value;
}

/** The computation blocked; the task now waits on `awaited` and the lease is gone. */
export interface Suspended {
  readonly _tag: "Suspended";
  readonly id: string;
  readonly awaited: ReadonlyArray<string>;
}

/**
 * The engine let the task go without finishing it. Not a failure — every reason
 * here is the protocol saying this process has no work to do on this task.
 *
 * - `unclaimable`: the lease could not be taken. Gone, or held by someone else.
 * - `lease-lost`: the lease went stale mid-pass (expired, or stolen after it
 *   expired). Whatever this process computed no longer counts; the worker that
 *   holds the lease now will redo it.
 * - `blocked-on-nothing`: the pass blocked on an empty set, so there is nothing
 *   to register a callback on and nothing that could ever resume it. The lease
 *   is dropped rather than released: a released task is re-dispatched at once,
 *   which would spin. It simply lapses, and whatever settles the promise —
 *   a deadline, another worker — dispatches the task again.
 */
export interface Dropped {
  readonly _tag: "Dropped";
  readonly id: string;
  readonly reason: "unclaimable" | "lease-lost" | "blocked-on-nothing";
  readonly message: string;
}

export type Outcome = Completed | Suspended | Dropped;

export type EngineError = TransportError | ServerError | ProtocolError | Executor.ExecutionError;

// =============================================================================
// ENGINE
// =============================================================================

export interface EngineOptions {
  /** This process's identifier, recorded on every lease it takes. */
  readonly pid: string;
  /** Lease duration in milliseconds, requested on every claim. */
  readonly ttl: number;
}

export interface EngineService {
  /**
   * Drive one task as far as it goes: claim it if it is not already claimed,
   * run passes until the computation completes or blocks on promises this
   * process cannot settle, and leave the task in the state that follows.
   */
  readonly process: (task: Task) => Effect.Effect<Outcome, EngineError>;
}

export class Engine extends Context.Service<Engine, EngineService>()("@resonatehq/effect/Engine") {}

type Requirements = Connection.Connection | Correlation.Correlation | Executor.Executor;

/**
 * The loop, as one function over the ambient connection and executor.
 *
 *     claim ─▶ execute ─┬─ completed ──▶ fulfill ────────▶ done
 *                ▲      └─ suspended ──▶ suspend ─┬─ 200 ─▶ done
 *                │                                └─ 300 ─┘
 *                └────────── continue ────────────────────┘
 *
 * The 300 on suspend is the whole reason this is a loop and not a straight
 * line. Between the pass deciding it is blocked and the server hearing about
 * it, one of the awaited promises can settle. The server answers "do not
 * suspend, you are not blocked any more" and hands back a fresh branch; the
 * lease was never given up, so the engine executes again on the spot rather
 * than waiting to be re-dispatched.
 */
export const process =
  (options: EngineOptions) =>
  (task: Task): Effect.Effect<Outcome, EngineError, Requirements> =>
    Effect.flatMap(claim(options, task), (claim) => (claim._tag === "Dropped" ? Effect.succeed(claim) : run(claim)));

/** Take the lease if this process does not already hold it. */
const claim = (options: EngineOptions, task: Task): Effect.Effect<Claimed | Dropped, EngineError, Requirements> => {
  if (task._tag === "Claimed") return Effect.as(assertAcquired(task.task), task);

  return Effect.gen(function* () {
    const res = yield* Connection.send(
      yield* Requests.taskAcquire({ id: task.id, version: task.version, pid: options.pid, ttl: options.ttl }),
    );

    // 404: the task is gone. 409: it is not pending, or the version moved on —
    // either way another worker got there first. Neither is an error; there is
    // simply nothing here to run.
    if (res.head.status === 404 || res.head.status === 409) {
      return drop(task.id, "unclaimable", res.data);
    }
    if (!isSuccess(res)) return yield* Effect.fail(serverError(res));

    yield* assertAcquired(res.data.task);
    return claimed(res.data.task, res.data.promise, res.data.preload);
  });
};

/** Execute passes until the task completes, suspends, or slips away. */
const run = (claim: Claimed): Effect.Effect<Outcome, EngineError, Requirements> => {
  const { task, promise } = claim;

  const pass = (preload: ReadonlyArray<PromiseRecord>): Effect.Effect<Outcome, EngineError, Requirements> =>
    Effect.suspend(() =>
      Effect.flatMap(Executor.executeUntilBlocked({ task, promise, preload }), (execution) =>
        execution._tag === "Completed"
          ? fulfill(task, execution)
          : execution.awaited.length === 0
            ? Effect.succeed(
                drop(task.id, "blocked-on-nothing", "the pass blocked without naming a promise to wait on"),
              )
            : Effect.flatMap(suspend(task, promise, execution.awaited), (suspension) =>
                // 300 — settled while we were deciding to block. Same lease,
                // fresh branch, straight back into the loop.
                suspension._tag === "Continue" ? pass(suspension.preload) : Effect.succeed(suspension.outcome),
              ),
      ),
    );

  // Whatever goes wrong, hand the lease back rather than making the next worker
  // wait out the ttl. Best effort: if the release itself fails the original
  // failure is the one worth reporting.
  return Effect.onError(pass(claim.preload), () => Effect.ignore(release(task)));
};

const fulfill = (task: TaskRecord, execution: Executor.Completed): Effect.Effect<Outcome, EngineError, Requirements> =>
  Effect.gen(function* () {
    const res = yield* Connection.send(
      yield* Requests.taskFulfill({
        id: task.id,
        version: task.version,
        action: yield* Requests.promiseSettle({
          id: execution.id,
          state: execution.state,
          value: execution.value,
        }),
      }),
    );

    if (res.head.status === 404 || res.head.status === 409) {
      return drop(task.id, "lease-lost", res.data);
    }
    if (!isSuccess(res)) return yield* Effect.fail(serverError(res));

    return { _tag: "Completed", id: execution.id, state: execution.state, value: execution.value } satisfies Completed;
  });

type Suspension =
  | { readonly _tag: "Continue"; readonly preload: ReadonlyArray<PromiseRecord> }
  | {
      readonly _tag: "Settled";
      readonly outcome: Outcome;
    };

const suspend = (
  task: TaskRecord,
  promise: PromiseRecord,
  awaited: ReadonlyArray<string>,
): Effect.Effect<Suspension, EngineError, Requirements> =>
  Effect.gen(function* () {
    // The task suspends against the promises it is blocked on by registering a
    // callback on each: settling any one of them dispatches this task again.
    const actions = yield* Effect.all(
      awaited.map((id) => Requests.promiseRegisterCallback({ awaiter: promise.id, awaited: id })),
    );
    const res = yield* Connection.send(yield* Requests.taskSuspend({ id: task.id, version: task.version, actions }));

    if (isRedirect(res)) return { _tag: "Continue", preload: res.data.preload } as const;
    if (res.head.status === 404 || res.head.status === 409) {
      return { _tag: "Settled", outcome: drop(task.id, "lease-lost", res.data) } as const;
    }
    if (!isSuccess(res)) return yield* Effect.fail(serverError(res));

    return {
      _tag: "Settled",
      outcome: { _tag: "Suspended", id: promise.id, awaited } satisfies Suspended,
    } as const;
  });

const release = (task: TaskRecord): Effect.Effect<void, EngineError, Requirements> =>
  Effect.flatMap(Requests.taskRelease({ id: task.id, version: task.version }), Connection.send);

const assertAcquired = (task: TaskRecord): Effect.Effect<void, ProtocolError> =>
  task.state === "acquired"
    ? Effect.void
    : Effect.fail(
        new ProtocolError({ message: `task ${task.id} is claimed but in state '${task.state}', expected 'acquired'` }),
      );

const drop = (id: string, reason: Dropped["reason"], data: unknown): Dropped => ({
  _tag: "Dropped",
  id,
  reason,
  message: typeof data === "string" ? data : reason,
});

// =============================================================================
// WIRING
// =============================================================================

/** An engine bound to the ambient connection, correlation source, and executor. */
export const make = (options: EngineOptions): Effect.Effect<EngineService, never, Requirements> =>
  Effect.map(Effect.context<Requirements>(), (context) => ({
    process: (task) => Effect.provide(process(options)(task), context),
  }));

/** A layer providing an engine. */
export const layer = (options: EngineOptions): Layer.Layer<Engine, never, Requirements> =>
  Layer.effect(Engine, make(options));
