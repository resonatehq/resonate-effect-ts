import { Context, Data, Effect, Layer } from "effect";
import type { ServerError, TransportError } from "./errors.js";
import type { PromiseRecord, TaskRecord, Value } from "./protocol.js";

// =============================================================================
// EXECUTION
// =============================================================================
//
// What one pass of a computation can end in. There are exactly two outcomes:
// the computation ran to a value (or an error, which is still a value the
// promise can hold), or it blocked on promises it cannot settle itself.

/** The computation finished. Its root promise can be settled with `value`. */
export interface Completed {
  readonly _tag: "Completed";
  readonly id: string;
  readonly state: "resolved" | "rejected" | "rejected_canceled";
  readonly value: Value;
}

/**
 * The computation blocked. It can make no further progress until every promise
 * in `awaited` settles, and it holds no in-process state worth keeping — the
 * next pass replays from the durable record.
 */
export interface Suspended {
  readonly _tag: "Suspended";
  readonly awaited: ReadonlyArray<string>;
}

export type Execution = Completed | Suspended;

export const completed = (id: string, state: Completed["state"], value: Value = {}): Completed => ({
  _tag: "Completed",
  id,
  state,
  value,
});

export const suspended = (awaited: ReadonlyArray<string>): Suspended => ({ _tag: "Suspended", awaited });

/** An execution could not be carried out — not a rejected result, a broken run. */
export class ExecutionError extends Data.TaggedError("ExecutionError")<{
  readonly id: string;
  readonly message: string;
  readonly cause?: unknown;
}> {}

// =============================================================================
// EXECUTOR
// =============================================================================

/** Everything a pass starts from: the lease, the root promise, and its branch. */
export interface Invocation {
  /** The task, held on an acquired lease. */
  readonly task: TaskRecord;
  /** The root promise of the computation this task drives. */
  readonly promise: PromiseRecord;
  /**
   * Every promise sharing the root's `resonate:branch`, as the server had them
   * when the lease was taken or last continued. A replay reads its children
   * from here instead of asking the server for each one.
   */
  readonly preload: ReadonlyArray<PromiseRecord>;
}

/**
 * Runs one pass of a computation and returns where it got to.
 *
 * Deliberately the one thing the engine does not know how to do. The engine
 * owns the lease and the protocol around a pass — claim, fulfill, suspend,
 * continue — and calls this for the pass itself. The Effect-native
 * implementation lands behind this interface without the engine changing.
 */
/** What a pass can fail with: a broken run, or the server refusing a write. */
export type ExecutorError = ExecutionError | TransportError | ServerError;

export interface ExecutorService {
  readonly executeUntilBlocked: (invocation: Invocation) => Effect.Effect<Execution, ExecutorError>;
}

export class Executor extends Context.Tag("@resonatehq/effect/Executor")<Executor, ExecutorService>() {}

/** Run one pass with the ambient executor. */
export const executeUntilBlocked = (invocation: Invocation): Effect.Effect<Execution, ExecutorError, Executor> =>
  Effect.flatMap(Executor, (executor) => executor.executeUntilBlocked(invocation));

/** An executor from a bare function. */
export const make = (executeUntilBlocked: ExecutorService["executeUntilBlocked"]): ExecutorService => ({
  executeUntilBlocked,
});

/** A layer providing an executor from a bare function. */
export const layer = (executeUntilBlocked: ExecutorService["executeUntilBlocked"]): Layer.Layer<Executor> =>
  Layer.succeed(Executor, make(executeUntilBlocked));

/**
 * A placeholder executor that fails every pass. Lets the engine be wired up and
 * exercised before there is anything to run.
 */
export const layerNotImplemented: Layer.Layer<Executor> = layer(({ promise }) =>
  Effect.fail(new ExecutionError({ id: promise.id, message: "no executor is installed" })),
);
