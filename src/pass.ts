import { Cause, Clock, Effect, Exit, Fiber, Layer } from "effect";
import { Codec, type CodecService } from "./codec.js";
import type { Connection } from "./connection.js";
import * as Ctx from "./context.js";
import type { Correlation } from "./correlation.js";
import * as Edge from "./edge.js";
import * as Executor from "./executor.js";
import { completed, type Execution, ExecutionError, type Invocation, suspended } from "./executor.js";
import type { PromiseRecord } from "./protocol.js";
import { Registry } from "./registry.js";
import * as Retries from "./retries.js";
import * as Store from "./store.js";

// =============================================================================
// ONE PASS
// =============================================================================
//
// Run the computation this task carries as far as it goes, and report where it
// got to. Nothing in this process survives the pass: on a `task.suspend` that
// answers 300 the engine calls straight back in with a fresh preload, and the
// pass rebuilds everything from the durable record.
//
// The shape, in one paragraph. A sentinel defect is thrown at the blocking
// await and unwinds like an exception, passing through user frames untouched
// but inspected at every Resonate frame it crosses — a frame whose subtree
// reached an edge refuses to settle and re-raises — until it is caught once,
// here. What it does *not* do is decide anything: the outcome comes from the
// edge set, which every blocking op writes before it throws, which keeps
// filling after the unwind has begun because sibling fibers run on to their own
// edges, and which outranks whatever the body returned. A body that returns a
// value while a remote child is still outstanding suspends, exactly as it does
// in the generator engine.

export interface PassOptions {
  /** Where this worker is addressed, stamped on the promises it creates. */
  readonly target: string;
  /** Dependencies available through `ctx.getDependency`. */
  readonly dependencies?: ReadonlyMap<string, unknown>;
}

export type PassError = ExecutionError | Store.StoreError;

export const executeUntilBlocked = (
  invocation: Invocation,
  options: PassOptions,
): Effect.Effect<Execution, PassError, Registry | Codec | Connection | Correlation> =>
  Effect.gen(function* () {
    const root = invocation.promise;

    // A `resonate:timer` promise is a durable sleep. It names no function; it
    // carries a task only because the target that gets the server to schedule
    // its deadline also spawns one. Not yet due: drop the task and let the
    // deadline settle the promise, which is what wakes the sleepers. Blocked on
    // nothing is exactly how the engine hears "drop it".
    if (root.tags["resonate:timer"] === "true" && root.state === "pending") {
      return suspended([]);
    }

    // Already settled: nothing to run. Hand back what is recorded so the engine
    // fulfills the task with it; the server dedups the settle.
    if (root.state !== "pending") {
      return completed(root.id, root.state === "resolved" ? "resolved" : "rejected", root.value);
    }

    const codec = yield* Codec;
    const registry = yield* Registry;
    const param = yield* decodeParam(root, codec);
    const registered = registry.get(param.func, param.version);
    if (!registered) {
      return yield* new ExecutionError({
        id: root.id,
        message: `function '${param.func}' version ${param.version} is not registered`,
      });
    }

    const pass: Ctx.Pass = {
      store: yield* Store.make({ id: invocation.task.id, version: invocation.task.version }, invocation.preload),
      registry,
      codec,
      dependencies: options.dependencies ?? new Map(),
      target: options.target,
    };

    const now = yield* Clock.currentTimeMillis;

    // A fresh frame per attempt, so a retried body replays its children onto
    // the same ids and dedups.
    let attempt = 0;
    let frame!: Ctx.Frame;
    const body = Effect.suspend(() => {
      frame = Ctx.rootFrame(root, registered.name, registered.version, ++attempt);
      return registered.workflow(Ctx.make(pass, frame), ...param.args);
    });

    // The body gets its own fiber. It has to: the sentinel tears down every
    // frame above the edge, and if the pass shared a fiber with the body it
    // would go down with it and never read the edge set.
    //
    // And the body runs uninterruptible as a whole, not merely one durable
    // frame at a time. When a branch reaches its edge, Effect interrupts its
    // siblings; a sibling inside a durable frame defers that and finishes, but
    // one *between* frames — in a `catchAll` handler about to start the
    // recovery, in the code after a child returned — has no such cover, and
    // would be cut off there. That loses work the pass could have done, and
    // worse, it silently shifts every id minted after it: the next pass runs
    // the operation that was skipped, and every sibling behind it in the
    // sequence gets a different id and a second promise. A pass ends when its
    // work is done or blocked, never in the middle of a step.
    const exit = yield* Fiber.await(
      yield* Effect.fork(Effect.uninterruptible(Retries.apply(body, param.retry, root.timeoutAt, now))),
    );

    // The root frame's edges decide — before the exit is even looked at.
    const edges = Ctx.edgesOf(frame);
    if (edges.length > 0) return suspended(edges);

    if (Exit.isSuccess(exit)) {
      return completed(root.id, "resolved", yield* codec.encode(exit.value));
    }

    // An edge with an empty set cannot happen: every op writes the set before
    // it throws, inside the same uninterruptible region as the create.
    const edge = Edge.first(exit.cause);
    if (edge) {
      return yield* new ExecutionError({
        id: root.id,
        message: `reached edge '${edge.id}' but no edge was recorded`,
      });
    }

    // Genuine interruption — the engine is giving up on this pass, not the
    // computation. Propagate; the task is released rather than settled.
    if (Cause.isInterruptedOnly(exit.cause)) return yield* Effect.failCause(exit.cause);

    // A typed failure is a value the promise can hold, so the computation is
    // finished: reject it. A defect is a bug, and rejecting is still better
    // than a task that is redelivered forever.
    const failure = Cause.failureOption(exit.cause);
    const error = failure._tag === "Some" ? failure.value : squash(exit.cause);
    return completed(root.id, "rejected", yield* codec.encode(error));
  });

interface Param {
  readonly func: string;
  readonly args: unknown[];
  readonly retry: Retries.Policy;
  readonly version: number;
}

const decodeParam = (root: PromiseRecord, codec: CodecService): Effect.Effect<Param, ExecutionError> =>
  codec.decode(root.param).pipe(
    Effect.mapError((cause) => new ExecutionError({ id: root.id, message: "promise param is not decodable", cause })),
    Effect.flatMap((decoded) => {
      const d = decoded as Partial<Param> | null;
      if (d === null || typeof d !== "object" || typeof d.func !== "string" || !Array.isArray(d.args)) {
        return Effect.fail(
          new ExecutionError({ id: root.id, message: "promise param does not name a function and arguments" }),
        );
      }
      return Effect.succeed<Param>({
        func: d.func,
        args: d.args,
        version: typeof d.version === "number" ? d.version : 1,
        retry: Retries.isPolicy(d.retry) ? d.retry : Retries.never,
      });
    }),
  );

const squash = (cause: Cause.Cause<unknown>): unknown => [...Cause.defects(cause)][0] ?? new Error(Cause.pretty(cause));

/**
 * The executor the engine drives: one pass per call, everything it needs bound
 * in — the registry that turns a promise's function name back into code, the
 * codec that moves values across the wire, and the connection the fenced store
 * writes through.
 */
export const layer = (
  options: PassOptions,
): Layer.Layer<Executor.Executor, never, Registry | Codec | Connection | Correlation> =>
  Layer.effect(
    Executor.Executor,
    Effect.map(Effect.context<Registry | Codec | Connection | Correlation>(), (context) =>
      Executor.make((invocation) => Effect.provide(executeUntilBlocked(invocation, options), context)),
    ),
  );
