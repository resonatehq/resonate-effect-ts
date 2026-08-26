import { Cause, Data, Effect } from "effect";

// =============================================================================
// THE EDGE
// =============================================================================
//
// A pass ends where the computation meets a promise it cannot settle itself —
// the edge of what this process can do. Reaching one is not a failure of the
// user's program, so it must not appear in its error type or be reachable by
// its error handling. It travels as a defect: opaque to every combinator that
// operates on `E` (`catchAll`, `catchTag`, `either`, `option`, `match`,
// `merge`, `orElse`, `retry`, `timeout`, ...), and visible only to the ones
// that operate on a `Cause` or an `Exit` (`catchAllCause`, `catchAllDefect`,
// `sandbox`, `Effect.exit`, `Fiber.await`).
//
// Those last few cannot be hidden from — that is what they are for — so nothing
// load-bearing rests on the opacity. Correctness comes from three guards the
// SDK owns, in `context.ts`: an id is not minted on a path that already reached
// an edge, a frame whose subtree reached one does not settle, and the pass
// decides on the accumulated edge set rather than on what the body returned.
// Swallowing the defect therefore costs some pure local work and nothing else.

/**
 * A promise this pass is blocked on. A tagged value rather than an Error: it is
 * never something the user sees, and constructing an Error would capture a
 * stack on every block for nothing.
 */
export class Edge extends Data.TaggedClass("ResonateEdge")<{ readonly id: string }> {}

const TAG = "ResonateEdge";

/**
 * Recognize an edge by its tag, never by `instanceof` — two copies of this
 * package in one dependency tree give two classes, and `instanceof` would
 * silently stop matching, turning a suspend into a completion.
 */
export const isEdge = (u: unknown): u is Edge =>
  typeof u === "object" && u !== null && (u as { _tag?: unknown })._tag === TAG;

/** Reach an edge: unwind to the root of the pass. */
export const reach = (id: string): Effect.Effect<never> => Effect.die(new Edge({ id }));

/** Re-raise an edge unchanged. Wrapping it would grow the cause with the depth of the onion. */
export const reraise = (edge: Edge): Effect.Effect<never> => Effect.die(edge);

/** The edges named by a cause, if any. */
export const of = (cause: Cause.Cause<unknown>): ReadonlyArray<Edge> => [...Cause.defects(cause)].filter(isEdge);

/** The first edge named by a cause. */
export const first = (cause: Cause.Cause<unknown>): Edge | undefined => of(cause)[0];
