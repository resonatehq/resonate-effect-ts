import { Duration, Effect, Schedule } from "effect";

// =============================================================================
// RETRY POLICIES
// =============================================================================
//
// A retry policy is encoded into the promise's param so that any worker picking
// the computation up applies the same one. The wire shape matches the
// TypeScript SDK's.
//
// Retries re-run the body under a *fresh* frame, so durable children replay
// onto identical ids and dedup — an in-process retry is a replay that skipped
// the network. Reaching an edge is not a failure and is never retried: it
// travels as a defect, and `Effect.retry` only ever retries typed failures.

export type Policy =
  | { readonly type: "never"; readonly data?: unknown }
  | { readonly type: "constant"; readonly data?: { delay?: number; maxAttempts?: number } }
  | { readonly type: "linear"; readonly data?: { delay?: number; maxAttempts?: number } }
  | { readonly type: "exponential"; readonly data?: { delay?: number; factor?: number; maxAttempts?: number } };

/** The default. Effect users compose their own `Schedule` for anything richer. */
export const never: Policy = { type: "never" };

export const isPolicy = (u: unknown): u is Policy =>
  typeof u === "object" &&
  u !== null &&
  typeof (u as { type?: unknown }).type === "string" &&
  ["never", "constant", "linear", "exponential"].includes((u as { type: string }).type);

/**
 * A schedule for `policy`, never running past `deadline` — the promise's own
 * timeout, after which a retry could not finish anyway.
 */
export const toSchedule = (policy: Policy, deadline: number, now: number): Schedule.Schedule<unknown> | undefined => {
  const remaining = Math.max(0, deadline - now);
  const bound = <O>(s: Schedule.Schedule<O>): Schedule.Schedule<unknown> =>
    Schedule.intersect(s, Schedule.recurUpTo(Duration.millis(remaining))) as Schedule.Schedule<unknown>;

  switch (policy.type) {
    case "never":
      return undefined;
    case "constant": {
      const { delay = 1_000, maxAttempts = 3 } = policy.data ?? {};
      return bound(Schedule.intersect(Schedule.spaced(Duration.millis(delay)), Schedule.recurs(maxAttempts - 1)));
    }
    case "linear": {
      const { delay = 1_000, maxAttempts = 3 } = policy.data ?? {};
      return bound(Schedule.intersect(Schedule.linear(Duration.millis(delay)), Schedule.recurs(maxAttempts - 1)));
    }
    case "exponential": {
      const { delay = 1_000, factor = 2, maxAttempts = 5 } = policy.data ?? {};
      return bound(
        Schedule.intersect(Schedule.exponential(Duration.millis(delay), factor), Schedule.recurs(maxAttempts - 1)),
      );
    }
  }
};

/**
 * Run `attempt` under `policy`. Each retry re-evaluates `attempt`, so a caller
 * that builds its frame inside it gets a fresh one per attempt.
 */
export const apply = <A, E, R>(
  attempt: Effect.Effect<A, E, R>,
  policy: Policy,
  deadline: number,
  now: number,
): Effect.Effect<A, E, R> => {
  const schedule = toSchedule(policy, deadline, now);
  return schedule ? Effect.retry(attempt, schedule) : attempt;
};
