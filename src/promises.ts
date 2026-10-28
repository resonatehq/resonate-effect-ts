import { Clock, Deferred, Effect } from "effect";
import * as Codec from "./codec.js";
import * as Connection from "./connection.js";
import type { Correlation } from "./correlation.js";
import { Canceled, ProtocolError, type ServerError, serverError, TimedOut, type TransportError } from "./errors.js";
import * as Ids from "./ids.js";
import { isSuccess, type PromiseRecord } from "./protocol.js";
import * as Requests from "./requests.js";

// =============================================================================
// PROMISES
// =============================================================================
//
// Durable promises from outside any workflow: the latent promise a workflow
// waits on, settled by a webhook handler; a root's result, read by whoever
// started it. None of this goes through a lease — these are the server's
// promises as anyone may see and settle them.

export type PromiseError = TransportError | ServerError | ProtocolError | Codec.CodecError;

type Services = Connection.Connection | Correlation | Codec.Codec;

const DAY = 24 * 60 * 60 * 1000;

/** The promise `id` as the server has it. */
export const get = (id: string): Effect.Effect<PromiseRecord, PromiseError, Services> =>
  Effect.gen(function* () {
    const res = yield* Connection.send(yield* Requests.promiseGet({ id }));
    return isSuccess(res) ? res.data.promise : yield* Effect.fail(serverError(res));
  });

export interface CreateOptions {
  /** How long it may stay pending, in milliseconds. Defaults to a day. */
  readonly timeout?: number;
  /** What the promise carries, encoded with the ambient codec. */
  readonly data?: unknown;
  /** Extra tags. The `resonate:` namespace is the protocol's and is dropped. */
  readonly tags?: Record<string, string>;
}

/**
 * Create a promise with no function behind it — one that something outside
 * settles. Idempotent: creating one that exists returns the existing one.
 */
export const create = (id: string, options: CreateOptions = {}): Effect.Effect<PromiseRecord, PromiseError, Services> =>
  Effect.gen(function* () {
    const invalid = Ids.validateRoot(id);
    if (invalid) return yield* Effect.fail(invalid);
    const now = yield* Clock.currentTimeMillis;
    const res = yield* Connection.send(
      yield* Requests.promiseCreate({
        id,
        timeoutAt: now + (options.timeout ?? DAY),
        param: yield* Codec.encode(options.data),
        tags: {
          ...userTags(options.tags),
          "resonate:scope": "global",
          "resonate:origin": id,
          "resonate:branch": id,
          "resonate:parent": id,
        },
      }),
    );
    return isSuccess(res) ? res.data.promise : yield* Effect.fail(serverError(res));
  });

const settle = (id: string, state: "resolved" | "rejected" | "rejected_canceled", value: unknown) =>
  Effect.gen(function* () {
    const res = yield* Connection.send(yield* Requests.promiseSettle({ id, state, value: yield* Codec.encode(value) }));
    return isSuccess(res) ? res.data.promise : yield* Effect.fail(serverError(res));
  });

/** Resolve `id` with `value`. Settling a settled promise returns it unchanged. */
export const resolve = (id: string, value?: unknown): Effect.Effect<PromiseRecord, PromiseError, Services> =>
  settle(id, "resolved", value);

/** Reject `id` with `error`. */
export const reject = (id: string, error?: unknown): Effect.Effect<PromiseRecord, PromiseError, Services> =>
  settle(id, "rejected", error);

/** Cancel `id`: whoever awaits it sees {@link Canceled}. */
export const cancel = (id: string): Effect.Effect<PromiseRecord, PromiseError, Services> =>
  settle(id, "rejected_canceled", undefined);

export interface SearchOptions {
  readonly state?: PromiseRecord["state"];
  readonly tags?: Record<string, string>;
  readonly limit?: number;
  readonly cursor?: string;
}

/** One page of promises matching `options`. Pass `cursor` back for the next. */
export const search = (
  options: SearchOptions = {},
): Effect.Effect<
  { readonly promises: ReadonlyArray<PromiseRecord>; readonly cursor?: string },
  PromiseError,
  Services
> =>
  Effect.gen(function* () {
    const res = yield* Connection.send(yield* Requests.promiseSearch(options));
    return isSuccess(res) ? res.data : yield* Effect.fail(serverError(res));
  });

/**
 * What a settled promise holds, as an Effect: its value, or its error in the
 * failure channel. A pending promise has no result yet — that is a mistake by
 * the caller, so it is a defect rather than a typed failure.
 */
export const result = <A = unknown, E = unknown>(
  record: PromiseRecord,
): Effect.Effect<A, E | Canceled | TimedOut | Codec.CodecError, Codec.Codec> => {
  switch (record.state) {
    case "resolved":
      return Effect.map(Codec.decode(record.value), (v) => v as A);
    case "rejected":
      return Effect.flatMap(Codec.decode(record.value), (e) => Effect.fail(e as E));
    case "rejected_canceled":
      return Effect.fail(new Canceled({ id: record.id }));
    case "rejected_timedout":
      return Effect.fail(new TimedOut({ id: record.id }));
    case "pending":
      return Effect.die(new Error(`promise '${record.id}' is still pending`));
  }
};

export interface WaitOptions {
  /** How often to ask the server, in milliseconds. Defaults to 250. */
  readonly pollInterval?: number;
  /** Ask again at once when this completes, rather than at the next interval. */
  readonly wake?: Deferred.Deferred<void>;
}

/**
 * Wait for `id` to settle, then hand back its result.
 *
 * By asking: this is the one place the SDK polls. A listener registration and
 * the `unblock` message would replace it; until then the cost is up to one
 * interval of latency per wait, and `wake` removes it for the case that
 * matters most — a root this process is driving itself.
 */
export const wait = <A = unknown, E = unknown>(
  id: string,
  options: WaitOptions = {},
): Effect.Effect<A, E | Canceled | TimedOut | PromiseError, Services> =>
  Effect.gen(function* () {
    const interval = options.pollInterval ?? 250;
    let wake = options.wake;
    for (;;) {
      const record = yield* get(id);
      if (record.state !== "pending") return yield* result<A, E>(record);
      if (wake) {
        const woken = wake;
        // A wake fires once. After that it is done for good, so racing it again
        // would spin; fall back to the interval.
        yield* Effect.raceFirst(Effect.sleep(interval), Deferred.await(woken));
        if (Deferred.isDoneUnsafe(woken)) wake = undefined;
      } else {
        yield* Effect.sleep(interval);
      }
    }
  });

/** Caller tags without the protocol's namespace. */
export const userTags = (tags: Record<string, string> = {}): Record<string, string> =>
  Object.fromEntries(Object.entries(tags).filter(([k]) => !k.startsWith("resonate:")));

/** A record that is not the call the caller is making under this id. */
export const differentCall = (id: string): ProtocolError =>
  new ProtocolError({ message: `promise '${id}' already exists and holds a different call` });
