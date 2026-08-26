import { Effect, HashMap, Ref } from "effect";
import * as Connection from "./connection.js";
import type { Correlation } from "./correlation.js";
import { type ServerError, serverError, type TransportError } from "./errors.js";
import { isSuccess, type PromiseCreateReq, type PromiseRecord, type PromiseSettleReq } from "./protocol.js";
import * as Requests from "./requests.js";

// =============================================================================
// STORE
// =============================================================================
//
// The promise store as one pass sees it, through the lease it holds.
//
// Every write goes through `task.fence`, which carries the task id and version:
// the server applies the write only if this process still holds the lease, so a
// worker whose lease lapsed cannot settle a promise out from under the worker
// that replaced it. Each fence answers with a refreshed view of the branch.
//
// Reads come out of a cache seeded from the claim's preload — every promise
// sharing the root's `resonate:branch`, which is exactly this computation's own
// local children. That makes replay local: a pass re-walking its history reads
// the children it already created instead of asking the server for each one.
// The cache is only an optimization. `promise.create` and `promise.settle` are
// both idempotent and return the existing record, so a miss is a round trip
// that arrives at the same answer.

export type StoreError = TransportError | ServerError;

export interface StoreService {
  /** Create a promise, or read back the one already there. */
  readonly create: (req: PromiseCreateReq) => Effect.Effect<PromiseRecord, StoreError>;
  /** Settle a promise, or read back the settlement already there. */
  readonly settle: (req: PromiseSettleReq) => Effect.Effect<PromiseRecord, StoreError>;
}

export const make = (
  task: { readonly id: string; readonly version: number },
  preload: ReadonlyArray<PromiseRecord> = [],
): Effect.Effect<StoreService, never, Connection.Connection | Correlation> =>
  Effect.gen(function* () {
    const cache = yield* Ref.make(HashMap.fromIterable(preload.map((p) => [p.id, p] as const)));
    // Captured once so the returned service needs no environment of its own.
    const context = yield* Effect.context<Connection.Connection | Correlation>();

    const remember = (promises: ReadonlyArray<PromiseRecord>) =>
      Ref.update(cache, (m) => promises.reduce((acc, p) => HashMap.set(acc, p.id, p), m));

    const fence = (action: PromiseCreateReq | PromiseSettleReq) =>
      Effect.gen(function* () {
        const res = yield* Connection.send(yield* Requests.taskFence({ id: task.id, version: task.version, action }));
        if (!isSuccess(res)) return yield* Effect.fail(serverError(res));
        yield* remember(res.data.preload);
        const inner = res.data.action;
        if (!isSuccess(inner)) return yield* Effect.fail(serverError(inner));
        yield* remember([inner.data.promise]);
        return inner.data.promise;
      }).pipe(Effect.provide(context));

    return {
      create: (req) =>
        Effect.flatMap(Ref.get(cache), (m) => {
          const cached = HashMap.get(m, req.data.id);
          return cached._tag === "Some" ? Effect.succeed(cached.value) : fence(req);
        }),
      settle: (req) =>
        Effect.flatMap(Ref.get(cache), (m) => {
          const cached = HashMap.get(m, req.data.id);
          // A cached *pending* record says nothing about the settlement we are
          // about to make, so only a settled one short-circuits.
          return cached._tag === "Some" && cached.value.state !== "pending" ? Effect.succeed(cached.value) : fence(req);
        }),
    };
  });
