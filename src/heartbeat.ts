import { Context, Effect, Layer, type Scope } from "effect";
import * as Connection from "./connection.js";
import type { Correlation } from "./correlation.js";
import * as Requests from "./requests.js";

// =============================================================================
// HEARTBEAT
// =============================================================================
//
// A lease has a ttl. A pass that runs longer than that loses the lease, and its
// next fenced write is refused. `task.heartbeat` extends every lease a pid holds
// in one request, so it belongs to one component per process, beside the
// engine rather than inside it: the engine says which tasks it holds, and this
// keeps all of them alive.
//
// Optional. An engine with no heartbeat in its context drives tasks exactly as
// before, which is what keeps it testable on a bare connection.

export interface HeartbeatService {
  /** Keep `task`'s lease alive until the enclosing scope closes. */
  readonly hold: (task: { readonly id: string; readonly version: number }) => Effect.Effect<void, never, Scope.Scope>;
}

export class Heartbeat extends Context.Service<Heartbeat, HeartbeatService>()("@resonatehq/effect/Heartbeat") {}

export interface HeartbeatOptions {
  /** This process, as recorded on every lease it takes. */
  readonly pid: string;
  /** The lease ttl the engine requests, in milliseconds. */
  readonly ttl: number;
  /** How often to beat. Defaults to a third of the ttl, so two can be lost. */
  readonly interval?: number;
}

/**
 * One loop for the process, running for the life of the layer. It beats only
 * while something is held, and a failed beat is logged and left for the next
 * one: a lease that does lapse shows up where it matters, as a refused write.
 */
export const layer = (options: HeartbeatOptions): Layer.Layer<Heartbeat, never, Connection.Connection | Correlation> =>
  Layer.effect(
    Heartbeat,
    Effect.gen(function* () {
      const held = new Map<string, number>();
      const interval = options.interval ?? Math.max(1, Math.floor(options.ttl / 3));

      const beat = Effect.gen(function* () {
        if (held.size === 0) return;
        const tasks = [...held].map(([id, version]) => ({ id, version }));
        const res = yield* Connection.send(yield* Requests.taskHeartbeat({ pid: options.pid, tasks }));
        if (res.head.status !== 200) {
          yield* Effect.logWarning(`heartbeat answered ${res.head.status}`, res.data);
        }
      }).pipe(Effect.catchCause((cause) => Effect.logWarning("heartbeat failed", cause)));

      yield* Effect.forkScoped(Effect.forever(Effect.andThen(Effect.sleep(interval), beat)));

      return Heartbeat.of({
        hold: (task) =>
          Effect.acquireRelease(
            Effect.sync(() => held.set(task.id, task.version)),
            () => Effect.sync(() => held.delete(task.id)),
          ),
      });
    }),
  );

/** Hold `task` on the ambient heartbeat, if there is one. */
export const hold = (task: {
  readonly id: string;
  readonly version: number;
}): Effect.Effect<void, never, Scope.Scope> =>
  Effect.flatMap(Effect.serviceOption(Heartbeat), (heartbeat) =>
    heartbeat._tag === "Some" ? heartbeat.value.hold(task) : Effect.void,
  );
