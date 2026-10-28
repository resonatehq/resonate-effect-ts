import { Effect } from "effect";
import * as Codec from "./codec.js";
import * as Connection from "./connection.js";
import type { Callable } from "./context.js";
import type { Correlation } from "./correlation.js";
import { ProtocolError, serverError } from "./errors.js";
import * as Ids from "./ids.js";
import type { PromiseError } from "./promises.js";
import { userTags } from "./promises.js";
import { isSuccess, type ScheduleRecord } from "./protocol.js";
import * as Requests from "./requests.js";
import * as Retries from "./retries.js";

// =============================================================================
// SCHEDULES
// =============================================================================
//
// A schedule is the server creating a root promise on a cron, which the target
// then runs like any other. The param is the same `{ func, args, retry,
// version }` a root carries, so a scheduled run is indistinguishable from one
// started by hand.

type Services = Connection.Connection | Correlation | Codec.Codec;

const DAY = 24 * 60 * 60 * 1000;

export interface ScheduleOptions {
  /**
   * The id of each promise the schedule creates. A template; the default is
   * `<id>-{{.timestamp}}`. It becomes an origin, so `:` may appear only inside
   * a substitution.
   */
  readonly promiseId?: string;
  /** How long each run may take, in milliseconds. Defaults to a day. */
  readonly timeout?: number;
  /** Where each run is dispatched. Defaults to the caller's target. */
  readonly target?: string;
  readonly tags?: Record<string, string>;
  /** Which registered version to run. Defaults to the registration's. */
  readonly version?: number;
  readonly retry?: Retries.Policy;
}

/** Run `fn(...args)` on `cron`, dispatched to `target`. Idempotent by `id`. */
export const create = (
  id: string,
  cron: string,
  fn: Callable,
  args: ReadonlyArray<unknown>,
  target: string,
  options: ScheduleOptions = {},
): Effect.Effect<ScheduleRecord, PromiseError, Services> =>
  Effect.gen(function* () {
    const invalid = Ids.validateRoot(id);
    if (invalid) return yield* Effect.fail(invalid);
    const promiseId = options.promiseId ?? `${id}-{{.timestamp}}`;
    if (promiseId.replaceAll(/\{\{[^}]*\}\}/g, "").includes(Ids.ORIGIN_SEP)) {
      return yield* Effect.fail(
        new ProtocolError({ message: `promise id '${promiseId}' may contain ':' only inside a substitution` }),
      );
    }
    const func = typeof fn === "string" ? fn : fn.name;
    const version = options.version ?? (typeof fn === "string" ? 1 : fn.version);
    const res = yield* Connection.send(
      yield* Requests.scheduleCreate({
        id,
        cron,
        promiseId,
        promiseTimeout: options.timeout ?? DAY,
        promiseParam: yield* Codec.encode({ func, args, retry: options.retry ?? Retries.never, version }),
        promiseTags: {
          ...userTags(options.tags),
          "resonate:scope": "global",
          "resonate:target": options.target ?? target,
        },
      }),
    );
    return isSuccess(res) ? res.data.schedule : yield* Effect.fail(serverError(res));
  });

export const get = (id: string): Effect.Effect<ScheduleRecord, PromiseError, Services> =>
  Effect.gen(function* () {
    const res = yield* Connection.send(yield* Requests.scheduleGet({ id }));
    return isSuccess(res) ? res.data.schedule : yield* Effect.fail(serverError(res));
  });

/** Stop the schedule. Runs already started are not affected. */
export const remove = (id: string): Effect.Effect<void, PromiseError, Services> =>
  Effect.gen(function* () {
    const res = yield* Connection.send(yield* Requests.scheduleDelete({ id }));
    if (!isSuccess(res)) return yield* Effect.fail(serverError(res));
  });

export const search = (
  options: { readonly tags?: Record<string, string>; readonly limit?: number; readonly cursor?: string } = {},
): Effect.Effect<
  { readonly schedules: ReadonlyArray<ScheduleRecord>; readonly cursor?: string },
  PromiseError,
  Services
> =>
  Effect.gen(function* () {
    const res = yield* Connection.send(yield* Requests.scheduleSearch(options));
    return isSuccess(res) ? res.data : yield* Effect.fail(serverError(res));
  });
