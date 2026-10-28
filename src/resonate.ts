import { Clock, type Context, Deferred, Effect, Layer, type Scope } from "effect";
import * as Codec from "./codec.js";
import * as Connection from "./connection.js";
import { type Args, type Callable, type Err, type Ok, splitArgs } from "./context.js";
import * as Correlation from "./correlation.js";
import * as Engine from "./engine.js";
import { type Canceled, serverError, type TimedOut } from "./errors.js";
import * as Heartbeat from "./heartbeat.js";
import * as Ids from "./ids.js";
import * as Messages from "./messages.js";
import * as Pass from "./pass.js";
import * as Promises from "./promises.js";
import type { PromiseRecord, ScheduleRecord } from "./protocol.js";
import * as Registry from "./registry.js";
import * as Requests from "./requests.js";
import * as Retries from "./retries.js";
import * as Schedules from "./schedules.js";
import * as Worker from "./worker.js";

// =============================================================================
// RESONATE
// =============================================================================
//
// The front door: everything under it assembled once, for one process.
//
//   const resonate = yield* Resonate.make({ url, group: "orders", functions: [checkout] })
//   yield* resonate.start                                  // be a worker
//   const receipt = yield* resonate.run("order-42", checkout, order)
//
// `make` is scoped. The heartbeat runs for as long as the scope does — one per
// process, covering every task it holds — and so does the worker `start` forks.
// Closing the scope stops both; the leases they held lapse back to the server.

export interface ResonateOptions {
  /** Server url. Defaults to `RESONATE_URL`, then `http://localhost:8001`. */
  readonly url?: string;
  /** Bearer token. Defaults to `RESONATE_TOKEN`. */
  readonly token?: string;
  /** The group this process works for. Defaults to `default`. */
  readonly group?: string;
  /** This process. Defaults to a random id. */
  readonly pid?: string;
  /** Lease duration in milliseconds. Defaults to a minute. */
  readonly ttl?: number;
  /** Where this process is addressed. Defaults to `poll://any@<group>/<pid>`. */
  readonly target?: string;
  /** Default timeout for a root, in milliseconds. Defaults to a day. */
  readonly timeout?: number;
  /** How often a wait asks the server, in milliseconds. Defaults to 250. */
  readonly pollInterval?: number;
  /** What this process can run. */
  readonly functions?: Iterable<Registry.Registration>;
  /** Values available to every function through `ctx.getDependency`. */
  readonly dependencies?: Readonly<Record<string, unknown>>;
  /** A connection to use instead of HTTP. Tests pass one in. */
  readonly connection?: Connection.ConnectionService;
  /** A message stream to use instead of server-sent events. */
  readonly messages?: Messages.MessagesService;
}

/** Everything a call from outside a workflow can fail with besides its own error. */
export type InvocationError = Promises.PromiseError | Canceled | TimedOut;

/** The services `make` assembles, for code that wants to work below the front door. */
export type Services =
  | Connection.Connection
  | Correlation.Correlation
  | Codec.Codec
  | Registry.Registry
  | Messages.Messages
  | Heartbeat.Heartbeat
  | Engine.Engine;

export interface Resonate {
  readonly pid: string;
  readonly group: string;
  readonly target: string;

  /** Be a worker: run the tasks the server dispatches here, until the scope closes. */
  readonly start: Effect.Effect<void, never, Scope.Scope>;

  /**
   * Start `fn` as root `id` and drive it here, then return its result.
   *
   * Idempotent: `id` names the computation. Running an id that already exists
   * waits for it instead of starting another, provided it is the same call.
   */
  run<F extends Registry.Workflow>(
    id: string,
    fn: Registry.Registration<F>,
    ...args: Args<F>
  ): Effect.Effect<Ok<F>, Err<F> | InvocationError>;
  run<F extends Registry.Workflow>(
    id: string,
    fn: Registry.Registration<F>,
    args: Args<F>,
    opts: RootOptions,
  ): Effect.Effect<Ok<F>, Err<F> | InvocationError>;
  run<A = unknown, E = unknown>(
    id: string,
    fn: Callable,
    args: unknown[],
    opts: RootOptions,
  ): Effect.Effect<A, E | InvocationError>;

  /** Start `fn` as root `id` on whichever worker the server picks, then return its result. */
  rpc<F extends Registry.Workflow>(
    id: string,
    fn: Registry.Registration<F>,
    ...args: Args<F>
  ): Effect.Effect<Ok<F>, Err<F> | InvocationError>;
  rpc<F extends Registry.Workflow>(
    id: string,
    fn: Registry.Registration<F>,
    args: Args<F>,
    opts: RootOptions,
  ): Effect.Effect<Ok<F>, Err<F> | InvocationError>;
  rpc<A = unknown, E = unknown>(
    id: string,
    fn: Callable,
    args: unknown[],
    opts: RootOptions,
  ): Effect.Effect<A, E | InvocationError>;

  /** Wait for the promise `id` — any root, any latent promise — and return its result. */
  get<A = unknown, E = unknown>(id: string): Effect.Effect<A, E | InvocationError>;

  readonly promises: {
    readonly get: (id: string) => Effect.Effect<PromiseRecord, Promises.PromiseError>;
    readonly create: (
      id: string,
      options?: Promises.CreateOptions,
    ) => Effect.Effect<PromiseRecord, Promises.PromiseError>;
    readonly resolve: (id: string, value?: unknown) => Effect.Effect<PromiseRecord, Promises.PromiseError>;
    readonly reject: (id: string, error?: unknown) => Effect.Effect<PromiseRecord, Promises.PromiseError>;
    readonly cancel: (id: string) => Effect.Effect<PromiseRecord, Promises.PromiseError>;
    readonly search: (
      options?: Promises.SearchOptions,
    ) => Effect.Effect<
      { readonly promises: ReadonlyArray<PromiseRecord>; readonly cursor?: string },
      Promises.PromiseError
    >;
  };

  readonly schedules: {
    readonly create: (
      id: string,
      cron: string,
      fn: Callable,
      args: ReadonlyArray<unknown>,
      options?: Schedules.ScheduleOptions,
    ) => Effect.Effect<ScheduleRecord, Promises.PromiseError>;
    readonly get: (id: string) => Effect.Effect<ScheduleRecord, Promises.PromiseError>;
    readonly delete: (id: string) => Effect.Effect<void, Promises.PromiseError>;
    readonly search: (options?: {
      readonly tags?: Record<string, string>;
      readonly limit?: number;
      readonly cursor?: string;
    }) => Effect.Effect<
      { readonly schedules: ReadonlyArray<ScheduleRecord>; readonly cursor?: string },
      Promises.PromiseError
    >;
  };

  /** The assembled services, for running anything below the front door. */
  readonly context: Context.Context<Services>;
}

export interface RootOptions {
  readonly target?: string;
  readonly timeout?: number;
  readonly tags?: Record<string, string>;
  readonly version?: number;
  readonly retry?: Retries.Policy;
}

const DAY = 24 * 60 * 60 * 1000;

export const make = (options: ResonateOptions = {}): Effect.Effect<Resonate, never, Scope.Scope> =>
  Effect.gen(function* () {
    const pid = options.pid ?? crypto.randomUUID().replaceAll("-", "");
    const group = options.group ?? "default";
    const target = options.target ?? `poll://any@${group}/${pid}`;
    const ttl = options.ttl ?? 60_000;
    const timeout = options.timeout ?? DAY;
    const pollInterval = options.pollInterval ?? 250;
    const registry = Registry.make(options.functions ?? []);

    const base = Layer.mergeAll(
      Layer.succeed(
        Connection.Connection,
        options.connection ?? Connection.http({ url: options.url, token: options.token }),
      ),
      Layer.succeed(
        Messages.Messages,
        options.messages ?? Messages.poll({ url: options.url, token: options.token, group, pid }),
      ),
      Layer.succeed(Registry.Registry, registry),
      Correlation.layerUuid,
      Codec.layerJson,
    );
    const executor = Pass.layer({ target, dependencies: new Map(Object.entries(options.dependencies ?? {})) }).pipe(
      Layer.provide(base),
    );
    const layer = Layer.mergeAll(
      base,
      Heartbeat.layer({ pid, ttl }).pipe(Layer.provide(base)),
      Engine.layer({ pid, ttl }).pipe(Layer.provide(Layer.merge(base, executor))),
    );
    const context = yield* Layer.build(layer);
    const provide = <A, E>(effect: Effect.Effect<A, E, Services>): Effect.Effect<A, E> =>
      Effect.provideContext(effect, context);

    // A root's param is the same `{ func, args, retry, version }` every child
    // carries, so a root started here and one started by any other SDK are the
    // same thing to the worker that runs it.
    const root = (kind: "run" | "rpc", id: string, fn: Callable, rest: unknown[]) =>
      provide(
        Effect.gen(function* () {
          const invalid = Ids.validateRoot(id);
          if (invalid) return yield* Effect.fail(invalid);
          const { args, opts } = splitArgs(rest);
          const registered = typeof fn === "string" ? registry.get(fn, opts.version ?? 0) : fn;
          const func = registered?.name ?? (fn as string);
          const version = registered?.version ?? (opts.version || 1);
          const param = yield* Codec.encode({ func, args, retry: opts.retry ?? Retries.never, version });
          const now = yield* Clock.currentTimeMillis;
          const create = yield* Requests.promiseCreate({
            id,
            timeoutAt: now + (opts.timeout ?? timeout),
            param,
            tags: {
              ...Promises.userTags(opts.tags),
              "resonate:scope": "global",
              "resonate:origin": id,
              "resonate:branch": id,
              "resonate:parent": id,
              "resonate:target": opts.target ?? target,
            },
          });

          // The id names the computation, so it must name this one. A record
          // under it that holds another call is someone else's.
          const sameCall = (record: PromiseRecord) =>
            record.param?.data === param.data ? Effect.void : Effect.fail(Promises.differentCall(id));

          if (kind === "rpc") {
            const res = yield* Connection.send(create);
            if (res.head.status !== 200 || typeof res.data === "string") return yield* Effect.fail(serverError(res));
            yield* sameCall(res.data.promise);
            return yield* Promises.wait(id, { pollInterval });
          }

          const res = yield* Connection.send(yield* Requests.taskCreate({ pid, ttl, action: create }));
          // 409: the computation exists and someone holds it. Wait for it.
          if (res.head.status === 409) {
            yield* sameCall(yield* Promises.get(id));
            return yield* Promises.wait(id, { pollInterval });
          }
          if (res.head.status !== 200 || typeof res.data === "string") return yield* Effect.fail(serverError(res));

          const { task, promise, preload } = res.data;
          const wake = yield* Deferred.make<void>();
          if (task.state === "acquired") {
            if (promise.param?.data !== param.data) {
              yield* Effect.ignore(
                Effect.flatMap(Requests.taskRelease({ id: task.id, version: task.version }), Connection.send),
              );
              return yield* Effect.fail(Promises.differentCall(id));
            }
            // The lease came back with the create, so drive it here and now —
            // no round trip to claim what is already held. Whatever happens to
            // it, the wait below reads the outcome from the server.
            const engine = yield* Engine.Engine;
            yield* Effect.forkChild(
              engine.process(Engine.claimed(task, promise, preload)).pipe(
                Effect.catchCause(() => Effect.void),
                Effect.ensuring(Deferred.succeed(wake, undefined)),
              ),
            );
          } else {
            yield* sameCall(promise);
          }
          return yield* Promises.wait(id, { pollInterval, wake });
        }),
      );

    const resonate: Resonate = {
      pid,
      group,
      target,
      start: Effect.asVoid(
        Effect.forkScoped(provide(Effect.catchCause(Worker.run, (cause) => Effect.logError("worker stopped", cause)))),
      ),
      run: ((id: string, fn: Callable, ...rest: unknown[]) => root("run", id, fn, rest)) as Resonate["run"],
      rpc: ((id: string, fn: Callable, ...rest: unknown[]) => root("rpc", id, fn, rest)) as Resonate["rpc"],
      get: (id) => provide(Promises.wait(id, { pollInterval })),
      promises: {
        get: (id) => provide(Promises.get(id)),
        create: (id, o) => provide(Promises.create(id, o)),
        resolve: (id, value) => provide(Promises.resolve(id, value)),
        reject: (id, error) => provide(Promises.reject(id, error)),
        cancel: (id) => provide(Promises.cancel(id)),
        search: (o) => provide(Promises.search(o)),
      },
      schedules: {
        create: (id, cron, fn, args, o) => provide(Schedules.create(id, cron, fn, args, target, o)),
        get: (id) => provide(Schedules.get(id)),
        delete: (id) => provide(Schedules.remove(id)),
        search: (o) => provide(Schedules.search(o)),
      },
      context,
    };
    return resonate;
  });
