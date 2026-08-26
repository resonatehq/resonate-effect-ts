import { Cause, Clock, Deferred, Effect, Exit, FiberId, HashSet, Ref } from "effect";
import type { CodecService } from "./codec.js";
import * as Edge from "./edge.js";
import * as Ids from "./ids.js";
import type { PromiseCreateReq, PromiseRecord, Value } from "./protocol.js";
import { VERSION } from "./protocol.js";
import type { Registration, RegistryService, Workflow } from "./registry.js";
import * as Requests from "./requests.js";
import * as Retries from "./retries.js";
import type { StoreError, StoreService } from "./store.js";

// =============================================================================
// OPTIONS
// =============================================================================

export interface Options {
  /** An explicit promise id. Starts a fresh lineage anchored on itself. */
  readonly id?: string;
  /** Where a remote call is dispatched. */
  readonly target?: string;
  /** How long the promise may take, in milliseconds. Capped by the parent's. */
  readonly timeout?: number;
  /** Extra tags on the durable promise. */
  readonly tags?: Record<string, string>;
  /** Which registered version to call. 0 means the latest. */
  readonly version?: number;
  /** Retry policy for a local call, and the one a remote worker will apply. */
  readonly retry?: Retries.Policy;
}

const DAY = 24 * 60 * 60 * 1000;

// =============================================================================
// CONTEXT
// =============================================================================

/** Identity and metadata, available to every function. */
export interface Info {
  /** This invocation's durable promise id. */
  readonly id: string;
  /** The lineage origin — everything before the first `:` of every id here. */
  readonly originId: string;
  /** The nearest ancestor with a target, and the key the preload is grouped by. */
  readonly branchId: string;
  /** The invocation that created this one. */
  readonly parentId: string;
  /** When this invocation's promise times out. */
  readonly timeoutAt: number;
  /** 1 on the first try of this pass. */
  readonly attempt: number;
  /** The registered version being run. */
  readonly version: number;
  /** The registered name being run. */
  readonly func: string;
}

/** A durable promise this pass created and may await later. */
export interface Handle<A, E = never> {
  readonly id: string;
  readonly await: Effect.Effect<A, E>;
}

/** Anything nameable: a registration (typed) or a bare name (not). */
export type Callable<F extends Workflow = Workflow> = Registration<F> | string;

type Args<F> = F extends (ctx: any, ...args: infer P) => any ? P : never[];
type Ok<F> = F extends (ctx: any, ...args: any[]) => Effect.Effect<infer A, any, any> ? A : never;
type Err<F> = F extends (ctx: any, ...args: any[]) => Effect.Effect<any, infer E, any> ? E : never;

/**
 * The durable operations.
 *
 * Every one of these is a create followed, for the ones that can block, by an
 * await. Creating never blocks; awaiting is what ends a pass. Concurrency is
 * Effect's own — `Effect.all`, `Effect.forEach`, `Effect.fork` + `Fiber.join` —
 * there is no `begin` variant, because forking a `ctx.rpc` and joining it later
 * already is one.
 */
export interface Context extends Info {
  /** Call a function here, durably. Its result is recorded before it returns. */
  run<F extends Workflow>(fn: Registration<F>, ...args: Args<F>): Effect.Effect<Ok<F>, Err<F>>;
  run<A = unknown, E = unknown>(fn: string, ...args: unknown[]): Effect.Effect<A, E>;
  run<A = unknown, E = unknown>(fn: Callable, args: unknown[], opts: Options): Effect.Effect<A, E>;

  /** Call a function somewhere else. Awaiting it is what suspends this task. */
  rpc<F extends Workflow>(fn: Registration<F>, ...args: Args<F>): Effect.Effect<Ok<F>, Err<F>>;
  rpc<A = unknown, E = unknown>(fn: string, ...args: unknown[]): Effect.Effect<A, E>;
  rpc<A = unknown, E = unknown>(fn: Callable, args: unknown[], opts: Options): Effect.Effect<A, E>;

  /** Sleep durably. The wake is a deadline the server owns, not a timer here. */
  sleep(ms: number): Effect.Effect<void>;

  /** A promise with no function behind it, settled out of band. */
  promise<A = unknown>(opts?: Options): Effect.Effect<Handle<A, unknown>>;

  /** Spawn a workflow as its own root. It outlives this one and is never awaited. */
  detached<F extends Workflow>(fn: Registration<F>, ...args: Args<F>): Effect.Effect<Handle<Ok<F>, Err<F>>>;
  detached<A = unknown, E = unknown>(fn: string, ...args: unknown[]): Effect.Effect<Handle<A, E>>;

  /** A dependency registered on the Resonate instance. */
  getDependency<T>(key: string): T | undefined;
}

// =============================================================================
// FRAME
// =============================================================================
//
// One layer of the onion: one durable invocation, and the mutable bookkeeping
// that belongs to it. Because there is always a Resonate frame between any two
// user frames, every level of the durable tree has a place to keep this and a
// place to check it.

interface Frame extends Info {
  /** The next Dewey segment to hand out. */
  seq: number;
  /** This frame's subtree reached an edge. Set by an op, read at the settle. */
  blocked: boolean;
  /** Tail of the creation chain — see `claim`. */
  tail: Effect.Effect<void>;
  /** Every gate claimed here, released when the frame ends. */
  gates: Array<() => void>;
}

/** What every frame in one pass shares. */
export interface Pass {
  /** Every promise this pass is blocked on. The pass decides on this, last. */
  readonly edges: Ref.Ref<HashSet.HashSet<string>>;
  readonly store: StoreService;
  readonly registry: RegistryService;
  readonly codec: CodecService;
  readonly dependencies: ReadonlyMap<string, unknown>;
  readonly target: string;
}

export const rootFrame = (promise: PromiseRecord, func: string, version: number, attempt = 1): Frame => ({
  id: promise.id,
  // The dispatcher wrote the origin onto the promise; a tag-less promise is its
  // own origin, derived the same way the server derives it.
  originId: promise.tags["resonate:origin"] ?? Ids.origin(promise.id),
  branchId: promise.tags["resonate:branch"] ?? promise.id,
  parentId: promise.tags["resonate:parent"] ?? promise.id,
  timeoutAt: promise.timeoutAt,
  attempt,
  version,
  func,
  seq: Ids.FIRST_SEGMENT,
  blocked: false,
  tail: Effect.void,
  gates: [],
});

const childFrame = (
  parent: Frame,
  id: string,
  record: PromiseRecord,
  func: string,
  version: number,
  attempt: number,
): Frame => ({
  id,
  originId: parent.originId,
  branchId: record.tags["resonate:branch"] ?? parent.branchId,
  parentId: parent.id,
  timeoutAt: record.timeoutAt,
  attempt,
  version,
  func,
  seq: Ids.FIRST_SEGMENT,
  blocked: false,
  tail: Effect.void,
  gates: [],
});

// =============================================================================
// THE CREATION CHAIN
// =============================================================================
//
// Creates within one frame are issued one at a time, each waiting for the one
// before it to come back from the server. Two things depend on it:
//
//   - ids. A slot is claimed *synchronously*, where the operation appears in
//     the source, so `Effect.all([ctx.rpc(a), ctx.rpc(b)])` orders a before b
//     by construction rather than by whichever fiber the scheduler starts.
//   - semantics. The server's answer to the first create can change what the
//     second one should be, and a replay must see the same order it did the
//     first time.
//
// A gate claimed by an operation that never runs would stall the chain, so the
// frame releases every gate it handed out when it ends.

const claim = (frame: Frame): { ready: Effect.Effect<void>; release: () => void } => {
  const ready = frame.tail;
  const gate = Deferred.unsafeMake<void>(FiberId.none);
  const release = () => Deferred.unsafeDone(gate, Effect.void);
  frame.tail = Deferred.await(gate);
  frame.gates.push(release);
  return { ready, release };
};

const closeFrame = (frame: Frame) =>
  Effect.sync(() => {
    for (const release of frame.gates) release();
    frame.gates.length = 0;
  });

// =============================================================================
// TAGS
// =============================================================================

const head = () => ({ corrId: "", version: VERSION });

const createReq = (data: PromiseCreateReq["data"]): PromiseCreateReq => ({
  kind: "promise.create",
  head: head(),
  data,
});

// =============================================================================
// CONTEXT CONSTRUCTION
// =============================================================================

export const make = (pass: Pass, frame: Frame): Context => {
  const options = (opts: Options = {}) => ({
    target: opts.target ?? pass.target,
    timeout: opts.timeout ?? DAY,
    tags: opts.tags ?? {},
    version: opts.version ?? 0,
    retry: opts.retry ?? Retries.never,
    id: opts.id,
  });

  // --- the prologue check -------------------------------------------------
  //
  // A durable operation on a path that already reached an edge is running on a
  // value the user substituted for one this pass never produced. Letting it
  // proceed would mint an id on a branch the next pass will not take, so the
  // same id would name a different call on replay. Refuse before minting.
  const diverged = (op: string) =>
    Effect.die(
      new Error(
        `ctx.${op} was called after this invocation reached an edge (id=${frame.id}). ` +
          `Something caught the block — Effect.exit, catchAllCause or catchAllDefect around a ` +
          `durable operation — and kept going on a substituted value. Durable operations must ` +
          `not follow a caught block.`,
      ),
    );

  const resolve = (fn: Callable, version: number): Registration | undefined =>
    typeof fn === "string" ? pass.registry.get(fn, version) : fn;

  // Split `(fn, ...args)` from `(fn, args, opts)`.
  const split = (rest: unknown[]): { args: unknown[]; opts: Options } =>
    rest.length === 2 && Array.isArray(rest[0]) && isOptions(rest[1])
      ? { args: rest[0] as unknown[], opts: rest[1] as Options }
      : { args: rest, opts: {} };

  const replay = (record: PromiseRecord): Effect.Effect<any, any> =>
    Effect.flatMap(pass.codec.decode(record.value), (value) =>
      record.state === "resolved" ? Effect.succeed(value) : Effect.fail(value),
    );

  // --- the durable create -------------------------------------------------
  //
  // Uninterruptible from taking the chain slot through recording the edge, so
  // a teardown started elsewhere can never split minting an id from creating
  // its promise: the id would name a promise that does not exist, and
  // `task.suspend` would answer 422.
  const create = (
    slot: { ready: Effect.Effect<void>; release: () => void },
    req: Effect.Effect<PromiseCreateReq, unknown>,
  ): Effect.Effect<PromiseRecord, StoreError | unknown> =>
    Effect.uninterruptible(
      Effect.ensuring(
        Effect.flatMap(slot.ready, () => Effect.flatMap(req, pass.store.create)),
        Effect.sync(slot.release),
      ),
    );

  const encodeParam = (func: string, args: unknown[], version: number, retry: Retries.Policy) =>
    pass.codec.encode({ func, args, retry, version });

  // --- run: a local child, executed in this fiber -------------------------
  const run = (fn: Callable, ...rest: unknown[]): Effect.Effect<any, any> => {
    if (frame.blocked) return diverged("run");
    const { args, opts } = split(rest);
    const o = options(opts);
    const registered = resolve(fn, o.version);
    if (!registered) {
      return Effect.die(new Error(`function '${typeof fn === "string" ? fn : fn.name}' is not registered`));
    }
    const breaks = o.id !== undefined;
    const id = breaks ? (o.id as string) : mint(frame);
    const slot = claim(frame);

    // The whole frame is uninterruptible: when one branch of a fan-out reaches
    // its edge and Effect interrupts its siblings, a sibling defers that
    // interruption, runs on to *its* edge, records it, and stops there. That is
    // what makes one pass discover every frontier instead of one per pass.
    return Effect.uninterruptible(
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        const record = yield* create(
          slot,
          Effect.map(encodeParam(registered.name, args, registered.version, o.retry), (param) =>
            createReq({
              id,
              timeoutAt: Math.min(now + o.timeout, frame.timeoutAt),
              param,
              tags: {
                "resonate:scope": "local",
                "resonate:branch": breaks ? id : frame.branchId,
                "resonate:parent": breaks ? id : frame.id,
                "resonate:origin": breaks ? id : frame.originId,
                ...o.tags,
              },
            }),
          ),
        );

        if (record.state !== "pending") return yield* replay(record);

        // Each attempt gets a fresh frame, so its children replay onto the same
        // ids and dedup — an in-process retry is a replay that skipped the wire.
        let child!: Frame;
        let attempt = 0;
        const body = Effect.suspend(() => {
          child = childFrame(frame, id, record, registered.name, registered.version, ++attempt);
          return Effect.ensuring(registered.workflow(make(pass, child), ...args), closeFrame(child));
        });

        const exit = yield* Effect.exit(Retries.apply(body, o.retry, record.timeoutAt, now));

        // --- the settle guard ---
        // Whatever the body did, this frame does not record a result for a
        // subtree that stopped at an edge. Checked before the settle rather
        // than trusted to the unwind, because user code between the edge and
        // here may have caught it.
        if (Exit.isFailure(exit)) {
          const edge = Edge.first(exit.cause);
          if (edge) {
            frame.blocked = true;
            return yield* Edge.reraise(edge);
          }
        }
        if (child.blocked) {
          frame.blocked = true;
          return yield* Edge.reach(id);
        }

        if (Exit.isSuccess(exit)) {
          yield* settle(id, "resolved", exit.value);
          return exit.value;
        }
        if (Cause.isInterruptedOnly(exit.cause)) return yield* Effect.failCause(exit.cause);

        const failure = Cause.failureOption(exit.cause);
        const error = failure._tag === "Some" ? failure.value : squash(exit.cause);
        yield* settle(id, "rejected", error);
        return yield* Effect.fail(error);
      }),
    );
  };

  const settle = (id: string, state: "resolved" | "rejected", value: unknown) =>
    Effect.flatMap(pass.codec.encode(value), (encoded) =>
      pass.store.settle({ kind: "promise.settle", head: head(), data: { id, state, value: encoded } }),
    );

  // --- rpc: someone else runs it ------------------------------------------
  const remote =
    (kind: "rpc" | "timer" | "latent" | "detached") =>
    (
      fn: Callable | undefined,
      rest: unknown[],
      extra: Options = {},
    ): Effect.Effect<{ id: string; record: PromiseRecord }, any> => {
      if (frame.blocked) return diverged(kind);
      const { args, opts } = split(rest);
      const o = options({ ...opts, ...extra });
      const registered = fn !== undefined ? resolve(fn, o.version) : undefined;
      const name = registered ? registered.name : typeof fn === "string" ? fn : "";
      const breaks = o.id !== undefined;
      const seed = mint(frame);
      const id = breaks ? (o.id as string) : kind === "detached" ? Ids.detached(frame.originId, seed) : seed;
      const slot = claim(frame);

      return Effect.uninterruptible(
        Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          const cap = kind === "detached" ? Number.MAX_SAFE_INTEGER : frame.timeoutAt;
          const param: Value =
            kind === "timer"
              ? { headers: {}, data: "" }
              : kind === "latent"
                ? yield* pass.codec.encode(o.tags.data)
                : yield* encodeParam(name, args, registered?.version ?? (o.version || 1), o.retry);

          const record = yield* create(
            slot,
            Effect.succeed(
              createReq({
                id,
                timeoutAt: Math.min(kind === "timer" ? o.timeout : now + o.timeout, cap),
                param,
                tags: {
                  "resonate:scope": "global",
                  "resonate:branch": id,
                  // A detached child hangs off the origin, so the origin is the
                  // parent it must declare — the server requires an id to extend it.
                  "resonate:parent": kind === "detached" && !breaks ? frame.originId : breaks ? id : frame.id,
                  "resonate:origin": breaks ? id : frame.originId,
                  ...(kind === "latent" ? {} : { "resonate:target": o.target }),
                  ...(kind === "timer" ? { "resonate:timer": "true" } : {}),
                  ...o.tags,
                },
              }),
            ),
          );

          return { id, record };
        }),
      );
    };

  /**
   * Awaiting a global promise: settled replays, pending is an edge.
   *
   * Recording the edge sits in the same uninterruptible region as the create
   * that discovered it. Split them and a sibling torn down in the gap leaves a
   * promise on the server that this pass never names, so the next pass pays a
   * round trip to rediscover what it already made.
   */
  const awaitRemote = (id: string, record: PromiseRecord): Effect.Effect<any, any> =>
    record.state !== "pending"
      ? replay(record)
      : Effect.gen(function* () {
          yield* Ref.update(pass.edges, HashSet.add(id));
          frame.blocked = true;
          return yield* Edge.reach(id);
        });

  // One region from claiming the chain slot to recording the edge. `remote` is
  // called here, synchronously, so the id is minted where the call appears in
  // the source rather than wherever the scheduler starts the fiber.
  const createThenAwait = (started: ReturnType<ReturnType<typeof remote>>) =>
    Effect.uninterruptible(Effect.flatMap(started, ({ id, record }) => awaitRemote(id, record)));

  const rpc = (fn: Callable, ...rest: unknown[]) => createThenAwait(remote("rpc")(fn, rest));

  const sleep = (ms: number) =>
    Effect.flatMap(Clock.currentTimeMillis, (now) =>
      Effect.asVoid(createThenAwait(remote("timer")(undefined, [], { timeout: now + ms }))),
    );

  const promise = (opts: Options = {}) =>
    Effect.map(remote("latent")(undefined, [{}, opts] as unknown[]), ({ id, record }) => ({
      id,
      await: Effect.uninterruptible(awaitRemote(id, record)),
    }));

  const detached = (fn: Callable, ...rest: unknown[]) =>
    Effect.map(remote("detached")(fn, rest), ({ id, record }) => ({
      id,
      await: Effect.uninterruptible(awaitRemote(id, record)),
    }));

  return {
    id: frame.id,
    originId: frame.originId,
    branchId: frame.branchId,
    parentId: frame.parentId,
    timeoutAt: frame.timeoutAt,
    attempt: frame.attempt,
    version: frame.version,
    func: frame.func,
    run,
    rpc,
    sleep,
    promise,
    detached,
    getDependency: <T>(key: string) => pass.dependencies.get(key) as T | undefined,
  } as Context;
};

// The Dewey segment for the next child, consumed in source order.
const mint = (frame: Frame): string => Ids.join(frame.id, frame.seq++);

const isOptions = (u: unknown): u is Options =>
  typeof u === "object" &&
  u !== null &&
  !Array.isArray(u) &&
  ["id", "target", "timeout", "tags", "version", "retry"].some((k) => k in (u as object));

const squash = (cause: Cause.Cause<unknown>): unknown => {
  const defect = [...Cause.defects(cause)][0];
  return defect ?? new Error(Cause.pretty(cause));
};

export { Requests };
