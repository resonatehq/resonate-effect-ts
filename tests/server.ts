import { Effect, Layer } from "effect";
import * as Connection from "../src/connection.js";
import * as Correlation from "../src/correlation.js";
import type {
  PromiseCreateReq,
  PromiseRecord,
  PromiseSettleReq,
  Request,
  Response,
  TaskRecord,
} from "../src/protocol.js";
import { VERSION } from "../src/protocol.js";

// A promise store that answers `task.fence`, so a pass can be run for real.
// Records the order of every create and settle, and can be given per-id latency
// to reproduce the skew that decides whether a fan-out discovers one edge or all
// of them.

export interface Server {
  readonly layer: Layer.Layer<Connection.Connection | Correlation.Correlation>;
  /** Every fenced write, in the order the server saw it. */
  readonly journal: string[];
  /** Every request kind the server handled, in order. */
  readonly kinds: string[];
  readonly promises: Map<string, PromiseRecord>;
  readonly seed: (promise: PromiseRecord) => void;
  /** Callbacks registered by task.suspend, as (awaiter, awaited) pairs. */
  readonly callbacks: Array<{ awaiter: string; awaited: string }>;
  /** Settle a promise from outside, the way another worker would. */
  readonly settleExternally: (id: string, value: unknown) => void;
}

export const server = (
  opts: { latency?: (id: string) => number; refuse?: (req: Request, nth: number) => number | undefined } = {},
): Server => {
  const promises = new Map<string, PromiseRecord>();
  const journal: string[] = [];
  const latency = opts.latency ?? (() => 0);

  const record = (req: PromiseCreateReq): PromiseRecord => ({
    id: req.data.id,
    state: "pending",
    param: req.data.param,
    value: {},
    tags: req.data.tags,
    timeoutAt: req.data.timeoutAt,
    createdAt: 0,
  });

  const apply = (action: PromiseCreateReq | PromiseSettleReq): PromiseRecord => {
    if (action.kind === "promise.create") {
      const existing = promises.get(action.data.id);
      if (existing) {
        journal.push(`dedup ${action.data.id}`);
        return existing;
      }
      const created = record(action);
      promises.set(created.id, created);
      journal.push(`create ${created.id}`);
      return created;
    }
    const existing = promises.get(action.data.id);
    if (!existing) throw new Error(`settle of unknown promise ${action.data.id}`);
    if (existing.state !== "pending") return existing;
    const settled: PromiseRecord = { ...existing, state: action.data.state, value: action.data.value, settledAt: 1 };
    promises.set(settled.id, settled);
    journal.push(`settle ${settled.id} ${action.data.state}`);
    return settled;
  };

  const callbacks: Array<{ awaiter: string; awaited: string }> = [];
  const kinds: string[] = [];

  const ok = (req: Request, data: unknown, status = 200): Response =>
    ({ kind: req.kind, head: { corrId: req.head.corrId, status, version: VERSION }, data }) as unknown as Response;

  const branchOf = (id: string) => promises.get(id)?.tags["resonate:branch"];
  const preloadFor = (id: string) => [...promises.values()].filter((p) => p.tags["resonate:branch"] === branchOf(id));

  const send = (req: Request): Effect.Effect<Response, never> =>
    Effect.gen(function* () {
      kinds.push(req.kind);
      const refused = opts.refuse?.(req, kinds.length);
      if (refused !== undefined) return ok(req, `refused with ${refused}`, refused);

      if (req.kind === "task.acquire") {
        const promise = promises.get(req.data.id);
        if (!promise) return ok(req, "no such task", 404);
        return ok(req, {
          task: task({ id: req.data.id, version: req.data.version, state: "acquired" }),
          promise,
          preload: preloadFor(req.data.id),
        });
      }

      if (req.kind === "task.suspend") {
        // 300 when any awaited promise has already settled: the worker is not
        // blocked after all and keeps the lease it already holds.
        const settled = req.data.actions.some((a) => promises.get(a.data.awaited)?.state !== "pending");
        if (settled) return ok(req, { preload: preloadFor(req.data.id) }, 300);
        for (const a of req.data.actions) callbacks.push({ awaiter: a.data.awaiter, awaited: a.data.awaited });
        return ok(req, {});
      }

      if (req.kind === "task.fulfill") {
        const promise = apply(req.data.action);
        return ok(req, { promise });
      }

      if (req.kind !== "task.fence") {
        return ok(req, {});
      }
      const action = req.data.action;
      journal.push(`>${action.kind === "promise.create" ? "create" : "settle"} ${action.data.id}`);
      yield* Effect.sleep(`${latency(action.data.id)} millis`);
      const promise = apply(action);
      const branch = promise.tags["resonate:branch"];
      const preload = [...promises.values()].filter((p) => p.tags["resonate:branch"] === branch);
      return {
        kind: "task.fence",
        head: { corrId: req.head.corrId, status: 200, version: VERSION },
        data: {
          action: {
            kind: action.kind,
            head: { corrId: action.head.corrId, status: 200, version: VERSION },
            data: { promise },
          },
          preload,
        },
      } as unknown as Response;
    });

  return {
    layer: Layer.merge(
      Connection.layer(((req: Request) => send(req)) as Connection.ConnectionService["send"]),
      Correlation.layerSeq(),
    ),
    journal,
    kinds,
    promises,
    callbacks,
    seed: (p) => promises.set(p.id, p),
    settleExternally: (id, value) => {
      const existing = promises.get(id);
      if (!existing) throw new Error(`no such promise ${id}`);
      promises.set(id, {
        ...existing,
        state: "resolved",
        value: { data: Buffer.from(JSON.stringify(value), "utf8").toString("base64") },
        settledAt: 1,
      });
      journal.push(`external ${id}`);
    },
  };
};

export const task = (over: Partial<TaskRecord> = {}): TaskRecord => ({
  id: "foo",
  state: "acquired",
  version: 1,
  resumes: [],
  ttl: 30_000,
  pid: "pid-1",
  ...over,
});

/** A root promise carrying `func`/`args`, encoded the way the dispatcher would. */
export const root = (func: string, args: unknown[] = [], over: Partial<PromiseRecord> = {}): PromiseRecord => ({
  id: "foo",
  state: "pending",
  param: { headers: {}, data: Buffer.from(JSON.stringify({ func, args, version: 1 }), "utf8").toString("base64") },
  value: {},
  tags: { "resonate:origin": "foo", "resonate:branch": "foo", "resonate:parent": "foo" },
  timeoutAt: 10_000_000,
  createdAt: 0,
  ...over,
});

export const decode = (v: { data?: string } | undefined): unknown =>
  v?.data ? JSON.parse(Buffer.from(v.data, "base64").toString("utf8")) : undefined;
