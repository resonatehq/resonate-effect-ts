import { Effect, Fiber, Layer, Stream } from "effect";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as Codec from "../src/codec.js";
import * as Connection from "../src/connection.js";
import type { Context } from "../src/context.js";
import * as Correlation from "../src/correlation.js";
import * as Engine from "../src/engine.js";
import * as Messages from "../src/messages.js";
import * as Pass from "../src/pass.js";
import * as Registry from "../src/registry.js";
import * as Resonate from "../src/resonate.js";
import { FakeServer } from "./fake-server.js";

// The front door, end to end: a worker on the fake server's message stream, a
// client on its connection, and nothing in between but the SDK.

const factorial = Registry.register(
  "factorial",
  (ctx: Context, n: number): Effect.Effect<number, unknown> =>
    n <= 1 ? Effect.succeed(1) : Effect.map(ctx.run(factorial, n - 1), (m) => n * m),
);

const remoteFactorial = Registry.register(
  "remote-factorial",
  (ctx: Context, n: number): Effect.Effect<number, unknown> =>
    n <= 1 ? Effect.succeed(1) : Effect.map(ctx.rpc(remoteFactorial, n - 1), (m) => n * m),
);

const world = (functions: Registry.Registration[], options: Partial<Resonate.ResonateOptions> = {}) =>
  Effect.gen(function* () {
    const server = yield* FakeServer.make();
    const resonate = yield* Resonate.make({
      connection: server.connection,
      messages: server.messages,
      pollInterval: 1,
      functions,
      ...options,
    });
    return { server, resonate };
  });

const scoped = <A, E>(effect: Effect.Effect<A, E, import("effect").Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(effect));

describe("run", () => {
  it("drives a root here, recursing locally under Dewey ids", async () => {
    const { value, server } = await scoped(
      Effect.gen(function* () {
        const { server, resonate } = yield* world([factorial]);
        const value = yield* resonate.run("fact-5", factorial, 5);
        return { value, server };
      }),
    );

    expect(value).toBe(120);
    expect([...server.promises.keys()].sort()).toEqual([
      "fact-5",
      "fact-5:1",
      "fact-5:1.1",
      "fact-5:1.1.1",
      "fact-5:1.1.1.1",
    ]);
    expect(server.tasks.get("fact-5")?.state).toBe("fulfilled");
    // The lease came back with task.create; the root was never claimed twice.
    expect(server.history.filter((r) => r.kind === "task.acquire")).toEqual([]);
  });

  it("returns the recorded result when the same call runs again", async () => {
    let calls = 0;
    const counted = Registry.register("counted", (_c: Context, n: number) => Effect.sync(() => ++calls + n));
    const out = await scoped(
      Effect.gen(function* () {
        const { resonate } = yield* world([counted]);
        const first = yield* resonate.run("counted-1", counted, 10);
        const second = yield* resonate.run("counted-1", counted, 10);
        return [first, second];
      }),
    );

    expect(out).toEqual([11, 11]);
    expect(calls).toBe(1);
  });

  it("refuses an id that already names a different call", async () => {
    const error = await scoped(
      Effect.gen(function* () {
        const { resonate } = yield* world([factorial]);
        yield* resonate.run("fact-x", factorial, 3);
        return yield* Effect.flip(resonate.run("fact-x", factorial, 4));
      }),
    );

    expect(error).toMatchObject({ _tag: "ProtocolError" });
    expect(String((error as { message: string }).message)).toContain("holds a different call");
  });

  it("puts a rejection in the failure channel", async () => {
    const failing = Registry.register("failing", (_c: Context) => Effect.fail({ reason: "no stock" }));
    const error = await scoped(
      Effect.gen(function* () {
        const { resonate } = yield* world([failing]);
        return yield* Effect.flip(resonate.run("failing-1", failing));
      }),
    );

    expect(error).toEqual({ reason: "no stock" });
  });
});

describe("rpc and the worker", () => {
  it("dispatches a root and every recursive call through the server", async () => {
    const { value, server } = await scoped(
      Effect.gen(function* () {
        const { server, resonate } = yield* world([remoteFactorial]);
        yield* resonate.start;
        const value = yield* resonate.rpc("rfact-4", remoteFactorial, 4);
        return { value, server };
      }),
    );

    expect(value).toBe(24);
    // Each level was its own task, run by the worker off an execute message.
    expect([...server.tasks.values()].every((t) => t.state === "fulfilled")).toBe(true);
    expect(server.tasks.size).toBe(4);
  });

  it("resumes a workflow when a promise it waits on is settled from outside", async () => {
    const approval = Registry.register("approval", (ctx: Context) =>
      Effect.gen(function* () {
        const handle = yield* ctx.promise<string>();
        return `approved by ${yield* handle.await}`;
      }),
    );
    const value = await scoped(
      Effect.gen(function* () {
        const { server, resonate } = yield* world([approval]);
        yield* resonate.start;
        const result = yield* Effect.forkChild(resonate.run("approval-1", approval));
        yield* waitUntil(() => server.tasks.get("approval-1")?.state === "suspended");
        yield* resonate.promises.resolve("approval-1:1", "alice");
        return yield* Fiber.join(result);
      }),
    );

    expect(value).toBe("approved by alice");
  });
});

describe("heartbeat", () => {
  it("beats for a task while the engine holds it, and stops after", async () => {
    const slow = Registry.register("slow", (_c: Context) =>
      Effect.as(
        Effect.promise(() => new Promise((r) => setTimeout(r, 60))),
        "done",
      ),
    );
    const server = await scoped(
      Effect.gen(function* () {
        const { server, resonate } = yield* world([slow], { ttl: 30 });
        yield* resonate.run("slow-1", slow);
        const beats = server.history.filter((r) => r.kind === "task.heartbeat").length;
        yield* Effect.sleep(40);
        return { server, beats };
      }),
    );

    expect(server.beats).toBeGreaterThan(0);
    const tasks = server.server.history.flatMap((r) => (r.kind === "task.heartbeat" ? r.data.tasks : []));
    expect(new Set(tasks.map((t) => t.id))).toEqual(new Set(["slow-1"]));
    // Nothing held, nothing sent.
    expect(server.server.history.filter((r) => r.kind === "task.heartbeat").length).toBe(server.beats);
  });

  it("is optional: an engine without one drives tasks as before", async () => {
    const server = await Effect.runPromise(FakeServer.make());
    const base = Layer.mergeAll(
      Layer.succeed(Connection.Connection, server.connection),
      Correlation.layerUuid,
      Codec.layerJson,
      Registry.layer([factorial]),
    );
    const pass = Pass.layer({ target: "poll://any@default/test" });
    const layer = Engine.layer({ pid: "p", ttl: 1_000 }).pipe(
      Layer.provide(Layer.merge(base, pass.pipe(Layer.provide(base)))),
    );
    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const engine = yield* Engine.Engine;
        const res = yield* server.send({
          kind: "task.create",
          head: { corrId: "c", version: "2026-04-01" },
          data: {
            pid: "p",
            ttl: 1_000,
            action: {
              kind: "promise.create",
              head: { corrId: "c2", version: "2026-04-01" },
              data: {
                id: "bare",
                timeoutAt: Date.now() + 60_000,
                param: yield* Codec.encode({ func: "factorial", args: [3], version: 1 }),
                tags: { "resonate:origin": "bare", "resonate:branch": "bare", "resonate:parent": "bare" },
              },
            },
          },
        });
        if (typeof res.data === "string") throw new Error(res.data);
        return yield* engine.process(Engine.claimed(res.data.task, res.data.promise, res.data.preload));
      }).pipe(Effect.provide(Layer.merge(layer, base))),
    );

    expect(outcome).toMatchObject({ _tag: "Completed", state: "resolved" });
    expect(server.history.some((r) => r.kind === "task.heartbeat")).toBe(false);
  });
});

describe("promises and schedules", () => {
  it("creates, settles, and reads promises from outside any workflow", async () => {
    const out = await scoped(
      Effect.gen(function* () {
        const { resonate } = yield* world([]);
        yield* resonate.promises.create("webhook-1", { data: { order: 42 } });
        yield* resonate.promises.resolve("webhook-1", "paid");
        const value = yield* resonate.get<string>("webhook-1");
        yield* resonate.promises.create("webhook-2");
        yield* resonate.promises.cancel("webhook-2");
        const canceled = yield* Effect.flip(resonate.get("webhook-2"));
        const found = yield* resonate.promises.search({ state: "resolved" });
        return { value, canceled, found: found.promises.map((p) => p.id) };
      }),
    );

    expect(out.value).toBe("paid");
    expect(out.canceled).toMatchObject({ _tag: "Canceled", id: "webhook-2" });
    expect(out.found).toEqual(["webhook-1"]);
  });

  it("creates a schedule whose runs carry the same param as a root", async () => {
    const out = await scoped(
      Effect.gen(function* () {
        const { server, resonate } = yield* world([factorial]);
        const schedule = yield* resonate.schedules.create("nightly", "0 3 * * *", factorial, [5]);
        const fetched = yield* resonate.schedules.get("nightly");
        yield* resonate.schedules.delete("nightly");
        const param = yield* Codec.decode(schedule.promiseParam).pipe(Effect.provide(Codec.layerJson));
        return { schedule, fetched, param, left: server.schedules.size };
      }),
    );

    expect(out.schedule.promiseId).toBe("nightly-{{.timestamp}}");
    expect(out.schedule.promiseTags["resonate:target"]).toMatch(/^poll:\/\/any@default\//);
    expect(out.fetched.id).toBe("nightly");
    expect(out.param).toEqual({ func: "factorial", args: [5], retry: { type: "never" }, version: 1 });
    expect(out.left).toBe(0);
  });
});

describe("server-sent events", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("parses one message per event, across chunk boundaries, skipping non-data lines", async () => {
    const execute = { kind: "execute", head: {}, data: { task: { id: "t1", version: 1 } } };
    const unblock = {
      kind: "unblock",
      head: {},
      data: {
        promise: { id: "p", state: "resolved", param: {}, value: {}, tags: {}, timeoutAt: 0, createdAt: 0 },
      },
    };
    const text = `: hello\r\nevent: message\r\ndata: ${JSON.stringify(execute)}\r\n\r\ndata: ${JSON.stringify(unblock)}\n\n`;
    const bytes = new TextEncoder().encode(text);
    const split = Math.floor(bytes.length / 2);
    const fetch = vi.fn(
      async (_url: string) =>
        new Response(
          new ReadableStream({
            start(c) {
              c.enqueue(bytes.slice(0, split));
              c.enqueue(bytes.slice(split));
            },
          }),
          { status: 200 },
        ),
    );
    vi.stubGlobal("fetch", fetch);

    const messages = Messages.poll({ url: "http://resonate.test/", group: "g", pid: "p1" });
    const got = await Effect.runPromise(Stream.runCollect(Stream.take(messages.stream, 2)));

    expect([...got].map((m) => m.kind)).toEqual(["execute", "unblock"]);
    expect(fetch.mock.calls[0]?.[0]).toBe("http://resonate.test/poll/g/p1");
  });

  it("gives up on a refusal no reconnect can fix", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("no", { status: 401 })),
    );
    const messages = Messages.poll({ url: "http://resonate.test", group: "g", pid: "p1", token: "bad" });

    const error = await Effect.runPromise(Effect.flip(Stream.runDrain(messages.stream)));

    expect(error).toMatchObject({ _tag: "PollError", status: 401 });
  });
});

const waitUntil = (predicate: () => boolean, deadline = Date.now() + 2_000): Effect.Effect<void> =>
  Effect.suspend(() => {
    if (predicate()) return Effect.void;
    if (Date.now() >= deadline) return Effect.die(new Error("condition not met before deadline"));
    return Effect.andThen(Effect.sleep(2), waitUntil(predicate, deadline));
  });
