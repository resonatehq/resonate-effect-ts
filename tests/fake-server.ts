import { Effect, Queue, Stream } from "effect";
import type { ConnectionService } from "../src/connection.js";
import type { MessagesService } from "../src/messages.js";
import type {
  ExecuteMsg,
  Message,
  PromiseCreateReq,
  PromiseRecord,
  Request,
  Response,
  ScheduleRecord,
  TaskRecord,
} from "../src/protocol.js";

// A whole server in memory, ported from the effect-ts SDK's test suite: tasks,
// leases, callbacks, and dispatch. Creating a promise with a target creates a
// task and sends an `execute` down the message stream; settling a promise that
// a suspended task awaits makes that task pending again and sends another. So
// a worker on `messages` and a client on `connection` see what they would see
// against `resonate dev`, minus timers.

type ResponseFor<R extends Request> = Extract<Response, { kind: R["kind"] }>;

interface MutableTask {
  id: string;
  state: "pending" | "acquired" | "suspended" | "halted" | "fulfilled";
  version: number;
  resumes: string[];
  ttl?: number;
  pid?: string;
}

export class FakeServer {
  readonly messages: MessagesService;
  readonly promises = new Map<string, PromiseRecord>();
  readonly tasks = new Map<string, MutableTask>();
  readonly schedules = new Map<string, ScheduleRecord>();
  readonly callbacks = new Map<string, Set<string>>();
  readonly forcedSettlements = new Map<
    string,
    { readonly state: "resolved" | "rejected" | "rejected_canceled"; readonly value: Value }
  >();
  readonly history: Request[] = [];
  private now = 1_000;

  private constructor(private readonly queue: Queue.Queue<Message>) {
    this.messages = { stream: Stream.fromQueue(queue) };
  }

  static make(): Effect.Effect<FakeServer> {
    return Effect.map(Queue.unbounded<Message>(), (queue) => new FakeServer(queue));
  }

  readonly connection: ConnectionService = {
    send: (request) => this.send(request) as never,
  };

  readonly send = <R extends Request>(request: R): Effect.Effect<ResponseFor<R>> =>
    Effect.gen(
      function* (this: FakeServer) {
        this.history.push(request);
        const response = yield* this.apply(request);
        return response as ResponseFor<R>;
      }.bind(this),
    );

  private apply(request: Request): Effect.Effect<Response> {
    switch (request.kind) {
      case "promise.get": {
        const promise = this.promises.get(request.data.id);
        return Effect.succeed(
          promise === undefined
            ? this.response(request, 404, "Promise not found")
            : this.response(request, 200, { promise }),
        );
      }
      case "promise.create":
        return Effect.map(this.createPromise(request), (promise) => this.response(request, 200, { promise }));
      case "promise.settle":
        return Effect.map(this.settlePromise(request.data.id, request.data.state, request.data.value), (promise) =>
          this.response(request, 200, { promise }),
        );
      case "promise.register_callback": {
        const awaited = this.promises.get(request.data.awaited);
        if (awaited === undefined) return Effect.succeed(this.response(request, 404, "Promise not found"));
        if (awaited.state === "pending") {
          const callbacks = this.callbacks.get(awaited.id) ?? new Set<string>();
          callbacks.add(request.data.awaiter);
          this.callbacks.set(awaited.id, callbacks);
        }
        return Effect.succeed(this.response(request, 200, { promise: awaited }));
      }
      case "promise.register_listener":
        return Effect.succeed(this.response(request, 501, "Not implemented"));
      case "promise.search": {
        const promises = Array.from(this.promises.values()).filter(
          (promise) =>
            (request.data.state === undefined || promise.state === request.data.state) &&
            (request.data.tags === undefined ||
              Object.entries(request.data.tags).every(([key, value]) => promise.tags[key] === value)),
        );
        return Effect.succeed(this.response(request, 200, { promises }));
      }
      case "task.create":
        return Effect.gen(
          function* (this: FakeServer) {
            const promise = yield* this.createPromise(request.data.action, false);
            let task = this.tasks.get(promise.id);
            if (
              task !== undefined &&
              (task.state === "acquired" || task.state === "suspended" || task.state === "halted")
            ) {
              return this.response(request, 409, "Task already active");
            }
            if (task === undefined) {
              task = {
                id: promise.id,
                state: "acquired",
                version: 1,
                resumes: [],
                ttl: this.now + request.data.ttl,
                pid: request.data.pid,
              };
              this.tasks.set(task.id, task);
            }
            return this.response(request, 200, {
              task: this.taskRecord(task),
              promise,
              preload: this.preload(promise),
            });
          }.bind(this),
        );
      case "task.acquire": {
        const task = this.tasks.get(request.data.id);
        const promise = this.promises.get(request.data.id);
        if (task === undefined || promise === undefined) {
          return Effect.succeed(this.response(request, 404, "Task not found"));
        }
        if (task.state !== "pending" || task.version !== request.data.version) {
          return Effect.succeed(this.response(request, 409, "Task is not pending"));
        }
        task.state = "acquired";
        task.version += 1;
        task.pid = request.data.pid;
        task.ttl = this.now + request.data.ttl;
        return Effect.succeed(
          this.response(request, 200, {
            task: this.taskRecord(task),
            promise,
            preload: this.preload(promise),
          }),
        );
      }
      case "task.release": {
        const task = this.tasks.get(request.data.id);
        if (task === undefined) return Effect.succeed(this.response(request, 404, "Task not found"));
        task.state = "pending";
        delete task.pid;
        delete task.ttl;
        return Effect.as(this.dispatch(task), this.response(request, 200, {}));
      }
      case "task.suspend": {
        const task = this.tasks.get(request.data.id);
        if (task === undefined) return Effect.succeed(this.response(request, 404, "Task not found"));
        const settled = request.data.actions.some(
          (action) => this.promises.get(action.data.awaited)?.state !== "pending",
        );
        if (settled) {
          const root = this.promises.get(task.id);
          return Effect.succeed(this.response(request, 300, { preload: root === undefined ? [] : this.preload(root) }));
        }
        for (const action of request.data.actions) {
          const callbacks = this.callbacks.get(action.data.awaited) ?? new Set<string>();
          callbacks.add(action.data.awaiter);
          this.callbacks.set(action.data.awaited, callbacks);
        }
        task.state = "suspended";
        delete task.pid;
        delete task.ttl;
        return Effect.succeed(this.response(request, 200, {}));
      }
      case "task.fulfill":
        return Effect.gen(
          function* (this: FakeServer) {
            const task = this.tasks.get(request.data.id);
            if (task === undefined) return this.response(request, 404, "Task not found");
            const promise = yield* this.settlePromise(
              request.data.action.data.id,
              request.data.action.data.state,
              request.data.action.data.value,
            );
            task.state = "fulfilled";
            delete task.pid;
            delete task.ttl;
            return this.response(request, 200, { promise });
          }.bind(this),
        );
      case "task.fence": {
        const task = this.tasks.get(request.data.id);
        if (task === undefined || task.state !== "acquired" || task.version !== request.data.version) {
          return Effect.succeed(this.response(request, 409, "Stale task fence"));
        }
        if (request.data.action.kind === "promise.create") {
          return Effect.map(this.createPromise(request.data.action), (promise) =>
            this.response(request, 200, {
              action: this.response(request.data.action, 200, { promise }),
              preload: this.preload(this.promises.get(task.id) as PromiseRecord),
            }),
          );
        }
        const forced = this.forcedSettlements.get(request.data.action.data.id);
        if (forced !== undefined) {
          this.forcedSettlements.delete(request.data.action.data.id);
          const current = this.promises.get(request.data.action.data.id);
          if (current !== undefined && current.state === "pending") {
            this.promises.set(current.id, {
              ...current,
              state: forced.state,
              value: forced.value,
              settledAt: this.now++,
            });
          }
        }
        return Effect.map(
          this.settlePromise(
            request.data.action.data.id,
            request.data.action.data.state,
            request.data.action.data.value,
          ),
          (promise) =>
            this.response(request, 200, {
              action: this.response(request.data.action, 200, { promise }),
              preload: this.preload(this.promises.get(task.id) as PromiseRecord),
            }),
        );
      }
      case "task.heartbeat":
        return Effect.succeed(this.response(request, 200, {}));
      case "task.get": {
        const task = this.tasks.get(request.data.id);
        return Effect.succeed(
          task === undefined
            ? this.response(request, 404, "Task not found")
            : this.response(request, 200, { task: this.taskRecord(task) }),
        );
      }
      case "schedule.create": {
        const existing = this.schedules.get(request.data.id);
        const schedule: ScheduleRecord = existing ?? {
          id: request.data.id,
          cron: request.data.cron,
          promiseId: request.data.promiseId,
          promiseTimeout: request.data.promiseTimeout,
          promiseParam: request.data.promiseParam,
          promiseTags: request.data.promiseTags,
          createdAt: this.now++,
          nextRunAt: this.now + 60_000,
        };
        this.schedules.set(schedule.id, schedule);
        return Effect.succeed(this.response(request, 200, { schedule }));
      }
      case "schedule.get": {
        const schedule = this.schedules.get(request.data.id);
        return Effect.succeed(
          schedule === undefined
            ? this.response(request, 404, "Schedule not found")
            : this.response(request, 200, { schedule }),
        );
      }
      case "schedule.delete":
        this.schedules.delete(request.data.id);
        return Effect.succeed(this.response(request, 200, {}));
      case "schedule.search":
        return Effect.succeed(
          this.response(request, 200, {
            schedules: Array.from(this.schedules.values()).filter(
              (schedule) =>
                request.data.tags === undefined ||
                Object.entries(request.data.tags).every(([key, value]) => schedule.promiseTags[key] === value),
            ),
          }),
        );
      case "task.halt":
      case "task.continue":
      case "task.search":
      case "debug.start":
      case "debug.reset":
      case "debug.tick":
      case "debug.snap":
      case "debug.stop":
        return Effect.succeed(this.response(request, 501, "Not implemented"));
    }
  }

  private createPromise(request: PromiseCreateReq, dispatch = true): Effect.Effect<PromiseRecord> {
    const existing = this.promises.get(request.data.id);
    if (existing !== undefined) return Effect.succeed(existing);

    const promise: PromiseRecord = {
      id: request.data.id,
      state: "pending",
      param: request.data.param,
      value: {},
      tags: request.data.tags,
      timeoutAt: request.data.timeoutAt,
      createdAt: this.now++,
    };
    this.promises.set(promise.id, promise);

    if (request.data.tags["resonate:target"] === undefined || !dispatch) return Effect.succeed(promise);
    const task: MutableTask = {
      id: promise.id,
      state: "pending",
      version: 0,
      resumes: [],
    };
    this.tasks.set(task.id, task);
    return Effect.as(this.dispatch(task), promise);
  }

  private settlePromise(
    id: string,
    state: "resolved" | "rejected" | "rejected_canceled",
    value: Value,
  ): Effect.Effect<PromiseRecord> {
    const current = this.promises.get(id);
    if (current === undefined) return Effect.die(new Error(`Unknown promise '${id}'`));
    if (current.state !== "pending") return Effect.succeed(current);

    const promise: PromiseRecord = {
      ...current,
      state,
      value,
      settledAt: this.now++,
    };
    this.promises.set(id, promise);

    return Effect.gen(
      function* (this: FakeServer) {
        for (const awaiter of this.callbacks.get(id) ?? []) {
          const task = this.tasks.get(awaiter);
          if (task !== undefined && task.state === "suspended") {
            task.state = "pending";
            yield* this.dispatch(task);
          }
        }
        this.callbacks.delete(id);
        return promise;
      }.bind(this),
    );
  }

  private dispatch(task: MutableTask): Effect.Effect<void> {
    const message: ExecuteMsg = {
      kind: "execute",
      head: {},
      data: { task: { id: task.id, version: task.version } },
    };
    return Effect.asVoid(Queue.offer(this.queue, message));
  }

  private preload(root: PromiseRecord): ReadonlyArray<PromiseRecord> {
    const branch = root.tags["resonate:branch"];
    return Array.from(this.promises.values()).filter(
      (promise) => promise.id !== root.id && promise.tags["resonate:branch"] === branch,
    );
  }

  private taskRecord(task: MutableTask): TaskRecord {
    return {
      id: task.id,
      state: task.state,
      version: task.version,
      resumes: task.resumes,
      ...(task.ttl === undefined ? {} : { ttl: task.ttl }),
      ...(task.pid === undefined ? {} : { pid: task.pid }),
    };
  }

  private response<R extends Request>(request: R, status: number, data: unknown): ResponseFor<R> {
    return {
      kind: request.kind,
      head: {
        corrId: request.head.corrId,
        status,
        version: request.head.version,
      },
      data,
    } as ResponseFor<R>;
  }
}

type Value = PromiseRecord["value"];
