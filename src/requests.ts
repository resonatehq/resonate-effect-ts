import { Effect } from "effect";
import * as Correlation from "./correlation.js";
import { type Request, type Response, VERSION } from "./protocol.js";

// =============================================================================
// KIND-INDEXED VIEWS OF THE PROTOCOL
// =============================================================================
//
// Requests and responses are one closed union each, correlated by `kind`. These
// aliases let a caller name the request, response, or status variant it means
// without restating the union.

export type Kind = Request["kind"];

export type RequestOf<K extends Kind> = Extract<Request, { kind: K }>;
export type ResponseOf<K extends Kind> = Extract<Response, { kind: K }>;
export type DataOf<K extends Kind> = RequestOf<K>["data"];

/** The success (200) variant of a response. */
export type Ok<K extends Kind> = Extract<ResponseOf<K>, { head: { status: 200 } }>;
/** The redirect (300) variant of a response — only `task.suspend` has one. */
export type Redirect<K extends Kind> = Extract<ResponseOf<K>, { head: { status: 300 } }>;

// =============================================================================
// CONSTRUCTORS
// =============================================================================
//
// One constructor per request kind. Each takes exactly the `data` the protocol
// defines and fills the head: a fresh correlation id and this SDK's protocol
// version. `head.auth` is left to the connection, which holds the credential.
//
// Nested requests — the `action` of a task.create/fulfill/fence, the `actions`
// of a task.suspend — are themselves full requests with their own heads, so
// they are built with these same constructors and passed in.

const make = <K extends Kind>(kind: K, data: DataOf<K>): Effect.Effect<RequestOf<K>, never, Correlation.Correlation> =>
  Effect.map(Correlation.next, (corrId) => ({ kind, head: { corrId, version: VERSION }, data }) as RequestOf<K>);

// Promises

export const promiseGet = (data: DataOf<"promise.get">) => make("promise.get", data);
export const promiseCreate = (data: DataOf<"promise.create">) => make("promise.create", data);
export const promiseSettle = (data: DataOf<"promise.settle">) => make("promise.settle", data);
export const promiseRegisterCallback = (data: DataOf<"promise.register_callback">) =>
  make("promise.register_callback", data);
export const promiseRegisterListener = (data: DataOf<"promise.register_listener">) =>
  make("promise.register_listener", data);
export const promiseSearch = (data: DataOf<"promise.search">) => make("promise.search", data);

// Tasks

export const taskGet = (data: DataOf<"task.get">) => make("task.get", data);
export const taskCreate = (data: DataOf<"task.create">) => make("task.create", data);
export const taskAcquire = (data: DataOf<"task.acquire">) => make("task.acquire", data);
export const taskRelease = (data: DataOf<"task.release">) => make("task.release", data);
export const taskSuspend = (data: DataOf<"task.suspend">) => make("task.suspend", data);
export const taskHalt = (data: DataOf<"task.halt">) => make("task.halt", data);
export const taskContinue = (data: DataOf<"task.continue">) => make("task.continue", data);
export const taskFulfill = (data: DataOf<"task.fulfill">) => make("task.fulfill", data);
export const taskFence = (data: DataOf<"task.fence">) => make("task.fence", data);
export const taskHeartbeat = (data: DataOf<"task.heartbeat">) => make("task.heartbeat", data);
export const taskSearch = (data: DataOf<"task.search">) => make("task.search", data);

// Schedules

export const scheduleGet = (data: DataOf<"schedule.get">) => make("schedule.get", data);
export const scheduleCreate = (data: DataOf<"schedule.create">) => make("schedule.create", data);
export const scheduleDelete = (data: DataOf<"schedule.delete">) => make("schedule.delete", data);
export const scheduleSearch = (data: DataOf<"schedule.search">) => make("schedule.search", data);

// Debug

export const debugStart = () => make("debug.start", {});
export const debugReset = () => make("debug.reset", {});
export const debugTick = (data: DataOf<"debug.tick">) => make("debug.tick", data);
export const debugSnap = () => make("debug.snap", {});
export const debugStop = () => make("debug.stop", {});
