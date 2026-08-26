import { Effect, Layer } from "effect";
import * as Connection from "../src/connection.js";
import * as Correlation from "../src/correlation.js";
import type { PromiseRecord, Request, Response, TaskRecord } from "../src/protocol.js";
import { VERSION } from "../src/protocol.js";

/** A response to `req`, with the head the server would have echoed. */
export const respond = (req: Request, status: number, data: unknown): Response =>
  ({ kind: req.kind, head: { corrId: req.head.corrId, status, version: VERSION }, data }) as unknown as Response;

/**
 * A connection that answers from `script` and keeps every request it was given,
 * so a test can assert on the conversation and not just the result.
 */
export const scripted = (script: (req: Request, nth: number) => Response) => {
  const sent: Request[] = [];
  const connection = Connection.layer(<K extends Request["kind"]>(req: Request) => {
    sent.push(req);
    return Effect.succeed(script(req, sent.length) as Extract<Response, { kind: K }>);
  });
  return { sent, layer: Layer.merge(connection, Correlation.layerSeq()) };
};

export const task = (over: Partial<TaskRecord> = {}): TaskRecord => ({
  id: "foo",
  state: "acquired",
  version: 1,
  resumes: [],
  ttl: 5_000,
  pid: "pid-1",
  ...over,
});

export const promise = (over: Partial<PromiseRecord> = {}): PromiseRecord => ({
  id: "foo",
  state: "pending",
  param: { data: JSON.stringify({ func: "foo", args: [] }) },
  value: {},
  tags: { "resonate:origin": "foo" },
  timeoutAt: 60_000,
  createdAt: 0,
  ...over,
});
