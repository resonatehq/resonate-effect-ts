import { Context, Duration, Effect, Layer } from "effect";
import { TransportError } from "./errors.js";
import { isResponse } from "./protocol.js";
import type { Kind, RequestOf, ResponseOf } from "./requests.js";

// =============================================================================
// CONNECTION
// =============================================================================
//
// The whole of the SDK's contact with the server is one method: give it a
// request, eventually get back the response to that request. Everything above
// this line — the engine included — is written against `send` alone, so a
// transport is a single function and a test is a single function.
//
// `send` is total over the protocol: every status the protocol defines,
// including 300 and every error status, comes back as a response. The failure
// channel is reserved for round trips that did not complete, which is the one
// case where the caller cannot know what the server did.

export interface ConnectionService {
  readonly send: <K extends Kind>(request: RequestOf<K>) => Effect.Effect<ResponseOf<K>, TransportError>;
}

export class Connection extends Context.Service<Connection, ConnectionService>()("@resonatehq/effect/Connection") {}

/** Send a request over the ambient connection. */
export const send = <K extends Kind>(request: RequestOf<K>): Effect.Effect<ResponseOf<K>, TransportError, Connection> =>
  Effect.flatMap(Connection, (connection) => connection.send(request));

/** A connection from a bare send function. */
export const make = (send: ConnectionService["send"]): ConnectionService => ({ send });

/** A layer providing a connection from a bare send function. */
export const layer = (send: ConnectionService["send"]): Layer.Layer<Connection> =>
  Layer.succeed(Connection, make(send));

// =============================================================================
// HTTP
// =============================================================================

export interface HttpOptions {
  /** Server url. Defaults to `RESONATE_URL`, then `http://localhost:8001`. */
  readonly url?: string;
  /** Per-request timeout. Defaults to 10 seconds. */
  readonly timeout?: Duration.Input;
  /** Bearer token, sent both as an `Authorization` header and as `head.auth`. */
  readonly token?: string;
  /** Extra headers, merged over the defaults. */
  readonly headers?: Record<string, string>;
}

/**
 * The reference transport: one request per POST, the envelope as the body.
 *
 * The server mirrors `head.status` onto the HTTP status and always returns the
 * envelope, so the HTTP status is not consulted — a body that parses as a
 * well-formed response to this request *is* the response, whatever status
 * carried it. Anything else is a transport failure.
 */
export const http = (options: HttpOptions = {}): ConnectionService => {
  const url = options.url ?? globalThis.process?.env?.RESONATE_URL ?? "http://localhost:8001";
  const timeout = Duration.fromInputUnsafe(options.timeout ?? Duration.seconds(10));
  const token = options.token ?? globalThis.process?.env?.RESONATE_TOKEN;
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...options.headers,
  };

  return make(<K extends Kind>(request: RequestOf<K>): Effect.Effect<ResponseOf<K>, TransportError> => {
    const req = token ? ({ ...request, head: { ...request.head, auth: token } } as RequestOf<K>) : request;
    const fail = (reason: TransportError["reason"], message: string, cause?: unknown) =>
      new TransportError({ kind: request.kind, corrId: request.head.corrId, reason, message, cause });

    return Effect.tryPromise({
      try: (signal) => fetch(url, { method: "POST", headers, body: JSON.stringify(req), signal }),
      catch: (cause) => fail("unreachable", cause instanceof Error ? cause.message : String(cause), cause),
    }).pipe(
      Effect.flatMap((response) =>
        Effect.tryPromise({
          try: () => response.text(),
          catch: (cause) => fail("malformed", "failed to read response body", cause),
        }),
      ),
      Effect.flatMap((body) =>
        Effect.try({
          try: () => JSON.parse(body) as unknown,
          catch: (cause) => fail("malformed", "failed to parse response body as json", cause),
        }),
      ),
      Effect.filterOrFail(
        (res): res is ResponseOf<K> =>
          isResponse(res) && res.kind === request.kind && res.head.corrId === request.head.corrId,
        () => fail("malformed", "response did not match request"),
      ),
      Effect.timeoutOrElse({
        duration: timeout,
        orElse: () => Effect.fail(fail("timeout", `no response within ${Duration.format(timeout)}`)),
      }),
    );
  });
};

/** A layer providing an HTTP connection. */
export const layerHttp = (options: HttpOptions = {}): Layer.Layer<Connection> =>
  Layer.succeed(Connection, http(options));
