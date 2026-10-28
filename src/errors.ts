import { Data } from "effect";
import type { Request, Response } from "./protocol.js";

/**
 * The round trip did not complete: the server was unreachable, the request
 * timed out, or what came back was not a well-formed response to the request
 * that was sent. Nothing can be concluded about whether the server applied the
 * request, so a retry must be safe under the protocol's idempotency rules.
 */
export class TransportError extends Data.TaggedError("TransportError")<{
  readonly kind: Request["kind"];
  readonly corrId: string;
  readonly reason: "unreachable" | "timeout" | "malformed";
  readonly message: string;
  readonly cause?: unknown;
}> {}

/**
 * The round trip completed and the server answered with a status the caller
 * cannot act on. The response is well-formed — this is the server saying no.
 */
export class ServerError extends Data.TaggedError("ServerError")<{
  readonly kind: Request["kind"];
  readonly status: number;
  readonly message: string;
}> {}

/**
 * The server answered with a well-formed response that contradicts the
 * protocol — an acquired task that is not in the acquired state, say. External
 * condition, not an internal invariant, so it is a typed failure rather than a
 * defect.
 */
export class ProtocolError extends Data.TaggedError("ProtocolError")<{
  readonly message: string;
}> {}

/** Lift a non-actionable response into a {@link ServerError}. */
export const serverError = (res: Response): ServerError =>
  new ServerError({
    kind: res.kind,
    status: res.head.status,
    // Every non-200/300 response carries its description in `data` as a string.
    message: typeof res.data === "string" ? res.data : `${res.kind} failed with status ${res.head.status}`,
  });

/** The promise was settled as cancelled: someone stopped waiting for it. */
export class Canceled extends Data.TaggedError("Canceled")<{
  readonly id: string;
}> {}

/** The promise reached its `timeoutAt` before anything settled it. */
export class TimedOut extends Data.TaggedError("TimedOut")<{
  readonly id: string;
}> {}
