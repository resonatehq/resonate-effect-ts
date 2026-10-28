import { Cause, Effect, Stream } from "effect";
import * as Engine from "./engine.js";
import { Messages, type PollError } from "./messages.js";

// =============================================================================
// WORKER
// =============================================================================
//
// Dispatch: the loop that turns the server's messages into engine calls. Every
// `execute` names a task this process may run; the engine claims it, and may
// lose that race to another worker, which is fine. Tasks run side by side, one
// fiber each, in the worker's scope — stopping the worker stops them, and the
// leases they held lapse back to the server.
//
// `unblock` messages are not consumed yet: nothing here registers a listener.
// Waiting on a result is done by asking (see `Promises.wait`).

/** Run the worker until its scope closes or the message stream is refused. */
export const run: Effect.Effect<void, PollError, Messages | Engine.Engine> = Effect.gen(function* () {
  const messages = yield* Messages;
  const engine = yield* Engine.Engine;

  yield* Effect.scoped(
    Stream.runForEach(messages.stream, (message) =>
      message.kind === "execute"
        ? Effect.asVoid(
            Effect.forkScoped(
              engine.process(Engine.fromExecuteMsg(message)).pipe(
                // A task that fails has already handed its lease back; the
                // server redelivers it. Nothing else to do here but say so.
                Effect.catchCause((cause) =>
                  Effect.logError(`task ${message.data.task.id} failed`, Cause.pretty(cause)),
                ),
              ),
            ),
          )
        : Effect.void,
    ),
  );
});
