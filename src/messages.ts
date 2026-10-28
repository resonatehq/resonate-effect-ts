import { Context, Data, Layer, Stream } from "effect";
import { isMessage, type Message } from "./protocol.js";

// =============================================================================
// MESSAGES
// =============================================================================
//
// The other direction. The connection is how this process talks to the server;
// messages are how the server talks back without being asked: an `execute`
// when a task targeted at this process is ready to run, an `unblock` when a
// promise someone listens on settles.
//
// One stream, consumed by the worker. Like the connection it is a service, so a
// test hands in a queue and the worker cannot tell.

/** The message stream could not be held open, and retrying would not help. */
export class PollError extends Data.TaggedError("PollError")<{
  readonly status?: number;
  readonly message: string;
  readonly cause?: unknown;
}> {}

export interface MessagesService {
  readonly stream: Stream.Stream<Message, PollError>;
}

export class Messages extends Context.Service<Messages, MessagesService>()("@resonatehq/effect/Messages") {}

/** Messages from any stream. Tests use a queue. */
export const layer = (stream: Stream.Stream<Message, PollError>): Layer.Layer<Messages> =>
  Layer.succeed(Messages, { stream });

// =============================================================================
// SERVER-SENT EVENTS
// =============================================================================

export interface PollOptions {
  /** Server url. Defaults to `RESONATE_URL`, then `http://localhost:8001`. */
  readonly url?: string;
  /** The group this process listens for, as in `poll://any@<group>/<pid>`. */
  readonly group: string;
  /** This process. */
  readonly pid: string;
  /** Bearer token. Defaults to `RESONATE_TOKEN`. */
  readonly token?: string;
  /** Extra headers, merged over the defaults. */
  readonly headers?: Record<string, string>;
  /** How long to wait before reconnecting a dropped stream. Defaults to 250ms. */
  readonly reconnectDelay?: number;
}

/**
 * The reference listener: `GET /poll/<group>/<pid>` as server-sent events, one
 * message per event.
 *
 * A dropped connection is reconnected after `reconnectDelay`, forever — the
 * server redelivers a task whose lease nobody took, so a gap costs latency, not
 * work. The only thing that ends the stream is a refusal no retry can fix: a
 * 401 or 403.
 */
export const poll = (options: PollOptions): MessagesService => {
  const base = (options.url ?? globalThis.process?.env?.RESONATE_URL ?? "http://localhost:8001").replace(/\/+$/, "");
  const token = options.token ?? globalThis.process?.env?.RESONATE_TOKEN;
  const url = `${base}/poll/${encodeURIComponent(options.group)}/${encodeURIComponent(options.pid)}`;
  const headers: Record<string, string> = {
    Accept: "text/event-stream",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...options.headers,
  };
  const reconnectDelay = options.reconnectDelay ?? 250;

  return {
    stream: Stream.suspend(() =>
      Stream.fromAsyncIterable(events(url, headers, reconnectDelay), (cause) =>
        cause instanceof PollError ? cause : new PollError({ message: "message stream failed", cause }),
      ),
    ),
  };
};

/** A layer providing the server-sent-events listener. */
export const layerPoll = (options: PollOptions): Layer.Layer<Messages> => Layer.succeed(Messages, poll(options));

const events = (url: string, headers: Record<string, string>, reconnectDelay: number): AsyncIterable<Message> => {
  const controller = new AbortController();
  const decoder = new TextDecoder();
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let buffer = "";
  let data: string[] = [];
  const pending: Message[] = [];
  let timer: ReturnType<typeof setTimeout> | undefined;
  let wake: (() => void) | undefined;

  const close = async () => {
    const current = reader;
    reader = undefined;
    buffer = "";
    data = [];
    if (current) {
      await current.cancel().catch(() => undefined);
      current.releaseLock();
    }
  };

  const pause = () =>
    new Promise<void>((resolve) => {
      wake = resolve;
      timer = setTimeout(() => {
        timer = undefined;
        wake = undefined;
        resolve();
      }, reconnectDelay);
    });

  const connect = async () => {
    const response = await fetch(url, { headers, signal: controller.signal });
    if (response.status === 401 || response.status === 403) {
      throw new PollError({ status: response.status, message: `message stream refused with ${response.status}` });
    }
    if (!response.ok || !response.body) throw new Error(`message stream answered ${response.status}`);
    reader = response.body.getReader();
  };

  // One event per blank line; its `data:` lines joined are one message. Lines
  // that are not data — comments, event names, ids — carry nothing here.
  const parse = (chunk: string) => {
    buffer += chunk;
    for (let newline = buffer.indexOf("\n"); newline >= 0; newline = buffer.indexOf("\n")) {
      const raw = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      const line = raw.endsWith("\r") ? raw.slice(0, -1) : raw;
      if (line === "") {
        if (data.length > 0) {
          const parsed: unknown = JSON.parse(data.join("\n"));
          if (isMessage(parsed)) pending.push(parsed);
          data = [];
        }
      } else if (line.startsWith("data:")) {
        data.push(line.slice(5).trimStart());
      }
    }
  };

  const iterator: AsyncIterator<Message> = {
    next: async () => {
      while (!controller.signal.aborted) {
        const next = pending.shift();
        if (next) return { done: false, value: next };
        try {
          if (!reader) await connect();
          const chunk = await (reader as ReadableStreamDefaultReader<Uint8Array>).read();
          if (chunk.done) {
            await close();
            await pause();
          } else {
            parse(decoder.decode(chunk.value, { stream: true }));
          }
        } catch (cause) {
          if (controller.signal.aborted) break;
          await close();
          if (cause instanceof PollError) throw cause;
          await pause();
        }
      }
      return { done: true, value: undefined };
    },
    return: async () => {
      controller.abort();
      if (timer) clearTimeout(timer);
      wake?.();
      await close();
      return { done: true, value: undefined };
    },
  };

  return { [Symbol.asyncIterator]: () => iterator };
};
