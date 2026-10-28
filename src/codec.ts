import { Context, Data, Effect, Layer } from "effect";
import type { Value } from "./protocol.js";

// =============================================================================
// CODEC
// =============================================================================
//
// User values cross the wire inside a protocol `Value`, whose `data` the spec
// requires to be base64. The default is JSON under that, matching the
// TypeScript SDK byte for byte so the two can read each other's promises.

export class CodecError extends Data.TaggedError("CodecError")<{
  readonly direction: "encode" | "decode";
  readonly message: string;
  readonly cause?: unknown;
}> {}

export interface CodecService {
  readonly encode: (value: unknown) => Effect.Effect<Value, CodecError>;
  readonly decode: (value: Value | undefined) => Effect.Effect<unknown, CodecError>;
}

export class Codec extends Context.Service<Codec, CodecService>()("@resonatehq/effect/Codec") {}

// Web-standard rather than Buffer, so the SDK runs wherever `fetch` does. The
// bytes are the same: base64 over the UTF-8 encoding of the JSON text.
const utf8 = new TextEncoder();
const utf8Decoder = new TextDecoder();
const b64encode = (s: string) => {
  let binary = "";
  for (const byte of utf8.encode(s)) binary += String.fromCharCode(byte);
  return btoa(binary);
};
const b64decode = (s: string) => utf8Decoder.decode(Uint8Array.from(atob(s), (c) => c.charCodeAt(0)));

// Infinity and Error survive the round trip; JSON has no native form for either.
const INF = "__INF__";
const NEG_INF = "__NEG_INF__";

const replacer = (_: string, v: unknown) => {
  if (v === Number.POSITIVE_INFINITY) return INF;
  if (v === Number.NEGATIVE_INFINITY) return NEG_INF;
  if (v instanceof Error) return { __type: "error", name: v.name, message: v.message, stack: v.stack };
  return v;
};

const reviver = (_: string, v: unknown) => {
  if (v === INF) return Number.POSITIVE_INFINITY;
  if (v === NEG_INF) return Number.NEGATIVE_INFINITY;
  if (typeof v === "object" && v !== null && (v as { __type?: string }).__type === "error") {
    const e = v as { name?: string; message?: string; stack?: string };
    const err = new Error(e.message ?? "unknown error");
    if (e.name) err.name = e.name;
    if (e.stack) err.stack = e.stack;
    return err;
  }
  return v;
};

/** JSON, base64-encoded. The default. */
export const json: CodecService = {
  encode: (value) =>
    value === undefined
      ? Effect.succeed<Value>({ headers: {}, data: "" })
      : Effect.try({
          try: (): Value => ({ headers: {}, data: b64encode(JSON.stringify(value, replacer)) }),
          catch: (cause) => new CodecError({ direction: "encode", message: "value is not encodable", cause }),
        }),
  decode: (value) =>
    !value?.data
      ? Effect.succeed(undefined)
      : Effect.try({
          try: () => JSON.parse(b64decode(value.data as string), reviver) as unknown,
          catch: (cause) => new CodecError({ direction: "decode", message: "value is not decodable", cause }),
        }),
};

export const layerJson: Layer.Layer<Codec> = Layer.succeed(Codec, json);

export const encode = (value: unknown): Effect.Effect<Value, CodecError, Codec> =>
  Effect.flatMap(Codec, (c) => c.encode(value));

export const decode = (value: Value | undefined): Effect.Effect<unknown, CodecError, Codec> =>
  Effect.flatMap(Codec, (c) => c.decode(value));
