import { Context, Effect, Layer } from "effect";

/**
 * Source of `head.corrId`, the value the server echoes back so a response can
 * be matched to its request. A service rather than a bare call to
 * `crypto.randomUUID()` so a test can make correlation ids deterministic.
 */
export class Correlation extends Context.Tag("@resonatehq/effect/Correlation")<
  Correlation,
  { readonly next: Effect.Effect<string> }
>() {}

/** The next correlation id. */
export const next: Effect.Effect<string, never, Correlation> = Effect.flatMap(Correlation, (c) => c.next);

/** Correlation ids from `crypto.randomUUID()`. The default. */
export const layerUuid: Layer.Layer<Correlation> = Layer.succeed(Correlation, {
  next: Effect.sync(() => crypto.randomUUID()),
});

/** Correlation ids from a counter — `corr-1`, `corr-2`, ... — for tests. */
export const layerSeq = (prefix = "corr"): Layer.Layer<Correlation> =>
  Layer.sync(Correlation, () => {
    let n = 0;
    return { next: Effect.sync(() => `${prefix}-${++n}`) };
  });
