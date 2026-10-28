/**
 * Recursive Fibonacci with location transparency: the same function, called
 * two ways.
 *
 * - even n: both halves run here, as local children (`ctx.run`)
 * - odd n:  both halves are dispatched through the server (`ctx.rpc`), to
 *           whichever worker in the group picks them up
 *
 * Either way the two halves run side by side under `Effect.all`, and the
 * caller cannot tell where the work happened.
 *
 * Start `resonate dev`, then:
 *   npm run example:fibonacci -- 10
 */
import { Effect } from "effect";
import { type Context, Registry, Resonate } from "../src/index.js";

export const fibonacci = Registry.register(
  "fibonacci",
  (ctx: Context.Context, n: number): Effect.Effect<number, unknown> =>
    n < 2
      ? Effect.succeed(n)
      : Effect.map(
          Effect.all(
            n % 2 === 0
              ? [ctx.run(fibonacci, n - 1), ctx.run(fibonacci, n - 2)]
              : [ctx.rpc(fibonacci, n - 1), ctx.rpc(fibonacci, n - 2)],
            { concurrency: "unbounded" },
          ),
          ([a, b]) => a + b,
        ),
);

const main = async (n: number) => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const resonate = yield* Resonate.make({ group: "effect-fibonacci", functions: [fibonacci] });
        yield* resonate.start;
        return yield* resonate.run(`fibonacci-${n}-${Date.now()}`, fibonacci, n);
      }),
    ),
  );
  console.log(`fib(${n}) = ${result}`);
};

if (import.meta.filename === process.argv[1]) {
  const n = Number(process.argv[2] ?? "10");
  if (!Number.isSafeInteger(n) || n < 0) throw new TypeError(`n must be a non-negative safe integer, received ${n}`);
  await main(n);
}
