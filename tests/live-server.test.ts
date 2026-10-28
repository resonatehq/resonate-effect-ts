import { Effect } from "effect";
import { describe, expect, it } from "vitest";
import { fibonacci } from "../examples/fibonacci.js";
import { agent, research, search } from "../examples/research.js";
import * as Resonate from "../src/resonate.js";

// Against a real server: `resonate dev`, then
//   RESONATE_LIVE_URL=http://localhost:8001 npm test
// Skipped otherwise. Each run uses a fresh group so stale tasks from an earlier
// run cannot be dispatched to this one.

const url = globalThis.process?.env?.RESONATE_LIVE_URL;

describe.skipIf(url === undefined)("live Resonate server", () => {
  it("runs both examples end to end", async () => {
    const nonce = crypto.randomUUID().slice(0, 8);
    const out = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const resonate = yield* Resonate.make({
            url,
            group: `effect-sdk-${nonce}`,
            pollInterval: 10,
            functions: [fibonacci, research, agent, search],
          });
          yield* resonate.start;
          const fib = yield* resonate.run(`live-fib-${nonce}`, fibonacci, 9);
          const report = yield* resonate.run(`live-research-${nonce}`, research, "durable execution");
          return { fib, report };
        }),
      ),
    );

    expect(out.fib).toBe(34);
    expect(out.report).toContain("# durable execution");
  }, 60_000);
});
