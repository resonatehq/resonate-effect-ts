/**
 * The flagship Resonate example, in Effect: plan the searches, fan them out
 * through the server, then write a report from what came back.
 *
 * The model and search bodies are deterministic stand-ins so this runs without
 * credentials. Replace them with real services; the durable orchestration in
 * `research` does not change.
 *
 * Start `resonate dev`, then:
 *   npm run example:research -- "What is durable execution?"
 */
import { Effect, Result } from "effect";
import { type Context, Registry, Resonate, type Retries } from "../src/index.js";

export type AgentRequest =
  | { readonly kind: "plan"; readonly question: string }
  | { readonly kind: "report"; readonly question: string; readonly results: ReadonlyArray<SearchOutcome> };

export type AgentResponse =
  | { readonly kind: "plan"; readonly queries: ReadonlyArray<string> }
  | { readonly kind: "report"; readonly report: string };

export interface SearchResult {
  readonly query: string;
  readonly text: string;
}

export type SearchOutcome =
  | { readonly status: "fulfilled"; readonly value: SearchResult }
  | { readonly status: "rejected"; readonly query: string; readonly error: string };

/** A model call. Retried by the SDK, durably, if it fails. */
export const agent = Registry.register(
  "research-agent",
  (_ctx: Context.Context, request: AgentRequest): Effect.Effect<AgentResponse> => {
    if (request.kind === "plan") {
      return Effect.succeed({
        kind: "plan",
        queries: [
          `${request.question} fundamentals`,
          `${request.question} failure recovery`,
          `${request.question} practical examples`,
        ],
      });
    }
    const citations = request.results
      .map((result, i) =>
        result.status === "fulfilled"
          ? `${i + 1}. ${result.value.query}: ${result.value.text}`
          : `${i + 1}. ${result.query}: search failed (${result.error})`,
      )
      .join("\n");
    return Effect.succeed({ kind: "report", report: `# ${request.question}\n\n${citations}` });
  },
);

/** A search call. Each one is its own durable computation, on whichever worker takes it. */
export const search = Registry.register(
  "research-search",
  (_ctx: Context.Context, query: string): Effect.Effect<SearchResult> =>
    Effect.succeed({ query, text: `Replace this function with a search provider result for "${query}".` }),
);

const retry: Retries.Policy = { type: "exponential", data: { delay: 100, maxAttempts: 4 } };

export const research = Registry.register("research", (ctx: Context.Context, question: string) =>
  Effect.gen(function* () {
    // Plan the searches. The plan is recorded, so a replay never asks again.
    const plan = yield* ctx.run(agent, [{ kind: "plan", question }], { retry });
    if (plan.kind !== "plan") return yield* Effect.die("agent returned a report while planning");

    // Fan out through the server, every search at once. One that fails is
    // reported as failed instead of failing the whole report.
    const results = yield* Effect.forEach(
      plan.queries,
      (query) =>
        Effect.map(
          Effect.result(ctx.rpc(search, [query], { retry })),
          (r): SearchOutcome =>
            Result.isSuccess(r)
              ? { status: "fulfilled", value: r.success }
              : { status: "rejected", query, error: String(r.failure) },
        ),
      { concurrency: "unbounded" },
    );

    // Write the report from the recorded results.
    const report = yield* ctx.run(agent, [{ kind: "report", question, results }], { retry });
    if (report.kind !== "report") return yield* Effect.die("agent returned a plan while reporting");
    return report.report;
  }),
);

const main = async (question: string) => {
  const report = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const resonate = yield* Resonate.make({ group: "effect-research", functions: [research, agent, search] });
        yield* resonate.start;
        return yield* resonate.run(`research-${crypto.randomUUID()}`, research, question);
      }),
    ),
  );
  console.log(report);
};

if (import.meta.filename === process.argv[1]) {
  await main(process.argv[2] ?? "What is durable execution?");
}
