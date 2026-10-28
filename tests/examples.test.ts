import { Effect } from "effect";
import { describe, expect, it } from "vitest";
import { fibonacci } from "../examples/fibonacci.js";
import { agent, research, search } from "../examples/research.js";
import type { Registration } from "../src/registry.js";
import * as Resonate from "../src/resonate.js";
import { FakeServer } from "./fake-server.js";

// The examples, exactly as they ship, on the in-memory server: a worker and a
// client in one process, which is what `resonate dev` plus `npm run example:*`
// amounts to.

const run = <A>(functions: Registration[], start: (r: Resonate.Resonate) => Effect.Effect<A, unknown>) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* FakeServer.make();
        const resonate = yield* Resonate.make({
          connection: server.connection,
          messages: server.messages,
          pollInterval: 1,
          functions,
        });
        yield* resonate.start;
        const value = yield* start(resonate);
        return { server, value };
      }),
    ),
  );

describe("fibonacci", () => {
  it("adds up, running even levels here and odd levels through the server", async () => {
    const { server, value } = await run([fibonacci], (r) => r.run("fib-7", fibonacci, 7));

    expect(value).toBe(13);
    const promises = [...server.promises.values()];
    // Odd n dispatched both halves: those are tasks of their own.
    const dispatched = promises.filter((p) => p.tags["resonate:scope"] === "global" && p.id !== "fib-7");
    expect(dispatched.length).toBeGreaterThan(0);
    // Even n ran both halves here: local children on their parent's branch.
    const local = promises.filter((p) => p.tags["resonate:scope"] === "local");
    expect(local.length).toBeGreaterThan(0);
    // Every id is Dewey under the one origin.
    expect(promises.every((p) => p.id === "fib-7" || p.id.startsWith("fib-7:"))).toBe(true);
    expect(promises.every((p) => p.state === "resolved")).toBe(true);
  });
});

describe("research", () => {
  it("plans, fans the searches out through the server, and writes the report", async () => {
    const { server, value } = await run([research, agent, search], (r) =>
      r.run("research-1", research, "What is durable execution?"),
    );

    expect(value).toContain("# What is durable execution?");
    expect(value).toContain("1. What is durable execution? fundamentals");
    expect(value).toContain("3. What is durable execution? practical examples");
    // plan, three searches, report — in that order, under one origin.
    expect([...server.promises.keys()].filter((id) => id !== "research-1")).toEqual([
      "research-1:1",
      "research-1:2",
      "research-1:3",
      "research-1:4",
      "research-1:5",
    ]);
    const searches = ["research-1:2", "research-1:3", "research-1:4"].map((id) => server.promises.get(id));
    expect(searches.every((p) => p?.tags["resonate:scope"] === "global")).toBe(true);
    expect(server.tasks.get("research-1:2")?.state).toBe("fulfilled");
  });
});
