import { Effect, Layer } from "effect";
import { describe, expect, it } from "vitest";
import * as Codec from "../src/codec.js";
import type { Context } from "../src/context.js";
import { ServerError } from "../src/errors.js";
import * as Pass from "../src/pass.js";
import type { Request } from "../src/protocol.js";
import * as Registry from "../src/registry.js";
import { root, server, task } from "./server.js";

// A failed write says nothing about the computation. It has not failed; it has
// not been run. If it reaches the workflow's error channel it is
// indistinguishable from a domain error — and a fulfilled task is immutable, so
// recording it makes a rate limit permanent.

const go = (
  workflows: Registry.Registration[],
  name: string,
  refuse?: (req: Request, nth: number) => number | undefined,
) => {
  const s = server({ latency: () => 0, refuse });
  const effect = Pass.executeUntilBlocked(
    { task: task(), promise: root(name), preload: [] },
    { target: "poll://worker" },
  ).pipe(Effect.provide(Layer.mergeAll(s.layer, Codec.layerJson, Registry.layer(workflows))));
  return { server: s, effect };
};

const leaf = Registry.register("leaf", (_c: Context, n: number) => Effect.succeed(n));

describe("a write that did not land is never a result", () => {
  it.each([429, 500, 401, 403, 400])("gives the lease back on %i rather than recording it", async (status) => {
    const wf = Registry.register("wf", (ctx: Context) => ctx.run(leaf, 1));
    const { effect, server: s } = go([wf, leaf], "wf", (req) => (req.kind === "task.fence" ? status : undefined));

    const error = await Effect.runPromise(Effect.flip(effect));

    // Surfaced as what it is, so the engine releases the task and the next
    // delivery tries again.
    expect(error).toBeInstanceOf(ServerError);
    expect((error as ServerError).status).toBe(status);
    // Nothing was written, and the root was not fulfilled with a 429.
    expect(s.kinds).not.toContain("task.fulfill");
  });

  it("cannot be swallowed by the workflow as a business failure", async () => {
    // The shape that makes this unrecoverable: a user compensating for what
    // they think is a domain error, and durably recording the compensation.
    const wf = Registry.register("wf", (ctx: Context) =>
      ctx.run(leaf, 1).pipe(Effect.catch(() => Effect.succeed("COMPENSATED"))),
    );
    const { effect, server: s } = go([wf, leaf], "wf", (req) => (req.kind === "task.fence" ? 429 : undefined));

    const error = await Effect.runPromise(Effect.flip(effect));

    expect(error).toBeInstanceOf(ServerError);
    expect(s.kinds).not.toContain("task.fulfill");
  });

  it("still records a genuine domain failure", async () => {
    const failing = Registry.register("failing", (_c: Context) => Effect.fail("declined"));
    const wf = Registry.register("wf", (ctx: Context) => ctx.run(failing));
    const { effect, server: s } = go([wf, failing], "wf");

    const out = await Effect.runPromise(effect);

    expect(out).toMatchObject({ _tag: "Completed", state: "rejected" });
    expect(s.journal).toContain("settle foo:1 rejected");
  });
});

describe("options that were being dropped", () => {
  it("honours an explicit id on ctx.promise", async () => {
    const wf = Registry.register("wf", (ctx: Context) =>
      Effect.map(ctx.promise({ id: "awaiting-signature" }), (h) => h.id),
    );
    const { effect, server: s } = go([wf], "wf");

    await Effect.runPromise(effect);

    expect([...s.promises.keys()]).toEqual(["awaiting-signature"]);
  });

  it("refuses an explicit id the server would reject", async () => {
    const wf = Registry.register("wf", (ctx: Context) => ctx.run(leaf, [1], { id: "has:colon" }));
    const { effect, server: s } = go([wf, leaf], "wf");

    const out = await Effect.runPromise(effect);

    expect(out).toMatchObject({ _tag: "Completed", state: "rejected" });
    // Nothing was put on the wire.
    expect([...s.promises.keys()]).toEqual([]);
  });

  it("does not let a user tag overwrite the protocol's own", async () => {
    const wf = Registry.register("wf", (ctx: Context) =>
      ctx.run(leaf, [1], { tags: { "resonate:origin": "hijacked", mine: "kept" } }),
    );
    const { effect, server: s } = go([wf, leaf], "wf");

    await Effect.runPromise(effect);

    expect(s.promises.get("foo:1")?.tags["resonate:origin"]).toBe("foo");
    expect(s.promises.get("foo:1")?.tags.mine).toBe("kept");
  });
});

describe("ctx.race edge cases", () => {
  it("says what is wrong when given no branches", async () => {
    const wf = Registry.register("wf", (ctx: Context) => ctx.race([]));
    const { effect } = go([wf], "wf");

    const out = await Effect.runPromise(effect);

    expect(out).toMatchObject({ _tag: "Completed", state: "rejected" });
  });
});
