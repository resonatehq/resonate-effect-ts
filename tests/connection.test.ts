import { Effect } from "effect";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as Connection from "../src/connection.js";
import * as Correlation from "../src/correlation.js";
import { TransportError } from "../src/errors.js";
import { VERSION } from "../src/protocol.js";
import * as Requests from "../src/requests.js";

const request = Effect.runSync(Requests.promiseGet({ id: "foo" }).pipe(Effect.provide(Correlation.layerSeq("test"))));

const stubFetch = (body: unknown, status = 200) => {
  const fetch = vi.fn(async () => new Response(JSON.stringify(body), { status }));
  vi.stubGlobal("fetch", fetch);
  return fetch;
};

const envelope = (status: number, data: unknown) => ({
  kind: "promise.get",
  head: { corrId: request.head.corrId, status, version: VERSION },
  data,
});

const send = (options: Connection.HttpOptions = {}) =>
  Connection.http({ url: "http://server", ...options }).send(request);

afterEach(() => vi.unstubAllGlobals());

describe("Connection.http", () => {
  it("posts the request envelope and returns the response envelope", async () => {
    const promise = { id: "foo", state: "pending", param: {}, value: {}, tags: {}, timeoutAt: 1, createdAt: 0 };
    const fetch = stubFetch(envelope(200, { promise }));

    const res = await Effect.runPromise(send());

    expect(res).toEqual(envelope(200, { promise }));
    const [url, init] = fetch.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("http://server");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body as string)).toEqual(request);
  });

  it("returns error statuses as responses — the server said no, the call worked", async () => {
    stubFetch(envelope(404, "promise not found"), 404);

    const res = await Effect.runPromise(send());

    expect(res.head.status).toBe(404);
    expect(res.data).toBe("promise not found");
  });

  it("sends the token as a header and on the head", async () => {
    const fetch = stubFetch(envelope(200, { promise: null }));

    await Effect.runPromise(Effect.result(send({ token: "t0ken" })));

    const [, init] = fetch.mock.calls[0] as unknown as [string, RequestInit];
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer t0ken");
    expect(JSON.parse(init.body as string).head.auth).toBe("t0ken");
  });

  it("fails when the response does not answer the request that was sent", async () => {
    stubFetch({ ...envelope(200, { promise: null }), head: { ...envelope(200, null).head, corrId: "someone-else" } });

    const error = await Effect.runPromise(Effect.flip(send()));

    expect(error).toBeInstanceOf(TransportError);
    expect((error as TransportError).reason).toBe("malformed");
  });

  it("fails when the body is not json", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("<html>gateway</html>", { status: 200 })),
    );

    const error = await Effect.runPromise(Effect.flip(send()));

    expect((error as TransportError).reason).toBe("malformed");
  });

  it("fails when the server cannot be reached", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        throw new Error("ECONNREFUSED");
      }),
    );

    const error = await Effect.runPromise(Effect.flip(send()));

    expect((error as TransportError).reason).toBe("unreachable");
    expect((error as TransportError).corrId).toBe(request.head.corrId);
  });
});
