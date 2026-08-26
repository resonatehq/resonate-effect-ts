# Resonate SDK for Effect

A Resonate SDK built on [Effect](https://effect.website), speaking the
[Resonate protocol](https://github.com/resonatehq/resonate-protocol)
(version `2026-04-01`) to the Resonate server.

Built outside in. What exists today is the outermost layer: the protocol as
data, the server as one method, and the engine that runs the task loop between
them. The part that runs user code sits behind an interface and is not written
yet.

## The pieces

| Module | What it is |
|--------|------------|
| `src/protocol.ts` | Every request, response, record, and message the protocol defines. Vendored verbatim from `resonate-protocol/spec/types-raw.ts`. |
| `src/requests.ts` | One constructor per request kind. Fills the head, leaves `data` to the protocol. |
| `src/connection.ts` | The server, as one method: `send(request) -> Effect<response>`. |
| `src/engine.ts` | The task loop: claim, execute, fulfill or suspend, continue. |
| `src/executor.ts` | The seam where running a computation will plug in. |
| `src/correlation.ts` | Where `head.corrId` comes from. A service, so tests can make it deterministic. |

## The engine

```
claim ─▶ execute ─┬─ completed ──▶ fulfill ────────▶ done
           ▲      └─ suspended ──▶ suspend ─┬─ 200 ─▶ done
           │                                └─ 300 ─┘
           └────────── continue ────────────────────┘
```

`Engine.process` takes one task and drives it as far as it goes.

A task arrives in one of two conditions. **Unclaimed** is what an `execute`
message carries — an id and a version, and no right to run anything; the engine
acquires the lease first and may lose that race. **Claimed** is what
`task.acquire` and `task.create` return — the lease is already held by this
process, and the root promise and its branch came back with it. A worker that
creates a task for itself is handed one of these and goes straight to
executing, with no second round trip to claim what it already holds.

With the lease in hand the engine runs a pass. A pass either completes, and the
task is fulfilled with the settlement, or it blocks on promises this process
cannot settle, and the task suspends against them — one `promise.register_callback`
per awaited promise, so settling any one of them dispatches the task again.

The 300 on suspend is why this is a loop and not a straight line. Between a
pass deciding it is blocked and the server hearing about it, one of the awaited
promises can settle. The server answers *do not suspend, you are not blocked
any more* and hands back a fresh branch. The lease was never given up, so the
engine executes again on the spot rather than waiting to be re-dispatched.

`process` returns an `Outcome` rather than throwing at the protocol's normal
answers:

- **Completed** — the computation settled and the task was fulfilled.
- **Suspended** — the task now waits on the promises it named.
- **Dropped** — the engine let the task go without finishing it. The lease could
  not be taken (`unclaimable`), went stale mid-pass (`lease-lost`), or the pass
  blocked on an empty set (`blocked-on-nothing`), in which case the lease is
  dropped rather than released — a released task is re-dispatched at once, which
  would spin.

The failure channel is for the rest: `TransportError` (the round trip did not
complete), `ServerError` (a status the caller cannot act on), `ProtocolError`
(a well-formed response that contradicts the protocol), and `ExecutionError`
(the pass itself broke). Any failure hands the lease back with `task.release`
rather than making the next worker wait out the ttl.

```ts
import { Effect, Layer } from "effect";
import { Connection, Correlation, Engine, Executor } from "@resonatehq/effect";

const layer = Layer.mergeAll(
  Connection.layerHttp({ url: "http://localhost:8001" }),
  Correlation.layerUuid,
  Executor.layerNotImplemented,
);

const outcome = Engine.process({ pid: "worker-1", ttl: 30_000 })(
  Engine.unclaimed("foo", 1),
).pipe(Effect.provide(layer));
```

## The connection

Everything above the connection is written against one method, so a transport
is a single function and so is a test double.

```ts
Connection.layer((request) => Effect.succeed(myResponse));
```

`send` is total over the protocol: every status it defines, 300 and the error
statuses included, comes back as a response. The failure channel is reserved
for round trips that did not complete, which is the one case where the caller
cannot know what the server did with the request.

The HTTP transport posts the request envelope and reads the response envelope.
The server mirrors `head.status` onto the HTTP status and always returns the
envelope, so the HTTP status is not consulted — a body that parses as a
well-formed response to *this* request is the response, whatever status carried
it.

## Requests

Requests are built, not hand-rolled, so the head is right every time:

```ts
yield* Requests.taskAcquire({ id: "foo", version: 1, pid: "worker-1", ttl: 30_000 });
```

Nested requests — the `action` of a `task.fulfill`, the `actions` of a
`task.suspend` — are themselves full requests with their own heads, and are
built with the same constructors.

`head.corrId` comes from the `Correlation` service. `head.version` is this
SDK's protocol version. `head.auth` is filled by the connection, which holds
the credential.

## Not yet here

- **The executor.** `Executor.executeUntilBlocked` is an interface and a
  placeholder layer. Running a computation — an Effect-native durable
  `ctx.run`, replay against the preloaded branch, child promise creation — is
  the next layer in.
- **Heartbeating.** A lease has a ttl and nothing extends it yet. `task.heartbeat`
  covers every task a pid holds at once, so it belongs to a process-wide
  component beside the engine rather than inside `process`.
- **Dispatch.** Something has to receive `execute` messages and hand them to the
  engine. `Engine.fromExecuteMsg` is the adapter; the poller/listener is not
  written.
- **Codecs.** The engine moves protocol `Value`s and does not look inside them.
  Encoding and decoding user values belongs with the executor.

## Development

```shell
npm install
npm run build        # tsc -> dist/
npm run type-check
npm test             # vitest
npm run check        # biome
```

Requires Node >= 22.

`src/protocol.ts` is vendored, not authored: resync it from
`resonate-protocol/spec/types-raw.ts` rather than editing it in place.
