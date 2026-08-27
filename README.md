# Resonate SDK for Effect

A Resonate SDK built on [Effect](https://effect.website), speaking the
[Resonate protocol](https://github.com/resonatehq/resonate-protocol)
(version `2026-04-01`) to the Resonate server.

Built outside in: the protocol as data, the server as one method, the engine
that runs the task loop, and the pass that runs the computation.

## The pieces

| Module | What it is |
|--------|------------|
| `src/protocol.ts` | Every request, response, record, and message the protocol defines. Vendored verbatim from `resonate-protocol/spec/types-raw.ts`. |
| `src/requests.ts` | One constructor per request kind. Fills the head, leaves `data` to the protocol. |
| `src/connection.ts` | The server, as one method: `send(request) -> Effect<response>`. |
| `src/engine.ts` | The task loop: claim, execute, fulfill or suspend, continue. |
| `src/executor.ts` | The seam between the two: one pass in, completed or suspended out. |
| `src/pass.ts` | One pass of a computation — decode, run, decide. |
| `src/context.ts` | The durable operations, and the three guards that keep replay honest. |
| `src/edge.ts` | The sentinel that ends a pass. |
| `src/store.ts` | The promise store as one pass sees it: fenced writes, preloaded reads. |
| `src/registry.ts` | Names back into code. |
| `src/codec.ts` | Values across the wire — JSON in base64, matching the TypeScript SDK. |
| `src/ids.ts` | The Dewey id format the server validates against. |
| `src/retries.ts` | Retry policies, as `Schedule`s. |
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

## The pass

A workflow is an ordinary Effect. `ctx.run` calls a function here and records
its result; `ctx.rpc` calls one somewhere else and awaits it.

```ts
const charge = Registry.register("charge", (ctx: Context, order: Order) =>
  Effect.gen(function* () {
    const total = yield* ctx.run(price, order)          // runs here, settled durably
    const receipt = yield* ctx.rpc(payments, total)     // runs elsewhere — the edge
    return receipt
  }),
)
```

Creating a promise never blocks; **awaiting** one this process cannot settle
does. That is the whole of `executeUntilBlocked`:

> A sentinel defect is thrown at the blocking await and unwinds like an
> exception, passing through user frames untouched but inspected at every
> Resonate frame it crosses — a frame whose subtree reached an edge refuses to
> settle and re-raises — until it is caught once, at the root. What it does not
> do is decide anything. The outcome comes from the **edge set**, which every
> blocking op writes before it throws, which keeps filling after the unwind has
> begun because sibling fibers run on to their own edges, and which outranks
> whatever the body returned.

That last clause matters: a body that returns a value while a remote child is
still outstanding suspends anyway, exactly as in the generator engine.

**Concurrency is Effect's.** `Effect.all`, `Effect.forEach`, `Effect.fork` +
`Fiber.join`. There is no `begin` variant, because forking a `ctx.rpc` and
joining it later already is one. Fire-and-forget `fork` with no join is the one
shape durable execution cannot express — the parent would settle over an
outstanding promise — so join your children.

`Effect.race` is the same shape, so racing gets its own operation:

```ts
yield* ctx.race([
  (c) => c.rpc(payments, order),
  (c) => Effect.as(c.sleep(30_000), "timed out"),
])
```

`ctx.race` has a durable promise of its own, and the winner is written to it —
so a later pass replays the decision rather than re-running the race. The losers
are cancelled (`rejected_canceled`), which is what lets the pass complete:
abandoning them would leave them in the edge set and the race would wait for
everyone it just beat. Every branch runs, and the first *successful* one in
declaration order wins — not the fastest, because across passes "fastest" means
nothing. A branch either replays from a settled promise or blocks, so what
decides a race is which promises are settled when the pass runs; declaration
order makes that stable, and the winner record makes it permanent.

### Three guards, and why the sentinel is not one of them

The sentinel is opaque to every combinator that operates on `E` — `catchAll`,
`catchTag`, `either`, `option`, `match`, `merge`, `orElse`, `retry`, `timeout`
— and visible to the few that operate on a `Cause` or an `Exit`:
`catchAllCause`, `catchAllDefect`, `sandbox`, `Effect.exit`, `Fiber.await`.
Those cannot be hidden from; that is what they are for. So nothing rests on the
opacity. Correctness comes from three checks the SDK owns:

1. **Atomic claim and create.** Minting an id and creating its promise happen in
   one uninterruptible region, so a teardown elsewhere can never leave an id
   naming a promise that does not exist — which `task.suspend` would answer 422.
2. **The settle guard.** A frame whose subtree reached an edge does not record a
   result, whatever its body returned. This is what stops a caught block from
   writing a substituted value into a durable promise.
3. **The prologue check.** A durable operation on a path that already reached an
   edge is refused *before* an id is minted, so a swallowed block cannot fork
   the id sequence and have the same id name a different call next pass.

The blast radius of catching the sentinel is therefore some pure local code
running and then stopping.

### Two properties worth knowing

**Creates within a frame are serialized.** Each waits for the server to answer
the one before it, and slots in that chain are claimed synchronously where the
call appears in the source. So `Effect.all([ctx.rpc(a), ctx.rpc(b)])` orders `a`
before `b` by construction, not by whichever fiber the scheduler happens to
start.

**Durable frames run uninterruptible.** When one branch of a fan-out reaches its
edge and Effect interrupts its siblings, a sibling defers that interruption,
runs on to *its* edge, records it, and stops there — so one pass discovers every
frontier instead of one per pass. The cost is that genuine interruption
(shutdown, a lost lease) waits for the body to reach its next edge or finish,
and that latency scales with the depth of the call tree. A sibling whose fiber
has not started yet is not protected by this and is simply found next pass.

## Not yet here

- **Heartbeating.** A lease has a ttl and nothing extends it yet. `task.heartbeat`
  covers every task a pid holds at once, so it belongs to a process-wide
  component beside the engine rather than inside `process`.
- **Dispatch.** Something has to receive `execute` messages and hand them to the
  engine. `Engine.fromExecuteMsg` is the adapter; the poller/listener is not
  written.
- **A `Resonate` front door.** Registering workflows, `run`/`rpc` from outside a
  workflow, schedules. Everything under it exists; nothing assembles it yet.
- **Schema-typed registration.** The codec is JSON today, so a typed `E` round
  trips as data rather than as itself. Deriving the codec from an
  `effect/Schema` per registered function is the natural next layer.
- **Cancelling deeper than the edge.** `ctx.race` cancels the promises its
  losing branches were blocked on. A losing branch that blocked *below* a local
  child leaves that intermediate promise pending until it times out — inert,
  since nothing is running it, but untidy.

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
