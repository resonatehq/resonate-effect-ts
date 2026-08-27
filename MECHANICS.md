# Mechanics

The moving parts, stated flat. Rationale lives in [README](README.md); this is
the list you check an implementation against.

## Task loop

- The engine drives one task. `claim → execute → fulfill or suspend`.
- A task arrives **unclaimed** (an id and a version, from an execute message) or
  **claimed** (lease, promise and branch already in hand, from acquire/create).
- `task.suspend` answering **300** means the block resolved underneath us. Same
  lease, fresh preload, execute again. That is why it is a loop.
- Outcomes: **Completed**, **Suspended**, **Dropped**. Dropped is not a failure —
  the lease was unclaimable, went stale, or the pass blocked on nothing.
- Any failure releases the lease rather than making the next worker wait out the ttl.

## Pass

- A pass is one attempt. Nothing in this process survives it.
- Settled root → replay it. Timer root not yet due → blocked on nothing, drop the lease.
- The pass forks the body. It must: the sentinel tears down every frame above the
  edge, and a shared fiber would go down with it.

## Operations

- **Local** (`ctx.run`): create the promise, run the function here, settle it.
- **Remote** (`ctx.rpc`): create a promise with a target. Someone else runs it.
- Also `sleep` (a timer promise), `promise()` (latent, settled out of band),
  `detached` (own origin, never awaited), `race`.
- **Create never blocks. Await blocks.** Two distinct durable events.
- Create is idempotent. That *is* replay: a create that comes back settled is a
  dedup — return the value, run nothing.

## The edge

- An edge is a promise this process cannot settle, awaited while pending.
- Reaching one throws a sentinel: `Effect.die(new Edge({ id }))`. A tagged value,
  not an Error — no stack capture. Matched by `_tag`, never `instanceof`.
- It unwinds like an exception. Through user frames untouched, inspected at every
  Resonate frame, caught once at the root.
- Opaque to every combinator over `E` — `catchAll` `catchTag` `either` `option`
  `match` `merge` `orElse` `retry` `timeout` `Fiber.join`.
- Visible to every combinator over a `Cause` or `Exit` — `catchAllCause`
  `catchAllDefect` `sandbox` `Effect.exit` `Fiber.await`. Unavoidable.
- **Nothing rests on that opacity.** The guards carry correctness.

## What decides

- The **edge set** decides, not the sentinel. Read last, outranks the body.
- Body returned a value with a remote child outstanding → **suspend**.
- One `Set` per frame, merged into the parent when a frame ends, on any exit.
  Per frame because a frame must answer "did *my* subtree block" — for its settle
  guard, and for `race`, which must tell its branches from a sibling's.
- The root frame's set is the pass's answer. Sorted, so a run reports the same
  thing twice.

## Structured concurrency

- A frame does not finish while work it spawned is still running.
- `ctx.run` awaits the body; the body's combinators join what they fork. An
  `Effect.fork` the body never joins is attached to the fiber, not to any
  combinator — nothing else waits for it.
- So each frame flushes: after the body, await the fibers *it* added. Not ones
  the caller already had running, or a frame could wait on a sibling waiting on it.
- The flush runs inside the frame, before its edges merge upward — a fork that
  reaches its edge during the flush records it on that frame.
- Without it a frame settles over a child still in flight, and a remote one is
  then live with nobody awaiting it. This is `flushLocalWork`.

## Three guards

1. **Atomic claim and create.** Minting an id and creating its promise are one
   uninterruptible region. Split them and an id names a promise that does not
   exist — `task.suspend` answers 422.
2. **Settle guard.** A frame whose subtree reached an edge does not settle,
   whatever its body returned. Stops a caught block writing a substituted value.
3. **Divergence check.** A durable write on a path that caught an edge is
   refused. Per *path* (a `FiberRef`, set on the way to an edge), not per frame —
   a sibling blocking is not this branch's problem. Runs **after** minting:
   refusing must consume the same sequence slot the real run would.

## Interruptibility

- **The body runs uninterruptible as a whole.** A pass ends when its work is done
  or blocked, never mid-step.
- When a branch reaches its edge, Effect interrupts its siblings. A sibling defers
  that and runs on: to its own edge (so one pass finds every frontier), or to
  completion if it was never blocked (so no work is abandoned).
- Between frames counts. A catch handler about to start its recovery has no frame
  to shelter in — cut it off and you lose the work *and* shift every id behind it.
- Cost: genuine interruption waits for the body to finish or block. A fiber that
  never started is not covered — nothing was created, so nothing is lost.

## Creation order

- Creates within a frame are serialized. Each waits for the server to answer the
  one before it.
- Slots in that chain are claimed **synchronously**, where the call appears in the
  source. `Effect.all([ctx.rpc(a), ctx.rpc(b)])` orders `a` before `b` by
  construction, not by which fiber the scheduler starts.
- A slot claimed by an operation that never runs is released when the frame ends.

## Ids

- Dewey under the lineage origin: `foo` → `foo:1` → `foo:1.2`. Segments from 1.
- A counter per frame, taken at construction.
- Stable for any code whose shape does not depend on timing — every concurrent
  combinator included, since branches are built before any of them runs.
- **Sharp edge**: a call built inside a timing-dependent callback (a `catchAll`
  handler, a `forEach` body) while a concurrent branch shares the counter. Replay
  is faster than the first run, callbacks fire in a different order, two branches
  swap ids and read each other's promises. Pin those with `options({ id })`.
- Every dedup is checked: the encoded param that came back must match the call
  being made. An id taken twice stops the pass with a diagnostic. It does not make
  such a workflow correct — only the explicit id does.

## Concurrency

- Effect's own. `Effect.all`, `Effect.forEach`, `Effect.fork` + `Fiber.join`.
- No `begin` variant. Forking a `ctx.rpc` and joining it later already is one.
- Fire-and-forget `fork` is the one shape durable execution cannot express: the
  parent would settle over an outstanding promise. **Join your children.**

## Race

- `Effect.race` is that same shape, so racing is its own operation.
- `ctx.race` has a durable promise. **The winner is written to it.** A later pass
  replays the decision; the branches are never built.
- Losers are cancelled (`rejected_canceled`). Abandoning them would leave them in
  the edge set and the race would wait for what it just beat.
- Every branch runs. First **success in declaration order** wins — not the
  fastest. Across passes a branch either replays from a settled promise or blocks,
  so what decides a race is which promises are settled when the pass runs.
- No winner and something outstanding → nothing recorded, suspend on all of them.

## Store

- Every write goes through **`task.fence`** with the task id and version. A lapsed
  lease cannot settle anything.
- Reads come from a cache seeded by the claim's **preload** — every promise on the
  root's branch, which is exactly this computation's local children. Replay is
  local.
- The cache is only an optimization. Create and settle are idempotent, so a miss
  is a round trip to the same answer.

## Retries

- A fresh frame per attempt, so children replay onto the same ids and dedup. An
  in-process retry is a replay that skipped the wire.
- Bounded by the promise's own `timeoutAt`.
- An edge is never retried. `Effect.retry` only retries typed failures, and the
  sentinel is a defect.
