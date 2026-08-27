# The Art of a Resonate SDK

An SDK is correct when a computation can be stopped anywhere, resumed anywhere,
and nobody can tell.

Every law below is stated about **one invocation**. None is stated about the
tree. That is the point: the tree is whatever falls out.

---

## The node

**1. Every invocation is a promise.** The promise is the invocation's memory, and
its only memory.

**2. Nothing an attempt holds survives it.** An attempt may be cut short at any
moment. Whatever is worth keeping is recorded before it ends.

**3. An attempt reads from the record, never from a previous attempt.** There is
no previous attempt. There is only what was written down.

**4. The path may depend only on recorded values.** A clock, a random number, a
mutable global: read it through a durable call or do not read it. Anything else
makes the same code a different computation on the next attempt.

---

## The descent

*Down the tree, an attempt creates. The descent assigns identity.*

**5. A child's id is its parent's id and the next segment.** Nothing else. The id
is the path, and the path is the id.

**6. Segments are handed out in execution order**, one per durable call, at the
moment the call is made.

**7. Create *n* does not begin until create *n−1* has returned.** Identity is
assigned by creation, so creation must be ordered. Concurrency below a node is
free; issuing the creates is not.

**8. Ids depend on the path, never on the clock.** Two attempts down the same
path mint the same ids in the same order. A replay is faster than a first run —
if speed can change an id, the id is wrong.

**9. Creating is idempotent.** Creating a promise that exists returns the
existing one. That is the whole of replay; there is no other mechanism.

**10. A create that comes back settled is an answer.** Take the value. Do not run
the function.

**11. Creating never blocks.** Making a child is not waiting for one.

---

## The ascent

*Up the tree, an attempt awaits. The ascent assigns value.*

**12. Awaiting blocks.** Awaiting a promise this process cannot settle ends the
attempt. Nothing else ends it.

**13. Blocking travels up, never down.** A node awaiting a blocked child is
itself blocked, and so is its parent, to the root.

**14. An attempt ends at a node boundary.** Never in the middle of one.

**15. Report every promise you are blocked on, not the first.** One wake should
resume everything that can proceed. Reporting one costs a round trip per
dependency.

**16. If it has already settled, do not block.** Continue where you are, on the
lease you already hold.

**17. A failure is a value.** A function that failed has *finished*. Its promise
records the failure and the parent sees it as a result, not as a broken attempt.

---

## What a node owes its children

**18. A node settles only when everything it created has settled or is
accounted for.** Including what it created and never awaited. Creating a child is
the commitment; awaiting it is only how you read the answer.

**19. A node settles only when everything it started has finished.** Work handed
to a background task is still this node's work.

**20. Detachment is the only way to leave a child behind**, and a detached child
is re-rooted so that nothing above it is waiting.

**21. Cancellation, not abandonment.** To stop waiting for a child, settle it.
A promise nobody will ever await and nobody will ever settle is a leak.

---

## What a node owes the attempt

**22. Work that is not blocked runs to completion.** An attempt stops at edges,
not wherever it happened to be.

**23. Work completed is recorded before the attempt ends.** The next attempt
replays it. It does not redo it.

**24. Only the lease holder may record.** A write from a stale lease is refused,
not merged. Two workers may believe they own a computation; only one may write.

**25. Losing the lease loses the right to record, not what is already recorded.**

---

## The induction

**Base.** A node with no durable calls runs and settles. It obeys every law
above, trivially.

**Step.** A node whose children obey the laws obeys them itself:

- its children's ids extend its own, so the id space stays a tree;
- it is blocked exactly when a child is, so blocking is the union below it;
- it settles only after they have, so a settled node has a settled subtree.

**Therefore** the whole tree obeys, and the root's promise is the computation's
answer.

This is why there is no law about the tree. Write the laws once, for one node,
and hold every node to them. The rest is turtles.

---

## The two failures

Everything above exists to prevent exactly two things.

**Answering twice.** The same call gets two ids, or two calls get one id, and a
computation returns something that never happened. Guarded by the descent: ids
from the path, creates in order, idempotent creation.

**Answering never.** A node settles over a child still outstanding, or an attempt
throws away work it will have to redo, and progress stops. Guarded by the ascent:
blocking travels up, nodes wait for what they made, unblocked work finishes.

An SDK that prevents both is finished. One that prevents neither is a job queue.
