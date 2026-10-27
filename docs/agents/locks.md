# Container locks

Code: `node_locks.go` (lock table + node side), `node_forward.go` (hand-over queue, accepted hand-over ids). `WithSkipRaft`: no locks.

## Basics
- An id is open on one node only. `StartContainer` acquires the lock through the log and waits while another node holds it (bounded by ctx). One goroutine per node acquires a given id (`starting` map).
- Borrowed starts skip `starting` (a hand-over of the same id may wait for exactly the lent lock → deadlock otherwise). One container per id: a borrowed start loads an open one; a start that just got the lock waits until a borrowed container of the previous holder closed (`openContainer`).
- Locks belong to nodes. A heartbeat every renew interval renews all of a node's locks; every other entry of the node too (`touchLocked`). Expired = holder's last entry ts + holder's lease < acquiring entry's ts (deterministic). `WithLeaseTimeout`, default 10s/3s.
- Losing a lock (expired, taken by another node) closes the local container (`lockChanged`; not for own releases/cancels).
- Tokens `nodeId-seq`; `lockSeq` starts at `UnixMicro` (unique over restarts). A release only applies with the current token.
- The ctx of `StartContainer` only bounds waiting (ready, lock, slot). Containers run on `n.ctx` (cancelled after stop/failure + `Shutdown`).

## Fair queue [U]
- `containerLock.queue`: waiting nodes with tokens, log order. `applyAcquire`: free → granted; holder with empty queue → re-granted with the new token; else queued (or the node's token updated). Release/expiry hand the lock to the first waiter alive at the entry's ts (`grantNextLocked`).
- Re-propose an acquire only if not queued and `needsPropose` (propose failed, term changed, 10s unapplied), or the holder's lease ended (at most every `proposeRetryInterval`).
- Giving up: `acquireLock` proposes `LOCK_CANCEL` (`cancelLock`, until out of the queue). A cancel of a token holding the lock passes it on. `lockChanged` releases a lock granted to a token not in `lockTokens`. A token stays in `lockTokens` from the attempt until `releaseLock` (`dropLockToken`): the apply goroutine wakes the acquirer before `lockChanged` runs.
- Waking: a queued waiter sleeps until its lock changes or the holder's lease may end (`waitTimeout`), no polling. `lockTable.get(id)` channel closed on the next change of that lock (`changedLocked` in `storeLocked`); liveness has its own lazy channel (`hasLiveness`); `changedAllLocked` on snapshot restore. Expiry isn't a change: queued attempts wait on `lockTable.expiry(holder)` (closed by `watchExpiry`); attempts not queued yet use a timer.
- Removal (`lockTable.removeNode`) drops the node from all queues and hands its locks on without liveness check.

## Retention [U: keep until asked]
- On close `removeF` → `endLock`: keep the lock (`retained[id] = token`, token stays in `lockTokens`) unless somebody is queued or the node is leaving, else release (retried until committed). Registers first, checks the queue after; `lockChanged` releases a kept lock once a waiter queues (`unretain`).
- `createContainer` takes a kept lock (`reuseRetained`: same token, empty queue) without a log entry. A kept lock lost by expiry/removal is forgotten.
- Metrics `asm_lock_reuses_total`, `asm_locks_retained`.

## Lending [U]
- `WithWaitRemote()` forwards borrow the lock (`ForwardEventRequest.wait`): sender blocks; receiver `verifyLending` → `startContainer(borrowed=true)`, runs on the sent entity, returns it. Init/save/close funcs run in borrowed containers too (the receiver's may differ).
- Lend chain: `lend_chain` (first = holder in the lock table, last = sender = `sender_node_id`). A container on a lent lock (`Container.lent`, set in `processRemote`, cleared when the forwarded events are released) lends it on for waited emits. `verifyLending`: one lender → `verifyHolder` (lock table; clients watch the lock via their member); longer → `CheckLending` rpc to the last lender. A chain containing the receiver is refused.
- Only waited events lend. Fire-and-forget is always handed over, also from a lent lock.

## No deadlocks of waited calls [U: goal; details A]
- `ForwardEventRequest.wait_path` = lock tokens of the containers waiting for the answer. `Container.waitPathOf(events)` = union of the events' paths (`remoteExecution.waitPath`; emitted: calling transition via `c.callEvents`) + `c.lockToken` (empty on a lent lock).
- Other-entity receivers run calls with `callContainer`: an accepting container of the id with its own lock is joined through the log (`LOCK_ACQUIRE` with `join` = its token), else the lock is acquired with the path. Locks record running calls (`containerLock.calls`); `LOCK_DONE` (async, `lockDone`) ends one; release/re-grant/expiry drop all calls.
- Edges: every token on the path of a queued waiter or call waits for the holder token. `applyAcquire` with a path refuses when the holder waits for a token on the path (`waitsForLocked`, BFS over index `lt.waitedFor`, maintained in `putLocked`). Refused = lock unchanged, `refusedTokens`, `ErrWouldDeadlock`, rpc `Aborted`. Acquires without a path (StartContainer, hand-over, Event rpc) are never refused. Waited target calls (`WithTarget`, `containers.md`) use the same path as calls of another entity.
- Cap [U: 64; A: constant]: `maxWaitPath` 64 tokens. `applyAcquire` refuses a longer path first (deterministic, same refusal as above); `ForwardEvent` rejects it with `InvalidArgument`, `acquireLock` before proposing (`checkWaitPath`). A constant, not an option: members must decide the same.
- Waited starts wait nowhere else: borrowed starts and calls take a slot only if free (`Container.slot`), calls skip `starting`, a joined closing container is waited for while the call is recorded. A root re-acquire of the holder node only re-grants when no call runs. A joined call finding its container closing waits for `doneC` and calls again.

## Hand-over (no wait) [U: semantics; A: async details]
- Receiver accepts at once (`acceptHandOver`), queues per id (`enqueueForwardedLocked`; one drain per id keeps order), runs in its own container on `n.ctx` once it has the lock: init func loads the entity, its matching own func transitions run, save on change, close. Entity not sent. Transition errors only logged. Once the execution's done channel fired (`handOverRan`; other entity: all executions of the hand-over) the senders get `executed`. Durability on the sender: `forwarding.md` (Durable hand-overs).
- Ids [A]: `forwarded.handOvers` by `handover_id` (`acceptedHandOver`: executions left, reply funcs of every call which sent it). Known + done → `executed` at once; known + queued/running → reply func added, no second run. Done ids kept `doneHandOverTTL` 1 min, at most `maxDoneHandOvers` 65536 (FIFO `forwarded.done`, pruned on every finish). Failed back → id forgotten (sent again it runs). No id (unary rpc) → not tracked, dropped with an error log if it can't run.
- Giving up (`giveUpForwarded` → `failHandOver`): the hand-over goes back to its senders with the error (no gRPC code → `Unavailable`, so they re-cover): node leaving/stopped (drain), container didn't open (resume error), `sendToContainer` failed.
- Leave [U]: `acceptHandOver` refuses new hand-overs with `Unavailable` once `leaving` (a known done id still gets `executed`). `Leave` calls `dropPending(Unavailable, cancel=true)`: pending acquires are cancelled in the lock table (`cancelLock`) and resume with the error → drains give the head item and then the whole queue back. Running executions finish while the containers close.
- No goroutine waits: without a container of the id, `drainForwarded` calls `acquireAsync` and ends. It takes the `starting` entry, reuses a retained lock or registers a `pendingAcquire`. `runPendingAcquires` (one per node) proposes new ones together on a kick; every `proposeRetryInterval` re-proposes per `needsPropose` or expired holder. `lockChanged` sees the grant (`takePending`) → `openPending` (open, end start, resume drain; open failure gives the head item up). On stop: `dropPending(ErrNodeStopped, false)`. With a container (open/closing) or a running start the drain sends synchronously. Gauge `asm_handover_lock_waits`.

## Restart
- After a WAL restart `endPreviousLife` releases the node's old locks, cancels its old queue entries, ends its old calls (tokens not in `lockTokens`). See `cluster.md`.

## Gotcha
- `StartContainer` inside a transition to wait for another container can deadlock (the lock table doesn't see it). Use `Emit` + `WithWaitRemote()`.
