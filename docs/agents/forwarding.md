# Forwarding events

Code: `container.go` (`runEvent`, `routingTransition`, `forward`, `processRemote`, `applyChangedState`, `mergeChangedFields`), `node_transitions.go` (`Node.forward`, `coverTransitions`, `sendForward`), `node_stream.go`, `node_grpc.go` (`ForwardEvent`, `forwardOtherEntity`, `changedState`).

## Rules [U]
- The node receiving a forwarded event decides which of its transitions run.
- Priority order kept: the sender walks the event's transitions, collecting matching remote ones; before the next local transition they are routed together (`route()` in `processEvent`, sets `evt.remoteMatches`), so with wait that transition sees the result.
- Only nodes owning a matched transition receive the event (no delegating unmatched ones).
- The save func doesn't run on the sender for forwarded events.

## Sender [A unless marked]
- Routing goes through the routing transition of the event name (`routingTransition`, key `routingPrefix + name`), reusing queues/timers. If it batches (count > 1) the event joins the batch once, after the loop. Batch config = remote transition of that name with the largest count, then the longest period (only use of synced `Batching`). Routing transitions cached per container per published transitions (`routing`, `routingFor`).
- `Node.forward`: set cover over matched remote transitions (node owning most first, random ties). Requests to picked nodes run in parallel (single one inline). `exclude` = all func transitions of the event the sender knows minus the ones assigned to that request. `Unavailable` → re-cover with other owners; uncovered → errors.
- `WithWaitRemote()`: sender blocks, applies changed bits per property (`applyChangedState`: `mergeState` + `ApplyStateToEntity`) and merges returned entities (`mergeChangedFields`: fields changed against the sent entity, nested recursively), in response order (later wins), then re-evaluates. One result is merged into the sent entity in place (the builder's, stable under `c.mu`), several into a copy. Without wait (hand-over): see "Durable hand-overs" below.
- Errors of the receiver's transitions: `codes.Aborted` (wait) or logged on the receiver.
- Waited `Emit` [U: Q22 c, agent's choice on the user's "do what you think is best", 2026-10-25]: the emitted event carries an `emitCall` while `Emit` runs; `Container.forward` appends forward errors to it (else `sendError`) and passes every reply to `WithReply`'s func, so `Emit` returns the errors. A batched event forwarded after `Emit` returned reports to the error func as before. Errors of local transitions of the emitted event still go to the error func.
- Replies: receivers of waited executions (`remoteExecution.waited`: lent same-entity, other entity, target calls) keep `c.Reply` values; `addReply` puts them into `ForwardEventResponse.replies` (`Any`, protobuf only, else `Aborted`), one per container; `sendForward` decodes them into `forwardResult.replies`. A result with only replies isn't merged.

## Durable hand-overs [U: end-to-end ack (option C), 2026-10-24; A: details]
- Protocol: two replies per hand-over call: accept (code OK), then `ForwardReply.executed` (same call id) once the work ran, also if its transitions failed (those errors stay on the receiver). Error code instead = refused or failed back. A done id is answered with `executed` in the first reply.
- Sender keeps the call in the stream's `handOvers` (func: nil = ran, else failure) until `executed`; the accept changes nothing. Failure signals, all → `handOverFailed`: error reply; stream broke (`fail`); receiver removed (`closeForwardStream`); receiver's lease ended (`watchLease`, one goroutine per stream, waits on `lockTable.expiry`, `failHandOvers`; the stream stays open, waited calls don't need the lease). No timeouts otherwise: long lock waits are no failure. `receiverLeaseEnded` ignores the receiver's lease while this node's own lease ended (stale view, this node cut off); unknown nodes have none. `sendHandOver` refuses a node whose lease ended (`Unavailable`, re-covered at once).
- `handOverFailed`: `Unavailable` → `forwardTo` again on `n.ctx` for that node's transitions, skipping it (`handOverRetry`, ≤ `maxHandOverAttempts` 3); other errors and the last one → `handOverErrF` → `Container.lateError` (error func while open, logged once closed). Not once the node stopped (`raftStopC`): its outstanding hand-overs stay with the receivers, nothing reported.
- Ids [A]: `ForwardEventRequest.handover_id` = `<node>-<seq>/<sorted transitions>`; seq (`streams.handOverSeq`) starts at `UnixMicro` (unique over restarts); the base stays over attempts, so the same work to the same node has the same id.
- Limit [U: blocks; A: name/default]: `WithMaxPendingHandOvers` (default 10000) = `streams.slots` (buffered channel). `sendHandOver` takes a slot before sending (`takeHandOverSlot`, waits up to the container's ctx, error → container error path), freed when the call ends (ran, failed, not sent). Gotcha: a container handing over ≥ limit work for its own id while open waits forever (that work waits for its lock). `pendingHandOvers()` for specs.
- Semantics [U]: at-least-once while the sender lives. Resent to another owner it may run twice (receiver alive, stream broke, reply lost) and may overtake later work of the container. A sender crash loses its outstanding hand-overs.
- Receiver: see `locks.md` (Hand-over).

## Transport [A]
- `sendForward` uses one `ForwardEvents` stream per peer (`forwardStreamTo`, opened lazily on `n.ctx`, kept until broken). One goroutine sends queued calls + cancellations as one `ForwardCalls`; one dispatches `ForwardReplies` by id (`pending` channels of waited calls, `handOvers` funcs of kept hand-overs). Receiver (`ForwardEvents`): waited calls run in their own goroutine, hand-overs are queued inline by the receiving goroutine (`forwardEvent` with a reply func for the second reply), so they keep the stream's order. A call whose ctx ends sends its id in `cancel`. Broken stream → pending calls and kept hand-overs fail `Unavailable` (re-covered); next call reopens. Closed on removal (`closeForwardStream`) and stop.
- Receiver `ForwardEvents` runs each waited call in its own goroutine through `ForwardEvent` (unary rpc stays, answers hand-overs with the accept only) and batches replies (`sendReplies`); running calls are cancelled when the stream ends. `replyC` is never closed (second replies may come later, dropped once the stream ctx ended); `endC` stops `sendReplies`.

## Receiver (same entity)
- `ForwardEvent` (= `forwardEvent` without reply func): own-name checks (TLS), then wait → `verifyLending` + borrowed container (see `locks.md`); no wait → `acceptHandOver` → hand-over queue (`locks.md`).
- `processRemote`: each payload becomes a sub-event (`parent` = forwarded event) through its own func transitions (`isOwnFuncTransition`) minus `exclude`, with its own conditions and batching. Close-after right after the pass (partial batches run at close). `releaseEvent` of the forwarded event sets `r.result`, `r.err` (from `r.errs`, `actionFailed`) before done.
- `ForwardEvent` gets a copy of the sent entity. Response: `changed_state` (bits of changed properties, `changedBits`) if applying them to the sent entity reproduces the result exactly (`changedState`, applies to the decoded sent entity in place), else the entity.

## Holder routing [U: waited calls and target calls only, not hand-overs, 2026-10-25; A: details]
- Code: `node_holder.go`. A waited call on the target's own lock (call of another entity, target call) runs on the node holding the target's lock: `redirectToHolder` sends the request on once (`redirected`, `sender_node_id` = this node) when the lock is held by another node that owns every own func transition of the event this node would run (`ownsTransitionsOf`, minus exclude). Free lock, own lock, other owners or `Unavailable` holder: runs here as before.
- Holder lookup `lockHolder`: members read the lock table; clients answer locks they hold themselves locally, else watch the lock (`watchLock`, the member answers a new watch with the lock) and wait for the change, at most `holderLookupTimeout` 1s. Cost on clients: one round trip to the member per call they don't hold.
- Target calls: `callTarget` builds the request (`targetRequest`: `target_id`, payload must be protobuf) and redirects before running locally; the reply comes back in `replies`. Receiver: `forwardTarget` (wait only) runs it like a call of another entity (`callOtherEntity`).
- Calls of another entity: `forwardOtherEntity` redirects per picked container (`payloadsOf` = its payloads and traces); `Aborted` errors join the others, replies are re-added.
- Metric `asm_forwards_redirected_total`.

## Receiver of another entity [A]
- Requests carry `sender_entity`. `forwardOtherEntity` picks each payload's container with `WithContainerIdFunc` (required, else `FailedPrecondition`) and groups payloads per container.
- Containers run on their own locks, also with wait: nothing lent, no lend chain. With wait (`callOtherEntity` → `callContainer` with the request's wait path) containers run one after another; response carries no entity; errors/refused calls → `Aborted`. Without wait: one hand-over, an execution per target id; `executed` once all ran.
- Runs its own func transitions and the state transitions of its entity (`receives`), minus `exclude`.
