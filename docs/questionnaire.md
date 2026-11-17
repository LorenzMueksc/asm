# Questionnaire

Open decisions for the user. Answer inline after **Answer:** (a letter, or your own words). Agents read this file
before working on the topics; answered questions move to the topic docs as [U] and are deleted here.

Closed: (Q1-Q12 answered 2026-10-24, Q13-Q21 answered or dropped 2026-10-24, Q22 decided by the agent
2026-10-25, Q23-Q27 locality answered 2026-10-31: region paths, `WithRegion` + raft log (certificate must match),
nearness first, next nearest on failure, clients with their own region; see the plan log).

Rounds (A1 deferred events, A2 round barrier, A3 round batches, A5 spawn and close; branch `work/rounds`,
2026-11-12): QR1-QR11 and QR13-QR17 answered by the user 2026-11-12 (answers below, implemented, marked [U] in
`docs/agents/rounds.md`). Open: QR12, decided by the agent to keep working and implemented ([A]). Ids `QR*` instead
of Q28+ so they don't collide with the questions of the `work/rw-modes` branch (merged into this tree 2026-11-12,
Q28-Q33 below).

## QR1 Sequence numbers of deferred events (A1)
a) Position in the sender's outbox since the last barrier (0, 1, ...), reset by every barrier: no state outlives a
   round, snapshots, rollback (A10) and replay (A9) need nothing extra; (round, sender, seq) identifies a message.
b) One counter per container life, never reset: (sender, seq) is unique over the whole run, but the counter is hidden
   state outside the entity, lost by snapshots and rollbacks unless stored with them.
Recommendation: a (implemented).
**Answer:** a, keep as implemented [U 2026-11-12]

## QR2 Rules of a deferred Emit (A1)
a) Target required; the sender itself is allowed (nothing waits, no deadlock); `WithWaitRemote`, `WithReply`,
   `WithDoneChannel` make `Emit` fail; nothing is run on the sender.
b) As a, but deferring to the sender is refused like a live `WithTarget` to itself.
c) Without a target: defer to the sender itself.
Recommendation: a (implemented).
**Answer:** a, keep as implemented [U 2026-11-12]

## QR3 What Round waits for (A2)
a) The round events it sent (one per target) and everything their transitions run synchronously (nested and waited
   `Emit`s). Hand-overs (`WithTarget` without wait) and batch timers run on their own and may outlive the round.
   No change to the hot event path.
b) Every container of the node idle: a node-wide counter of queued/running events and queued hand-overs, changed on
   every event of every container (also outside of rounds), more edits in container.go/node_forward.go.
Recommendation: a (implemented); rounds should use deferred events, not hand-overs.
**Answer:** a, plus `Round` returns an error (`ErrRoundOverlap`) if a hand-over without wait or a batch timer of a container still runs when it would otherwise return; under rounds those are bugs [U 2026-11-12]

## QR4 Errors and failures of a round (A2)
a) Deliver what can be delivered, run the round, then return the first error: barrier errors first (spawned id open
   already, unknown or closed target: those events are dropped), else the first transition error of the lowest
   container id. Transition errors also reach the error func. Inbox events whose transitions are all read-only (rw
   modes) run inside the round's writer, in order.
b) Validate all targets first and abort the round without delivering anything on a barrier error (outboxes kept).
c) As a, but return all errors joined.
Recommendation: a (implemented); the kernel rolls the month back on any error (A10).
**Answer:** b: every outbox target (open and accepting, or spawned at this barrier) and every spawn id (free, spawned once) is checked before anything is delivered or started; on a failure nothing is delivered, the outboxes and spawns are kept and `Round` returns `ErrUnknownTarget` / `ErrContainerExists`, so the caller rolls back a clean state; transition errors as before, the first by container id. A deferred event to an unknown or closed target is an error, never dropped silently (oeko kernel adapter) [U 2026-11-12]

## QR5 Round batches where no round runs (A3, rw modes)
A container without rounds (built with `NewContainer`, or a reader's view on the rw-modes branch, where read events
bypass the mailbox) would collect batch events nobody runs.
a) Run the transition at once with a one-message batch (`From` "", same `[]Message` type).
b) Drop the event with an error.
c) Refuse `WithBatchOnRound` together with `WithReadOnly` at registration (rw merge), and a.
Recommendation: c after the rw merge, a now (implemented). A `WithBatchOnRound` transition should count as a writer.
**Answer:** c: refuse `WithBatchOnRound` with `WithReadOnly` at registration once the rw branch merges (hook `checkRoundBatch` in `compileTransitionLocked`, merge item in plan.md); a round batch transition counts as a writer; containers without rounds keep the one-message batch [U 2026-11-12]

## QR6 Live events in round batches (A3)
a) A live event (`Event`, calls, hand-overs, `Emit` without `WithDeferred`) joins the next batch with `From` "",
   `Seq` 0, sorted first (stable, arrival order); it makes the container run in the next round even if nothing is
   delivered to it. Batches run after all events of the round, by priority then name. Pending batch events are dropped
   when the container closes. The payload slice is reused (valid during the call).
b) As a, but batches are flushed when the container closes (like `WithBatching`), and the slice is a fresh copy.
c) Live events don't join round batches (the transition runs only for delivered events).
Recommendation: a (implemented).
**Answer:** c: live events never join round batches; a round batch is exactly the round's sorted delivered inbox; live events run only their per-event transitions, a container needing both registers both (`From` "" path and its spec removed) [U 2026-11-12]

## QR7 Rounds on cluster nodes (A2)
a) `Round` returns `ErrNotStandalone` unless `WithSkipRaft`: a target open on another node would look unknown.
b) Rounds over the containers open on this node of a cluster, spawns acquiring locks.
Recommendation: a (implemented).
**Answer:** a, keep as implemented [U 2026-11-12]

## QR8 Spawned containers (A5)
a) Started at the barrier with `openContainer` as a waited start: a slot of the max container count if free, else
   without one (the barrier never blocks); held like a `StartContainer` result (calls don't close it); entity set at
   construction; the init func runs before its first event as usual; `Spawn` only from a transition.
b) As a, but fail with an error when no slot is free.
c) Block the barrier until a slot is free (can hang: the containers holding slots are idle).
Recommendation: a (implemented); size `WithMaxContainerCount` for the simulation.
**Answer:** a, keep as implemented [U 2026-11-12]

## QR9 When CloseAfterRound closes (A5)
a) At the end of the round, before `Round` returns; its outbox and spawns stay on the closed container and the next
   barrier delivers/starts them. Outside of a round: at the end of the next one.
b) The next barrier closes it right after taking its outbox (it is still open between the rounds).
Recommendation: a (implemented): when `Round` returns, the set of containers is final.
**Answer:** a, keep as implemented [U 2026-11-12]

## QR10 Round name for transitions (A2)
a) `asm.RoundOf(ctx)` for transitions run by the round's delivery (and their batches), "" for live events even
   during a round; `node.RoundName()` for the round running now.
b) Only `RoundName()`.
Recommendation: a (implemented).
**Answer:** a, keep as implemented [U 2026-11-12]

## QR11 Sender of a delivered event for per-event transitions (A2)
Per-event transitions only get the payload; only round batches see `Message.From`/`Seq`.
a) Keep it: senders put what receivers need into the payload.
b) Add `asm.MessageOf(ctx) (Message, bool)` for transitions of delivered events.
Recommendation: a for now, b if the kernel needs it.
**Answer:** b: `asm.MessageOf(ctx) (Message, bool)` for transitions of delivered events (From, Seq, Payload), false for live events [U 2026-11-12]

## QR12 Dropping what is queued for the next barrier (rollback of a failed round)
A caller that rolls a failed round back (eg. the oeko kernel adapter: a month fails, its state is restored) must not
let the events and spawns the failed round queued reach the next barrier; with QR4 b a failed check also keeps them
queued.
a) `Node.DropOutboxes(ctx) (int, error)`: waits for a running round (ctx), drops the deferred events and the spawns of
   every container, returns how many; closes (`CloseAfterRound`) stay requested.
b) As a, but only deferred events (spawns stay).
c) `Node.Rollback()` that also restores entities (plan A10): more than a caller with its own snapshot needs.
Recommendation: a (implemented, [A 2026-11-12]).
**Answer:**

## QR13 When spawned containers start (A5)
Today a container spawned in round R starts at the barrier of round R+1, before that round's delivery (QR8): it
doesn't exist when `Round` R returns, so a caller hashing the node's state after each round doesn't see it until the
next round has started it.
a) Start the spawns at the end of the round in which they were requested: after the closes (`CloseAfterRound`), in id
   order, the slot rule of QR8 unchanged (a slot if free, never blocking), checked like today (an open id or a double
   spawn fails the round with `ErrContainerExists`; the check moves to the end of the round, so the round's own work
   ran and only the start is refused). Events deferred to a spawned container are delivered by round R+1, as today.
   When `Round` returns, the set of containers is final, including the new ones.
b) Keep starting them at the next barrier (today).
c) Both: a node option choosing a or b.
Recommendation: a: a consumer that hashes or snapshots the state after each round needs the spawned containers to
exist then; b leaves state that belongs to round R invisible until R+1 runs. Open details for a: a spawn failing at
the end of R (id taken) fails R after its transitions ran, so the caller rolls R back (`DropOutboxes`); the parent's
other deferred events of R stay queued for R+1.
**Answer:** yes (a) [U 2026-11-12]: containers spawned in a round start at the end of that round, after its closes, in id order, before `Round` returns, so they exist (and listings see them) when it returns; they receive messages from the next round on; QR8 slot rule unchanged (a slot if free, else none, never blocking the barrier); the QR4 check of spawn ids stays at the start of the round; `WithRunAll`: a container spawned in a round takes part from the next round on. Built with these details [A 2026-11-12]: spawns queued between rounds (live events) still start at the start of the next round, checked there with the targets (QR4, unchanged); the round's own spawns get the same check at its end, after the closes (an id open then, or spawned twice: none of them starts, they stay queued for the next round or `DropOutboxes`, `Round` returns `ErrContainerExists` after any transition error); a round whose ctx ended starts them too, a failed check then waits for the next round's start check.

## QR14 Readers in rounds (rw merge)
A reader (`WithReadOnly`, run on a view next to the writer) calling `Emit(WithDeferred)`, `Spawn` or
`CloseAfterRound`.
a) All three return `ErrReadOnly` (built): a reader runs concurrently with the writer, so its position in the outbox
   (the sequence numbers) would depend on the scheduling, and a reader changes nothing by definition. Inbox entries
   whose transitions are all read-only still run inside the round's writer (the round event goes through the mailbox).
b) A new named error (`ErrReaderInRound`). Con: one more error for the same rule.
c) Let readers defer into the writer's outbox. Con: breaks the sorted, reproducible delivery.
Recommendation: a (implemented, [A 2026-11-12]).
**Answer:** a, keep as built [U 2026-11-12]

## QR15 `WithRunAll` details
Built: `Round(ctx, name, WithRunAll())` posts the round event to every container open on the node (closing ones are
left out, spawned ones of this barrier included); each runs its events as usual, then every round batch transition
of the node: those with events of the round with them, the others with an empty, non-nil `[]Message`. Without round
batch transitions on the node the option changes nothing. The containers without events get their round events in
the order of the node's map (no sort of 10k ids per round): which container starts first doesn't change what any of
them runs, so the results stay reproducible.
a) An empty batch runs only if the transition's conditions match the entity, evaluated once after the container's
   events of the round (built; a transition without conditions always runs).
b) Empty batches run regardless of conditions.
c) Refuse conditions on round batch transitions run with `WithRunAll`.
Recommendation: a: the same rule as for a delivered event, which runs a round batch only when it matches.
**Answer:** a, keep as built [U 2026-11-12]

## QR16 Strict overlap check (QR3)
Built (user: a hand-over without wait or a batch timer started during a round fails it): `nodeRounds.started` counts
the starts (`runsOn(+1)`) while a round runs (from `acquire` to the end of the closes), reset by every round; `Round`
returns `ErrRoundOverlap` if any started or any still runs at its end ("N ... started during the round, M running
after it"). The count is node-wide: a hand-over a live event (not part of the round) starts while a round runs counts
too.
a) Node-wide (built). Pro: no per-event bookkeeping on the hot path; under rounds live work during a round is a bug
   as well.
b) Only work started by the round's own transitions. Con: needs the round on every hand-over and timer.
Recommendation: a.
**Answer:** a, keep as built [U 2026-11-12]

## QR17 `RoundOf` and the transition ctx after the allocation work (A12)
To save allocations per container and round, `RoundOf(ctx)` reads the round from the container the transition runs in
(`containerRound.run`), not from a ctx value per round; and the transitions of a container's own events get one ctx
per container life instead of one `context.WithValue` per transition.
a) As built: `RoundOf` is valid while the transition runs; a goroutine the transition started that calls it later
   gets "" (or the next round's name). The ctx keeps its values and cancellation; it is only no longer a new value per
   transition.
b) Keep a ctx value per container and round: one allocation per container and round more, `RoundOf` keeps working
   after the transition returned.
Recommendation: a: transitions shouldn't leave work running past their round (QR3), and `MessageOf` already works this
way.
**Answer:** a, keep as built [U 2026-11-12]

QR18-QR19 (2026-11-16, round speed at scale): measured by the oeko container engine's agent on its largest world (a
standalone node with about 170,000 containers, 8 CPUs, 30 to 40 rounds a month, most with `WithRunAll`, 3.4 to 4.7
million deferred events a month). Not built: nothing in this tree was changed for them.

## QR18 The round dispatch's stall guard at scale (A12)
The stall guard (`roundDispatch.wait`: a tick every `roundStall` 5 ms; no target taken since the last tick → `drain`
posts every target left to its own goroutine) fires in heavy rounds whenever all workers are inside transitions longer
than 5 to 10 ms or a GC cycle holds them (assists, stop-the-world): then up to ~370,000 goroutines start at once, each
transition runs among all of them. On that world, 3 months, the same build with `roundStall` set to an hour in a copy
of this tree, one run each: month 28.8 → 26.9 s (-6.6%), the rounds' CPU -7.4% (goroutine starts, stack growth `newstack` /
`copystack`, scheduling; `drain` itself 2 s of serial barrier time), the transitions' summed wall time 117,000 → 300 s;
peak memory about the same there. On a smaller world (68,000 containers) before the consumer's own fixes: live heap
7.4 → 5.9 GB, mapped memory 23.9 → 17.7 GB (the working state of every transition in flight).
a) Keep as is.
b) A `RoundOption` (eg. `WithoutStallGuard()`): the caller asserts its transitions never wait for each other's rounds;
   the workers alone run the round, no drain.
c) A progressive guard: on a stall, start another `GOMAXPROCS` workers (up to one per target left) instead of a
   goroutine per target; transitions waiting for each other still progress, more slowly (one batch of workers per
   stall tick).
d) Drain only when every worker waits inside asm (a waited `Emit` or call): CPU-bound transitions never trigger it;
   transitions waiting outside asm (channels, as in the spec "waiting for each other's rounds") would need b or c too.
Recommendation: b (opt-in, no change for other users; a simulation's transitions never wait for each other), or c if
the guarantee should hold without an option.
**Answer:**

## QR19 Sorting the senders at every barrier
`Round` sorts the marked containers (every sender of the round: in `WithRunAll` rounds nearly every container) with a
stable sort by id (two lives of one id keep their marking order), and `deliverRound` sorts the targets (unique ids) with
a stable sort too: on that world 2.7 s and 0.5 s of serial barrier time over 3 months (about 3% of the month); the
check and the delivery also look every event's target up in the container map twice (1.2 s).
a) Keep as is.
b) Same orders, cheaper: the marked containers sorted unstably by (id, place in the marked list), the targets
   unstably (ids unique); the target found by `checkRound` kept for `deliverRound` (eg. in the outbox entry).
Recommendation: b (no change of order or semantics).
**Answer:**

Q28-Q33 (2026-11-12, reader and writer transitions, branch `work/rw-modes`): built by the agent with its
recommendations, answered by the user 2026-11-12 (Q28-Q31 a as built, Q32 c, Q33 b; implemented, marked [U] in
`docs/agents/containers.md`).

## Q28 Immutable-entity mode: API and scope of the switch
Built: `WithImmutableEntity()` (container option), `WithImmutableEntities()` (node default, added to the node's hooks,
so containers the node opens itself for calls, hand-overs and lent executions get it), `WithImmutableEntityCheck()`
(node option). No per-container opt-out.
- a) as built. Pro: the contract binds the transitions' code, which is per node. Con: three names.
- b) container option only. Con: internally opened containers stay in copy mode, so their writers may change the entity
  in place, and readers there pay a copy.
- c) node option only. Con: no per-container use (eg. `NewContainer` in tests).
- d) a) plus `WithContainerMutableEntity()` to opt out per container. Con: one node with two write contracts.
Recommendation: a.
**Answer:** a, keep as built [U 2026-11-12]

## Q29 Immutable-entity mode: which copies stay
Built: no copy for readers, the result of an execution for another node, or the trace snapshot. Copy-first (one copy) for
a state transition and for the merge of a waited forward (also with one result). The receiver of a lent forward keeps
its working copy (`ForwardEvent`): node code doesn't know the container's mode, and `changedState` writes the decoded
sent entity. Immutable mode also publishes without read-only transitions (a pointer store; the check needs it).
- a) as built. Pro: simple, every in-place write of the library goes to a copy. Con: one copy per state transition and
  per waited forward.
- b) a) plus: a single result returning a whole entity becomes the new entity without a merge copy (equal per
  `proto.Equal`, only unknown fields differ).
- c) a) plus: drop the receiver's working copy (move it into `processRemote` for copy-mode containers, `changedState` on
  its own copy).
Recommendation: a now; b and c if waited forwards show up in profiles of immutable nodes.
**Answer:** a, keep as built [U 2026-11-12]

## Q30 Mutation check
Built: when a writer (event, batch timer, error func) begins, the published entity is encoded deterministically; when it
ends, encoded again and compared. A difference sends `ErrEntityMutated` to the error func. Not checked: batches flushed
at close, readers (`-race` finds those), entities that don't encode.
- a) as built (node option, error func).
- b) panic on a difference. Pro: impossible to miss. Con: kills the process from a container goroutine.
- c) an environment variable (like oeko's `OEKO_CHECK_SHARING`) instead of an option. Con: global, hidden.
- d) compare a deep copy with `proto.Equal`. Con: NaN fields never compare equal.
Recommendation: a.
**Answer:** a, keep as built [U 2026-11-12]

## Q31 Read call-backs: mechanism in the lock table
Built (your rule: a call back of a read event into a container whose writer waits for the caller runs, only writer
cycles deadlock): the joining acquire carries `ContainerLock.read`; the lock table lets a read join in before the
deadlock check and records it as a call whose wait path adds no edges (snapshot field `Call.read`). A read call that must
acquire the lock (container not open on the node) waits and is checked as before. If the joined event doesn't run as a
reader after all (transitions changed meanwhile), the node calls again as a writer.
- a) as built: proto fields `read` (models.proto regenerated). Pro: every member decides from the log, the lock table
  knows the mode. Con: one log entry per read call, proto change.
- b) read calls into a container open on the node skip the lock table (no `LOCK_ACQUIRE`/`LOCK_DONE`, like
  `WithSkipRaft`). Pro: no raft round trip per read call, no proto change. Con: the lock table doesn't see them (fine
  today: a reader waits for nobody, and its pending count keeps the container and its lock).
Recommendation: a (as the rule asked); b worth measuring if read calls are frequent.
**Answer:** a, keep as built [U 2026-11-12]

## Q32 `WithMaxReaders`: details
Built: a slot channel per container life; a read event beyond the cap waits on its own goroutine (one parked goroutine
per waiting reader). A reader gives its slot back while it waits in a waited `Emit` to another container, else a read
call back into the container could wait for slots held by readers waiting for it (invisible to the lock table). No node
default: containers the node opens itself for calls are not capped.
- a) as built.
- b) a queue per container instead of parked goroutines (at most n reader goroutines). Con: more code; a reader giving
  its slot back during a call still needs its goroutine.
- c) a) plus a node option `WithMaxReadersPerContainer(n)` for all containers of the node.
Recommendation: a; c when calls reach containers the caller didn't start.
**Answer:** c: node option `WithMaxReadersPerContainer(n int)`, the default cap of every container of the node, including the containers the node opens itself; a container's own `WithMaxReaders` overrides it [U 2026-11-12]

## Q33 Counting entity copies
Built: specs count copies with an internal hook (`entityCopyHook`); the benchmark `BenchmarkLocalEventReaders` shows the
allocations. No metric.
- a) as built.
- b) a metric `asm_entity_copies_total`, so a consumer's allocation gate (oeko A12) can watch it in production. Con: an
  atomic add per copy, one more metric.
Recommendation: a; b if oeko wants the gate outside benchmarks.
**Answer:** b: metric `asm_entity_copies_total`, an atomic add per copy where the copy hook counts (`countCopy`), exposed like the other metrics (Inspect, `/metrics`); the test hook `entityCopyHook` stays [U 2026-11-12]
