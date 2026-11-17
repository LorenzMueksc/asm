# Rounds: deferred events, round barrier

Code: `outbox.go`, `round.go`, `round_batch.go`, `spawn.go`. Decisions: `docs/questionnaire.md` QR1-QR17 (QR1-QR11,
QR13-QR17 answered by the user 2026-11-12; QR12 [A]). Containers send each other deferred events within the rounds of
a standalone node; the node delivers them at the next round barrier in a sorted order, so runs are reproducible
regardless of goroutine scheduling (for a consumer running simulations in rounds).

## Deferred events (A1) [U 2026-11-12: QR1 a, QR2 a]
- `c.Emit(ctx, ev, payload, WithTarget(id), WithDeferred())` → `emitDeferred` (hook in `Emit` right after the
  options, before the `containerEvent` is built): appends an `envelope{to, event, seq, payload}` to
  `Container.round.outbox` (`containerRound`, under `containerRound.mu`: transitions append under `c.mu`, the barrier
  takes it from another goroutine) and marks the container for the next barrier (`nodeRounds.mark`: `marked`
  atomic CAS, then appended to `nodeRounds.marked` under `nodeRounds.mu`, so each container is listed once).
- seq = position in the outbox since the last barrier (0, 1, ...): reset when the barrier empties the outbox, so no
  counter outlives a round (snapshot, rollback and replay need no extra state); unique per (round, sender) (QR1).
- Target required (error otherwise), may be the sender itself (no deadlock: nothing waits). `WithWaitRemote`,
  `WithReply`, `WithDoneChannel` → error. Containers without a node (`round.node == nil`) → error (QR2).
- `WithDeferred()` returns one shared option (no allocation); `Emit` applies `WithTarget`/`WithDeferred` by type
  (`applyEventOptions`), so a deferred `Emit` allocates only the `WithTarget` option. The outbox slice is emptied, not
  reallocated.
- `resetContainer` calls `c.round.reset(&n.rounds)`: sets the node, empties the outbox (matters for `ResetContainer`).
- `Node.DropOutboxes(ctx)` [A 2026-11-12, QR12]: takes the round semaphore (waits for a running round), unmarks every
  marked container and empties its outbox, unlists every `spawning` one and empties its spawns (flag cleared before
  emptying: what is queued meanwhile is dropped or lists it again), returns the count. Closes stay requested. For a
  caller rolling a failed round back, and after a failed check.

## Round barrier (A2) [U 2026-11-12: QR3 a + overlap error, QR4 b, QR7 a, QR10 a]
- `Node.Round(ctx, name, opts...)` (standalone only, else `ErrNotStandalone`, QR7; `RoundOption`: `WithRunAll`, below;
  options parsed on the heap only when given): `waitReady` → `nodeRounds.acquire` (`sem`, cap 1, ctx-aware) →
  `started` = 0, `name` set → take `nodeRounds.marked` (swap with `spare`), sort by id (stable), clear each `marked`
  flag (later emits mark again).
- Check before anything (QR4 b): `takeSpawning` takes `nodeRounds.spawning` (containers with spawns queued between
  the rounds: live events, or kept by a failed check; sorted by id, flags cleared), `collectSpawns` copies their spawns
  (sorted by id, stable: ties keep parent id order; `spawnsTaken` per parent), `checkRound` under `containerMap.mu`
  RLock: `checkSpawnsLocked` (every spawn id not in the map and not spawned twice, `ErrContainerExists`), then per
  sender in id order, per outbox entry: target in the map and `isAccepting` (once per target and barrier:
  `containerRound.seen` = `nodeRounds.seq`), or among the spawns (`spawned`, binary search) → else `ErrUnknownTarget`
  (also a closing container). `checked` = outbox length at the check. On failure: nothing delivered or started,
  containers marked again (outboxes) and listed again (`keepSpawns`), kept for the next barrier or `DropOutboxes`,
  round released, error returned. The caller's rollback sees a clean state.
- Then `startSpawns` (removes the first `spawnsTaken` spawns of each parent, starts them in id order) →
  `deliverRound` under `containerMap.mu` RLock: per sender in id order, per checked outbox entry in seq order: append
  `envelope{from, seq, event, payload}` to `target.round.inbox`, collect targets (`round.listed` = `nodeRounds.seq`,
  once per barrier); entries queued after the check stay, renumbered from 0 → targets sorted by id → (`WithRunAll`:
  `allContainers` appends every map value, without touching the containers) → `roundDispatch` → wait `run.doneC`
  → `finishRound` (targets buffer reused) → `endRound` (closes, `startRoundSpawns`: the spawns of the round, below,
  `started` read, name nil, release `sem`).
- Dispatch [A 2026-11-12, A12] (`roundDispatch`): under 2×64 targets the barrier posts them itself (`drain` →
  `postRound`: like `Event` without options, the container's own round event `round.event`, reused every round:
  `eventOf`). Else one worker per CPU (`GOMAXPROCS`) takes targets by an atomic index (`take`; an index beyond the
  delivered ones whose container is `listed` with this seq is a duplicate: `run.done()`, skipped) and runs each in
  place (`runRound`): `claimRun` (under `lifecycleMu`: no goroutine running, started, accepting, empty mailbox and
  deferred list → `running = true`, counted like `addPending(true)`) → `processEvent(c.ctx, round event)` on the
  worker → `releaseRun` (work came meanwhile → `go runF()`, else `running = false`); a container not claimable gets
  `postRound`. No goroutine per container and round. Posting order never changes results (each container depends
  only on its inbox and entity; errors by lowest id; outboxes sorted next barrier). Stall guard: `wait` ticks every
  `roundStall` (5ms); an index that didn't move since the last tick → the barrier `drain`s the rest to mailboxes (own
  goroutines, as before), so transitions waiting for each other's rounds can't hang the workers. A post failing
  (closed since the check) drops the inbox; the error of the lowest index wins (`dropped`, `err`).
- Idle detection: `roundRun.left` (atomic) = targets (duplicates included) + 1 for the barrier while dispatching;
  `processRoundEvent` decrements last (a defer registered first), the last one closes `doneC`. No polling. Round
  waits only for its round events, which cover everything their transitions do synchronously.
- Overlap (QR3, strict QR16 a [U 2026-11-12]): `nodeRounds.background` counts work running on its own: hand-overs
  without wait (`+1` in `enqueueForwardedLocked`, `-1` in `giveUpForwarded` or after the execution's done channel,
  node_forward.go) and batch timers (`startBatchTimer` `+1`, `-1` when the timer func ends or `Stop` succeeds;
  `runsOn`, nil-safe). `runsOn(+1)` while `name` is set also counts `started`. After the round, with no other error:
  `started > 0` or `background > 0` → `ErrRoundOverlap` ("N ... started during the round, M running after it").
  Node-wide: also live events' hand-overs during a round.
- `processEventLocked` hook (first, before metrics and pending): payload `*roundRun` → `processRoundEvent` (metrics,
  `endPending(true)`, init func `initFirst`, `processRound`; `run.done()` last: the next round may reuse the round
  event right after; no `releaseEvent`, the round event has no done channel/close-after/remote). `processRound`:
  `round.run` (atomic) = run (`RoundOf`), each inbox entry through `runEvent` + `releaseEvent` like a sent event
  (traces, save, routing as usual) on the container's `round.delivered` event (reused across entries and rounds while
  no `WithBatching` batch holds it, `refs == 0`), empties the inbox (slice kept), round batches, `run` nil, reports
  `round.err` to `run.fail`. Other events run while a round transition waits (reentrant) clear `run`/`current`/
  `currentMsg` for their duration.
- A goroutine whose last event was a round event ends at once when idle (no keep-alive timer: the next round's work
  comes with the next round), `run` in container.go.
- Errors: `actionFailed` hook → `containerRound.failed` keeps the first error while `running` (under `c.mu`); the
  error still goes to `sendError`/error func. After a passed check `Round` returns: a delivery failure (closed since
  the check), else `firstError` = lowest container id, wrapped `round <name>: container <id>: <err>`, else the error
  of the round's spawns (`startRoundSpawns`), else overlap.
- ctx ends while the round runs: `Round` returns `ctx.Err()`, a goroutine goes on waiting (`wait`, stall guard
  included) and then `finishRound` (closes and spawns included; a failed spawn check stays queued, the next round's
  check reports it), so the next round can't take outboxes while transitions still run.
- Ordering argument: senders sorted by id, each outbox in seq order → every inbox is in (sender, seq) order without
  a sort; each container runs its inbox serially (on a worker or its goroutine). Two lives of the same id in one
  barrier keep their marking order (stable sort).
- `RoundOf(ctx)` only for transitions of delivered events and batches, `RoundName()` = running round or "" (QR10).
  [U 2026-11-12, QR17 a] `RoundOf` reads `round.run` of the transition's container (`messager.roundName`), no ctx value
  per round: valid while the transition runs. `MessageOf(ctx)` (QR11 b) = the delivered `Message{From, Seq, Payload}`
  for transitions of a delivered event: `transitionCtx.depth == 0` and the container's `currentMsg` (via `messager`);
  false for live events, events a transition emits (depth > 0) and round batches.

## Run all (`WithRunAll`) [U 2026-11-12, QR15 a]
- `Round(ctx, name, WithRunAll())`: `roundRun.runAll`; if the published transitions have round batches
  (`publishedTransitions.roundBatches()`: non-remote `roundBatch` ones by priority then name, built once,
  `roundBatchIndex`), every map value joins the targets (`allContainers`); closing ones fail `claimRun`/`postRound`
  with an empty inbox (no error). No round batches → nothing extra.
- Spawned containers [U 2026-11-12, QR13]: one spawned in a round starts at its end and takes part from the next
  round on, not in the round that spawned it; one spawned between rounds starts before `allContainers` and takes part
  in the next round.
- `runRoundBatches(ctx, saveAfter, all)`: batches with events as before, then `takeEmptyBatches`: every round batch of
  the node without events this round (`batchOf` per transition name, `len(taken) == 0`) runs with an empty, non-nil
  `[]Message` if its conditions match (`EvaluateEntityWithEvents(nil)` once, then `evaluateConditions`; none →
  runs). Same order (priority, name). Deterministic: no message needed per container.

## Performance (A12) [A 2026-11-12]
- Round path: round events and delivered events reused per container (`round.event`, `round.delivered`), batch event
  per `roundBatch` (`evt`; payload re-boxed only when the slice header changed: `boxed`), no ctx value per round,
  workers instead of a goroutine per container, no keep-alive timer after a round event. Shared with live events
  (`containers.md`, Hot path): options by type, transition ctx per life, single-event slices without allocation,
  `debugOn` guards, goroutine start without closure, keep-alive timer reused, `tablesOf` recent cache (`state.md`).
  Immutable mode copies nothing on the round path (spec counts with `entityCopyHook` and `asm_entity_copies_total`);
  copy mode one copy per writer pass when the node has read-only transitions.
- Measured (`BenchmarkRound`, 10k containers, Windows, 8 threads; before → after, details `testing.md`): one event to
  every container ~2.5 µs, 22 allocs, ~1.07 KB → ~0.52 µs, 2 allocs, ~26 B per event (left: the `WithTarget` option,
  a state evaluation); a round batch of every container via one deferred event each ~18-23 ms → ~5.5 ms; `WithRunAll`
  round without events ~2.1 ms (target < 5 ms); empty round ~0.26 → ~0.28 µs, 3 allocs.

## Round batches (A3) [U 2026-11-12: QR5 c, QR6 c]
- `WithBatchOnRound()` sets `Transition.roundBatch` and `Batching{Count: 1}` (copied by `copyTransition`; not synced to
  other nodes, standalone only). Hook at the top of `executeOrQueue`: `roundBatch` → `queueRoundBatch`, no `refs++`:
  the event's done channel and close-after don't wait for the batch.
- `queueRoundBatch` (QR6 c): only the inbox entry running now (`evt == round.current`, set by `processRound` around
  `runEvent` of each entry) joins, as `Message{From, Seq, Payload}`. A live event (`Event`, calls, hand-overs, `Emit`
  without `WithDeferred`, also one run while a round transition waits) doesn't: the batch transition doesn't run for
  it, its other transitions do. A round batch is exactly the round's sorted delivered events matching it. Batches per
  transition name in `round.batches` (`roundBatch{t, msgs, taken, evt, boxed}`, `batchOf`), `t` updated to the
  transition as published now.
- `processRound` runs `runRoundBatches` after the inbox (then `save` if needed): takes every batch with events,
  sorts by priority desc then name, sorts each batch by (From, Seq) if needed (stable), runs it via `executeActions`
  with one synthetic event whose payload is the `[]Message` (`runTaken` → `runBatch` on the batch's `evt`; a later
  `WithBatching`'s `Count != 1` is overridden on a copy). Slices reused: valid only during the call. Errors count for
  the round.
- Containers without rounds (`round.node == nil`: built with `NewContainer`) never collect: `queueRoundBatch` runs the
  transition at once with a one-message batch (`From` "", same payload type).
- Writer (QR5 c): `checkRoundBatch(t, t.ReadOnly)` in `compileTransitionLocked` refuses a read-only round batch (the
  node fails, like other compile errors); `readOnlyFunc` is false for `roundBatch`, so an event with a round batch
  transition is never a read event and reader views never reach `queueRoundBatch`.

## Spawn and close (A5) [U 2026-11-12: QR8 a, QR9 a, QR13 yes]
- `c.Spawn(ctx, id, entity, opts...)` (transition ctx, node, non-empty id): appends `spawnRequest{id, parent, entity,
  opts}` to `round.spawns` under `round.mu`, lists the container in `nodeRounds.spawning` (`markSpawning`: `spawning`
  CAS, once per container; not `marked`, which is for outboxes).
- When they start [U 2026-11-12, QR13; details A]: spawned in a round (any transition while it runs) → at its end,
  `endRound` → `startRoundSpawns` after `closeRoundContainers` (so an id closing in the round is free again):
  `takeSpawning`, `collectSpawns`, `checkSpawnsLocked` under `containerMap.mu` RLock (open after the closes, or twice
  → none starts, `keepSpawns`, `Round` returns `ErrContainerExists`), then `startSpawns` in id order. They exist when
  `Round` returns and get the events deferred to them from the next round on (in the map at its check). Spawned
  between rounds (live events) → at the start of the next round, checked with the targets (QR4, above), before
  `deliverRound`, so events deferred with them reach them in that round.
- `spawn` each: `hasContainer` → `ErrContainerExists`, else `openContainer(ctx, id, "", false, waited=true,
  withNodeHooks(opts + withSpawnEntity))` (takes a slot if free, never blocks the barrier; QR8), `hold()` (calls'
  close-after leaves it open, like `StartContainer`). The entity reaches the builder through the internal option
  `containerOptions.spawnEntity`: `resetContainer` → `setSpawnEntity` right after the builder is created, before the
  container is in the node's map, so no lock is needed and no reader or writer can see an empty entity (no new `c.mu`
  entity write). Nil entity → builder stays empty. `resetContainer` also sets `rw.newBuilderF`, so read events of
  spawned containers run as readers. Start order is visible in the trace (`CONTAINER_STARTED`).
- `c.CloseAfterRound(ctx)`: `round.closing` CAS → `nodeRounds.closing`. `endRound` → `closeRoundContainers`: sorted by
  id, `requestClose` all, wait for `done()` of all, then the round's spawns start (above) and the round ends (also in
  the ctx-cancel path). Its outbox stays on the closed container, which is still in `marked`: the next barrier
  delivers it; its spawns of the round start right after the closes. Called outside of a round: closed at the end of
  the next one (QR9).

## Locking [A 2026-11-12]
- Node: `nodeRounds.mu` guards `marked`, `spawning`, `closing`; the flags `marked`, `spawning` (atomic, CAS) keep each
  container listed once.
- Round state of a container: `outbox`, `spawns` under `round.mu` (any goroutine); `checked`, `spawnsTaken`, `seen`,
  `listed` barrier-only (read by its dispatch); `inbox` owned by the barrier between rounds and by the container while
  its round event runs (ordered by the round event and `run.doneC`); `batches`, `current`, `currentMsg`, `running`,
  `err`, `run`, `delivered` under the container's processing lock (`c.mu`, held by `processEvent`; `run` atomic for
  `RoundOf`). The round event (`event`) is written by the barrier or a worker (`eventOf`) only while no round of the
  container runs (`run.done()` comes last in `processRoundEvent`).
- Entity writes of rounds happen only inside `processEventLocked` (round events run through `processEvent`, from the
  mailbox or a dispatch worker: writers under `lockWrite`, inbox entries and round batches) and at construction
  (`setSpawnEntity`). No `c.mu.Lock()` of their own.
- Lock order in `checkRound`: `containerMap.mu` (R) → `round.mu` → `lifecycleMu` (`isAccepting`), as `startContainer`.
- Readers (rw modes, `containers.md`) [U 2026-11-12, QR14 a]: a reader's view keeps `round` zero (`readerView` lists
  its fields). A reader's `Emit(WithDeferred)`, `Spawn`, `CloseAfterRound` return `ErrReadOnly` (`c.rw.base != nil`,
  checked before `round.node`): a reader runs next to the writer, its outbox position would depend on scheduling.
  `MessageOf` is false in a reader (`currentMsg` nil); `actionFailed` → `containerRound.failed` on a view touches only
  its zero `round`. The round event never goes through `dispatchRead` (`postRound`, `runRound`): it is a writer, so
  inbox entries whose transitions are all read-only run inside the round's writer, in inbox order.
