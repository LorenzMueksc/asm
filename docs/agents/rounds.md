# Rounds: deferred events, round barrier

Code: `outbox.go`, `round.go`. Containers send each other deferred events within the rounds of a standalone node; the node
delivers them at the next round barrier in a sorted order, so runs are reproducible regardless of goroutine
scheduling (consumer: oeko's container engine).

## Deferred events (A1) [A 2026-11-12]
- `c.Emit(ctx, ev, payload, WithTarget(id), WithDeferred())` → `emitDeferred` (hook in `Emit` right after the
  options, before the `containerEvent` is built): appends an `envelope{to, event, seq, payload}` to
  `Container.round.outbox` (`containerRound`, under `containerRound.mu`: transitions append under `c.mu`, the barrier
  takes it from another goroutine) and marks the container for the next barrier (`nodeRounds.mark`: `marked`
  atomic CAS, then appended to `nodeRounds.marked` under `nodeRounds.mu`, so each container is listed once).
- seq = position in the outbox since the last barrier (0, 1, ...): reset when the barrier empties the outbox, so no
  counter outlives a round (snapshot, rollback and replay need no extra state); unique per (round, sender).
- Target required (error otherwise), may be the sender itself (no deadlock: nothing waits). `WithWaitRemote`,
  `WithReply`, `WithDoneChannel` → error. Containers without a node (`round.node == nil`) → error.
- `WithDeferred()` returns one shared option (no allocation). The outbox slice is emptied, not reallocated.
- `resetContainer` calls `c.round.reset(&n.rounds)`: sets the node, empties the outbox (matters for `ResetContainer`).

## Round barrier (A2) [A 2026-11-12]
- `Node.Round(ctx, name)` (standalone only, else `ErrNotStandalone`): `waitReady` → `nodeRounds.acquire` (`sem`, cap
  1, ctx-aware) → `name` set → take `nodeRounds.marked` (swap with `spare`), sort by id (stable), clear each
  `marked` flag (later emits mark again) → `deliverRound` under `containerMap.mu` RLock: per sender in id order, per
  outbox entry in seq order: lookup target (missing → `ErrUnknownTarget`, dropped), append
  `envelope{from, seq, event, payload}` to `target.round.inbox`, collect targets (`queued` flag) → targets sorted by
  id → one round event per target: `c.Event(roundEventName, run)` (payload `*roundRun`; `Event` failing → inbox
  dropped, error) → wait `run.doneC` → `endRound` (name nil, release `sem`).
- Idle detection: `roundRun.left` (atomic) = targets + 1 for the barrier while posting; `processRound` decrements in
  a defer, the last one closes `doneC`. No polling. Round waits only for its round events, which cover everything
  their transitions do synchronously; hand-overs and batch timers are not awaited (documented).
- `processEventLocked` hook: payload `*roundRun` → `processRound(ctx, run)` (after the init func, before
  `processRemote`). It puts the run into ctx (`roundCtxKey`, `RoundOf`), runs each inbox entry through `runEvent` +
  `releaseEvent` like a sent event (traces, save, routing as usual), reusing the `containerEvent` while no batch
  holds it (`refs == 0`), empties the inbox (slice kept), reports `round.err` to `run.fail`.
- Errors: `actionFailed` hook → `containerRound.failed` keeps the first error while `running` (under `c.mu`); the
  error still goes to `sendError`/error func. `Round` returns the first barrier error (unknown target, closed
  target), else `firstError` = lowest container id, wrapped `round <name>: container <id>: <err>`.
- ctx ends while the round runs: `Round` returns `ctx.Err()`, a goroutine waits for `doneC` and then `endRound`, so
  the next round can't take outboxes while transitions still run.
- Ordering argument: senders sorted by id, each outbox in seq order → every inbox is in (sender, seq) order without
  a sort; each container runs its inbox serially on its goroutine. Two lives of the same id in one barrier keep their
  marking order (stable sort).
- `RoundName()` = running round or "". Measured (`BenchmarkRound`, 10k containers, Windows, 8 threads): empty round
  ~0.35 µs, 3 allocs; a round delivering one event to each container ~30 ms (~3 µs and ~22 allocs per event, most
  in the existing event path: goroutine start, `Emit` options, state evaluation).
