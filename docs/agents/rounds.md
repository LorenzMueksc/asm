# Rounds: deferred events, round barrier

Code: `outbox.go`. Containers send each other deferred events within the rounds of a standalone node; the node
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
