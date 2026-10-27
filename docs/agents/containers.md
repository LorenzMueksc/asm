# Containers, processing, Event rpc, errors

Code: `container.go`, `node.go` (start/open/close), `node_grpc.go` (`Event`, `sendToContainer`).

## Model
- Not pooled [U]: every life is a new `Container`; a stale pointer only reaches its closed life (`ErrContainerClosed`). `ResetContainer` is for `NewContainer` containers (tests).
- Count capped by `n.sem` (`WithMaxContainerCount`, default 100). Node containers read the committed bitmap and published transitions through atomic pointers (`bitmapP`, `transitionsP` → `publishedTransitions`: `list()` by priority, `forEvent(name)`); `NewContainer` ones use their own `bitmap`/`transitions` (indexed once).
- `containerProcessing` = one life (`eventsC`, flags, pending, lazy `doneC`); replaced, never mutated. Node code closing containers captures `c.containerProcessing` under the map lock and calls `p.close()`.
- Options passed to `StartContainer` override the node's hooks (node hooks prepended, `withNodeHooks`).

## Lifecycle [U]
- `Close()`, ctx cancel, `DeleteContainer`, `Shutdown` only send a close signal. The container keeps accepting until `pending == 0` (no queued events, running work or batch timers).
- Goroutine on demand [U, 2026-10-24]: no goroutine for an idle container. `start(ctx, closedF)` (from `openContainer`; tests call it on `NewContainer` ones) stores ctx, `context.AfterFunc(ctx, requestClose)`, sets `runner`. Whoever makes work visible calls `schedule()` afterwards (`Event` after the send to `eventsC`, `sendTracedError`/`lateError`, `requestClose`, `donePending` when a closing container became idle): starts `run()` unless `running`, `closed` or not started. `run`: one event, the queued errors, `stopAccepting` → `closeIdle` + stop the ctx watch + `closedF`; nothing queued (`hasWork`) → waits up to `keepAlive` [U: node option `WithContainerKeepAlive`, default 5ms, 2026-10-24] on `eventsC`, `wakeC` (cap 1; `schedule` wakes a running goroutine for errors/close, `scheduleEvent` doesn't: the wait covers `eventsC`) or its timer (armed once, not reset per event: on firing re-armed for the rest of `keepAlive` since `lastWork`); then `stopRunning` (under `lifecycleMu`: no events, errors, or idle close → `running = false`, return). Batch timers keep their own goroutine under `c.mu`. Flags `closeRequested`, `running`, `closed` under `lifecycleMu` (lock order `lifecycleMu` → `errMu`).
- Context [U, 2026-10-24]: each container gets `context.WithCancel(n.ctx)` in `openContainer`, cancelled by `closedF` after closeIdle (and its late errors).
- `closeIdle` order: stop accepting → `flushBatches` → closeF → close `eventsC` → `status = closed` → `removeF` (removes from node map, `endLock`) → `markDone` → report errors queued meanwhile.
- `done()` returns `doneC`, created on the first call (`closedC` once already closed); `markDone` closes it if created. `closedF` cancels the ctx and frees the slot. `Event()` returns `ErrContainerClosed` once not accepting. `StartContainer` waits on `done()` of a closing container, then starts a new one.
- Per life allocated lazily: `states` (`setState`), batch maps, `doneC`. Logger: `containerLogger` (`WithLazy`; a logger that logs nothing is reused). `forwardF`/`nodeErrF` are the node's bound once (`n.forwardF`, `n.failureF`). `openContainer` allocates the container and its `containerProcessing` together and uses `resetContainer` (no `removeF`); public `ResetContainer` sets one. The node's hooks are applied once (`hookOptions`, `containerOptions`).
- Containers close only via `Close`, `DeleteContainer`, `Shutdown` or close-after.

## Processing [U]
- Container goroutine and batch timer callbacks serialize on `c.mu`.
- Batch queues/timers keyed by transition name (queue stores the transition): published transitions are replaced on every change. A stale timer (not in `transitionTimers`) does nothing.
- Done channel (`WithDoneChannel`) fires once per event: after every matching transition incl. batches, or at once if nothing matches (`containerEvent.refs`). The send blocks: use a buffered channel.
- rpc-sent events carry `closeAfter`: the container closes itself once the event is released (forwarded: right after the pass).
- `c.Emit(ctx, event, payload, opts...)`: nested in the calling transition (`runEvent` under held `c.mu`), only with the transition's ctx (`transitionCtx` marker from `executeActions`, else `ErrNotInTransition`), ≤ `maxEmitDepth` 16. A waited emit to a node already in the lend chain fails (cycle).
- Target calls [U: same-entity calls, shared DB, reply payload, deadlock-checked; 2026-10-24] [A: API, local only]: `Emit` with `WithTarget(id)` → `emitToTarget` → `targetF` = `Node.callTarget`: a `remoteExecution` with `otherEntity` (own func + state transitions of the entity, no remote ones) in the target container on this node. Waited (`targetCall`): `callContainer` with the caller's wait path (join if open here, else acquire; lock table refuses cycles), no wait path or `WithSkipRaft` → `sendToContainer`. Not waited: `acceptHandOver` without id (queued per id, `acquireAsync`). `c.Reply(ctx, v)` sets `remoteExecution.reply` of the calling events' parents (`ErrNoCaller` if none waits); read after done. Target = own id → error. A target transition owned by another node isn't run (no remote transitions on receivers). `c.Reply` also answers waited forwards (`forwarding.md`).

## Event rpc
- Container id from `WithContainerIdFunc` (none → `FailedPrecondition`, empty → `InvalidArgument`). Payload via `anypb.UnmarshalNew` (unknown → `InvalidArgument`).
- `meta["wait"]="true"` waits for done (or request ctx); otherwise fire-and-forget, request ctx only bounds waiting for the lock. `ErrContainerClosed` → retries `StartContainer`.
- Public listener (`WithPublicAddr`) and TLS rules: `security.md`.

## Errors
- No dependency on the former shared module. Log errors with `%+v`. Debug messages with args via `debugf` (formats only if debug enabled).
- Container errors are queued (`sendError`: `errs` slice + `schedule`; none dropped) and handled by `run`: logged, `errorF` under `c.mu`; non-nil return → `requestClose`. Close func errors arrive after the container closed.

## Gotchas
- `Close()` inside a transition or the error func deadlocks (waits for itself). `c.Event(...)` inside a transition is fine unless `eventsC` (buffer `maxInFlight`, default 10) is full; it runs after the current event.
- `StartContainer` inside a transition to wait for another container can deadlock: use `Emit` + `WithWaitRemote()`.
- Handing over at the limit (`WithMaxPendingHandOvers`) blocks the transition until a hand-over ran, bounded only by the container ctx: an open container handing over ≥ limit work for its own id waits forever (`forwarding.md`).
