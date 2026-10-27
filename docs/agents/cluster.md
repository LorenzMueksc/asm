# Cluster: raft, membership, clients, persistence

Code: `node_raft.go`, `node_propose.go`, `node_send.go`, `node_snapshot.go`, `node_wal.go` (+ `node_wal_linux.go`/`node_wal_other.go`), `node_async.go`, `node_membership.go`, `node_clients.go` (member side), `node_client.go` (client side).

## Raft log
- Carries `BITMAP_ADD`, `LOCK_ACQUIRE/RELEASE/CANCEL/DONE`, `NODE_HEARTBEAT`, `CLIENT_ADD/REMOVE`, conf changes. Each normal raft entry = `NodeLogs` batch (binary proto) of `NodeLog`s with `oneof body`. Raft messages are `raftpb.Message.Marshal` in `SyncRequest.messages`.
- Lock/heartbeat entries of removed ids are ignored.

## Raft [A, agreed 2026-10-23]
- `propose` queues for the proposer goroutine (`queueProposals`: slice under `proposeMu`, `proposeWakeC` cap 1) and waits only on its own `errC`; returns once the batch was accepted by the leader (not committed). The proposer answers every queued proposal, also at stop (`stopProposer`, from `runProposer` and `stop()`; later ones refused). Proposer takes all waiting (≤1024) into one entry (`takeProposals`, marshaled with `MarshalVT`); followers/clients: one `Propose` rpc per batch; leader proposes it as one entry. Batches in flight [U: adaptive, 2026-10-24]: one, a further one (≤ `maxProposalsInFlight` 4, each in its own goroutine) only while a whole batch (1024) waits; an answered batch wakes the proposer. Fixed 2 or 4 in flight made batches smaller and cost throughput (start+close WAL ~95k → ~81k / ~65k/s). Batches in flight may reach the leader in any order: dependent proposals are made after `propose` returned (leader appended). Leader/term from atomics (`n.leader`, `n.term`) updated per Ready, not `Status()` (also the `Propose` rpc's leader check).
- `MaxUncommittedEntriesSize` 64 MB: beyond fails, retried (backpressure).
- Without data dir: synchronous Ready loop (`serveChannels`): snapshot + entries into `MemoryStorage`, send, apply, `maybeSnapshot`, `Advance`. With data dir: `AsyncStorageWrites` (Persistence). Ticks from their own goroutine (`runTicks`, `raftTickInterval` 100ms; ends on `raftStopC`), not the Ready loop. `ElectionTick` 20 (2-4s), `PreVote` on.
- `propose` doesn't campaign without a leader; callers retry.

## Sending [A]
- `sendMessages` groups per peer, enqueues non-blocking (128 batches; full = unreachable + drop). One sender goroutine per peer merges queued batches (≤2 MB) onto a `SyncStream` (`syncStream`: acks in order, ≤256 unacked; only snapshot batches wait for their ack). Failure → reopen, 3 retries with backoff, then `ReportUnreachable` (+ `SnapshotFailure`). Unary `Sync` kept (same `stepMessages`). Senders stop on removal and `stop()`.

## Snapshots
- `maybeSnapshot` after each Ready: after `snapshotCount` entries (default 10000, keep 5000 catch-up) and ≥ `snapshotInterval` (default 30s, 0 = none) since the last, or once 64 MB applied since. Applying only copies the state (`copySnapshotState`: `maps.Clone` of the lock table, liveness, peers; locks and layout are replaced as a whole on every change) and sets the counters; a goroutine (`writeSnapshot`, one at a time: `snapshotting`) marshals (`MarshalVT`), `CreateSnapshot(index, confState, data)` (`ErrSnapOutOfDate` after a received snapshot: dropped), writes the file, `Compact(index - catchUp)`. With a wal it counts in `storageWG`. Metrics `asm_raft_snapshots_total`, `asm_raft_snapshot_bytes`, `asm_raft_snapshot_seconds`.
- `NodeSnapshot`: layout, peers, removed, locks (queues, calls), liveness. `restoreSnapshot` (on `rd.Snapshot`) replaces peers + removed set, detaches clients. Senders report `MsgSnap` results.

## Persistence `WithDataDir` [U: own WAL files; A: details]
- `<dir>/wal/<seq>.wal`: records `[len][crc32c][type][data]` (entry, hard state, snapshot marker, join index); `<dir>/snap/<term>-<index>.snap` = `[crc32c][raftpb.Snapshot]` (tmp, sync, rename). Without data dir: memory only.
- `AsyncStorageWrites` [U: only with a data dir, 2026-10-23; A: details] (`node_async.go`): Ready loop `serveChannelsAsync` sends the network messages first (leader sends while it writes, raft thesis 10.2.1), then queues `MsgStorageAppend` → `runAppends`, `MsgStorageApply` → `runApplies` (queues 256 each, full blocks the loop). Never `Advance`.
- `wal.save` without entries and with only the commit index moved (term, vote unchanged): the hard state record stays buffered, goes out with the next durable write (raft's `MustSync`; a lost commit index is learned again).
- Append goroutine: drains its queue (group commit) → `appendBatch`: per message `MemoryStorage.Append` (raft reads stable entries only), one `wal.save(appends...)` (entries in order + last hard state, one durable write) → `deliverResponses`: peers via `sendMessages` (follower acks, votes), own via `Step` (`MsgStorageAppendResp`, leader's self-ack). Received snapshot: earlier appends saved first, `saveSnapshot(replacesLog)` (wal marker: log replaced, recovery drops earlier entries), `storage.ApplySnapshot`, restore queued for the apply goroutine (raft emits no committed entries while a snapshot isn't stable, so order holds). Write error fails the node.
- Apply goroutine: drains its queue → `publishEntries` / `restoreSnapshot` in order → `maybeSnapshot` → `MsgStorageApplyResp` via `Step`. Owns `appliedIndex`, `confState`, snapshot counters.
- `stop()`: closes `storageStopC`, stops raft (a `Step` in progress returns), waits `storageWG`, then closes the wal.
- Segments rotate at 64 MB (`segmentSize`), each starts with hard state + join index. `writeSnapshot` writes the file, deletes older snapshots and the prefix of covered segments (prefix only: later segments may replace entries).
- Recovery `openWAL`: newest snapshot, segments in order; reused index replaces it and later ones; gap fails start; torn tail of the last segment cut; damage elsewhere fails start; commit clamped to [snapshot, last entry], term raised to snapshot's (vote 0).
- Restart: snapshot + entries into `MemoryStorage`, `restoreSnapshot`, `RestartNode` (`Applied` = snapshot index), no join (join addr ignored), no campaign, `joined` with persisted join index. Same id + listen address required. `becomeReady` waits for a heartbeat of the new life (`waitForHeartbeat(startedAt)`), then `endPreviousLife`.
- A node that left or was removed stops on restart; delete the data dir to rejoin. Clients and `WithSkipRaft` ignore the data dir.
- Write-through [U, 2026-10-23]: segments opened with `walSyncFlag` (`node_wal_linux.go`: `O_DSYNC`; `node_wal_other.go`: `O_SYNC` = Windows `FILE_FLAG_WRITE_THROUGH`, data + NTFS metadata with FUA). No `f.Sync` on segments: `wal.sync` = flush of the bufio writer, every `save` (MustSync or not). Snapshot files still fsync (tmp + rename), written outside `wal.mu` under `snapMu` (appends go on); one older than the latest saved (`snapIndex`) is dropped (local snapshot finishing after a received one). Not preallocated [U: not now].
- Cost (one SSD): fsync ~1.1 ms, write-through ~0.1 ms. start+close 128 goroutines, 3 nodes: ~91k/s (async; sync loop ~57-60k/s, fsync ~25k/s) vs ~127k/s in memory; 5 nodes ~61k/s vs ~95k/s. Metrics `asm_wal_bytes_total`, `asm_wal_sync_seconds` (flush time).

## Startup
- `NewNode` non-blocking. `Ready()` once own bitmap committed, a heartbeat committed, transitions compiled and exchanged. `Err()` (buffered 1) gets the failure; a failed node stops raft, gRPC and containers.
- `StartContainer(ctx, id, opts...) (*Container, bool, error)` blocks until ready, returns the failure; `Container.Event` too.
- `WithSkipRaft`: local bitmap = committed, ready inside `NewNode`; `Leave` only closes containers.
- Joining node starts with `RestartNode` (no peers), gets config from the leader. Nodes must join the current leader (`Join` on a follower: "not the leader"). Leader serializes `Join` (`joinMu`), waits until applied (node in `peers`), re-proposes if raft dropped it (one pending conf change).

## Membership [U]
- `Leave(ctx)`: set `leaving` (under `containerMap.mu`), close all containers and wait (ctx error aborts, stays member); leader first transfers leadership (highest match); then `RemovePeer` at the leader; then `requestStop` (no `Err()`). `StartContainer` returns `ErrNodeStopped` once leaving. Hand-overs: new ones refused (`Unavailable`), queued ones given back to their senders (`dropPending` with cancel, see `locks.md`).
- Closing the user's stop channel runs `Leave` with `leaveTimeout` (30s), stops anyway on failure. Internal `stopC` closed only by `requestStop`.
- `RemoveNode(ctx, id)`: any node, retries via current leader; own id = `Leave`. Leader serializes with joins (`joinMu`), waits until applied. `RemovePeer` on the leader for itself transfers leadership and returns `Unavailable` (callers retry). With TLS removing another node needs the admin role.
- Applying a removal: delete peer + client, add to `removed`, stop sender, free locks + liveness (`lockTable.removeNode`), drop as transition owner. `Sync`/`Propose` reject removed ids with `PermissionDenied`; a node getting that stops. With TLS every rpc rejects the removed id's cert and its connections close after 1s (`closeRemovedConns`, see `security.md`).
- A removed node stops (own removal applied, snapshot lists it, or `PermissionDenied`). No automatic rejoin, no automatic removal of unreachable nodes. Batches don't wait for acks: the SyncStream ack reader stops the node on `PermissionDenied` (`deniedF`), `sendOnStream` returns it for a stream that broke with it. A node stopping during `becomeReady` doesn't report `ErrNodeStopped` on `Err()`.
- Rejoin with an old id allowed (also with TLS: `authorizeMethod` exempts `Join` from the removed-id check): the add clears `removed`; `JoinResponse.added_index` (`addedAt`) makes the joiner ignore replayed removals of its id up to that index (`selfRemoved`, `joinedAt`).

## Client nodes `WithClientMode` [U: per-node option; A: details]
- `n.client`: no raft node (`n.node == nil`), no log. `n.skipRaft` (= `WithSkipRaft`) means no locks at all. Code meaning "no locks" checks `skipRaft`; code needing raft checks `n.node`.
- Member side (`Attach`): `addClient` proposes `CLIENT_ADD` until peer; full state (layout, membership incl. `n.members`, liveness, locks involving the client: holder, waiter, or call with its token), then queued changes; liveness every 500ms. Hooks: `applyLockLog` → `notifyLock` (involved before/after or watching), `notifyRefused`; layout/membership/leader changes → `notifyClients`/`notifyLeader`. Slow client (4096 queued) detached. Snapshot restore detaches all.
- Client side: lock table = pushed locks (`lockTable.set`; full update replaces). `lockChanged` per pushed change, `own` = token no longer in use (`ownChange`). Leader/term from updates; proposals via `Propose` to the leader. `verifyHolder` watches the lock (`watchLock`/`unwatchLock`). Own bitmap committed = merging it into the committed layout adds nothing (`checkPendingBitmaps`, also right after they become pending). Membership updates replace peers/members/removed; a removed client stops. Fails over to another member (`attachLoop`, `nextMember`).
- `Leave` on a client: close containers, `leaveAsClient` proposes `CLIENT_REMOVE` until removed. `RemoveNode` of a client: leader proposes `CLIENT_REMOVE` (`removeClientAsLeader`). Applied like a removed member.
- `Join`, `Sync`, `Propose` return `FailedPrecondition` on clients; `Attach` only on members.

## Gotcha
- `Node` implements the gRPC server itself: rpc names must not clash with public methods (hence `RemovePeer`, not `RemoveNode`).
- raft logs through the node's logger (`raftLogger`, logging.go, named `raft`) [U]. Its "...; ignoring" Infof (stable entry acked again: with async writes on most appends) goes to Debug; the default dev logger shows it.
- `stepMessages` rejects local raft message types from peers (`raft.IsLocalMsg`): a forged `MsgStorageAppendResp` would pass raft's own check.
