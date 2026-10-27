# Code map (v2)

Where things live and the main call flows. Grep the symbol names; line numbers change. Update when files or flows change.

## Flows
- **Startup**: `NewNode` (node.go: options, `setSecurityOptions`, `serveMetrics`, `checkOwnCertificate`, `servePublic`) → `WithSkipRaft`: ready at once | `startClient` (node_client.go) | `startRaft` (node_raft.go: `openWAL` → `RestartNode`/`StartNode` → `serveGRPC`, `serveChannels`, `runTicks`, `runProposer` → `Join` rpc or `Campaign`) → `becomeReady`: `heartbeat`, `watchExpiry`, `proposeLocalBitmap`, `waitForHeartbeat`, (`endPreviousLife` after restart), `compileTransitions`, `syncTransitions`, `markReady`.
- **Ready loop** `serveChannels` (node_raft.go, no wal): `updateLeader` (atomics, `notifyLeader`) → `storage.ApplySnapshot` + `restoreSnapshot` → `storage.Append` → `sendMessages` → `publishEntries` (`applyLog` per `NodeLog`: `applyBitmapLog` | `applyLockLog` | `applyClientLog`; conf changes) → `maybeSnapshot` → `Advance`.
- **Ready loop with wal** `serveChannelsAsync` (node_async.go): `updateLeader` → `sendMessages` (network) → queue local messages. `runAppends` → `appendBatch` (`wal.save`, `saveSnapshot`, storage; restore queued) → `deliverResponses`. `runApplies` → `publishEntries` / `restoreSnapshot` → `maybeSnapshot` → `deliverResponses`.
- **Propose**: `propose(l)` → `queueProposals` → `runProposer` (`takeProposals`) → `proposeBatch` (leader: `proposeLogs`; else `Propose` rpc). Retry policy: `needsPropose`/`proposeTracked`. Batch of many: `proposeMany`.
- **Start container**: `StartContainer` → `startContainer` (wait ready; open/closing check; `starting` map) → `createContainer` (`reuseRetained` | `acquireLock`) → `openContainer` (slot `n.sem`, wait for closing/borrowed one, `ResetContainer`, `removeF` = `removeContainer` + `endLock` + trace) → `Container.start` (no goroutine yet; `schedule` → `run` while work is queued).
- **Event**: `Container.Event` → `eventsC` + `schedule` → `run` → `processEvent` → `runEvent` (merge bits; per transition: `matches`/`matchesRemote`; local → `executeOrQueue` → `executeActions`; remote matches collected, `route()` before the next local one → routing transition → `Container.forward` → `Node.forward` → `forwardTo` → `coverTransitions` → `sendForward` → `forwardStreamTo().call` | hand-over: `sendHandOver` (`takeHandOverSlot`) → `handOver`, kept until `executed`; failure later (error reply, broken stream, `watchLease`, removal) → `handOverFailed`) → `releaseEvent` (refs, done channel, close-after). Nested: `Emit` → `runEvent`; with `WithTarget(id)`: `emitToTarget` → `Node.callTarget` (`redirectToHolder` | `callContainer` | `sendToContainer` | `acceptHandOver`).
- **Receive forward**: `ForwardEvents` (node_stream.go) → `ForwardEvent` (node_grpc.go): own-name checks → other entity: `forwardOtherEntity` (wait: `callOtherEntity` → `callContainer`; else `acceptHandOver`) | wait: `verifyLending` → `startContainer(borrowed)` → `sendToContainer` + `withRemoteExecution` → `processRemote` → `changedState` | no wait: `acceptHandOver` (ids, refuses while leaving) → `drainForwarded` (`acquireAsync` or `sendToContainer`; done → `handOverRan` → `executed` reply; can't run → `giveUpForwarded` → `failHandOver` → error reply).
- **Lock acquire**: `acquireLock(ctx, id, waitPath, join)` (`checkWaitPath`) → `LOCK_ACQUIRE` → apply `applyLockLog` → `lockTable.applyAcquire` (refuse over `maxWaitPath` / grant / queue / refuse via `waitsForLocked`) → `lockChanged` + `notifyLock` → waiter wakes on `lockTable.get` channel. Async (hand-overs): `acquireAsync` → `runPendingAcquires` → grant seen in `lockChanged` → `takePending` → `openPending`.
- **Close**: `Close` → `requestClose` (flag + `schedule`) → `run` notices idle (`stopAccepting`) → `closeIdle` (`flushBatches`, closeF, `removeF`, `markDone`) → `closedF` (cancel the container ctx, free the slot).
- **Leave**: `Leave` (node_membership.go) → `dropPending` (hand-overs waiting for locks go back to their senders) → close containers → `transferLeadership` (leader) → `removeSelf` → `RemovePeer` at leader → `requestStop`. Clients: `leaveAsClient`.
- **Transitions**: `AddFuncTransition`/`AddStateTransition` → `addLocalTransition` → `compileTransitionLocked` → `publishLocked` (copy-on-write) → `pushTransitions`. Remote: `addRemoteTransitions` → `mergeRemoteLocked`.

## Files (v2/)
| File | Key symbols |
|---|---|
| bits.go | `Bits` (immutable ops), `toProto`/`bitsFromProto` |
| bitmaps.go | `LoadBitMapFromDisk`, `BuildBitmap`, `EvaluateEntityProperties`, `ApplyStateToEntity`, `evaluateConditions`, `changedBits`, `mergeState` |
| bitmap_tables.go | `tablesOf` (weak-cached per bitmap), value lookups, field evaluators/setters |
| bitmap_layout.go | wrapperspb values, `bitmapToProto`/`FromProto`, `applyBitmapEntry` (append-only merge) |
| transitions.go | `Transition`, `Condition`, `Operator`, options (`WithPriority`, `WithSaveAfter`, `WithConditions`, `WithBatching`, `WithFuncKey`), `computeSignature`, `compiledKey`, proto conversion |
| container.go | `Container`, `containerProcessing` (`schedule`, `stopRunning`), `start`, `run`, `closeIdle`, `lateError`, `processEvent`, `runEvent`, `Emit`, `emitToTarget`, `Reply`, `waitPathOf`, `lendChain`, `matches(Remote)`, `executeOrQueue`, `routingTransition`, `processRemote`, `executeActions`, `forward`, `forwardTraceOf`, `applyChangedState`, `mergeChangedFields`, `releaseEvent`, `storeSnapshot`, `Event`, `Close`, `NewContainer` |
| node.go | `Node` fields, `NewNode`, `Ready`/`Err`/`fail`, `StartContainer`, `callContainer`, `callTarget`, `createContainer`, `openContainer`, `Shutdown`, `DeleteContainer`, `Add*Transition`, condition helpers (`AtLeastOne`, `All`, `None`, `OneOf`) |
| node_opts.go, container_opts.go, event_opts.go | functional options (`WithDataDir`, `WithTLS`, `WithPublicAddr`, `WithClientMode`, ...; `WithWaitRemote`, `WithDoneChannel`, internal `withCloseAfter`, `withRemoteExecution`) |
| node_transitions.go | registry: compile, `publishLocked`, `publishedTransitions` (`list`, `forEvent`), `entityName`, `addRemoteTransitions`, `syncTransitions`, `pushTransitions`, `Node.forward`, `forwardTo`, `handOverId`, `coverTransitions`, `handOverFailed`, `sendHandOver`, `sendForward` |
| node_forward.go | hand-over queue and ids: `acceptHandOver`, `enqueueForwardedLocked`, `drainForwarded`, `handOverRan`, `failHandOver`, `giveUpForwarded`, `forgetDoneLocked` |
| node_grpc.go | rpcs `Event`, `Join`, `Sync`, `SyncStream`, `stepMessages`, `Propose`, `Get/AddTransitions`, `ForwardEvent`, `forwardOtherEntity`, `addReply`, `callOtherEntity`, `CheckLending`, `changedState`, `sendToContainer` |
| node_holder.go | holder routing: `lockHolder`, `ownsTransitionsOf`, `redirectToHolder`, `forwardTarget`, `targetRequest` |
| node_stream.go | `forwardStream` (call, handOver, failHandOvers, sendLoop, recvLoop, fail), `forwardStreamTo`, `watchLease`, `receiverLeaseEnded`, `lockTable.leaseEnded`, hand-over slots (`takeHandOverSlot`), `newHandOverId`, rpc `ForwardEvents`, `sendReplies` |
| node_raft.go | raft node fields, `startRaft`, `serveGRPC`, `becomeReady`, `proposeLocalBitmap`, `applyLog`, `applyBitmapLog`, ready loop `serveChannels`, `updateLeader`, `stop`, `requestStop`, `applyRemoveNode`, `selfRemoved`, `joinedAt`, `publishEntries`, `getPeerClient` |
| node_propose.go | `propose`, `proposeMany`, `queueProposals`, `queuedProposals`, `takeProposals`, `stopProposer`, `runProposer`, `proposeBatch`, `proposeLogs`, `needsPropose` |
| node_send.go | per-peer senders: `sendMessages`, `runPeerSender`, `sendBatch`, `sendOnStream`, `syncStream`/`readAcks` |
| node_snapshot.go | `snapshotState` (`copySnapshotState`, `marshal`), `restoreSnapshot`, `maybeSnapshot`, `writeSnapshot` |
| node_codec.go | `vtCodec` (gRPC codec: vtprotobuf code of the node's messages, proto otherwise), `codecServerOption`, `codecDialOption` |
| node_async.go | `AsyncStorageWrites` loop: `serveChannelsAsync`, `runAppends`, `appendBatch`, `runApplies`, `deliverResponses`, `storageApply` |
| node_wal.go | `wal` (segments, `save(walAppend...)`, `saveSnapshot`, `setJoinIndex`), `openWAL` recovery, `Node.openWAL` |
| node_wal_linux.go, node_wal_other.go | `walSyncFlag` (write-through open flag of the segments) |
| node_locks.go | `lockTable` (`applyAcquire/Release/Cancel/Done`, `grantNextLocked`, `waitsForLocked`, `putLocked`, `get`, `expiry`), `acquireLock`, `endLock`/`reuseRetained`, `releaseLock`, `lockDone`, `heartbeat`, `applyLockLog`, `lockChanged`, `verifyLending`/`verifyHolder`, async acquires (`acquireAsync` ... `dropPending`), `endPreviousLife` |
| node_membership.go | `leaveOnStop`, `Leave`, `RemoveNode`, rpc `RemovePeer`, `removeAsLeader`, `removeClientAsLeader`, `transferLeadership` |
| node_clients.go | member side: rpc `Attach`, `clientSession`, `addClient`, `fullClientUpdate`, `notifyLock/Refused/Clients/Leader`, `applyClientLog`, `removeClientNode` |
| node_client.go | client side: `startClient`, `attachLoop`, `attach`, `applyClientUpdate` (layout, membership, locks), `watchLock`, `leaveAsClient` |
| node_tls.go | `Role*`, `methodRoles`, `LoadTLSFiles`, `ToolCredentials`, `setSecurityOptions`, `serverOptions`/`authorizeMethod`/`checkPeerCertificate`, `identityStream` (removed callers), `trackingCredentials`/`connTracker`/`trackedConn` (connections by cert, expiry), `closeRemovedConns`, `authorizeHTTP`, `ownName`, `checkProposal`, `dialOption`, `servePublic` |
| node_inspect.go | rpc `Inspect` (node, transitions, layout, locks, containers; `limit`) |
| node_trace.go | `eventTrace` ring buffer, `recordTrace`, `traceRefs`, rpc `WatchEvents` |
| node_metrics.go | `nodeMetrics`, `histogram`, `collectMetrics`, `MetricsHandler`, `prometheusText` (1s cache), `serveMetrics` (HTTPS with TLS), `writePrometheus` |
| logging.go, util.go | `debugf`, `containerLogger`, `raftLogger`; small helpers |
| certs/certs.go | `NewCA`, `Issue`, `TLSCertificate`, `WriteCA`, `WriteFiles` |

## Elsewhere
- `proto/models.proto` (Bits, Bitmap, Transition, NodeLog(s), ContainerLock, NodeSnapshot), `proto/services.proto` (rpcs and messages of `Node`). Generated in `service/`: `*.pb.go`, `*_grpc.pb.go`, `*_vtproto.pb.go` (vtprotobuf: `MarshalVT`, `UnmarshalVT`, `SizeVT`, `CloneVT`, `EqualVT`; raft entries, snapshots, `vtCodec`).
- `devtool/`: `server.go` (polling, SSE), `cmd/asmdev/main.go`, `web/src` (React; `views/*.tsx`, `decode.ts`, `types.ts`).
- `examples/gamemeta/`: `main.go` (roles, flags, `clusterOpts`), `launcher.go`, `node.go` (Player nodes, transitions), `arena.go` (Match node), `simulate.go` (pacing), `store.go` (SQLite), `stats.go`, `gamepb/`.
