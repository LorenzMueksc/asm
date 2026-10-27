# Tests and numbers

## Conventions
- Ginkgo v2 + Gomega, package `asm` (internal). Suite + helpers in `v2/asm_test.go`: `mockBuilder`, `TestStateEntity`, `TestEvent`, `createSimpleTestBitmap`, `getTestBitmapByType`, `mask` (int → Bits).
- `TestStateEntity`/`TestEvent` are fake protos (`ProtoReflect` nil): no `anypb`. RPC and cluster specs use `wrapperspb` types (`*wrapperspb.StringValue` entity, `stringValueBuilder`, `clusterBitmap`, `freeAddr` in cluster_test.go).
- Label `cluster` = real raft nodes on localhost (slow). Inside `Eventually` polls use `func(g Gomega)`; a plain `Expect` there fails the spec on intermediate states.

## Harness `testCluster` (cluster_multi_test.go)
- `tc.start(id, joinAddr, bitmap, setup)`, `tc.startClient` (cluster_client_test.go), `tc.node(id)`, `tc.addr(id)`, `tc.leader()`, `tc.opts` (all nodes), `tc.nodeOpts(id)` (per node, eg. certificates), `tc.dataDir` (data dir per id, address kept over restarts).
- `tc.stop(id)` closes the stop channel → node leaves. `tc.crash(id)` stops raft + gRPC only (a crashed follower object may still propose over rpc). `tc.kill(id)` = `requestStop` + wait for closed wal (restart specs). `tc.killAll()` in AfterEach when data dirs are used (Windows can't delete open files). `tc.forget(id)` marks a left/removed node stopped (else `tc.leader()` sees `Lead == 0` from its stopped raft). `tc.stopAll()`.
- Helpers: `expectReady`, `sendWaiting`, `peerIds`, `publishedTransition`, `callLog` (cluster_route_test.go, calls per "node/transition"), `goroutinesIn(function)` (stack dump count).

## Spec files
| File | Covers |
|---|---|
| bits_test, bitmaps_test, bitmap_layout_test, transitions_test | Bits, evaluation/apply/merge, tables, layout merge, signatures |
| container_test, container_lifecycle_test | processing, batching, done channel, close-when-idle |
| node_target_test | target calls (`WithTarget`): reply, errors, hand-over, own id; cluster: deadlock refused, run on the holder, replies/errors of waited forwards |
| node_test | node without raft: error func, Leave, stop channel, Emit, stale container pointers, no goroutine for idle containers |
| node_stream_test | forward stream with a fake client stream: hand-overs kept until executed, failures (error reply, broken stream), lease end (`watchLease`), slot limit |
| node_grpc_test | Event rpc |
| node_codec_test | vtCodec: generated code, pooled buffers, proto fallback |
| node_locks_test | lock table: queue, re-acquire, cancel, expiry, removal, deadlock check |
| node_inspect_test, node_metrics_test | Inspect, trace, metrics, `MetricsHandler` + 1s cache |
| node_wal_test | wal: torn tail, damage, replaced entries, snapshot marker, segment deletion, commit clamp, stale snapshot after a received one, batched appends |
| cluster_test | 2 raft nodes |
| cluster_multi_test | 3/5 nodes: layout, sync, forwarding, leader change, joins |
| cluster_lock_test | locking, lease expiry, borrowing, queue fairness, giving up, retention, compaction |
| cluster_forward_test | hand-over: after release, conditions, order, lending |
| cluster_route_test | set cover, exclude, matched nodes, priority, bits vs entity, batching, lend chains, traces, async hand-overs |
| cluster_membership_test | leave, leader leave, RemoveNode, rejoin |
| cluster_client_test | client nodes: locks, waited forwards, failover, leave, RemoveNode |
| cluster_handover_test | durable hand-overs: receiver crash → other owner, Leave gives queued ones back, same id runs once, limit blocks (`pendingHandOvers`) |
| cluster_entities_test | second entity (`*wrapperspb.Int64Value`, `counterNode`), deadlock specs, refused hand-over to the sender's error func, replies across entities, holder routing |
| cluster_wal_test | restart of one member, whole cluster from snapshots, catch-up by a leader snapshot then restart from it, left node stops |
| cluster_tls_test | certificates, refused certs, foreign CA, roles per rpc, own name, admin removal, public listener, removed id rejected + its stream closed, certificate check hook (rpc + dial), metrics HTTPS by role, wait path cap (rpc + `applyAcquire`) (`as(node, id, roles...)`) |
| bench_test | benchmarks (`getBenchCluster(b, size, wal)`; `BenchmarkHandOverParallel` waits until the owner ran all hand-overs) |

## Current numbers (2026-10-23, i7-9700K 8 cores, localhost)
- Benchmarks (2026-10-24 evening, quiet machine; ±10-15% run to run): local event ~2.9 µs (~340k/s); start+close serial ~4.2k/s (3 nodes), 128 goroutines ~129k/s (3) / ~104k/s (5); with WAL ~2.2k/s serial, ~88k/s (3) / ~67k/s (5); waited forward serial ~6.1k/s, 128 goroutines ~148k/s (3) / ~133k/s (5); session (start, 3 forwards, close) ~30k/s (3) / ~27k/s (5), ~39 KB and ~600 allocs per session; hand-over end to end (start, hand over, close, owner runs it) ~47k/s (3) / ~36k/s (5). Idle container ~2.7 KB, no goroutine.
- Demo, 100k online players, TLS on, trace on: no errors; gateways ~9.3k events/s, ~4.5k forwards/s, 50k containers, ~100k goroutines, ~0.9 GB each; members ~3.9k log entries/s, apply 4-5%; progression ~200 goroutines.
- History of numbers: `docs/history/2026-10.md`.
