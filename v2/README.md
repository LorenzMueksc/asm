# ASM v2

ASM (Adaptive State Machine): a generic, type-safe way to model entity state, transitions and distributed coordination (etcd raft), with protobuf messages for the wire.

Packages of the module `rumue.io/asm`:

- `rumue.io/asm/v2` (package `asm`): this package.
- `rumue.io/asm/v2/certs`: a CA and certificates for tests and development.
- `rumue.io/asm/service`: Go code generated from the .proto files.

## Protobuf definitions

The schemas live in [`proto/models.proto`](../proto/models.proto) (bitsets, bitmaps, transitions, raft log entries, locks, snapshots) and [`proto/services.proto`](../proto/services.proto) (the `Node` gRPC service and its messages). The generated Go code is in `service/` (`models.pb.go`, `services.pb.go`, `services_grpc.pb.go`); never edit it by hand.

To regenerate it, install `protoc`, `protoc-gen-go` v1.36.5 (matching the protobuf runtime in go.mod) and `protoc-gen-go-grpc` v1.5.1, then run from the repository root (add `-I <protoc include dir>` if protoc doesn't find the well-known types):

```
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
protoc -I proto --go_out=service --go_opt=paths=source_relative   --go-grpc_out=service --go-grpc_opt=paths=source_relative proto/models.proto proto/services.proto
```

## Behavior and concepts

ASM v2 centers around a generic Node that manages Containers, Transitions and state encoded as bitsets. Nodes form a raft cluster in which a container's state means the same entity values on every node.

Key concepts:
- Bitmap: Defines the properties and values of every root entity (eg. `Order`). Every value has a bit index within its root entity. See bitmaps.go.
- Bits: A packed bitset (`[]uint64`). Bit `i` is stored in word `i/64`, and a new word is only added once the previous one is full. There is no limit on the number of values.
- Node[T, K]: A generic orchestrator for entities of protobuf type T built by a Builder K. Provides lifecycle and transition APIs, and joins a raft cluster unless `WithSkipRaft()` is set.
- Container: A per-entity runtime container (identified by string id) that holds the current state and runs transitions. Each container processes its events one at a time in its own goroutine.
- Transition: A named rule triggered by an event name that either sets a new state (AddStateTransition) or executes arbitrary code (AddFuncTransition). Transitions have conditions and a priority (higher runs first, may be negative), can batch events, and can trigger the save func.
- Condition: A predicate over one or more entity states, using the operators AT_LEAST_ONE, ALL, NONE, ONE_OF.

How state is tracked:
- The Node maps entities and events to their root entity bitmap using a GetBitmapByTypeFunc.
- `EvaluateEntityProperties(entity, bitmap)` computes the Bits of the property values of the entity. Properties are matched by their json tag.
- Every bitmap gets lookup tables on first use (the bits of every property, a map from value to bit index, the struct fields per type), so evaluating and merging cost about the same for a few hundred or several thousand values. A bitmap must not be changed once it is used.
- Zero values count as values: if `false` or `0` is a value in the bitmap, an unset bool or int field sets that bit.
- Before running transitions, a container merges the bits of the event and then of the current entity into its state. Merging replaces the bits per property, so a property never holds two values at once. The entity is merged last, so its values win over an event using the same bitmap.

How conditions are evaluated (evaluateConditions in bitmaps.go):
- Each Condition contains a map of bitmap name -> list of states (Bits) to check.
- AT_LEAST_ONE: any provided state must be present in the container's current state.
- ALL: all provided states must be present.
- NONE: none of the provided states may be present.
- ONE_OF: exactly one of the provided states must be present.

### The raft log

The raft log carries the bitmap, the container locks, the node heartbeats and the cluster configuration (nodes joining and leaving). Transitions are not in the log; they are synced directly between nodes.

- Log entries are binary protobuf. Every raft entry is a batch (`NodeLogs`): one proposer goroutine per node takes all log entries waiting at that moment and proposes them together; a follower sends the batch to the leader in one `Propose` rpc. While a batch is in flight the next one fills up, so batches grow with the load without delaying single entries.
- An entry is only proposed again when proposing failed (after 1s), the term changed since (it may have been dropped with the old leader), or it wasn't applied for 10s. So a slow log doesn't fill up with duplicates. The leader accepts at most 64 MB of uncommitted entries; proposals beyond fail and are retried.
- Raft messages are sent before the committed entries are applied, so replication doesn't wait for the apply.
- Elections: a node starts one after 2-4s without a heartbeat (100ms ticks), with PreVote, so a node which only missed heartbeats (eg. on a loaded machine) doesn't raise the term while the others still hear the leader.

Raft messages to each peer are sent by a goroutine of that peer with its own queue, over one stream to the peer (`SyncStream`), so a slow or unreachable peer doesn't stall the others. A failed send is retried 3 times with backoff; then the peer is reported unreachable and the messages are dropped, and raft sends them again later. A full queue drops messages the same way.

### The shared bitmap

The committed bitmap is append-only:

- On start, every node proposes each root entity bitmap of its local config (`WithBitmap`/`WithBitmapByPath`). With raft, the children of the root bitmap passed to `WithBitmap` must be root entity bitmaps, as `LoadBitMapFromDisk` returns them.
- The first committed definition of a root entity is adopted with its indices. Later definitions only append values which are missing, each with the next free index. Children are walked in name order, so every node assigns the same indices.
- Indices never move. Nodes listing the same values in a different order, or a subset or superset of them, all use the committed indices.
- A node whose definition conflicts fails: a value of a different type in the same property (eg. `"1"` and `1`), or a property which is nested on one node and has values on another.
- Values are sent as wrapperspb types (String, Int64, UInt64, Bool, Double). Ints are normalized to int64, uints to uint64 and floats to float64.
- Containers always use the committed bitmap (`node.Bitmap()`).

### Container locks

A container id can only be open on one node of the cluster at a time:

- `StartContainer` acquires the lock of the id through the raft log. While another node holds it, the node queues and `StartContainer` waits until the lock is handed to it, or its ctx is done.
- Attempts are queued in the order they reach the log. A released or expired lock goes to the first waiter whose node is still alive (waiters with an expired lease are skipped). A holder asking for the lock again while others wait queues behind them; if nobody waits, it gets it back right away.
- A waiter whose ctx ends cancels its place (`LOCK_CANCEL`). If the lock was already handed to it, the cancel passes it on; a lock handed to an attempt which already gave up is released by its node.
- When the container closes, the node keeps the lock as long as no other node waits for it: opening the container again on that node needs no log entry. Once another node queues for it, the node releases it right away (also when it closes the container while somebody waits). A node leaving the cluster releases its locks on close. Each acquire carries a token and a release only applies with the current token.
- Locks belong to nodes. Every node writes a heartbeat entry every renew interval, which renews all its locks; every other entry of the node renews them as well. A lock expires when its holder's last entry is older than the holder's lease (`WithLeaseTimeout`, default 10s lease and 3s renew interval). Expiry is judged from the timestamps in the log entries, so the clocks of the nodes must be in sync well below the lease.
- A node that loses a lock to another node after its lease expired closes its container for that id.
- Events forwarded with `WithWaitRemote()` borrow the lock: the sender keeps holding it and blocks its container until the receiving nodes have finished. A receiver checks that the sender holds the lock and runs without acquiring it. Without `WithWaitRemote()` the work is handed over and the receiver acquires the lock itself (see below).
- A borrowed lock can be lent on: when a transition on the receiver emits a waited event (`c.Emit`, see below) whose transitions run on a third node, that node borrows the lock through the receiver. Requests carry the chain of lenders; the first one is checked against the lock table, a longer chain by asking its last lender whether it runs on the lent lock right now. Every lender waits, so the holder keeps the lock until the whole chain answered. A chain back to a node already in it is refused, that node would wait for itself. Fire-and-forget events are never lent: the receiver queues for the lock and gets it once the holder released it.
- Waited calls can't deadlock. A waited request carries its wait path: the lock tokens of the containers waiting for the answer, up the chain of waited calls. A receiver of another entity acquires the lock of its container with that path, or joins the open container of the id on its node through the log (`join`). So the lock table knows which containers wait for which: the containers on the path of a queued acquire, or of a call running in the holder's container, wait for the holder. An acquire which would wait for a container on its own path, directly or through other locks, would close a cycle; it is refused (`ErrWouldDeadlock`, `Aborted` for the sender). Every node decides the same from the log, and normal contention keeps waiting. A call ends with `LOCK_DONE`, or with the release of the container's lock.
- Waited calls wait nowhere but the lock table: borrowed starts and waited calls of another entity don't wait for a slot of the max container count (they run without one when all are taken), nor for a start of the same id on their node.
- With `WithSkipRaft()` there are no locks.

### Log compaction

After N applied entries since the last snapshot (`WithSnapshotCount`, default 10000, keeping 5000 entries for slow followers), but at most every 30s (`WithSnapshotInterval`, 0 for no minimum) unless 64 MB of log built up, the log is compacted into a snapshot of the state it builds: the bitmap, the peers, the removed nodes, the locks and the node heartbeats. Followers behind the compacted index and joining nodes receive the snapshot and restore the state from it.

### Persistence

Without a data directory the log and snapshots are kept in memory only, so a node that stopped starts empty and joins again. `WithDataDir(dir)` persists them for a raft member:

- The entries and the raft state of every raft round are written to a write ahead log in `dir/wal` (records with checksums) and are on disk before the node sends the messages that depend on them (the leader sends while it writes, as raft allows). Writing and applying run in their own goroutines (raft's asynchronous storage writes), so raft keeps exchanging messages while the disk writes. The log files are opened write-through (`O_SYNC` on Windows, `O_DSYNC` on Linux) instead of syncing after each write; this relies on the disk honoring write-through (FUA) writes. Snapshots go to `dir/snap`; once a snapshot is written, the log segments it covers are deleted.
- A node starting with a data directory that holds state restarts from it: it restores the snapshot, replays the log after it and catches up with the cluster. It doesn't join again; the join address is ignored. It must use the same id and listen address as before.
- The locks the node held before the restart, its waiting acquires and the calls into its containers are ended once the node has caught up (its containers are gone).
- A torn record at the end of the log (a crash while writing) is cut off. Any other damaged record fails the start.
- A node that left the cluster or was removed stops again on restart. Delete its data directory before it joins again.
- Clients and nodes with `WithSkipRaft()` don't use it.
- Writing costs throughput: on one SSD, a 3-node cluster starts and closes ~90k containers per second with data directories, ~125k without.

### Node startup

- `NewNode` doesn't block. `node.Ready()` is closed once the node's bitmap and a heartbeat are committed, its transitions are compiled against the committed bitmap and exchanged with the other nodes.
- Nodes join the current leader (`WithJoinAddr`). The leader adds joining nodes one after another, because raft only accepts one pending configuration change.
- `node.Err()` receives the error a node failed with (bitmap or transition conflict, failing to join or listen). A failed node stops raft and closes its containers.
- `StartContainer` blocks until the node is ready and returns the failure as error. `Container.Event` returns it too.
- With `WithSkipRaft()` the local bitmap is used directly and the node is ready right away.
- A raft member with a data directory holding state restarts from it instead of joining (see Persistence).

### Client nodes

A node started with `WithClientMode()` is a client: it doesn't run raft and applies no log, so its CPU goes to containers. The raft members (the other nodes) hold the lock table and the bitmap.

- A client attaches to a member (`WithJoinAddr`, a member's address) with the `Attach` stream. The member adds it to the cluster (`CLIENT_ADD` in the log, so every node knows it as a peer for forwarding and transitions) and keeps it up to date: leader and term, the committed bitmap, the nodes of the cluster, their liveness, the locks the client holds, waits for or calls into, and its refused acquires. The client keeps these locks in its own lock table, so containers, locks, lending, waited calls and retention work on it like on a member.
- A client proposes its log entries (locks, heartbeats, its bitmap) through the leader, like a follower.
- To check a lender of a lock it doesn't hold, a client asks its member to watch that lock for a moment.
- If its member fails, the client attaches to another member and gets the full state again.
- `Leave` on a client closes its containers and removes it (`CLIENT_REMOVE`); `RemoveNode` removes a client from any node. Like a removed member, a removed client loses its locks and transitions and stops.

### Leaving the cluster

- `node.Leave(ctx)` removes the node from the cluster and stops it. It closes all containers and waits until they are closed, so their locks are released; `StartContainer` returns `ErrNodeStopped` from then on. A leader hands its leadership to the most up to date peer first. Then the leader removes the node, and the node stops raft, the gRPC server and its containers. `Err()` receives nothing. If ctx ends first, `Leave` returns the error and the node stays a member, still rejecting new containers.
- Closing the stop channel (`WithStopChannel`) runs `Leave` with a 30s timeout. The node stops even if leaving fails.
- `node.RemoveNode(ctx, id)` removes another node, eg. one that crashed. It works on every node, because it goes through the leader (`RemovePeer` rpc), and returns once the leader applied the removal. Removing the own id is `Leave`.
- Applying a removal, every node frees the locks of the removed node right away (no lease wait), drops it as transition owner and rejects its messages and proposals with `PermissionDenied`.
- A removed node that is still running stops: when it applies its own removal, or when a peer rejects it as removed. It doesn't rejoin by itself.
- A node can join again with the id of a node that left. It ignores the removal of the earlier node with its id when it replays the log.

### Transitions

- `AddStateTransition(name, event, toState, opts...)`: when the event occurs and the conditions are satisfied, replaces the bits of every property toState sets and applies the state to the entity. Zero-valued fields of toState are applied too, so list every property you don't want reset. Moving to a lower value of a property (eg. completed back to pending) works.
- `AddFuncTransition(name, event, f, opts...)`: when triggered, invokes `f(ctx, container, payload) (skipSave bool, err error)`. With batching, payload is `[]any` of all batched events. Afterwards the state is evaluated from the entity again (also when f failed), so lower-priority transitions of the event see its changes.
- Conditions are built with the Node helpers `AtLeastOne(...)`, `All(...)`, `None(...)`, `OneOf(...)` from example entities and attached with the `WithConditions(...)` option. Example entities are converted to bits against the committed bitmap, and again whenever it grows.
- `WithBatching(count, period)` collects events until count is reached, or until period passed since the last event. A batch without period waits for count, and runs when the container closes.
- `WithSaveAfter()` runs the save func once after the event, if a transition changed something.
- `c.Emit(ctx, event, payload, opts...)`, called in a func transition with its ctx, processes an event as part of the transition: the container's transitions matching it run right away, remote ones are routed (with `WithWaitRemote()` the transition waits for them and lends the lock on). `c.Event` called in a transition only queues the event, it runs after the current one.
- `c.Emit(ctx, event, payload, WithTarget(id), opts...)` runs the event in the container of another id of the same entity (eg. one trader calling another), on that container's own lock, like a call of another entity: the target runs its own func transitions and its state transitions matching the event; the node starts the target if it isn't open (init func loads it), waiting while another node holds its lock. With `WithWaitRemote()` the transition blocks until they ran: `Emit` returns their errors, `WithReply(func(reply any))` gets what a target transition set with `c.Reply(ctx, reply)` (`ErrNoCaller` if nobody waits). The call carries the caller's wait path, so a call closing a cycle (b calling back a, which waits for b) is refused with `ErrWouldDeadlock`. Without `WithWaitRemote()` the event is handed over to the target and runs once the node got its lock.
- `WithFuncKey(key)` identifies the func of a func transition in its signature. Without it the runtime func name is used (eg. `myapp/handlers.Promote`), which changes for closures when other closures are added or removed around them.

### Transitions in the cluster

Transitions are synced directly between nodes, not through the raft log:

- A node that is ready pulls the transitions of every peer (`GetTransitions`) and pushes its own to every peer (`AddTransitions`). Transitions added later are pushed to all peers immediately. Owners that leave the cluster are removed.
- Every transition has a signature: a hash of its event name, its conditions and its action (the target state, or the func key / func name), described by the example entities, not by their bits. So it doesn't change when the committed bitmap grows, and nodes owning the same transition agree whenever they compile it. Name, priority, batching and save-on-change are not part of it.
- The same name with the same signature on several nodes: all of them own it. The same name with a different signature: the node introducing it fails, the other nodes reject it and keep running.
- State transitions of other nodes run locally on every node.
- Func transitions of other nodes are remote transitions on this node. Events matching them are forwarded (`ForwardEvent`), and **the receiving node decides which of its transitions run**:
  - The sender evaluates the synced conditions of the remote transitions to decide whether and where to forward the event. Remote transitions keep their place in the priority order: the matching ones collected so far are forwarded together before the next local transition of the event is evaluated, so with `WithWaitRemote()` that transition sees their result.
  - Routing: only nodes owning a transition which matched on the sender receive the event. The matched remote transitions are covered with as few nodes as possible. The node owning most of them is picked first, ties are broken randomly, so transitions owned by several nodes are balanced. An unreachable node is skipped and its transitions are covered by other owners.
  - Every request carries an exclude list: the func transitions of the event the sender knows, except the ones assigned to that request. So no transition runs twice, and a receiver doesn't run transitions which didn't match on the sender.
  - The receiver runs its own func transitions matching each forwarded event, except the excluded ones, checking their conditions again against its entity. State transitions and remote transitions don't run on the receiver.
  - Batching: the sender batches forwarded events per event name, with the batching config of the remote transitions listening to that name (the largest count wins, then the longest period), and sends a batch in one request. A batched event joins the batch after its local transitions ran, since a batch can't return a result in time. The receiver passes the events one by one through its transitions: batching ones batch them with their own config, the others run once per event, in order. A receiver batch that isn't full runs when the forwarded container closes after the request.
  - **With the event option `WithWaitRemote()`** the sender lends its lock: it blocks its container, the receivers (called in parallel) start a container with the sender's container id on the lent lock, set the sender's entity and run their transitions. A receiver returns only the bits of the properties it changed (`changed_state`) if applying them to the sent entity gives its entity; otherwise it returns its entity. The sender applies changed bits per property and merges returned entities field by field against the sent entity (nested messages recursively, lists and maps as a whole), then re-evaluates its state. Two receivers changing the same property or field: the later response wins. The receiver's container closes once idle. The save func doesn't run on the sender for remote transitions.
  - **Without it the work is handed over:** the sender continues right after sending it, without waiting for the receivers to accept it. A receiver queues the work per container id and runs it in its own container, which acquires the lock once the sender released it, loads the entity with the receiver's init func (eg. from a shared database), runs the transitions whose conditions match that entity, saves on change and closes. Handed over work of one container id runs in the order it arrived. While the lock is held elsewhere no goroutine waits for it: the receiver proposes the acquire and opens the container when the lock table grants it, so a node can have hand-overs of very many container ids waiting.
  - Hand-overs are durable while the sender lives: the sender keeps each one until the receiver reports that its work ran (also if its transitions failed: those errors stay with the receiver). It is sent again to the other owners of its transitions when the receiver turns out unreachable before that: its stream to the sender breaks (eg. it crashed or stopped), its lease in the lock table ended, it was removed, or it refuses the work (a leaving node refuses new hand-overs and gives the queued ones back). A long wait for the lock is no failure, there is no timeout. After three attempts, and for other refusals, the error goes to the sender's error func, or is logged once the sender's container closed. Delivery is at least once: work sent again may run twice (the first receiver may still run it, eg. after its stream broke) and may overtake later work of the container. Every hand-over has an id: a receiver getting the same work again runs it once, if it still knows the id (it remembers ids of work that ran for a minute). If the sender crashes, its outstanding hand-overs are lost when their receivers fail too.
  - `WithMaxPendingHandOvers(n)` (default 10000) limits the hand-overs a node keeps: at the limit, a container handing work over waits until one of them ran or failed, bounded only by its context. A container handing over more work for its own id than the limit while it is open waits forever, since that work waits for its lock.
  - Errors of the receiver's transitions come back as `Aborted` (with wait) or are logged on the receiver (handed over). A transition that emitted the event with `WithWaitRemote()` gets them as the error of `Emit` (otherwise they go to the error func).
  - Waited calls on the target's own lock (calls of another entity, `WithTarget` calls) run on the node holding the target's lock: the node that would run the call sends it on to the holder once, so the lock of a busy container doesn't move between nodes for every call. That holder must own the transitions the call would run; if the lock is free, or the holder is unreachable, the call runs where it arrived. Hand-overs aren't sent on.
  - Replies: a receiver's transition can answer a waited call with `c.Reply(ctx, msg)` (a protobuf message). The emitting transition gets every reply through `WithReply(func(reply any))` before `Emit` returns, one per container that replied, also from containers of another entity.
  - Transport: forwarded events go over one `ForwardEvents` stream per pair of nodes, not a unary call each. The calls waiting at a moment are sent in one message, the receiver runs each one on its own and sends the replies back in batches. A call the sender gives up on is cancelled on the receiver. A broken stream fails its calls as unreachable, so they go to other owners, and the next call opens a new stream.
  - The entity and the event payloads of forwarded events must be generated protobuf messages.

### Several entities in a cluster

A node has one entity type (`Node[T, K]`), but a cluster can mix nodes of different entity types, eg. players and matches. They share the bitmap (every entity is a root bitmap), the locks and the transitions.

- Every transition knows its entity (the root bitmap name of the node's type). On a node, the transitions of another entity are remote, also state transitions.
- An event can match a transition of another entity. Its conditions on that entity are left to the receiver; conditions on the event are checked by the sender.
- The sender's container id belongs to its own entity, so the receiving node picks the target containers itself: it calls its `WithContainerIdFunc(eventName, payload)` for every payload of the request. The payload must carry what identifies the target (eg. an id field). Payloads with the same id go to one container, in their order; one request can reach several containers. Without the func the request fails with `FailedPrecondition`, an empty id with `InvalidArgument`.
- Those containers run on their own locks, also when the sender waits; nothing is lent. A waiting sender gets no entity back, since it couldn't merge it into its own type. Without wait, the work is handed over like any other.
- Waiting across entities works in both directions, eg. a player waits for its match and the match waits for another player. A waited call that would close a cycle (the match waiting for the player waiting for it, or two containers each waiting for the other's lock) is refused with `Aborted` ("waiting for the lock would deadlock"); the transition's error func gets it, the other calls go on. See Container locks.
- The receiver runs its own func transitions and the state transitions of its entity.

### Container lifecycle

- `StartContainer(ctx, id, opts...)`: creates or returns the running container with the id. If that container is closing, it waits until it is closed and starts a new one. ctx only bounds the waiting (node ready, container lock, max container count); timeouts and request contexts are safe to pass. Don't call it from a transition to wait for another container: that wait is invisible to the lock table and can deadlock. Emit a waited event instead.
- Each container runs on its own context, derived from the node's, which transitions and hooks receive. It is cancelled once the container closed, or when the node stopped or failed. A container lives until it is closed with `Close`, `DeleteContainer` or `Shutdown`.
- `Container.Event(name, payload, opts...)`: queues an event. Returns `ErrContainerClosed` once the container stopped accepting events. `WithDoneChannel(doneC)` signals doneC exactly once, after every matching transition (including batches and waited remote transitions) processed the event, or right away if no transition matches. The send blocks, so use a buffered channel.
- A container has a goroutine only while it has work: one is started when an event, an error or the close signal arrives, processes everything queued, closes the container if asked to, and waits a short keep-alive for more work before it ends (`WithContainerKeepAlive(d)`, default 5ms, 0 ends at once). An idle container costs no goroutine.
- `Container.Close()`: sends the close signal and blocks until the container is closed. After the close signal, the container keeps accepting events until it has no events in the pipeline and is not doing any work. Then it stops accepting events, runs waiting batches and the close func and removes itself from the node. A closed container stays closed: its methods return `ErrContainerClosed`, the next start of the id creates a new one.
- `DeleteContainer(id)`: closes the container with the id, like `Close()`.
- `Shutdown()`: closes all containers.
- `Leave(ctx)`: closes all containers, waits until they are closed and leaves the cluster (see above). Work other nodes handed over and that didn't run yet goes back to them, they hand it to other owners.

### Hooks and options (node_opts.go)

- Lifecycle hooks: `WithInitFunc` (runs once before the first event), `WithSaveFunc`, `WithCloseFunc`.
- `WithErrorFunc(func(ctx, c, err) error)` receives every error of a container: failing actions, save and init funcs, remote transitions, and the close func. Errors are logged as well. It runs on the container's goroutine, serialized with the transitions like one of them, so a slow error func delays events, and calling `c.Close()` from it deadlocks. Returning an error sends the container the close signal (it still finishes pending work). Errors of the close func arrive after the container closed.
- The same hooks can be set per container with the `WithContainer...Func` options of `StartContainer`. They take precedence over the node's.
- `WithBitmap`/`WithBitmapByPath` set the local bitmap config.
- `WithLogger`, `WithDependencies`, `WithMaxContainerCount`, `WithContainerKeepAlive` allow integration and resource tuning. The logger also gets raft's messages (named `raft`).
- `WithJoinAddr`, `WithStopChannel`, `WithSkipRaft`, `WithClientMode` configure the raft cluster. Closing the stop channel leaves the cluster.
- `WithLeaseTimeout(lease, renewInterval)` configures the container locks, `WithMaxPendingHandOvers(n)` the hand-overs kept until they ran, `WithSnapshotCount(count, catchUpEntries)` and `WithSnapshotInterval(interval)` the log compaction, `WithDataDir(dir)` the persistence of a raft member.
- `WithContainerIdFunc` enables the `Event` rpc, `WithPublicAddr(addr)` serves it on a separate listener for callers without a node certificate.
- `WithTLS(ca, cert)` secures the node port with mutual TLS, `WithCertificateCheck(check)` adds an own check of the peers' certificates, `WithMaxConnectionAge(d)` sets how long clients use a connection (default 1h) (see Security).
- `WithMetricsAddr(addr)` serves the node's metrics in the Prometheus text format at `/metrics`: `https://addr/metrics` for observers and admins with TLS, `http://addr/metrics` without. `node.MetricsHandler()` returns the same output as an `http.Handler` without any authorization, for an own HTTP server.

### Dev tool and introspection

- `Inspect` (gRPC) returns the state of a node as it sees the cluster: raft state, peers, liveness, transitions, the committed bitmap, the container locks with their queues, and the open containers with entity, states and waiting batches. A container that is processing isn't waited for; it is reported busy.
- `WithEventTrace(n)` keeps the last `n` trace entries of a node:
  - events processed by its containers: transitions that ran, transitions routed to other nodes;
  - forwards: target nodes with their transitions and errors;
  - events received from other nodes: sender, transitions that ran, excluded ones;
  - container errors, and containers starting and closing.
- Traces: with the trace on, every event sent to a container starts a trace, which follows it over all nodes: through forwards (handed over or waited), events emitted by transitions and calls of other entities. Every EVENT, FORWARD, RECEIVED and ERROR entry is a span (`span_id`, started `duration` before `ts`) of the traces it belongs to (`traces`: trace id and parent span); a batch of events of several traces belongs to all of them. Forward requests carry the trace of every payload. A node without the trace passes the traces on, the next spans attach to the last traced one.
- `WatchEvents` (gRPC, server streaming) sends the trace and then new entries. With the trace on, busy containers show the state they had when their current event started. The trace costs one JSON encoding per payload and per processed event, so it is off by default.
- `InspectRequest.limit` caps the locks and containers returned (sorted by id); `lock_count` and `container_count` have the totals.
- Metrics: every node counts on its hot paths with atomics, `Inspect` returns the metrics and `WithMetricsAddr` serves them for Prometheus (with TLS over HTTPS, for client certificates with the role `observer` or `admin`). The served output is collected at most once per second, so frequent scrapes don't load the node. Counters (`_total`), gauges and latency histograms (`_seconds`, buckets from 100µs to 30s):
  - raft: proposals, propose rpcs to the leader, entries applied and the time spent applying them, messages sent, send failures, commit and applied index, leader;
  - locks: acquires, joined calls, refused calls, releases, lock wait time, locks held, locks in the table, waiting attempts;
  - containers: started, open, events processed and their processing time;
  - forwarding: requests sent, failed, waited request time, requests received, queued hand-overs, container ids whose hand-overs wait for the lock, hand-overs sent and kept until they ran;
  - Go runtime: goroutines, heap, GC cycles, GC and user CPU time.
- `devtool/` is a web UI for all of this (`go run ./devtool/cmd/asmdev -join <node>`), see devtool/README.md.

### Security

Without TLS no rpc is authenticated: any process that reaches a node's port can join the cluster, propose log entries in any node's name, remove nodes, register transitions to receive entities, and read everything. Use `WithTLS` on every node of a cluster that isn't on a fully trusted network.

- `WithTLS(ca, cert)` requires mutual TLS on the node port. Every node and tool has a certificate of the cluster CA: the common name is the node id (or the tool's name), the organizational units are its roles:
  - `member`: a raft member (join, raft messages, serving clients),
  - `client`: a client node (`WithClientMode`, attaching to a member),
  - `admin`: may remove other nodes,
  - `observer`: may call `Inspect` and `WatchEvents` (dev tools) and read the metrics.
- A node's own certificate must name its id and carry `member` or `client` matching its mode; otherwise the node fails at start.
- Every rpc checks the caller's roles, and a caller may only act in its own name: its log entries, lock tokens, raft messages, transitions and forwarded events must carry its id. Removing another node needs `admin`; leaving works with the node's own certificate. Violations return `PermissionDenied`.
- Nodes verify each other's certificates when dialing: the CA, and the node id they expect (or the `member` role for the join address). Host names aren't checked.
- `LoadTLSFiles(caFile, certFile, keyFile)` loads PEM files. Package `rumue.io/asm/v2/certs` creates a CA and certificates for tests and development. Tools use `ToolCredentials(ca, cert)` to dial nodes.
- The `Event` rpc is for callers outside the cluster, which have no node certificate. `WithPublicAddr(addr)` serves it on a separate listener (with TLS, using the node's certificate as server certificate, no client certificate). The other rpcs are unimplemented there. On the node port `Event` needs a certificate of the CA.
- Removing a node (`Leave`, `RemoveNode`) revokes its id: every rpc with a certificate of a removed node id is rejected with `PermissionDenied` except `Join` (a node may join again with its id), its open streams take no more messages, and its connections are closed a second after the removal applied. Tool certificates (no node id) aren't affected.
- `WithCertificateCheck(func(*x509.Certificate) error)` adds an own check, eg. against a revocation list: it gets the peer's certificate, verified against the CA, for every rpc to the node port (an error returns `PermissionDenied`; a node rejected so stops like a removed one), every metrics request, and when the node dials another node (an error fails the handshake). It runs often, so keep it fast.
- Connections end once the client certificate expired; rpcs with an expired certificate return `Unauthenticated`. After `WithMaxConnectionAge(d)` (default 1h, 0 = off) a client opens a new connection for its next rpcs, so a new handshake checks both certificates again; streams on the old connection go on.
- The metrics endpoint (`WithMetricsAddr`) serves HTTPS with the node's certificate and requires a client certificate of the CA with the role `observer` or `admin` (others get 403).
- A waited call carries the lock tokens of the containers waiting for it (its wait path); a path of more than 64 tokens is refused: `ForwardEvent` returns `InvalidArgument`, and every member refuses such a lock acquire the same way.
- Trust model: a `member` or `client` certificate means the node runs trusted code. Raft members are fully trusted: raft doesn't tolerate a malicious member. Accepted risks: an authenticated node can claim a transition name first (the legitimate owner then fails as the introducing node), read the transitions of its peers, put real lock tokens on wait paths (false deadlock refusals), hold any lock, and hand over events to any container id. There is no built-in revocation besides removing the node id; use `WithCertificateCheck` for more.

### Event rpc

The `Event` rpc of the `Node` service sends an event to a container of the receiving node:

- The payload is decoded with the global protobuf registry, so its type must be a generated protobuf message linked into the binary. Unknown types are rejected with `InvalidArgument`.
- The container id comes from the func set with `WithContainerIdFunc(func(eventName string, payload proto.Message) string)`. Without it, requests are rejected with `FailedPrecondition`. An empty id is rejected with `InvalidArgument`.
- With `options.meta["wait"] = "true"`, the rpc responds after the event is fully processed. Otherwise it responds right away.
- In both cases the container gets a close signal once the event is done, and closes when it has no events in the pipeline and is not doing any work. Events sent to the same container in the meantime are still processed.

## Practical examples

Below are minimal examples based on the code and tests in this package. Adjust types and bitmaps to your domain.

Example 1: Define a Bitmap programmatically

```
bitPos := 0
props := &asm.Property{
    Name: "Order", // top-level entity type name
    Properties: []*asm.Property{
        {Name: "status", Values: []any{"pending", "processing", "completed"}},
        {Name: "priority", Values: []any{int64(1), int64(2)}},
        {Name: "express", Values: []any{true, false}},
    },
}
orderBM := asm.BuildBitmap(props, &bitPos)
rootBM := &asm.Bitmap{Name: "root", Children: map[string]*asm.Bitmap{"Order": orderBM}}
```

Example 2: Provide a GetBitmapByTypeFunc

```
getBM := func(entity any, root *asm.Bitmap) *asm.Bitmap {
    if root == nil {
        return nil
    }
    switch entity.(type) {
    case *pb.Order:
        return root.Children["Order"]
    }
    return nil
}
```

Example 3: Use your protobuf entity and a simple Builder

`pb.Order` is a generated protobuf message, so its fields carry json tags like `json:"status,omitempty"`:

```
type OrderBuilder struct{ v *pb.Order }

func (b *OrderBuilder) Build() *pb.Order  { return b.v }
func (b *OrderBuilder) Set(s *pb.Order)   { b.v = s }
func (b *OrderBuilder) IsSet() bool          { return b.v != nil }
```

Example 4: Start a cluster and add transitions

```
newBuilder := func() *OrderBuilder { return &OrderBuilder{} }

// the first node campaigns and becomes the leader
node1 := asm.NewNode[*pb.Order](1, getBM, newBuilder, "127.0.0.1:50051",
    asm.WithBitmap(rootBM),
    asm.WithInitFunc[*pb.Order, *OrderBuilder](func(ctx context.Context, c *asm.Container[*pb.Order, *OrderBuilder]) error {
        // load the entity, eg. from a database
        c.Builder().Set(&pb.Order{Status: "pending", Priority: 1})
        return nil
    }),
)

// Transition that moves to status=processing when event "start" happens,
// only if the order is currently pending. It is synced to all nodes and runs locally everywhere.
// Zero values are applied too, so priority and express are part of the target state.
node1.AddStateTransition(
    "start_processing",
    "start",
    &pb.Order{Status: "processing", Priority: 1},
    asm.WithConditions(node1.All(&pb.Order{Status: "pending"})),
)

// A func transition owned by node1. Other nodes forward matching events to node1.
node1.AddFuncTransition(
    "promote_priority",
    "promote",
    func(ctx context.Context, c *asm.Container[*pb.Order, *OrderBuilder], payload any) (bool, error) {
        s := c.Builder().Build()
        if s == nil {
            return true, nil // nothing changed, skip save
        }
        s.Priority = 2
        return false, nil
    },
    asm.WithConditions(node1.AtLeastOne(&pb.Order{Express: true})),
    asm.WithFuncKey("promote_priority.v1"),
    asm.WithSaveAfter(),
)

// further nodes join the leader
node2 := asm.NewNode[*pb.Order](2, getBM, newBuilder, "127.0.0.1:50052",
    asm.WithBitmap(rootBM),
    asm.WithJoinAddr("127.0.0.1:50051"),
)

select {
case <-node2.Ready():
case err := <-node2.Err():
    panic(err)
}
```

Example 5: Start and interact with a container

```
ctx := context.Background()
c, loaded, err := node2.StartContainer(ctx, "order-123")
if err != nil {
    // the node failed
}
// loaded is true if the container was already running

doneC := make(chan struct{}, 1)
// "promote" is forwarded to node1, WithWaitRemote applies node1's entity to this builder
if err := c.Event("promote", &pb.PromoteEvent{}, asm.WithDoneChannel(doneC), asm.WithWaitRemote()); err != nil {
    // asm.ErrContainerClosed: the container closed in the meantime, start it again
}
<-doneC // the event went through all matching transitions

c.Close() // closes once no events are in the pipeline
```

Example 6: Loading a Bitmap from TOML

```
bm, err := asm.LoadBitMapFromDisk("bitmap.config.toml")
if err != nil { panic(err) }
node3 := asm.NewNode[*pb.Order](3, getBM, newBuilder, "127.0.0.1:50053",
    asm.WithBitmap(bm),
    asm.WithJoinAddr("127.0.0.1:50051"),
)
```

## See also

- The Go implementation in this directory (`v2/`); a map of files and call flows is in `docs/agents/codemap.md`.
- A runnable 7-node demo: `examples/gamemeta`.
- An economic simulation with several entities, target calls and replies: the `econsim` project next to this repository (`../econsim`).
- The dev tool: `devtool/`.
