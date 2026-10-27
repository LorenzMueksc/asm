# ASM devtool

A web UI for looking into a running ASM cluster: nodes, containers, transitions, the bitmap, container locks and events. It only reads.

```
go run ./devtool/cmd/asmdev -join 127.0.0.1:7101
# open http://127.0.0.1:7700
```

| Flag | Default | |
|---|---|---|
| `-join` | `127.0.0.1:7101` | gRPC address of any node; the others are found through its peers |
| `-listen` | `127.0.0.1:7700` | address of the web UI |
| `-poll` | `1s` | how often the nodes are inspected |
| `-limit` | `500` | locks and containers shown per node, 0 shows all (the counts in the sidebar are the totals) |
| `-ca`, `-cert`, `-key` | | PEM files for a cluster with TLS (`WithTLS`): the CA certificate and a certificate with the observer role; all three or none (asmdev exits otherwise) |

To try it with the demo, run `go run ./examples/gamemeta` (its nodes listen on 127.0.0.1:7101–7105 and keep an event trace) and start asmdev in another terminal with the command the demo prints: its nodes talk mutual TLS, so asmdev needs `-ca`, `-cert` and `-key` (without them every node shows `error reading server preface`; with `-tls=false` the plain command works).

## What you see

- **Cluster**: every node with its raft state (leader, term, commit/applied/snapshot index; client nodes show `client` and the member they are attached to), readiness or failure, open containers (the total, also with `-limit`), own and remote transitions, peers with their last heartbeat and lease, removed nodes. An unreachable node shows its last inspection dimmed, with its time; the leader in the top bar is taken from the reachable nodes.
- **Containers**: the open containers of all nodes with status, pending work, state decoded to property values, waiting batches. Click one to see its entity as JSON and the raw state bits. A container that is processing shows the state it had when its current event started (needs the event trace).
- **Transitions**: all transitions of the cluster ordered by event and priority, with conditions and state targets decoded, func keys, batching, owners, where they are remote, and a warning if the signatures differ between nodes.
- **Bitmap**: the committed layout of every root entity: a grid of its words and bits colored by property, and every property with its values and bit indices.
- **Traces**: every event sent to a container is followed over all nodes: forwards, events emitted by transitions, calls of other entities. Pick a trace to see it as a waterfall of spans (event, forward, received, error) with the node, the container and the timings; click a span for its transitions, routes and payloads. Filter by container, event, node, errors or minimum duration. The traces are built from the events received (the last 20000), so long ago traces drop out. An event links to its traces.
- **Metrics**: per node, counters as rates per second and latencies as p50 / p99 over the last poll interval: raft (proposals, applied entries, apply time), locks (acquires, waits), containers, events, forwards, Go runtime. They need two inspections: `…` until then and after a node restarted (its counters went down).
- **Locks**: the container lock table with holder, token, the queue of waiting nodes and the waited calls of other entities running in the holder's container (hover for the wait path: the lock tokens of the containers waiting for them). Members hold the whole table; a client node only the locks involving it.
- **Events**: the event traces of all nodes, live, newest received first (in the order asmdev received them, not sorted by time). Events processed by containers (which transitions ran, which were routed), forwards (to which node, which transitions), received events (from which node), errors, and containers starting and closing. Filter by kind, node or text, pause (new events are kept, the last 20000), and click an event for its payloads. A duration the node measured as 0 shows as `<0.5ms` (Windows clock resolution).

## Requirements on the nodes

- The nodes serve the `Inspect` and `WatchEvents` rpcs of the `Node` gRPC service; nothing to set up.
- Events need `asm.WithEventTrace(n)` on the nodes: they keep their last `n` entries. It costs one JSON encoding per payload and per processed event, so it's off by default. Without it, everything but the events and the state of busy containers still works.

## How it works

asmdev is a Go server (`devtool/server.go`):
- It connects to the join node, inspects all known nodes every poll interval and adds the peers they report. It logs when a node becomes reachable or unreachable and when watching its events fails or works again.
- It streams the event trace of every node (`WatchEvents`) and resumes after the last entry it saw.
- It serves `GET /api/state` (all nodes with their latest inspection, protojson; an unreachable node keeps its last one, `inspected` is its time) and `GET /api/events` (server-sent events, the last 5000 first). Every event has the id `<epoch>-<counter>`: a browser that reconnects gets only the newer events (`Last-Event-ID`); after a restart of asmdev (another epoch) it gets a `reset` event, drops its events and gets the buffer again. Other `/api` paths answer 404, other methods 405.
- The built UI is embedded with `go:embed`.

## Developing the UI

The UI is React + TypeScript, built with Vite (`devtool/web`):

```
cd devtool/web
npm install
npm run dev     # hot reload on http://localhost:5173, proxies /api to asmdev on 127.0.0.1:7700
npm run build   # type check and build into web/dist, which asmdev embeds
```

Rebuild the UI before `go build`, the Go binary embeds `web/dist` as it is.
