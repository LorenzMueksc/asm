# Dev tool, trace, tracing, metrics

Code: `node_inspect.go`, `node_trace.go`, `node_metrics.go`, `devtool/`.

## Introspection [U: gRPC + standalone tool, read-only]
- `Inspect`: node info, transitions, layout, locks, containers (busy ones from their snapshot). `InspectRequest.limit` caps locks/containers (sorted by id, `firstIds`); `lock_count`/`container_count` = totals.
- Event trace opt-in `WithEventTrace(n)`: containers get `traceF` only then; the hot path collects names/snapshots only if `traceF != nil`. Entries: EVENT (end of `processEvent`), RECEIVED (end of `processRemote`), FORWARD (`Node.forward`, with routes), ERROR (`reportError` on `run`), CONTAINER_STARTED/CLOSED (`openContainer`/`removeF`).
- With trace on, containers store a snapshot (`storeSnapshot`: entity clone, states, batch sizes; fakes without proto reflection encoded at once) at start and end of processing; `Inspect` encodes it for busy containers (`c.mu.TryLock` fails).
- `WatchEvents`: subscribe before reading the buffer; a watcher whose `after_seq` is ahead (node restarted) gets everything.

## Tracing [U: follow one event, search + filters; no sampling, no OpenTelemetry]
- `containerEvent.trace` (trace id + parent span), `spanId`. `Event()` starts a trace when trace is on (not for forwarded executions). `runEvent` gives the event a span (EVENT entry). `Emit` takes the trace of the calling transition's first traced event (`callEvents`), parent = its span (`spanOrParent`).
- `c.forward` → `forwardTraceOf(events)`: FORWARD span with the events' spans as parents (`traceRefs`), per payload trace with FORWARD as parent (`ForwardEventRequest.traces`). Without trace on a node the refs pass through (`spanOrParent`).
- Receivers: `remoteExecution.traces` (grouped per container). `processRemote` makes one RECEIVED span; each sub-event gets its trace and RECEIVED as `spanId`; a sender without trace → receiver starts one trace.
- Action errors carry their events' traces (`sendTracedError`, `queuedError`); ERROR entry is a span of them.

## Metrics [U: Inspect + Prometheus]
- `nodeMetrics`: counters + latency histograms (`histogram`, `histogramBounds` 100µs-30s) via atomics on hot paths (propose, apply, send, acquire/release, open, processEvent via `Container.metrics`, forward, WAL sync). Gauges in `collectMetrics` (raft indexes, locks, containers, queued hand-overs, `asm_handover_lock_waits`, `asm_handovers_kept`, runtime).
- `Inspect` returns them (`Metric`: counter/gauge/histogram), always freshly collected. `WithMetricsAddr(addr)` serves `/metrics` (Prometheus text, hand-written `writePrometheus`). [U] With `WithTLS`: HTTPS with the node cert, client cert of the CA with role observer or admin (`authorizeHTTP`, see `security.md`); without TLS plain HTTP.
- `MetricsHandler()` [U]: same output, no auth, for own HTTP servers. [A] Output cached `metricsCacheTTL` 1s (`prometheusText`, `nodeMetrics.text`; concurrent scrapes wait for one collection), so scrapes don't each lock the lock table/containers and call raft `Status()`. Inspect not cached.

## Dev tool (`devtool/`)
- `server.go`: `NewServer(join, poll, limit, opts...)`, `WithCredentials` (asmdev `-ca -cert -key` → `asm.LoadTLSFiles` + `asm.ToolCredentials`, observer role; all three or none, else exit). Discovers peers, polls `Inspect` (`-limit` 500), merges `WatchEvents`, serves `GET /api/state`, `GET /api/events` (SSE), SPA fallback (`/api/*` else 404, non-GET 405). `embed.go` embeds `web/dist`.
- Failed poll keeps the last inspection (`inspected` = its time, `error` set). Logs reachable/unreachable changes (first poll too) and `traceErr` changes, not every failure.
- SSE [U, 2026-10-24: event-id counter dedup, arrival order]: buffer 5000 with ids `<epoch>-<counter>` (epoch = start time base36). `Last-Event-ID` of this epoch → only newer events; other epoch or id ahead → `event: reset` + whole buffer. Browser (`api.ts useEvents`) drops ids ≤ last seen, clears on reset/epoch change; `eventId` is the React key (a restarted node reuses `seq`). Arrival order, not sorted by `ts`. `pending` (paused) trimmed to `MAX_EVENTS`.
- `web/`: React 19 + TS 7 + Vite 8. `npm run build` before `go build`. `npm run dev` proxies `/api` to 127.0.0.1:7700.
- Views: cluster, containers (pause), transitions, bitmap, locks, events, metrics (rates and p50/p99 from two inspections), traces (`views/Traces.tsx`: built in the browser from ≤20000 events, filters container/event/node/errors/min duration, waterfall; `#traces/<id>`).
- `leaderOf`: reachable leader, reachable member, any reachable, then stale ones. Unreachable node card: dimmed, last inspection time, no leader accent. Container count = `containerCount` (total). Locks: client nodes hold only locks involving them. Metrics: `…` without a previous sample or after a counter/bucket went down (restart); samples keyed by `inspected`.
- Zero durations are left out by the node (`node_trace.go`; Windows monotonic clock ~0.5ms): EVENT/FORWARD/RECEIVED without one show `<0.5ms` (`decode.ts tookText`).
- Decoding (`src/decode.ts`): bits words are uint64 strings (BigInt); layout values are wrapperspb in Any; protojson omits enum zero values (eg. `AT_LEAST_ONE`) and false/0.
- UI only built and type-checked, not verified in a browser.
