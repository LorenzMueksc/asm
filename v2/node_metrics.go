package asm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"rumue.io/asm/service"
)

// Metrics of a node: counters and latency histograms, updated on the hot paths with atomics, and gauges,
// read when the metrics are collected. Inspect returns them, WithMetricsAddr serves them in the Prometheus text format
// (HTTPS for observers and admins with WithTLS), MetricsHandler for an own HTTP server.

// metricsCacheTTL is how long the Prometheus text of the metrics is served again: frequent scrapes don't each take
// the locks of the lock table and the containers, nor ask raft for its status
const metricsCacheTTL = time.Second

// histogramBounds are the upper bounds of the latency buckets
var histogramBounds = [...]time.Duration{
	100 * time.Microsecond, 250 * time.Microsecond, 500 * time.Microsecond,
	time.Millisecond, 2500 * time.Microsecond, 5 * time.Millisecond,
	10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond,
	100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond,
	time.Second, 2500 * time.Millisecond, 5 * time.Second, 10 * time.Second, 30 * time.Second,
}

type (
	nodeMetrics struct {
		proposals         atomic.Uint64
		proposalBatches   atomic.Uint64
		proposeRPCs       atomic.Uint64
		entriesApplied    atomic.Uint64
		logsApplied       atomic.Uint64
		applyNanos        atomic.Uint64
		messagesSent      atomic.Uint64
		sendFailures      atomic.Uint64
		lockAcquires      atomic.Uint64
		lockJoins         atomic.Uint64
		lockRefused       atomic.Uint64
		lockReleases      atomic.Uint64
		lockReuses        atomic.Uint64
		lockWait          histogram
		containersStarted atomic.Uint64
		events            atomic.Uint64
		eventTime         histogram
		forwards          atomic.Uint64
		forwardErrors     atomic.Uint64
		forwardWait       histogram
		forwardsReceived  atomic.Uint64
		// waited calls sent on to the node holding the target's lock
		forwardsRedirected atomic.Uint64
		snapshots          atomic.Uint64
		snapshotBytes      atomic.Uint64
		snapshotTime       histogram
		walBytes           atomic.Uint64
		walSync            histogram
		// text caches the Prometheus text output
		text metricsText
	}
	// metricsText is the Prometheus text output collected at a time
	metricsText struct {
		mu   sync.Mutex
		at   time.Time
		body []byte
	}
	// histogram counts observations per bucket of histogramBounds, the last bucket holds the ones above
	histogram struct {
		counts   [len(histogramBounds) + 1]atomic.Uint64
		sumNanos atomic.Uint64
	}
)

func (h *histogram) observe(d time.Duration) {
	i := 0
	for i < len(histogramBounds) && d > histogramBounds[i] {
		i++
	}
	h.counts[i].Add(1)
	h.sumNanos.Add(uint64(max(d, 0)))
}

// runtimeSamples are the runtime metrics collected with the node's metrics
var runtimeSamples = []struct {
	name, help, sample string
	kind               service.Metric_Kind
}{
	{"go_heap_objects_bytes", "memory occupied by live and not yet freed heap objects", "/memory/classes/heap/objects:bytes", service.Metric_GAUGE},
	{"go_gc_cycles_total", "completed GC cycles", "/gc/cycles/total:gc-cycles", service.Metric_COUNTER},
	{"go_gc_cpu_seconds_total", "estimated CPU time spent on GC", "/cpu/classes/gc/total:cpu-seconds", service.Metric_COUNTER},
	{"go_user_cpu_seconds_total", "estimated CPU time spent running Go code", "/cpu/classes/user:cpu-seconds", service.Metric_COUNTER},
}

// collectMetrics returns the metrics of the node.
func (n *Node[T, K]) collectMetrics() []*service.Metric {
	m := &n.metrics
	out := make([]*service.Metric, 0, 40)
	add := func(kind service.Metric_Kind, name, help string, v float64) {
		out = append(out, &service.Metric{Name: name, Help: help, Kind: kind, Value: v})
	}
	counter := func(name, help string, v uint64) { add(service.Metric_COUNTER, name, help, float64(v)) }
	gauge := func(name, help string, v float64) { add(service.Metric_GAUGE, name, help, v) }
	hist := func(name, help string, h *histogram) {
		metric := &service.Metric{Name: name, Help: help, Kind: service.Metric_HISTOGRAM, Sum: float64(h.sumNanos.Load()) / 1e9}
		for _, b := range histogramBounds {
			metric.Bounds = append(metric.Bounds, b.Seconds())
		}
		for i := range h.counts {
			c := h.counts[i].Load()
			metric.Counts = append(metric.Counts, c)
			metric.Count += c
		}
		out = append(out, metric)
	}

	counter("asm_raft_proposals_total", "log entries this node proposed", m.proposals.Load())
	counter("asm_raft_proposal_batches_total", "batches of log entries this node proposed, each one raft entry", m.proposalBatches.Load())
	counter("asm_raft_propose_rpcs_total", "propose rpcs this node sent to the leader", m.proposeRPCs.Load())
	counter("asm_raft_entries_applied_total", "committed raft entries applied, each a batch of log entries", m.entriesApplied.Load())
	counter("asm_raft_logs_applied_total", "committed log entries applied", m.logsApplied.Load())
	add(service.Metric_COUNTER, "asm_raft_apply_seconds_total", "time spent applying committed log entries", float64(m.applyNanos.Load())/1e9)
	counter("asm_raft_messages_sent_total", "raft messages sent to other nodes", m.messagesSent.Load())
	counter("asm_raft_send_failures_total", "raft message batches which could not be sent", m.sendFailures.Load())
	if n.node != nil {
		st := n.node.Status()
		gauge("asm_raft_commit_index", "index of the last committed log entry", float64(st.Commit))
		gauge("asm_raft_applied_index", "index of the last applied log entry", float64(st.Applied))
		leader := 0.0
		if st.Lead == uint64(n.id) {
			leader = 1
		}
		gauge("asm_raft_leader", "1 if this node is the leader", leader)
		gauge("asm_raft_term", "raft term", float64(st.Term))
	}
	counter("asm_raft_snapshots_total", "snapshots of the state the log builds, taken to compact the log", m.snapshots.Load())
	gauge("asm_raft_snapshot_bytes", "size of the last snapshot", float64(m.snapshotBytes.Load()))
	hist("asm_raft_snapshot_seconds", "time to take a snapshot and compact the log", &m.snapshotTime)
	counter("asm_wal_bytes_total", "bytes written to the wal and snapshot files (WithDataDir)", m.walBytes.Load())
	hist("asm_wal_sync_seconds", "time to write the wal through to the disk (WithDataDir)", &m.walSync)

	counter("asm_lock_acquires_total", "locks this node got", m.lockAcquires.Load())
	counter("asm_lock_joins_total", "waited calls of another entity which joined an open container of this node", m.lockJoins.Load())
	counter("asm_lock_refused_total", "waited calls refused because waiting would deadlock", m.lockRefused.Load())
	counter("asm_lock_releases_total", "locks this node released", m.lockReleases.Load())
	counter("asm_lock_reuses_total", "containers opened on a lock this node kept, without a log entry", m.lockReuses.Load())
	gauge("asm_locks_retained", "locks this node keeps after their containers closed, until another node asks", float64(n.retainedCount()))
	hist("asm_lock_wait_seconds", "time from asking for a lock until getting it or joining its container", &m.lockWait)
	held, total, waiters := n.locks.stats(n.Id())
	gauge("asm_locks_held", "locks this node holds", float64(held))
	gauge("asm_locks", "locks in the lock table", float64(total))
	gauge("asm_lock_waiters", "attempts waiting in the lock queues", float64(waiters))

	counter("asm_containers_started_total", "containers started", m.containersStarted.Load())
	n.containerMap.mu.RLock()
	open := len(n.containerMap.m)
	n.containerMap.mu.RUnlock()
	gauge("asm_containers_open", "open containers", float64(open))
	counter("asm_events_total", "events processed by the containers of this node, forwarded ones included", m.events.Load())
	hist("asm_event_seconds", "time to process an event, waiting for other nodes included", &m.eventTime)

	counter("asm_forwards_total", "forward requests sent to other nodes", m.forwards.Load())
	counter("asm_forward_errors_total", "forward requests which failed", m.forwardErrors.Load())
	hist("asm_forward_wait_seconds", "time a waited forward request took", &m.forwardWait)
	counter("asm_forwards_received_total", "forward requests received from other nodes", m.forwardsReceived.Load())
	counter("asm_forwards_redirected_total", "waited calls sent on to the node holding the target's lock", m.forwardsRedirected.Load())
	gauge("asm_handovers_queued", "handed over executions waiting in the queues of this node", float64(n.queuedHandovers()))
	gauge("asm_handover_lock_waits", "container ids whose handed over executions wait for the lock", float64(n.pendingCount()))
	gauge("asm_handovers_kept", "hand-overs this node sent and keeps until their receivers ran them", float64(n.pendingHandOvers()))

	gauge("go_goroutines", "goroutines", float64(runtime.NumGoroutine()))
	samples := make([]metrics.Sample, len(runtimeSamples))
	for i, s := range runtimeSamples {
		samples[i].Name = s.sample
	}
	metrics.Read(samples)
	for i, s := range runtimeSamples {
		var v float64
		switch samples[i].Value.Kind() {
		case metrics.KindUint64:
			v = float64(samples[i].Value.Uint64())
		case metrics.KindFloat64:
			v = samples[i].Value.Float64()
		default:
			continue
		}
		add(s.kind, s.name, s.help, v)
	}

	return out
}

// queuedHandovers returns the number of handed over executions waiting in the queues.
func (n *Node[T, K]) queuedHandovers() int {
	n.forwarded.mu.Lock()
	defer n.forwarded.mu.Unlock()
	count := 0
	for _, q := range n.forwarded.queues {
		count += len(q.items)
	}
	return count
}

// pendingCount returns the number of asynchronous acquires waiting for their lock.
func (n *Node[T, K]) pendingCount() int {
	n.pending.mu.Lock()
	defer n.pending.mu.Unlock()
	return len(n.pending.byId)
}

// stats returns the number of locks the node holds, the number of locks and the number of waiting attempts.
func (lt *lockTable) stats(nodeId string) (held, total, waiters int) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	for _, l := range lt.locks {
		if l.holder == nodeId {
			held++
		}
		waiters += len(l.queue)
	}
	return held, len(lt.locks), waiters
}

// MetricsHandler returns a handler serving the metrics of the node in the Prometheus text format, without any
// authorization: for an own HTTP server, which secures it as needed. The output is collected at most once per second.
func (n *Node[T, K]) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write(n.prometheusText())
	})
}

// prometheusText returns the metrics in the Prometheus text format, collected again once metricsCacheTTL passed.
// Concurrent scrapes wait for one collection.
func (n *Node[T, K]) prometheusText() []byte {
	t := &n.metrics.text
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.body == nil || time.Since(t.at) >= metricsCacheTTL {
		var buf bytes.Buffer
		writePrometheus(&buf, n.collectMetrics())
		t.body, t.at = buf.Bytes(), time.Now()
	}
	return t.body
}

// serveMetrics serves the metrics in the Prometheus text format on addr until the node stops. With TLS it serves
// HTTPS with the node's certificate to observers and admins.
func (n *Node[T, K]) serveMetrics(addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		n.logger.Error(fmt.Sprintf("failed to listen for metrics on %s: %v", addr, err))
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", n.authorizeHTTP(n.MetricsHandler(), RoleObserver, RoleAdmin))
	n.metricsServer = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	if n.tls != nil {
		n.metricsServer.TLSConfig = n.tls.serverConfig()
	}
	go func() {
		var err error
		if n.tls != nil {
			err = n.metricsServer.ServeTLS(l, "", "")
		} else {
			err = n.metricsServer.Serve(l)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			n.logger.Error(fmt.Sprintf("metrics server stopped: %v", err))
		}
	}()
}

// writePrometheus writes the metrics in the Prometheus text exposition format.
func writePrometheus(w io.Writer, ms []*service.Metric) {
	for _, m := range ms {
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n", m.GetName(), m.GetHelp())
		switch m.GetKind() {
		case service.Metric_COUNTER:
			_, _ = fmt.Fprintf(w, "# TYPE %s counter\n%s %s\n", m.GetName(), m.GetName(), formatFloat(m.GetValue()))
		case service.Metric_GAUGE:
			_, _ = fmt.Fprintf(w, "# TYPE %s gauge\n%s %s\n", m.GetName(), m.GetName(), formatFloat(m.GetValue()))
		case service.Metric_HISTOGRAM:
			_, _ = fmt.Fprintf(w, "# TYPE %s histogram\n", m.GetName())
			var cumulative uint64
			for i, b := range m.GetBounds() {
				cumulative += m.GetCounts()[i]
				_, _ = fmt.Fprintf(w, "%s_bucket{le=\"%s\"} %d\n", m.GetName(), formatFloat(b), cumulative)
			}
			_, _ = fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %s\n%s_count %d\n",
				m.GetName(), m.GetCount(), m.GetName(), formatFloat(m.GetSum()), m.GetName(), m.GetCount())
		}
	}
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}
