import { useEffect, useRef, useState } from 'react'
import { Empty } from '../components'
import type { Metric, NodeState } from '../types'

// The metrics of every node over the last poll interval: counters as rates per second, gauges as they are,
// latency histograms as p50 / p99 of the observations in the interval. Both need two samples; a node that
// restarted (counters went down) needs two new ones.

interface Sample {
  at: number
  metrics: Map<string, Metric>
}

interface Cell {
  text: string
  title?: string
}

// metrics shown, in this order; the others are listed below them
const SHOWN: { name: string; label: string }[] = [
  { name: 'asm_raft_leader', label: 'leader' },
  { name: 'asm_raft_term', label: 'term' },
  { name: 'asm_raft_proposals_total', label: 'proposed logs' },
  { name: 'asm_raft_proposal_batches_total', label: 'proposed batches' },
  { name: 'asm_raft_propose_rpcs_total', label: 'propose rpcs' },
  { name: 'asm_raft_logs_applied_total', label: 'logs applied' },
  { name: 'asm_raft_entries_applied_total', label: 'raft entries applied' },
  { name: 'asm_raft_apply_seconds_total', label: 'apply time (s/s)' },
  { name: 'asm_raft_messages_sent_total', label: 'raft messages sent' },
  { name: 'asm_raft_send_failures_total', label: 'raft send failures' },
  { name: 'asm_raft_commit_index', label: 'commit index' },
  { name: 'asm_raft_applied_index', label: 'applied index' },
  { name: 'asm_lock_acquires_total', label: 'lock acquires' },
  { name: 'asm_lock_joins_total', label: 'joined calls' },
  { name: 'asm_lock_refused_total', label: 'refused calls' },
  { name: 'asm_lock_releases_total', label: 'lock releases' },
  { name: 'asm_lock_wait_seconds', label: 'lock wait' },
  { name: 'asm_locks_held', label: 'locks held' },
  { name: 'asm_locks', label: 'locks' },
  { name: 'asm_lock_waiters', label: 'lock waiters' },
  { name: 'asm_containers_open', label: 'open containers' },
  { name: 'asm_containers_started_total', label: 'containers started' },
  { name: 'asm_events_total', label: 'events' },
  { name: 'asm_event_seconds', label: 'event time' },
  { name: 'asm_forwards_total', label: 'forwards sent' },
  { name: 'asm_forward_errors_total', label: 'forward errors' },
  { name: 'asm_forward_wait_seconds', label: 'waited forward' },
  { name: 'asm_forwards_received_total', label: 'forwards received' },
  { name: 'asm_handovers_queued', label: 'queued hand-overs' },
  { name: 'asm_handover_lock_waits', label: 'hand-overs waiting for a lock' },
  { name: 'go_goroutines', label: 'goroutines' },
  { name: 'go_heap_objects_bytes', label: 'heap' },
  { name: 'go_gc_cpu_seconds_total', label: 'GC CPU (s/s)' },
  { name: 'go_user_cpu_seconds_total', label: 'Go CPU (s/s)' },
]

export function MetricsView({ nodes }: { nodes: NodeState[] }) {
  // the sample before the current one, per node, to compute rates
  const previous = useRef(new Map<string, Sample>())
  const current = useRef(new Map<string, Sample>())
  const [, setTick] = useState(0)

  useEffect(() => {
    let changed = false
    for (const n of nodes) {
      const metrics = n.inspect?.metrics
      if (!metrics) continue
      // a failed poll keeps the last inspection: no new sample
      const at = Date.parse(n.inspected ?? n.updated)
      const cur = current.current.get(n.addr)
      if (cur && cur.at === at) continue
      if (cur) previous.current.set(n.addr, cur)
      current.current.set(n.addr, { at, metrics: new Map(metrics.map((m) => [m.name ?? '', m])) })
      changed = true
    }
    if (changed) setTick((t) => t + 1)
  }, [nodes])

  const shown = nodes.filter((n) => current.current.has(n.addr))
  if (shown.length === 0) return <Empty>No metrics yet.</Empty>

  const known = new Set(SHOWN.map((s) => s.name))
  const others = [...new Set(shown.flatMap((n) => [...current.current.get(n.addr)!.metrics.keys()]))]
    .filter((name) => !known.has(name))
    .sort()
  const rows = [...SHOWN, ...others.map((name) => ({ name, label: name }))]

  return (
    <div>
      <div className="toolbar">
        <span className="muted">counters per second, latencies p50 / p99 of the last poll interval</span>
      </div>
      <table className="table">
        <thead>
          <tr>
            <th>metric</th>
            {shown.map((n) => (
              <th key={n.addr} title={n.error}>
                {n.id ? `node ${n.id}` : n.addr}
                {n.error && <span className="error-text"> (unreachable)</span>}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.name}>
              <td title={current.current.get(shown[0].addr)?.metrics.get(row.name)?.help}>{row.label}</td>
              {shown.map((n) => {
                const cell = metricCell(row.name, current.current.get(n.addr), previous.current.get(n.addr))
                return (
                  <td key={n.addr} className="mono" title={cell.title}>
                    {cell.text}
                  </td>
                )
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function metricCell(name: string, cur?: Sample, prev?: Sample): Cell {
  const m = cur?.metrics.get(name)
  if (!m || !cur) return { text: '—' }
  const p = prev?.metrics.get(name)
  const seconds = prev ? (cur.at - prev.at) / 1000 : 0

  switch (m.kind ?? 'COUNTER') {
    case 'GAUGE':
      return { text: name.endsWith('_bytes') ? bytes(m.value ?? 0) : num(m.value ?? 0) }
    case 'COUNTER': {
      // a counter that went down was reset: the node restarted
      if (!p || seconds <= 0 || (m.value ?? 0) < (p.value ?? 0)) return { text: '…', title: `total ${num(m.value ?? 0)}` }
      const rate = ((m.value ?? 0) - (p.value ?? 0)) / seconds
      return { text: name.includes('seconds') ? rate.toFixed(3) : num(rate), title: `total ${num(m.value ?? 0)}` }
    }
    case 'HISTOGRAM': {
      const title = `${Number(m.count ?? 0)} observations in total`
      // without a previous sample the counts are the lifetime ones, not the interval's
      if (!p) return { text: '…', title }
      const counts = (m.counts ?? []).map(Number)
      const before = (p.counts ?? []).map(Number)
      const delta = counts.map((c, i) => c - (before[i] ?? 0))
      // a bucket that went down was reset: the node restarted
      if (delta.some((d) => d < 0)) return { text: '…', title }
      const total = delta.reduce((a, b) => a + b, 0)
      if (total === 0) return { text: '—', title }
      const bounds = m.bounds ?? []
      return {
        text: `${duration(quantile(bounds, delta, total, 0.5))} / ${duration(quantile(bounds, delta, total, 0.99))}`,
        title: `${total} observations in the interval`,
      }
    }
  }
  return { text: '—' }
}

// quantile interpolates linearly within the bucket holding the quantile; above the last bound it returns Infinity
function quantile(bounds: number[], counts: number[], total: number, q: number): number {
  const rank = q * total
  let seen = 0
  for (let i = 0; i < counts.length; i++) {
    if (seen + counts[i] >= rank && counts[i] > 0) {
      if (i >= bounds.length) return Infinity
      const lower = i === 0 ? 0 : bounds[i - 1]
      return lower + (bounds[i] - lower) * ((rank - seen) / counts[i])
    }
    seen += counts[i]
  }
  return bounds[bounds.length - 1] ?? 0
}

function duration(seconds: number): string {
  if (!isFinite(seconds)) return '>30s'
  if (seconds < 0.001) return `${(seconds * 1e6).toFixed(0)}µs`
  if (seconds < 1) return `${(seconds * 1000).toFixed(1)}ms`
  return `${seconds.toFixed(2)}s`
}

function num(v: number): string {
  if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(2)}M`
  if (Math.abs(v) >= 1e4) return `${(v / 1e3).toFixed(1)}k`
  return Number.isInteger(v) ? String(v) : v.toFixed(1)
}

function bytes(v: number): string {
  if (v >= 1 << 30) return `${(v / (1 << 30)).toFixed(2)} GiB`
  if (v >= 1 << 20) return `${(v / (1 << 20)).toFixed(1)} MiB`
  return `${(v / 1024).toFixed(0)} KiB`
}
