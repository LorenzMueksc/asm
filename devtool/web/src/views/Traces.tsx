import { useMemo, useState } from 'react'
import { eventKey } from '../api'
import { Badge, Empty, NodeSelect } from '../components'
import { BELOW_CLOCK, tookText } from '../decode'
import type { NodeState, TraceEvent, TraceKind } from '../types'
import { belowClockTitle, EventDetail, EventSummary, KIND_LABEL, KIND_TONE } from './Events'

// A trace follows one event sent to a container through forwards, emitted events and calls of other entities over
// all nodes. Every EVENT, FORWARD, RECEIVED and ERROR entry is a span of the traces it belongs to, linked to its
// parent span. The traces are built in the browser from the events received so far.

interface Span {
  event: TraceEvent
  parent: string
  start: number
  end: number
}

interface Trace {
  id: string
  spans: Span[]
  root: Span
  start: number
  end: number
  nodes: string[]
  error: boolean
}

// traces listed at most, the newest
const MAX_TRACES = 300

export function TracesView({ nodes, events, selected }: { nodes: NodeState[]; events: TraceEvent[]; selected?: string }) {
  const [container, setContainer] = useState('')
  const [eventName, setEventName] = useState('')
  const [nodeAddr, setNodeAddr] = useState('')
  const [errorsOnly, setErrorsOnly] = useState(false)
  const [minMs, setMinMs] = useState('')

  const traces = useMemo(() => buildTraces(events), [events])
  const nodeId = nodes.find((n) => n.addr === nodeAddr)?.id
  const min = parseFloat(minMs)

  const filtered = traces.filter(
    (t) =>
      (!container || t.spans.some((s) => (s.event.containerId ?? '').includes(container))) &&
      (!eventName || t.spans.some((s) => (s.event.eventName ?? '').includes(eventName))) &&
      (!nodeId || t.nodes.includes(nodeId)) &&
      (!errorsOnly || t.error) &&
      (Number.isNaN(min) || t.end - t.start >= min),
  )
  const current = traces.find((t) => t.id === selected)

  return (
    <div>
      <div className="toolbar">
        <input placeholder="container" value={container} onChange={(e) => setContainer(e.target.value)} />
        <input placeholder="event" value={eventName} onChange={(e) => setEventName(e.target.value)} />
        <NodeSelect nodes={nodes} value={nodeAddr} onChange={setNodeAddr} all />
        <label className="muted">
          <input type="checkbox" checked={errorsOnly} onChange={(e) => setErrorsOnly(e.target.checked)} /> errors
        </label>
        <input className="narrow" placeholder="min ms" value={minMs} onChange={(e) => setMinMs(e.target.value)} />
        <span className="muted">
          {filtered.length} of {traces.length} traces from the events received
        </span>
      </div>
      {traces.length === 0 && (
        <Empty>No traces yet. Traces need asm.WithEventTrace(n) on the nodes; every event sent to a container starts one.</Empty>
      )}
      <div className="traces">
        <div className="trace-list">
          <table className="table">
            <thead>
              <tr>
                <th>started</th>
                <th>event</th>
                <th>container</th>
                <th>nodes</th>
                <th>spans</th>
                <th>took</th>
              </tr>
            </thead>
            <tbody>
              {filtered.slice(0, MAX_TRACES).map((t) => (
                <tr
                  key={t.id}
                  className={`clickable ${t.id === selected ? 'selected' : ''}`}
                  onClick={() => (window.location.hash = `traces/${t.id}`)}
                >
                  <td className="mono small">{clock(t.start)}</td>
                  <td className="mono">
                    {t.error && <Badge tone="error">error</Badge>} {t.root.event.eventName}
                  </td>
                  <td className="mono">{t.root.event.containerId}</td>
                  <td className="mono small">{t.nodes.join(', ')}</td>
                  <td className="small">{t.spans.length}</td>
                  <td className="small">{t.end > t.start ? ms(t.end - t.start) : BELOW_CLOCK}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="trace-view">
          {current ? <Waterfall trace={current} /> : <Empty>Pick a trace.</Empty>}
        </div>
      </div>
    </div>
  )
}

function Waterfall({ trace }: { trace: Trace }) {
  const [open, setOpen] = useState<string>()
  const rows = useMemo(() => treeOrder(trace), [trace])
  const total = Math.max(trace.end - trace.start, 0.001)

  return (
    <div>
      <div className="toolbar">
        <span className="mono">trace {trace.id}</span>
        <span className="muted">
          {trace.spans.length} spans on {trace.nodes.length} nodes · {trace.end > trace.start ? ms(total) : BELOW_CLOCK}
        </span>
      </div>
      <table className="table waterfall">
        <tbody>
          {rows.map(({ span, depth }) => {
            const key = eventKey(span.event)
            const kind = span.event.kind as TraceKind
            const left = ((span.start - trace.start) / total) * 100
            const width = Math.max(((span.end - span.start) / total) * 100, 0.4)
            return (
              <FragmentRow key={key} open={open === key} onToggle={() => setOpen(open === key ? undefined : key)} span={span}>
                <td className="span-label" style={{ paddingLeft: 8 + depth * 16 }}>
                  <Badge tone={KIND_TONE[kind] ?? 'neutral'}>{KIND_LABEL[kind] ?? kind}</Badge>{' '}
                  <span className="mono">node {span.event.nodeId}</span> <span className="mono">{span.event.eventName}</span>{' '}
                  <span className="muted mono small">{span.event.containerId}</span>
                </td>
                <td className="span-bar-cell">
                  <div className="span-track">
                    <div
                      className={`span-bar span-${kind?.toLowerCase()} ${span.event.error ? 'span-error' : ''}`}
                      style={{ left: `${left}%`, width: `${width}%` }}
                      title={`${spanTook(span)} at +${ms(span.start - trace.start)}`}
                    />
                  </div>
                </td>
                <td className="small" title={span.event.duration ? undefined : belowClockTitle(span.event)}>
                  {spanTook(span)}
                </td>
              </FragmentRow>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function FragmentRow({
  children,
  open,
  onToggle,
  span,
}: {
  children: React.ReactNode
  open: boolean
  onToggle: () => void
  span: Span
}) {
  return (
    <>
      <tr className={`clickable ${open ? 'selected' : ''}`} onClick={onToggle}>
        {children}
      </tr>
      {open && (
        <tr className="expanded">
          <td colSpan={3}>
            <div className="small">
              <EventSummary e={span.event} />
            </div>
            <EventDetail e={span.event} />
          </td>
        </tr>
      )}
    </>
  )
}

// buildTraces groups the spans of the events by trace, newest trace first.
export function buildTraces(events: TraceEvent[]): Trace[] {
  const byId = new Map<string, Span[]>()
  for (const e of events) {
    if (!e.spanId || !e.traces) continue
    const end = Date.parse(e.ts ?? '')
    const start = end - seconds(e.duration) * 1000
    for (const ref of e.traces) {
      if (!ref.traceId) continue
      let spans = byId.get(ref.traceId)
      if (!spans) {
        spans = []
        byId.set(ref.traceId, spans)
      }
      spans.push({ event: e, parent: ref.parentSpanId ?? '', start, end })
    }
  }

  const traces: Trace[] = []
  for (const [id, spans] of byId) {
    const start = Math.min(...spans.map((s) => s.start))
    const end = Math.max(...spans.map((s) => s.end))
    const root = spans.find((s) => !s.parent) ?? spans.reduce((a, b) => (b.start < a.start ? b : a))
    traces.push({
      id,
      spans,
      root,
      start,
      end,
      nodes: [...new Set(spans.map((s) => s.event.nodeId ?? '?'))].sort(),
      error: spans.some((s) => s.event.kind === 'ERROR' || !!s.event.error),
    })
  }
  return traces.sort((a, b) => b.start - a.start)
}

// treeOrder lists the spans depth first, children by start time; spans whose parent isn't known are roots.
function treeOrder(trace: Trace): { span: Span; depth: number }[] {
  const ids = new Set(trace.spans.map((s) => s.event.spanId))
  const children = new Map<string, Span[]>()
  const roots: Span[] = []
  for (const s of trace.spans) {
    if (s.parent && ids.has(s.parent)) {
      const list = children.get(s.parent) ?? []
      list.push(s)
      children.set(s.parent, list)
    } else {
      roots.push(s)
    }
  }
  const out: { span: Span; depth: number }[] = []
  const visit = (s: Span, depth: number) => {
    out.push({ span: s, depth })
    const list = (children.get(s.event.spanId ?? '') ?? []).sort((a, b) => a.start - b.start)
    for (const c of list) visit(c, depth + 1)
  }
  for (const r of roots.sort((a, b) => a.start - b.start)) visit(r, 0)
  return out
}

// spanTook is the duration of a span; spans of a kind with a duration that came without one were too short to measure
function spanTook(span: Span): string {
  return span.event.duration ? ms(span.end - span.start) : tookText(span.event)
}

function seconds(d?: string): number {
  const s = parseFloat(d ?? '')
  return Number.isNaN(s) ? 0 : s
}

function ms(v: number): string {
  if (v < 1) return `${Math.round(v * 1000)}µs`
  if (v < 1000) return `${v.toFixed(1)}ms`
  return `${(v / 1000).toFixed(2)}s`
}

function clock(t: number): string {
  const d = new Date(t)
  return d.toLocaleTimeString(undefined, { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0')
}
