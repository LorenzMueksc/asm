import { Fragment, useState } from 'react'
import { eventKey } from '../api'
import { Badge, Empty, Json, List, NodeSelect } from '../components'
import { BELOW_CLOCK, timeText, tookText } from '../decode'
import type { NodeState, TraceEvent, TraceKind } from '../types'

const KINDS: TraceKind[] = ['EVENT', 'FORWARD', 'RECEIVED', 'ERROR', 'CONTAINER_STARTED', 'CONTAINER_CLOSED']
export const KIND_LABEL: Record<TraceKind, string> = {
  EVENT: 'event',
  FORWARD: 'forward',
  RECEIVED: 'received',
  ERROR: 'error',
  CONTAINER_STARTED: 'started',
  CONTAINER_CLOSED: 'closed',
}
export const KIND_TONE: Record<TraceKind, string> = {
  EVENT: 'ok',
  FORWARD: 'accent',
  RECEIVED: 'accent',
  ERROR: 'error',
  CONTAINER_STARTED: 'neutral',
  CONTAINER_CLOSED: 'neutral',
}
// rows rendered at most, the newest
const MAX_ROWS = 500

export function EventsView({
  nodes,
  events,
  paused,
  waiting,
  onPause,
  onClear,
}: {
  nodes: NodeState[]
  events: TraceEvent[]
  paused: boolean
  waiting: number
  onPause: (p: boolean) => void
  onClear: () => void
}) {
  const [kinds, setKinds] = useState<Set<TraceKind>>(new Set(KINDS))
  const [nodeAddr, setNodeAddr] = useState('')
  const [text, setText] = useState('')
  const [open, setOpen] = useState<string>()

  const nodeId = nodes.find((n) => n.addr === nodeAddr)?.id
  const filtered = events.filter(
    (e) =>
      kinds.has(e.kind as TraceKind) &&
      (!nodeAddr || e.nodeId === nodeId) &&
      (!text ||
        (e.containerId ?? '').includes(text) ||
        (e.eventName ?? '').includes(text) ||
        [...(e.transitions ?? []), ...(e.remoteTransitions ?? [])].some((t) => t.includes(text))),
  )
  const rows = filtered.slice(-MAX_ROWS).reverse()
  const tracing = nodes.filter((n) => n.inspect?.node?.traceCapacity).length

  const toggle = (k: TraceKind) =>
    setKinds((prev) => {
      const next = new Set(prev)
      if (next.has(k)) next.delete(k)
      else next.add(k)
      return next
    })

  return (
    <div>
      <div className="toolbar">
        <button className={paused ? 'active' : ''} onClick={() => onPause(!paused)}>
          {paused ? `resume${waiting ? ` (${waiting} new)` : ''}` : 'pause'}
        </button>
        <button onClick={onClear}>clear</button>
        <NodeSelect nodes={nodes} value={nodeAddr} onChange={setNodeAddr} all />
        <input placeholder="filter container, event, transition" value={text} onChange={(e) => setText(e.target.value)} />
        <span className="kind-filter">
          {KINDS.map((k) => (
            <button key={k} className={kinds.has(k) ? 'active' : ''} onClick={() => toggle(k)}>
              {KIND_LABEL[k]}
            </button>
          ))}
        </span>
        <span className="muted">
          {filtered.length} of {events.length} · {tracing}/{nodes.length} nodes tracing
        </span>
      </div>
      {tracing === 0 && <Empty>No node has an event trace. Start the nodes with asm.WithEventTrace(n).</Empty>}
      {rows.length > 0 && (
        <table className="table events">
          <thead>
            <tr>
              <th>time</th>
              <th>node</th>
              <th>kind</th>
              <th>container</th>
              <th>event</th>
              <th>what happened</th>
              <th>took</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((e) => {
              const key = eventKey(e)
              return (
                <Fragment key={key}>
                  <tr className={`clickable ${open === key ? 'selected' : ''}`} onClick={() => setOpen(open === key ? undefined : key)}>
                    <td className="mono small">{timeText(e.ts)}</td>
                    <td className="mono">{e.nodeId}</td>
                    <td>
                      <Badge tone={KIND_TONE[e.kind as TraceKind] ?? 'neutral'}>{KIND_LABEL[e.kind as TraceKind] ?? e.kind}</Badge>
                      {e.wait && <Badge>wait</Badge>}
                    </td>
                    <td className="mono">{e.containerId}</td>
                    <td className="mono">
                      {e.eventName}
                      {(e.payloads?.length ?? 0) > 1 && <span className="muted"> ×{e.payloads!.length}</span>}
                    </td>
                    <td className="small">
                      <EventSummary e={e} />
                    </td>
                    <td className="small" title={e.duration ? undefined : belowClockTitle(e)}>
                      {tookText(e)}
                    </td>
                  </tr>
                  {open === key && (
                    <tr className="expanded">
                      <td colSpan={7}>
                        <EventDetail e={e} />
                      </td>
                    </tr>
                  )}
                </Fragment>
              )
            })}
          </tbody>
        </table>
      )}
    </div>
  )
}

// belowClockTitle explains a duration shown as BELOW_CLOCK.
export function belowClockTitle(e: TraceEvent): string | undefined {
  return tookText(e) === BELOW_CLOCK ? 'measured as 0: shorter than the clock resolution of the node' : undefined
}

export function EventSummary({ e }: { e: TraceEvent }) {
  switch (e.kind) {
    case 'EVENT':
      return (
        <>
          ran <List items={e.transitions} empty="nothing" />
          {(e.remoteTransitions?.length ?? 0) > 0 && (
            <>
              {' '}
              · routed <List items={e.remoteTransitions} />
            </>
          )}
        </>
      )
    case 'FORWARD':
      return (
        <>
          {(e.routes ?? []).map((r, i) => (
            <span key={i} className={r.error ? 'error-text' : ''}>
              {i > 0 && ' · '}→ node {r.nodeId}: {(r.transitions ?? []).join(', ')}
              {r.error && ` (${r.error})`}
            </span>
          ))}
          {e.error && <span className="error-text"> {e.error}</span>}
        </>
      )
    case 'RECEIVED':
      return (
        <>
          from node {e.fromNode}: ran <List items={e.transitions} empty="nothing" />
        </>
      )
    case 'ERROR':
      return <span className="error-text">{e.error}</span>
    default:
      return null
  }
}

export function EventDetail({ e }: { e: TraceEvent }) {
  return (
    <div className="event-detail">
      <dl className="stats">
        <dt>sequence</dt>
        <dd className="mono">
          node {e.nodeId} #{e.seq}
        </dd>
        {(e.traces?.length ?? 0) > 0 && (
          <>
            <dt>traces</dt>
            <dd className="mono">
              {e.traces!.map((t, i) => (
                <a key={i} href={`#traces/${t.traceId}`} className="trace-link">
                  {t.traceId}
                </a>
              ))}
            </dd>
          </>
        )}
        {e.transitions && (
          <>
            <dt>transitions</dt>
            <dd>
              <List items={e.transitions} />
            </dd>
          </>
        )}
        {e.remoteTransitions && (
          <>
            <dt>remote transitions</dt>
            <dd>
              <List items={e.remoteTransitions} />
            </dd>
          </>
        )}
        {e.excluded && (
          <>
            <dt>excluded</dt>
            <dd>
              <List items={e.excluded} />
            </dd>
          </>
        )}
        {e.error && (
          <>
            <dt>error</dt>
            <dd className="error-text">{e.error}</dd>
          </>
        )}
      </dl>
      {(e.payloads ?? []).map((p, i) => (
        <div key={i}>
          <div className="mono muted small">
            payload {i + 1}: {p.type}
          </div>
          <Json text={p.json} />
        </div>
      ))}
    </div>
  )
}
