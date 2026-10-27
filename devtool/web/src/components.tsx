import type { ReactNode } from 'react'
import { prettyJson, type BitEntry } from './decode'
import type { NodeState } from './types'

export function Badge({ tone = 'neutral', children, title }: { tone?: string; children: ReactNode; title?: string }) {
  return (
    <span className={`badge badge-${tone}`} title={title}>
      {children}
    </span>
  )
}

export function Chips({ entries, empty = '—' }: { entries: BitEntry[]; empty?: string }) {
  if (entries.length === 0) return <span className="muted">{empty}</span>
  return (
    <span className="chips">
      {entries.map((e) => (
        <span key={e.index} className="chip" title={`bit ${e.index}`}>
          <span className="chip-key">{e.path}</span>
          <span className="chip-value">{e.value}</span>
        </span>
      ))}
    </span>
  )
}

export function Json({ text }: { text?: string }) {
  if (!text) return <span className="muted">—</span>
  return <pre className="json">{prettyJson(text)}</pre>
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="empty">{children}</div>
}

export function NodeSelect({
  nodes,
  value,
  onChange,
  all,
}: {
  nodes: NodeState[]
  value: string
  onChange: (v: string) => void
  all?: boolean
}) {
  return (
    <select value={value} onChange={(e) => onChange(e.target.value)}>
      {all && <option value="">all nodes</option>}
      {nodes.map((n) => (
        <option key={n.addr} value={n.addr}>
          {n.id ? `node ${n.id}` : n.addr}{' '}
          {n.error ? '(unreachable)' : n.inspect?.node?.raftState === 'StateLeader' ? '(leader)' : ''}
        </option>
      ))}
    </select>
  )
}

export function NodeName({ node }: { node?: NodeState }) {
  if (!node) return <span className="muted">?</span>
  return <span className="mono">{node.id ? `node ${node.id}` : node.addr}</span>
}

export function List({ items, empty = '—' }: { items?: string[]; empty?: string }) {
  if (!items || items.length === 0) return <span className="muted">{empty}</span>
  return <span className="mono">{items.join(', ')}</span>
}
