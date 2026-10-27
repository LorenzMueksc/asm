import { useMemo, useState } from 'react'
import { Badge, Chips, Empty, List } from '../components'
import { decode, describeCondition, indexLayout, leaderOf } from '../decode'
import type { InspectTransition, NodeState } from '../types'

interface Merged {
  name: string
  // the transition as the nodes see it, by node id
  byNode: Map<string, InspectTransition>
}

export function TransitionsView({ nodes }: { nodes: NodeState[] }) {
  const [text, setText] = useState('')
  const index = useMemo(() => indexLayout(leaderOf(nodes)?.inspect?.layout), [nodes])

  const merged = useMemo(() => {
    const byName = new Map<string, Merged>()
    for (const n of nodes) {
      for (const t of n.inspect?.transitions ?? []) {
        const name = t.transition?.name ?? '?'
        if (!byName.has(name)) byName.set(name, { name, byNode: new Map() })
        byName.get(name)!.byNode.set(n.id ?? n.addr, t)
      }
    }
    return [...byName.values()].sort((a, b) => {
      const ta = first(a).transition
      const tb = first(b).transition
      return (
        (ta?.eventName ?? '').localeCompare(tb?.eventName ?? '') ||
        (tb?.priority ?? 0) - (ta?.priority ?? 0) ||
        a.name.localeCompare(b.name)
      )
    })
  }, [nodes])

  const rows = merged.filter(
    (m) => !text || m.name.includes(text) || (first(m).transition?.eventName ?? '').includes(text),
  )

  return (
    <div>
      <div className="toolbar">
        <input placeholder="filter name or event" value={text} onChange={(e) => setText(e.target.value)} />
        <span className="muted">{rows.length} transitions, ordered by event and priority</span>
      </div>
      {rows.length === 0 ? (
        <Empty>No transitions.</Empty>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>event</th>
              <th>entity</th>
              <th>transition</th>
              <th>prio</th>
              <th>conditions</th>
              <th>action</th>
              <th>batching</th>
              <th>owners</th>
              <th>signature</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((m) => {
              const it = first(m)
              const t = it.transition ?? {}
              const signatures = new Set([...m.byNode.values()].map((x) => x.transition?.signature))
              const owners = new Set([...m.byNode.values()].flatMap((x) => x.nodeIds ?? []))
              const remoteOn = [...m.byNode.entries()].filter(([, x]) => x.remote).map(([id]) => id)
              const funcKey = [...m.byNode.values()].map((x) => x.funcKey).find(Boolean)
              const count = Number(t.batching?.count ?? 0)
              return (
                <tr key={m.name}>
                  <td className="mono">{t.eventName}</td>
                  <td className="mono">{t.entity ?? ''}</td>
                  <td>
                    <div className="mono strong">{m.name}</div>
                    {t.saveOnChange && <Badge>save on change</Badge>}
                  </td>
                  <td>{t.priority ?? 0}</td>
                  <td className="mono small">
                    {(t.conditions ?? []).map((c, i) => (
                      <div key={i}>{describeCondition(index, c)}</div>
                    ))}
                    {(t.conditions ?? []).length === 0 && <span className="muted">always</span>}
                  </td>
                  <td>
                    {t.state ? (
                      <>
                        <Badge tone="accent">state</Badge> <Chips entries={decode(index, t.state.bitmap ?? '', t.state.bits)} />
                      </>
                    ) : (
                      <>
                        <Badge tone="ok">func</Badge> <span className="mono small">{funcKey ?? ''}</span>
                      </>
                    )}
                  </td>
                  <td className="small">
                    {count > 1 ? `${count} events` : <span className="muted">—</span>}
                    {count > 1 && t.batching?.period && t.batching.period !== '0s' ? ` / ${t.batching.period}` : ''}
                  </td>
                  <td>
                    <List items={[...owners].sort()} />
                    {remoteOn.length > 0 && <div className="muted small">remote on {remoteOn.sort().join(', ')}</div>}
                  </td>
                  <td className="mono small">
                    {[...signatures].map((s) => (s ?? '').slice(0, 10)).join(' ')}
                    {signatures.size > 1 && <Badge tone="error">differs</Badge>}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      )}
    </div>
  )
}

function first(m: Merged): InspectTransition {
  return m.byNode.values().next().value ?? {}
}
