import { useMemo, useState } from 'react'
import { Badge, Chips, Empty, Json, NodeName, NodeSelect } from '../components'
import { decode, indexLayout, indices, wordHex } from '../decode'
import type { ContainerInfo, NodeState } from '../types'

interface Row {
  node: NodeState
  container: ContainerInfo
}

export function ContainersView({ nodes: live }: { nodes: NodeState[] }) {
  // while paused, the containers are shown as they were when pausing; polling goes on in the background
  const [frozen, setFrozen] = useState<{ nodes: NodeState[]; at: Date }>()
  const nodes = frozen?.nodes ?? live
  const [nodeFilter, setNodeFilter] = useState('')
  const [text, setText] = useState('')
  const [selected, setSelected] = useState<{ addr: string; id: string }>()

  const indexes = useMemo(() => new Map(nodes.map((n) => [n.addr, indexLayout(n.inspect?.layout)])), [nodes])

  const rows: Row[] = nodes
    .filter((n) => !nodeFilter || n.addr === nodeFilter)
    .flatMap((n) => (n.inspect?.containers ?? []).map((c) => ({ node: n, container: c })))
    .filter((r) => !text || (r.container.id ?? '').includes(text))

  const current = rows.find((r) => r.node.addr === selected?.addr && r.container.id === selected?.id)

  return (
    <div className="split">
      <div className="split-main">
        <div className="toolbar">
          <button className={frozen ? 'active' : ''} onClick={() => setFrozen(frozen ? undefined : { nodes: live, at: new Date() })}>
            {frozen ? 'resume' : 'pause'}
          </button>
          <NodeSelect nodes={nodes} value={nodeFilter} onChange={setNodeFilter} all />
          <input placeholder="filter container id" value={text} onChange={(e) => setText(e.target.value)} />
          <span className="muted">
            {rows.length} containers
            {shownOf(nodes.filter((n) => !nodeFilter || n.addr === nodeFilter))}
            {frozen && ` · paused at ${frozen.at.toLocaleTimeString(undefined, { hour12: false })}`}
          </span>
        </div>
        {rows.length === 0 ? (
          <Empty>No open containers.</Empty>
        ) : (
          <table className="table">
            <thead>
              <tr>
                <th>node</th>
                <th>entity</th>
                <th>container</th>
                <th>status</th>
                <th>pending</th>
                <th>state</th>
                <th>batches</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(({ node, container: c }) => {
                const index = indexes.get(node.addr)!
                const state = Object.entries(c.states ?? {}).flatMap(([entity, bits]) => decode(index, entity, bits))
                const isSelected = node.addr === selected?.addr && c.id === selected?.id
                return (
                  <tr
                    key={`${node.addr}/${c.id}`}
                    className={`clickable ${isSelected ? 'selected' : ''}`}
                    onClick={() => setSelected({ addr: node.addr, id: c.id ?? '' })}
                  >
                    <td>
                      <NodeName node={node} />
                    </td>
                    <td className="mono">{node.inspect?.node?.entity ?? ''}</td>
                    <td className="mono">{c.id}</td>
                    <td>
                      <Badge tone={c.status === 'running' ? 'ok' : 'warn'}>{c.status}</Badge>
                      {c.busy && <Badge tone="accent">busy</Badge>}
                    </td>
                    <td>{c.pending ?? 0}</td>
                    <td>
                      {c.busy && !c.states ? <span className="muted">processing…</span> : <Chips entries={state} />}
                    </td>
                    <td className="mono">
                      {Object.entries(c.batches ?? {})
                        .map(([name, count]) => `${name}: ${count}`)
                        .join(', ') || <span className="muted">—</span>}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>
      {current && (
        <aside className="split-side">
          <ContainerDetail row={current} index={indexes.get(current.node.addr)!} onClose={() => setSelected(undefined)} />
        </aside>
      )}
    </div>
  )
}

function ContainerDetail({
  row,
  index,
  onClose,
}: {
  row: Row
  index: ReturnType<typeof indexLayout>
  onClose: () => void
}) {
  const c = row.container
  return (
    <section className="detail">
      <div className="detail-head">
        <h3 className="mono">{c.id}</h3>
        <button onClick={onClose}>close</button>
      </div>
      <p className="muted">
        on <NodeName node={row.node} />, {c.status}, {c.pending ?? 0} pending
      </p>
      <h4>entity</h4>
      <p className="mono muted">{c.entityType}</p>
      <Json text={c.entityJson} />
      <h4>state</h4>
      {Object.entries(c.states ?? {}).map(([entity, bits]) => (
        <div key={entity} className="state-block">
          <div className="mono">{entity}</div>
          <Chips entries={decode(index, entity, bits)} />
          <div className="muted mono small">
            bits {indices(bits).join(', ') || '—'} · words {wordHex(bits).join(' ') || '—'}
          </div>
        </div>
      ))}
      {Object.keys(c.states ?? {}).length === 0 && <p className="muted">no state yet</p>}
    </section>
  )
}

// shownOf tells how many containers the nodes have when asmdev only got some of them (-limit)
function shownOf(nodes: NodeState[]): string {
  const total = nodes.reduce((sum, n) => sum + (n.inspect?.containerCount ?? 0), 0)
  const got = nodes.reduce((sum, n) => sum + (n.inspect?.containers?.length ?? 0), 0)
  return total > got ? ` of ${total} (asmdev -limit)` : ''
}
