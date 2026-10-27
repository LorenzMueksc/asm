import { useState } from 'react'
import { Empty, NodeSelect } from '../components'
import { leaderOf } from '../decode'
import type { NodeState } from '../types'

export function LocksView({ nodes }: { nodes: NodeState[] }) {
  const [addr, setAddr] = useState(leaderOf(nodes)?.addr ?? '')
  const [text, setText] = useState('')
  const node = nodes.find((n) => n.addr === addr) ?? leaderOf(nodes)
  const locks = (node?.inspect?.locks ?? []).filter((l) => !text || (l.containerId ?? '').includes(text))

  return (
    <div>
      <div className="toolbar">
        <NodeSelect nodes={nodes} value={node?.addr ?? ''} onChange={setAddr} />
        <input placeholder="filter container id" value={text} onChange={(e) => setText(e.target.value)} />
        <span className="muted">
          {node?.inspect?.node?.client
            ? 'client node: only the locks involving it (held, waited for or called in), pushed by its member'
            : 'lock table from the raft log, the same on every member once applied'}
          {(node?.inspect?.lockCount ?? 0) > (node?.inspect?.locks?.length ?? 0) &&
            ` · showing ${node?.inspect?.locks?.length ?? 0} of ${node?.inspect?.lockCount} locks (asmdev -limit)`}
        </span>
      </div>
      {locks.length === 0 ? (
        <Empty>No container is locked.</Empty>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>container</th>
              <th>holder</th>
              <th>token</th>
              <th>queue</th>
              <th>waited calls</th>
            </tr>
          </thead>
          <tbody>
            {locks.map((l) => (
              <tr key={l.containerId}>
                <td className="mono">{l.containerId}</td>
                <td className="mono">node {l.holder}</td>
                <td className="mono small">{l.token}</td>
                <td className="mono small">
                  {(l.waiters ?? []).map((w, i) => (
                    <span key={i} className="queue-item" title={waitTitle(w.token, w.waitPath)}>
                      {i + 1}. node {w.node}
                      {(w.waitPath ?? []).length > 0 && ' (waited)'}
                    </span>
                  ))}
                  {(l.waiters ?? []).length === 0 && <span className="muted">—</span>}
                </td>
                <td className="mono small">
                  {(l.calls ?? []).map((c, i) => (
                    <span key={i} className="queue-item" title={waitTitle(c.token, c.waitPath)}>
                      {c.token}
                    </span>
                  ))}
                  {(l.calls ?? []).length === 0 && <span className="muted">—</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

// waitTitle names the token and the lock tokens of the containers waiting for it
function waitTitle(token?: string, waitPath?: string[]): string {
  if (!waitPath || waitPath.length === 0) return token ?? ''
  return `${token ?? ''} waited for by ${waitPath.join(' > ')}`
}
