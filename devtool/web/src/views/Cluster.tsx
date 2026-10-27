import { Badge, Empty, List } from '../components'
import { sinceText, timeText } from '../decode'
import type { ClusterState, NodeState } from '../types'

export function ClusterView({ state }: { state: ClusterState }) {
  if (state.nodes.length === 0) return <Empty>No node found at {state.join} yet.</Empty>
  const now = Date.parse(state.time)
  return (
    <div className="grid">
      {state.nodes.map((n) => (
        <NodeCard key={n.addr} node={n} now={now} />
      ))}
    </div>
  )
}

function NodeCard({ node, now }: { node: NodeState; now: number }) {
  const info = node.inspect?.node
  // containers has at most asmdev -limit entries, containerCount is the total
  const containers = node.inspect?.containerCount ?? node.inspect?.containers?.length ?? 0
  // an unreachable node keeps its last inspection: shown dimmed, without claiming to be the leader
  const stale = !!node.error && !!info
  const leader = info?.raftState === 'StateLeader' && !stale
  return (
    <section className={`card ${node.error ? 'card-error' : ''} ${stale ? 'card-stale' : ''}`}>
      <div className="card-head">
        <h3>{node.id ? `node ${node.id}` : node.addr}</h3>
        <span className="mono muted">{node.addr}</span>
      </div>
      <div className="badges">
        {node.error ? (
          <Badge tone="error" title={node.error}>
            unreachable
          </Badge>
        ) : info?.error ? (
          <Badge tone="error" title={info.error}>
            failed
          </Badge>
        ) : info?.ready ? (
          <Badge tone="ok">ready</Badge>
        ) : (
          <Badge tone="warn">starting</Badge>
        )}
        {info?.entity && <Badge tone="accent">{info.entity}</Badge>}
        {info?.skipRaft && <Badge>no raft</Badge>}
        {info?.raftState && (
          <Badge tone={leader ? 'accent' : 'neutral'}>
            {info.raftState.replace('State', '').toLowerCase()}
            {stale && ' (last known)'}
          </Badge>
        )}
        {info?.traceCapacity ? (
          <Badge tone="ok">trace {info.traceCapacity}</Badge>
        ) : (
          <Badge tone="warn" title={node.traceError}>
            no event trace
          </Badge>
        )}
      </div>
      {node.error && <div className="error-text">{node.error}</div>}
      {stale && node.inspected && (
        <div className="muted small">
          last inspection {timeText(node.inspected)} ({sinceText(node.inspected, now)})
        </div>
      )}
      {info?.error && <div className="error-text">{info.error}</div>}
      {info && (
        <>
          <dl className="stats">
            <dt>term</dt>
            <dd>{info.term ?? 0}</dd>
            <dt>leader</dt>
            <dd>{info.leader ? `node ${info.leader}` : '—'}</dd>
            {info.client ? (
              <>
                <dt>attached to</dt>
                <dd className="mono">{info.attachedTo || '—'}</dd>
              </>
            ) : (
              <>
                <dt>commit / applied</dt>
                <dd>
                  {info.commitIndex ?? 0} / {info.appliedIndex ?? 0}
                </dd>
                <dt>snapshot</dt>
                <dd>{info.snapshotIndex ?? 0}</dd>
              </>
            )}
            <dt>containers</dt>
            <dd>
              {containers} / {info.maxContainers ?? 0}
            </dd>
            <dt>transitions</dt>
            <dd>
              {node.inspect?.transitions?.filter((t) => !t.remote).length ?? 0} own,{' '}
              {node.inspect?.transitions?.filter((t) => t.remote).length ?? 0} remote
            </dd>
            <dt>removed</dt>
            <dd>
              <List items={info.removed} />
            </dd>
          </dl>
          <table className="table compact">
            <thead>
              <tr>
                <th>peer</th>
                <th>address</th>
                <th>last heartbeat</th>
                <th>lease</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(info.peers ?? {})
                .sort(([a], [b]) => Number(a) - Number(b))
                .map(([id, addr]) => {
                  const l = info.liveness?.[id]
                  return (
                    <tr key={id}>
                      <td className="mono">{id}</td>
                      <td className="mono">{addr}</td>
                      <td>{sinceText(l?.last, now)}</td>
                      <td>{l?.lease ?? ''}</td>
                    </tr>
                  )
                })}
            </tbody>
          </table>
        </>
      )}
    </section>
  )
}
