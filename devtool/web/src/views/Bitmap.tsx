import { useState } from 'react'
import { Empty, NodeSelect } from '../components'
import { leaderOf, properties } from '../decode'
import type { Bitmap, NodeState } from '../types'

// colors of the properties in the bit grid
const PALETTE = ['#4f8cff', '#38b2ac', '#d69e2e', '#e5534b', '#9f7aea', '#48bb78', '#ed64a6', '#ed8936', '#4fd1c5', '#a0aec0']

export function BitmapView({ nodes }: { nodes: NodeState[] }) {
  const [addr, setAddr] = useState(leaderOf(nodes)?.addr ?? '')
  const node = nodes.find((n) => n.addr === addr) ?? leaderOf(nodes)
  const layout = node?.inspect?.layout

  return (
    <div>
      <div className="toolbar">
        <NodeSelect nodes={nodes} value={node?.addr ?? ''} onChange={setAddr} />
        <span className="muted">committed layout as this node sees it, indices never move</span>
      </div>
      {!layout || Object.keys(layout.children ?? {}).length === 0 ? (
        <Empty>No committed bitmap yet.</Empty>
      ) : (
        Object.keys(layout.children ?? {})
          .sort()
          .map((entity) => <EntityBitmap key={entity} name={entity} bitmap={layout.children![entity]} />)
      )}
    </div>
  )
}

function EntityBitmap({ name, bitmap }: { name: string; bitmap: Bitmap }) {
  const props = properties(bitmap)
  const all = props.flatMap((p, i) => p.values.map((v) => ({ ...v, color: PALETTE[i % PALETTE.length] })))
  const highest = all.reduce((max, v) => Math.max(max, v.index), -1)
  const words = Math.floor(highest / 64) + 1
  const byIndex = new Map(all.map((v) => [v.index, v]))

  return (
    <section className="card wide">
      <div className="card-head">
        <h3 className="mono">{name}</h3>
        <span className="muted">
          {props.length} properties · {all.length} values · {words} word{words === 1 ? '' : 's'}
        </span>
      </div>
      <div className="bitgrid">
        {Array.from({ length: words }, (_, w) => (
          <div key={w} className="bitword">
            <span className="muted mono small">word {w}</span>
            <div className="bitcells">
              {Array.from({ length: 64 }, (_, pos) => {
                const v = byIndex.get(w * 64 + pos)
                return (
                  <span
                    key={pos}
                    className="bitcell"
                    style={v ? { background: v.color } : undefined}
                    title={v ? `bit ${v.index}: ${v.path}=${v.value}` : `bit ${w * 64 + pos}: free`}
                  />
                )
              })}
            </div>
          </div>
        ))}
      </div>
      <table className="table compact">
        <thead>
          <tr>
            <th>property</th>
            <th>values (bit)</th>
          </tr>
        </thead>
        <tbody>
          {props.map((p, i) => (
            <tr key={p.path}>
              <td className="mono">
                <span className="swatch" style={{ background: PALETTE[i % PALETTE.length] }} />
                {p.path}
              </td>
              <td className="chips">
                {p.values.map((v) => (
                  <span key={v.index} className="chip">
                    <span className="chip-value">{v.value}</span>
                    <span className="chip-key">{v.index}</span>
                  </span>
                ))}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  )
}
