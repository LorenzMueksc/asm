import { useEffect, useState } from 'react'
import { useClusterState, useEvents } from './api'
import { Badge } from './components'
import { leaderOf } from './decode'
import { BitmapView } from './views/Bitmap'
import { ClusterView } from './views/Cluster'
import { ContainersView } from './views/Containers'
import { EventsView } from './views/Events'
import { LocksView } from './views/Locks'
import { MetricsView } from './views/Metrics'
import { TracesView } from './views/Traces'
import { TransitionsView } from './views/Transitions'

const TABS = ['cluster', 'containers', 'transitions', 'bitmap', 'locks', 'events', 'traces', 'metrics'] as const
type Tab = (typeof TABS)[number]

// the hash is the tab, the traces tab may name a trace: #traces/<trace id>
function tabFromHash(): Tab {
  const t = window.location.hash.slice(1).split('/')[0] as Tab
  return TABS.includes(t) ? t : 'cluster'
}

function traceFromHash(): string | undefined {
  const [tab, id] = window.location.hash.slice(1).split('/')
  return tab === 'traces' ? id : undefined
}

export function App() {
  const [tab, setTab] = useState<Tab>(tabFromHash)
  const [traceId, setTraceId] = useState(traceFromHash)
  const [paused, setPaused] = useState(false)
  const { state, error } = useClusterState()
  const events = useEvents(paused)

  useEffect(() => {
    const onHash = () => {
      setTab(tabFromHash())
      setTraceId(traceFromHash())
    }
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])

  const nodes = state?.nodes ?? []
  const reachable = nodes.filter((n) => n.inspect && !n.error).length
  const leader = leaderOf(nodes)
  const containers = nodes.reduce((sum, n) => sum + (n.inspect?.containerCount ?? n.inspect?.containers?.length ?? 0), 0)

  const counts: Record<Tab, number | undefined> = {
    cluster: nodes.length,
    containers,
    transitions: new Set(nodes.flatMap((n) => (n.inspect?.transitions ?? []).map((t) => t.transition?.name))).size,
    bitmap: Object.keys(leader?.inspect?.layout?.children ?? {}).length,
    locks: leader?.inspect?.lockCount ?? leader?.inspect?.locks?.length ?? 0,
    events: events.events.length,
    traces: undefined,
    metrics: undefined,
  }

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          ASM <span className="muted">devtool</span>
        </div>
        <div className="topbar-info">
          <span className="muted">cluster of</span> <span className="mono">{state?.join ?? '…'}</span>
          <Badge tone={reachable === nodes.length && nodes.length > 0 ? 'ok' : 'warn'}>
            {reachable}/{nodes.length} nodes reachable
          </Badge>
          {leader?.id && !leader.error && <Badge tone="accent">leader node {leader.inspect?.node?.leader ?? leader.id}</Badge>}
          <Badge tone={events.connected ? 'ok' : 'warn'}>{events.connected ? 'events live' : 'events offline'}</Badge>
          {error && <Badge tone="error">devtool: {error}</Badge>}
        </div>
      </header>
      <nav className="sidebar">
        {TABS.map((t) => (
          <a key={t} href={`#${t}`} className={t === tab ? 'active' : ''}>
            <span>{t}</span>
            {counts[t] !== undefined && <span className="count">{counts[t]}</span>}
          </a>
        ))}
      </nav>
      <main className="content">
        {!state && !error && <div className="empty">loading…</div>}
        {state && tab === 'cluster' && <ClusterView state={state} />}
        {state && tab === 'containers' && <ContainersView nodes={nodes} />}
        {state && tab === 'transitions' && <TransitionsView nodes={nodes} />}
        {state && tab === 'bitmap' && <BitmapView nodes={nodes} />}
        {state && tab === 'locks' && <LocksView nodes={nodes} />}
        {state && tab === 'metrics' && <MetricsView nodes={nodes} />}
        {state && tab === 'traces' && <TracesView nodes={nodes} events={events.events} selected={traceId} />}
        {tab === 'events' && (
          <EventsView
            nodes={nodes}
            events={events.events}
            paused={paused}
            waiting={events.waiting}
            onPause={setPaused}
            onClear={events.clear}
          />
        )}
      </main>
    </div>
  )
}
