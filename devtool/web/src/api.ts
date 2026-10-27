import { useEffect, useRef, useState } from 'react'
import type { ClusterState, TraceEvent } from './types'

const POLL_MS = 1000
// events kept in the browser, the oldest are dropped
export const MAX_EVENTS = 20000

// useClusterState polls the inspections of all nodes.
export function useClusterState(): { state?: ClusterState; error?: string } {
  const [state, setState] = useState<ClusterState>()
  const [error, setError] = useState<string>()

  useEffect(() => {
    let cancelled = false
    const load = async () => {
      try {
        const res = await fetch('/api/state')
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
        const next = (await res.json()) as ClusterState
        if (!cancelled) {
          setState(next)
          setError(undefined)
        }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e))
      }
    }
    load()
    const timer = setInterval(load, POLL_MS)
    return () => {
      cancelled = true
      clearInterval(timer)
    }
  }, [])

  return { state, error }
}

// useEvents streams the events of all nodes, in the order asmdev received them. While paused, new events are
// collected (the newest MAX_EVENTS) but not shown.
//
// asmdev sends every event with the id "<epoch>-<counter>". After a reconnect the browser sends the last id
// (Last-Event-ID) and asmdev sends only newer events; events not newer than the last one are dropped anyway.
// A "reset" event or an id of another epoch (asmdev restarted) drops the events received so far.
export function useEvents(paused: boolean) {
  const [events, setEvents] = useState<TraceEvent[]>([])
  const [connected, setConnected] = useState(false)
  const pending = useRef<TraceEvent[]>([])
  // the shown events are replaced by the pending ones with the next batch
  const reset = useRef(false)
  const pausedRef = useRef(paused)
  pausedRef.current = paused

  useEffect(() => {
    const source = new EventSource('/api/events')
    let epoch = ''
    let last = 0
    const drop = () => {
      pending.current = []
      reset.current = true
    }
    source.onopen = () => setConnected(true)
    source.onerror = () => setConnected(false)
    source.addEventListener('reset', () => {
      drop()
      epoch = ''
      last = 0
    })
    source.onmessage = (msg) => {
      const id = msg.lastEventId
      const dash = id.lastIndexOf('-')
      if (dash > 0) {
        const e = id.slice(0, dash)
        const counter = Number(id.slice(dash + 1))
        if (epoch && e !== epoch) drop()
        else if (e === epoch && counter <= last) return
        epoch = e
        last = counter
      }

      const event = JSON.parse(msg.data) as TraceEvent
      if (id) event.eventId = id
      pending.current.push(event)
      // trimmed in steps, not on every event
      if (pending.current.length > MAX_EVENTS + MAX_EVENTS / 10) {
        pending.current = pending.current.slice(pending.current.length - MAX_EVENTS)
      }
    }
    // render in batches, a busy cluster sends many events per second
    const timer = setInterval(() => {
      if (pausedRef.current || (pending.current.length === 0 && !reset.current)) return
      const batch = pending.current
      const replace = reset.current
      pending.current = []
      reset.current = false
      setEvents((prev) => {
        const next = replace ? batch : prev.concat(batch)
        return next.length > MAX_EVENTS ? next.slice(next.length - MAX_EVENTS) : next
      })
    }, 250)
    return () => {
      source.close()
      clearInterval(timer)
    }
  }, [])

  return {
    events,
    connected,
    waiting: pending.current.length,
    clear: () => {
      pending.current = []
      setEvents([])
    },
  }
}

// eventKey identifies an event in the browser: the id asmdev sent it with. A node that restarted reuses its
// sequence numbers, so nodeId and seq alone are not unique.
export function eventKey(e: TraceEvent): string {
  return e.eventId ?? `${e.nodeId}/${e.seq}/${e.ts}`
}
