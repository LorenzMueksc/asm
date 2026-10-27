import type { AnyValue, Bitmap, Bits, Condition, NodeState, TraceEvent } from './types'

// A bit of a root entity: the property path (nested properties joined with dots), its value and the bit index.
export interface BitEntry {
  path: string
  value: string
  index: number
}

// The bits of every root entity of a layout, by entity name and bit index.
export type LayoutIndex = Map<string, Map<number, BitEntry>>

// anyValue returns the value of a wrapperspb message packed in an Any, as the layout encodes bitmap values.
export function anyValue(a?: AnyValue): string {
  if (!a) return ''
  const type = a['@type'] ?? ''
  if (a.value === undefined || a.value === null) {
    if (type.endsWith('BoolValue')) return 'false'
    if (type.endsWith('StringValue')) return '""'
    return '0'
  }
  return String(a.value)
}

// indices returns the set bits of a bitset in ascending order. Bit i is in word i/64 at position i%64.
export function indices(bits?: Bits): number[] {
  const result: number[] = []
  ;(bits?.words ?? []).forEach((word, i) => {
    let w = BigInt(word)
    let pos = 0
    while (w > 0n) {
      if (w & 1n) result.push(i * 64 + pos)
      w >>= 1n
      pos++
    }
  })
  return result
}

export function indexLayout(layout?: Bitmap): LayoutIndex {
  const result: LayoutIndex = new Map()
  for (const [entity, bm] of Object.entries(layout?.children ?? {})) {
    const bitsOfEntity = new Map<number, BitEntry>()
    for (const p of properties(bm)) {
      for (const v of p.values) bitsOfEntity.set(v.index, v)
    }
    result.set(entity, bitsOfEntity)
  }
  return result
}

// properties returns the properties with values of a root entity bitmap, nested ones by their path.
export function properties(bm: Bitmap, path = ''): { path: string; values: BitEntry[] }[] {
  const result: { path: string; values: BitEntry[] }[] = []
  if (bm.values?.length) {
    result.push({
      path,
      values: bm.values
        .map((v) => ({ path, value: anyValue(v.value), index: Number(v.index ?? 0) }))
        .sort((a, b) => a.index - b.index),
    })
  }
  for (const name of Object.keys(bm.children ?? {}).sort()) {
    result.push(...properties(bm.children![name], path ? `${path}.${name}` : name))
  }
  return result
}

// decode returns the property values the bits of a root entity stand for.
export function decode(index: LayoutIndex, entity: string, bits?: Bits): BitEntry[] {
  const bitsOfEntity = index.get(entity)
  return indices(bits)
    .map((i) => bitsOfEntity?.get(i) ?? { path: '?', value: `bit ${i}`, index: i })
    .sort((a, b) => a.path.localeCompare(b.path) || a.index - b.index)
}

export function describeCondition(index: LayoutIndex, c: Condition): string {
  const states = Object.entries(c.states ?? {}).flatMap(([entity, s]) =>
    (s.states ?? []).map((bits) => {
      const values = decode(index, entity, bits).map((e) => `${e.path}=${e.value}`)
      return values.length > 1 ? `{${values.join(', ')}}` : (values[0] ?? '{}')
    }),
  )
  return `${c.operator ?? 'AT_LEAST_ONE'}(${states.join(', ')})`
}

// wordHex returns the words of a bitset as hex, lowest word first.
export function wordHex(bits?: Bits): string[] {
  return (bits?.words ?? []).map((w) => '0x' + BigInt(w).toString(16).padStart(16, '0'))
}

// leaderOf returns the node whose inspection to show for the cluster: the leader, else a member, else any node.
// Reachable nodes come first: an unreachable one keeps its last inspection and may still claim to be the leader.
export function leaderOf(nodes: NodeState[]): NodeState | undefined {
  const isLeader = (n: NodeState) => n.inspect?.node?.raftState === 'StateLeader'
  const live = nodes.filter((n) => n.inspect && !n.error)
  return (
    live.find(isLeader) ??
    live.find((n) => !n.inspect?.node?.client) ??
    live[0] ??
    nodes.find(isLeader) ??
    nodes.find((n) => n.inspect)
  )
}

// sinceText returns how long ago a timestamp was, relative to now.
export function sinceText(ts: string | undefined, now: number): string {
  if (!ts) return ''
  const ms = now - Date.parse(ts)
  if (ms < 1000) return `${Math.max(ms, 0)}ms ago`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s ago`
  return `${Math.round(ms / 60_000)}m ago`
}

export function timeText(ts?: string): string {
  if (!ts) return ''
  const d = new Date(ts)
  return d.toLocaleTimeString(undefined, { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0')
}

// durationText shortens a protojson duration like "0.001234s".
export function durationText(d?: string): string {
  if (!d) return ''
  const s = parseFloat(d)
  if (Number.isNaN(s)) return d
  if (s < 0.001) return `${Math.round(s * 1_000_000)}µs`
  if (s < 1) return `${(s * 1000).toFixed(1)}ms`
  return `${s.toFixed(2)}s`
}

// kinds of trace events that have a duration
const TIMED_KINDS = new Set(['EVENT', 'FORWARD', 'RECEIVED'])
// BELOW_CLOCK stands for a duration the node measured as 0 and left out: shorter than the resolution of its
// monotonic clock (about 0.5ms on Windows)
export const BELOW_CLOCK = '<0.5ms'

// tookText returns the duration of a trace event, BELOW_CLOCK if a kind with a duration came without one.
export function tookText(e: TraceEvent): string {
  if (e.duration) return durationText(e.duration)
  return TIMED_KINDS.has(e.kind ?? '') ? BELOW_CLOCK : ''
}

export function prettyJson(text?: string): string {
  if (!text) return ''
  try {
    return JSON.stringify(JSON.parse(text), null, 2)
  } catch {
    return text
  }
}
