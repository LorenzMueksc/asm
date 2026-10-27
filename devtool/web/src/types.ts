// Shapes of the API of asmdev. Inspections and events are protobuf messages encoded with protojson:
// fields are lowerCamelCase, 64-bit integers are strings, and fields with their zero value are left out.

export interface Bits {
  words?: string[]
}

export interface AnyValue {
  '@type'?: string
  value?: unknown
}

export interface BitValue {
  value?: AnyValue
  index?: string
}

export interface Bitmap {
  name?: string
  values?: BitValue[]
  children?: Record<string, Bitmap>
}

export interface Liveness {
  last?: string
  lease?: string
}

export interface NodeInfo {
  id?: string
  listenAddr?: string
  ready?: boolean
  error?: string
  skipRaft?: boolean
  raftState?: string
  leader?: string
  term?: string
  commitIndex?: string
  appliedIndex?: string
  snapshotIndex?: string
  peers?: Record<string, string>
  removed?: string[]
  liveness?: Record<string, Liveness>
  maxContainers?: number
  traceCapacity?: number
  // root bitmap name of the entity of the node
  entity?: string
  // client node (WithClientMode): no raft, attached to a member
  client?: boolean
  attachedTo?: string
}

export interface Condition {
  states?: Record<string, { states?: Bits[] }>
  // AT_LEAST_ONE is the zero value, so it is left out
  operator?: 'ALL' | 'NONE' | 'ONE_OF' | 'AT_LEAST_ONE'
}

export interface Transition {
  name?: string
  eventName?: string
  priority?: number
  saveOnChange?: boolean
  nodeId?: string
  conditions?: Condition[]
  batching?: { count?: string; period?: string }
  signature?: string
  state?: { bitmap?: string; bits?: Bits }
  // root bitmap name of the entity the transition runs on
  entity?: string
}

export interface InspectTransition {
  transition?: Transition
  nodeIds?: string[]
  remote?: boolean
  funcKey?: string
}

export interface LockWaiter {
  node?: string
  token?: string
  // lock tokens of the containers waiting for the waiter
  waitPath?: string[]
}

export interface LockCall {
  token?: string
  waitPath?: string[]
}

export interface Lock {
  containerId?: string
  holder?: string
  token?: string
  waiters?: LockWaiter[]
  // waited calls of another entity running in the holder's container
  calls?: LockCall[]
}

export interface ContainerInfo {
  id?: string
  status?: string
  pending?: string
  busy?: boolean
  entityType?: string
  entityJson?: string
  states?: Record<string, Bits>
  batches?: Record<string, number>
}

export interface Metric {
  name?: string
  help?: string
  // COUNTER is the zero value, so it is left out
  kind?: 'COUNTER' | 'GAUGE' | 'HISTOGRAM'
  value?: number
  // histogram: upper bounds in seconds, observations per bucket (one more above the last bound)
  bounds?: number[]
  counts?: string[]
  sum?: number
  count?: string
}

export interface InspectResponse {
  node?: NodeInfo
  transitions?: InspectTransition[]
  layout?: Bitmap
  // at most the limit of asmdev, lockCount and containerCount have the totals
  locks?: Lock[]
  containers?: ContainerInfo[]
  metrics?: Metric[]
  lockCount?: number
  containerCount?: number
}

export interface NodeState {
  addr: string
  id?: string
  error?: string
  traceError?: string
  // last poll; inspected is the last successful one, an unreachable node keeps that inspection
  updated: string
  inspected?: string
  inspect?: InspectResponse
}

export interface ClusterState {
  join: string
  nodes: NodeState[]
  time: string
}

export interface TracePayload {
  type?: string
  json?: string
}

export interface TraceRoute {
  nodeId?: string
  transitions?: string[]
  error?: string
}

export type TraceKind = 'EVENT' | 'FORWARD' | 'RECEIVED' | 'ERROR' | 'CONTAINER_STARTED' | 'CONTAINER_CLOSED'

export interface TraceEvent {
  seq?: string
  ts?: string
  nodeId?: string
  kind?: TraceKind
  containerId?: string
  eventName?: string
  payloads?: TracePayload[]
  transitions?: string[]
  remoteTransitions?: string[]
  routes?: TraceRoute[]
  wait?: boolean
  fromNode?: string
  excluded?: string[]
  error?: string
  duration?: string
  // span of the entry and the traces it belongs to, with its parent span in each
  spanId?: string
  traces?: TraceRef[]
  // set in the browser: the id asmdev sent the event with, unique per asmdev run (a restarted node reuses seq)
  eventId?: string
}

export interface TraceRef {
  traceId?: string
  parentSpanId?: string
}
