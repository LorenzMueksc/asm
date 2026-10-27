# State, bitmaps, transitions

## Bits and state [U]
- State = `Bits` per root entity bitmap name. Bit i in word i/64; words added only when needed.
- `mergeState` replaces bits per property: a property never holds two values.
- Per event: event bits merged first, entity bits last → entity wins.
- Conditions of a transition are ANDed; a matching `AtLeastOne` moves on to the next condition.
- State transition: merge target bits, then apply to the entity (`ApplyStateToEntity`). Moving backward works.
- After every func transition (also a failed one) the entity is re-evaluated (`EvaluateEntityWithEvents(nil)` in `executeActions`): lower-priority transitions of the same event see its changes.
- Zero values apply: if 0/false is a bitmap value, an unset field of a state target sets it.
- Nested structs are modified in place; a new one only when the field is nil.
- A bitmap must not change once used: `bitmap_tables.go` builds its tables once, cached by `weak.Pointer`. New layouts are new trees (`applyBitmapEntry` clones). Tables never reference the bitmap (else never collected).
- Field matching: json tag name before the first comma. Unexported fields skipped (evaluate) or printed (apply). A field value matches a bitmap value if it converts to its type and is equal (fast paths string/int64/uint64/float64/bool, else `reflect.Convert`). Values that can't convert aren't applied.
- `getValue`, `getFieldByJsonName`, `parseValueToType`, `getKeyByBit`: kept only for their specs; hot paths use the tables.

## Shared bitmap (raft) [U]
- Every node proposes each root entity bitmap of its config once, retrying until committed (`proposeLocalBitmap`).
- Apply (`applyBitmapEntry`): first committed definition of an entity adopted with its indices; later entries append missing values at the next free index; children walked in name order; indices never move.
- Conflict (different value type in a property, nested vs leaf): the proposing node fails; other nodes' conflicts are logged and ignored.
- Values encoded as wrapperspb (String, Int64, UInt64, Bool, Double); ints normalized to int64.
- With raft the root bitmap's children must be root entity bitmaps (like `LoadBitMapFromDisk`).

## Transitions
- State transitions: `Bits` actions (`AddStateTransition`). Func transitions: `transitionFunc (ctx, c, payload) (skipSave, error)` (`AddFuncTransition`). Remote func transitions have no actions. Sorted by priority, highest first; negative allowed (wire `int32`, clamped).
- Conditions and state targets keep their example entities (`Condition.entities`, `Transition.toState`, also in published copies), compiled against the committed layout and again on every change; a change of the compiled bits (`compiledKey`) is pushed to peers.
- Signature [U] = sha256 over event name, conditions by example entities (`%T` + JSON, order independent; without entities by states) and action (`func:` + func key or `state:` + target entity). Independent of the layout. Func key = `WithFuncKey` or the runtime func name. Not part of it: name, priority, batching, save-on-change.
- Same name + same signature on several nodes = owners in `NodeIds` (load balancing). A re-sent transition replaces the stored one (newest bits), keeping other owners. Same name + different signature [U]: the introducing node fails; others reject with `FailedPrecondition` and keep running.
- Sync: when ready pull from every peer (`GetTransitions`), push local ones (`AddTransitions`); runtime additions pushed at once; owners leaving the cluster removed. Pulled transitions whose `node_id` isn't the peer asked are dropped.
- State transitions of other nodes (same entity) run locally. Func transitions of other nodes are `Remote`.
- Published transitions are copies: never mutate them. Registry changes go through `n.registry.mu` + `publishLocked`, which stores a `publishedTransitions` (all by priority + by event name, built once per change). `runEvent`, `processRemote`, `Node.forward`, `routingTransition` read `forEvent(name)` only.
- Remote transitions need real protobuf messages for entity and payloads (`ProtoReflect() != nil`).

## Several entities [A, user let the agent decide]
- A cluster mixes nodes of different entity types; one per `Node`. `entityName()` = root bitmap name of T in the committed layout (cached).
- Transitions carry `Entity` (set in `compileTransitionLocked`, synced in `Transition.entity`). Transitions of another entity are remote on a node, also state ones (`publishLocked`).
- Sender: conditions on the other entity's bitmap are left to the receiver (`matchesRemote`); event conditions are evaluated.
- Receiver side: `forwarding.md` (`forwardOtherEntity`).
