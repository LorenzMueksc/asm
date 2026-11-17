package asm

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Reader and writer transitions.
//
// A transition is a writer unless it is marked WithReadOnly. A container runs one writer at a time: the events of its
// mailbox one after the other on its goroutine (interleaved at waits in a reentrant container), batch timers and the
// error func, all under c.mu (lockWrite). An event whose transitions are all read-only (rwIndex) doesn't enter the
// mailbox: Event starts it on a goroutine of its own (dispatchRead), so readers run concurrently with each other and
// with the running writer and never wait for it.
//
// A reader runs on a view of the container (readerView): its own builder holding the published entity, its own states
// and calling events, the transitions it was classified with. The published entity is a copy taken while no writer
// changes the builder's entity (rwPublish): a writer takes it when it begins unless the copy is still current, so
// readers starting while it runs get the entity as it was before it started; the writer itself changes the builder's
// entity as before. When the writer ends the copy is outdated: the next reader or writer takes a new one. Containers
// whose transitions have no read-only one take no copy.
//
// WithMaxReaders caps the readers running at once; one waiting in a waited Emit to another container gives its slot back.
//
// Immutable-entity mode (WithImmutableEntity, WithImmutableEntities) publishes the builder's entity itself instead of a
// copy: writers never change it, they Set a new one. The container's own writes to the entity copy first (applyState,
// the merge of a waited forward), and what it gives away (the result of an execution for another node, the trace
// snapshot) is the entity itself (sharedEntity). WithImmutableEntityCheck checks every writer against a fingerprint of
// the entity published when it began.

type (
	// rwIndex classifies the events of published transitions, built once on first use (readIndex).
	rwIndex struct {
		once sync.Once
		// readers is set if one of the transitions is read-only
		readers bool
		// direct holds the event names whose events sent to the container (Event, or Emit without target) run as
		// readers: every transition of the name is a read-only func transition of this node
		direct map[string]bool
		// executions holds the event names whose executions for other containers on the container's own lock (calls
		// of another entity, target calls, hand-overs) run as readers: every transition of the name the container
		// runs for them is read-only. Remote transitions don't run there.
		executions map[string]bool
	}
	// rwPublish is the entity published to the readers of one container life, guarded by mu.
	rwPublish struct {
		mu sync.Mutex
		// entity is the copy readers get (a T), current while fresh
		entity any
		fresh  bool
		// writing is set while a writer holds the container (lockWrite until unlockWrite)
		writing bool
		// changedC is closed once a copy was taken or the writer ended, made by a reader waiting for that (readEntity)
		changedC chan struct{}
		// inited is set once the init func ran; without one it isn't used (initialized)
		inited atomic.Bool
		// sum is the fingerprint of entity the running writer must leave as it is, set (summed) while the mutation
		// check of immutable mode holds one (sumLocked)
		sum    any
		summed bool
	}
	// rwContainer holds what a container needs for readers.
	rwContainer[T proto.Message, K Builder[T]] struct {
		// newBuilderF creates the builders of reader views; without it every event runs as a writer
		newBuilderF CreateBuilderFunc[T, K]
		// base is the container a reader view reads, nil for the container itself
		base *Container[T, K]
		// immutable: writers replace the entity instead of changing it, readers get it without a copy
		// (WithImmutableEntity); checkImmutable: a writer's end checks that it left the published entity as it was
		// (WithImmutableEntityCheck)
		immutable, checkImmutable bool
		// slots holds a token per running reader of the container life, nil without a cap (WithMaxReaders)
		slots chan struct{}
	}
)

// ErrReadOnly is returned by Emit in a read-only transition for an event of its own container which isn't a read
// event: it would run writers of the container or reach remote transitions. Events for other containers (WithTarget)
// are allowed. A reader can't take part in rounds either: Emit with WithDeferred, Spawn and CloseAfterRound return it
// (a reader runs next to the writer, so its place in the outbox would depend on the scheduling).
var ErrReadOnly = errors.New("a read-only transition can only emit read events to its own container")

// ErrEntityMutated goes to the error func of a container in immutable-entity mode when a writer changed the entity
// published when it began in place (WithImmutableEntityCheck).
var ErrEntityMutated = errors.New("a writer changed the published entity in place")

// entityCopyHook, if set, is called for every copy of an entity copyEntity and sharedEntity take (specs), which the
// node's metric asm_entity_copies_total counts too (countCopy).
var entityCopyHook atomic.Pointer[func()]

// WithReadOnly marks a func transition read-only: it only reads the entity. An event whose transitions on the node are
// all read-only func transitions is a read event: it runs right away on a goroutine of its own, concurrently with other
// read events and with the one writer the container runs, and never waits for it. Every other event is a writer, as
// before: writers run one at a time.
//
// A reader sees the entity as it was published by the last writer which ended before it started: a writer's changes
// become visible to readers when its event is done, never in between. c.Builder().Build() in a reader returns that
// entity, a copy shared with other readers: never change it. Set on the reader's builder only changes the reader's
// view and is dropped. A reader may Reply, Emit to other containers (WithTarget, waited or not), Emit read events of
// its own container (they run on the same entity) and send events with c.Event; any other Emit without target returns
// ErrReadOnly. WithSaveAfter does nothing on a read-only transition; one batching more than one event
// (WithBatching) runs as a writer.
//
// Read events sent to the container and calls of other containers on its own lock (WithTarget, other entities,
// hand-overs) run as readers; a waited call back into a container whose writer waits for the caller runs if it calls a
// read event (only cycles of writers are refused with ErrWouldDeadlock). Waited executions of remote transitions of the same entity bring the caller's entity
// and run as writers. Containers whose node has read-only transitions copy the entity once per writer (proto.Clone, or
// CloneMessageVT when the entity has it), except in immutable-entity mode (WithImmutableEntity): it publishes the
// entity itself.
func WithReadOnly() TransitionOption {
	return newFuncTransitionOption(func(t *Transition) {
		t.ReadOnly = true
	})
}

// WithImmutableEntity puts the container in immutable-entity mode: its entity is a value that is never changed in place.
// A writer (a transition that isn't read-only, the init and error funcs) treats c.Builder().Build() as read-only and
// replaces the entity with Set: a changed copy, or a new entity, which may share unchanged nested messages with the old
// one (never change those in place either). A writer that doesn't call Set leaves the entity as it is. Readers
// (WithReadOnly) get the entity published before the running writer began by pointer: the container copies nothing
// for them. The container's own writes copy first: a state transition applies its state to a copy, a waited forward
// merges the results into a copy. The result of an execution for another node, the trace snapshot and the save func
// get the entity itself, the one readers get next.
//
// Changing the entity in place in this mode is a bug of the caller: readers may read it at that moment, and whoever
// keeps it by reference sees it change. WithImmutableEntityCheck detects it in tests. Use WithImmutableEntities to put
// all containers of a node in this mode, including the ones the node opens for calls, hand-overs and executions of
// other nodes.
func WithImmutableEntity() ContainerOption {
	return newFuncContainerOption(func(options *containerOptions) {
		options.immutable = true
	})
}

// WithImmutableEntities puts every container of the node in immutable-entity mode (see WithImmutableEntity).
func WithImmutableEntities() *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.containerModes = append(options.containerModes, WithImmutableEntity())
	})
}

// WithImmutableEntityCheck makes the node's containers in immutable-entity mode check every writer (an event, a batch
// timer, the error func): when it ends, the entity published when it began must encode as it did then (deterministic
// protobuf encoding; entities without protobuf reflection are compared shallowly). Otherwise the writer changed it in
// place: the error func gets ErrEntityMutated. A debug option for tests, it encodes the entity twice per writer.
// Batches run when the container closes aren't checked.
func WithImmutableEntityCheck() *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.containerModes = append(options.containerModes, newFuncContainerOption(func(options *containerOptions) {
			options.checkImmutable = true
		}))
	})
}

// WithMaxReaders caps the read events (WithReadOnly) running at once in the container at n; 0 or less, the default,
// doesn't cap them. A read event beyond the cap waits for a running one to end, on its own goroutine: Event doesn't
// block or fail, and a call of it doesn't find the container busy. A reader waiting in a waited Emit to another
// container doesn't count meanwhile: a call back into the container must find a slot. Writers aren't affected.
func WithMaxReaders(n int) ContainerOption {
	return newFuncContainerOption(func(options *containerOptions) {
		options.maxReaders = n
	})
}

// WithMaxReadersPerContainer caps the read events running at once in every container of the node at n, like
// WithMaxReaders, including the containers the node opens itself for calls, hand-overs and executions of other nodes;
// 0 or less, the default, doesn't cap them. A container's own WithMaxReaders overrides it.
func WithMaxReadersPerContainer(n int) *funcNodeSetupOption {
	return newFuncNodeSetupOption(func(options *nodeSetupOptions) {
		options.containerModes = append(options.containerModes, WithMaxReaders(n))
	})
}

// setMode sets the entity mode and the reader cap of the container life from its options.
func (rw *rwContainer[T, K]) setMode(o *containerOptions) {
	rw.immutable, rw.checkImmutable = o.immutable, o.checkImmutable
	rw.slots = nil
	if o.maxReaders > 0 {
		rw.slots = make(chan struct{}, o.maxReaders)
	}
}

// withoutReadSlot runs call, a call of a reader to another container which may wait (Emit with WithTarget), without
// the reader's slot (WithMaxReaders): slots are for running readers, and a call back into the container may need one.
func (c *Container[T, K]) withoutReadSlot(call func()) {
	b := c.rw.base
	if b == nil || b.rw.slots == nil {
		call()
		return
	}
	<-b.rw.slots
	defer func() { b.rw.slots <- struct{}{} }()
	call()
}

// readOnlyFunc reports whether the transition runs as a reader: a read-only func transition of this node which
// doesn't batch, also not per round (WithBatchOnRound: a writer, refused with WithReadOnly when compiled).
func (t *Transition) readOnlyFunc() bool {
	return t.ReadOnly && !t.Remote && !t.roundBatch && t.Batching.Count <= 1 && t.toState == nil && t.stateBitmap == "" &&
		len(t.Actions) > 0 && !slices.ContainsFunc(t.Actions, func(a any) bool {
		_, ok := a.(Bits)
		return ok
	})
}

// readIndex returns the classification of the events of the transitions, nil without transitions.
func (p *publishedTransitions) readIndex() *rwIndex {
	if p == nil {
		return nil
	}
	p.rw.once.Do(func() {
		for name, ts := range p.byEvent {
			direct, executions, local := true, true, false
			for _, t := range ts {
				if t.readOnlyFunc() {
					p.rw.readers = true
				}
				if t.Remote {
					// routed to its owners, not run for executions of other containers
					direct = false
					continue
				}
				local = true
				if !t.readOnlyFunc() {
					direct, executions = false, false
				}
			}
			if !local {
				continue
			}
			if direct {
				if p.rw.direct == nil {
					p.rw.direct = make(map[string]bool)
				}
				p.rw.direct[name] = true
			}
			if executions {
				if p.rw.executions == nil {
					p.rw.executions = make(map[string]bool)
				}
				p.rw.executions[name] = true
			}
		}
	})
	return &p.rw
}

// hasReaders reports whether the container's transitions have a read-only one.
func (c *Container[T, K]) hasReaders() bool {
	idx := c.currentTransitions().readIndex()
	return idx != nil && idx.readers
}

// lockWrite takes the container for a writer: an event of the mailbox, a batch timer, the error func.
func (c *Container[T, K]) lockWrite() {
	c.mu.Lock()
	pub := &c.containerProcessing.pub
	pub.mu.Lock()
	pub.writing = true
	// readers starting while the writer runs get the entity as it is now; before the init func loaded it, the copy is
	// taken once it did (initDone). Immutable mode publishes without readers too: it costs nothing and the mutation
	// check needs it.
	if !pub.fresh && c.initialized() && (c.rw.immutable || c.hasReaders()) {
		c.publishLocked()
	}
	c.sumLocked()
	pub.mu.Unlock()
}

// unlockWrite releases the container taken by lockWrite. The writer may have changed the entity: the copy is
// outdated, the next reader or writer takes a new one.
func (c *Container[T, K]) unlockWrite() {
	pub := &c.containerProcessing.pub
	pub.mu.Lock()
	var err error
	if pub.summed {
		if e, _ := pub.entity.(T); changedSince(e, pub.sum) {
			err = fmt.Errorf("container %s: %w", c.id, ErrEntityMutated)
		}
		pub.sum, pub.summed = nil, false
	}
	pub.writing = false
	pub.fresh = false
	pub.wakeLocked()
	pub.mu.Unlock()
	if err != nil {
		c.sendError(err)
	}
	c.mu.Unlock()
}

// publishLocked publishes the builder's entity for the readers: a copy, in immutable mode the entity itself. The caller
// holds pub.mu, and no writer changes the entity: the caller is the writer beginning, or no writer runs.
func (c *Container[T, K]) publishLocked() {
	pub := &c.containerProcessing.pub
	if c.rw.immutable {
		pub.entity = c.builder.Build()
	} else {
		pub.entity = copyEntity(c.builder.Build(), c.metrics)
	}
	pub.fresh = true
	pub.wakeLocked()
}

// sumLocked takes the fingerprint of the published entity for the mutation check of immutable mode, once per writer:
// when it begins, or when the init func it ran loaded the entity. The caller holds pub.mu and is that writer.
func (c *Container[T, K]) sumLocked() {
	pub := &c.containerProcessing.pub
	if !c.rw.checkImmutable || !c.rw.immutable || !pub.fresh || pub.summed {
		return
	}
	e, _ := pub.entity.(T)
	pub.sum, pub.summed = fingerprint(e)
}

// wakeLocked wakes the readers waiting for a copy. The caller holds pub.mu.
func (pub *rwPublish) wakeLocked() {
	if pub.changedC != nil {
		close(pub.changedC)
		pub.changedC = nil
	}
}

// initialized reports whether the init func loaded the entity, or there is none.
func (c *Container[T, K]) initialized() bool {
	return c.initF == nil || c.containerProcessing.pub.inited.Load()
}

// initDone records that the init func ran. A writer which began before it did takes the readers' copy now.
func (c *Container[T, K]) initDone() {
	pub := &c.containerProcessing.pub
	if c.initF == nil || pub.inited.Load() {
		return
	}
	pub.mu.Lock()
	pub.inited.Store(true)
	if pub.writing && !pub.fresh && (c.rw.immutable || c.hasReaders()) {
		c.publishLocked()
		c.sumLocked()
	}
	pub.mu.Unlock()
}

// runInit runs the init func once, like processEventLocked. The caller holds the container as a writer.
func (c *Container[T, K]) runInit() {
	if c.initF != nil {
		c.initOnce.Do(func() {
			c.logger.Debug("executing container init func")
			err := c.initF(c.ctx, c)
			if err != nil {
				c.logger.Debug("container init func failed", zap.Error(err))
				c.sendError(err)
			}
		})
	}
	c.initDone()
}

// readEntity returns the entity for a reader: the current copy, or a new one if no writer runs. A running writer took
// one when it began, except before the init func loaded the entity: the reader waits until the writer ran it
// (initDone) or ended. Without writer and before the init func ran, the reader runs it as a writer.
func (c *Container[T, K]) readEntity() T {
	pub := &c.containerProcessing.pub
	for {
		pub.mu.Lock()
		if !pub.fresh && !pub.writing && c.initialized() {
			c.publishLocked()
		}
		if pub.fresh {
			entity, _ := pub.entity.(T)
			pub.mu.Unlock()
			return entity
		}
		if pub.writing {
			if pub.changedC == nil {
				pub.changedC = make(chan struct{})
			}
			changedC := pub.changedC
			pub.mu.Unlock()
			<-changedC
			continue
		}
		pub.mu.Unlock()

		c.lockWrite()
		c.runInit()
		c.unlockWrite()
	}
}

// copyEntity copies an entity for readers: CloneMessageVT if the entity has it (vtprotobuf), else proto.Clone.
// Entities without protobuf reflection (fakes of tests) are copied shallowly. The copy counts in metrics, nil for a
// container without a node.
func copyEntity[T proto.Message](e T, metrics *nodeMetrics) T {
	m := proto.Message(e)
	if isNilMessage(m) {
		return e
	}
	countCopy(metrics)
	if v, ok := m.(interface{ CloneMessageVT() proto.Message }); ok {
		return v.CloneMessageVT().(T)
	}
	if m.ProtoReflect() != nil {
		return proto.Clone(m).(T)
	}
	return shallowCopy(e)
}

// shallowCopy copies the struct a pointer points to; other values are returned as they are.
func shallowCopy[T any](e T) T {
	if rv := reflect.ValueOf(e); rv.Kind() == reflect.Pointer && !rv.IsNil() && rv.Elem().Kind() == reflect.Struct {
		cp := reflect.New(rv.Elem().Type())
		cp.Elem().Set(rv.Elem())
		return cp.Interface().(T)
	}
	return e
}

// countCopy counts a copy of an entity in the node's metrics (asm_entity_copies_total) and calls entityCopyHook if
// set. Nil metrics (a container without a node) count nothing.
func countCopy(metrics *nodeMetrics) {
	if metrics != nil {
		metrics.entityCopies.Add(1)
	}
	if f := entityCopyHook.Load(); f != nil {
		(*f)()
	}
}

// sharedEntity returns the entity for a holder outside the writer (the result of an execution for another node, the
// trace snapshot): a copy, in immutable mode the entity itself, since no writer changes it.
func (c *Container[T, K]) sharedEntity(e proto.Message) proto.Message {
	if c.rw.immutable {
		return e
	}
	countCopy(c.metrics)
	return proto.Clone(e)
}

// applyState applies the state of a state transition to the builder's entity: in place, in immutable mode to a copy
// which replaces it (copy-first: the published entity stays as it is).
func (c *Container[T, K]) applyState(entity T, state Bits, bm *Bitmap) {
	if c.rw.immutable && !isNilMessage(entity) {
		entity = copyEntity(entity, c.metrics)
		ApplyStateToEntity(entity, state, bm)
		c.builder.Set(entity)
		return
	}
	ApplyStateToEntity(entity, state, bm)
}

// fingerprint returns what the mutation check compares (changedSince): the deterministic encoding of a protobuf entity,
// a shallow copy of one without protobuf reflection (fakes of tests). ok is false for a nil entity and for one that
// doesn't encode (eg. invalid UTF-8 in a string): it isn't checked.
func fingerprint[T proto.Message](e T) (any, bool) {
	m := proto.Message(e)
	if isNilMessage(m) {
		return nil, false
	}
	if m.ProtoReflect() == nil {
		return shallowCopy(e), true
	}
	b, err := proto.MarshalOptions{Deterministic: true, AllowPartial: true}.Marshal(m)
	if err != nil {
		return nil, false
	}
	return b, true
}

// changedSince reports whether the entity differs from its fingerprint sum.
func changedSince[T proto.Message](e T, sum any) bool {
	cur, ok := fingerprint(e)
	if !ok {
		return true
	}
	if b, ok := sum.([]byte); ok {
		cb, _ := cur.([]byte)
		return !bytes.Equal(b, cb)
	}
	return !reflect.DeepEqual(sum, cur)
}

// dispatchRead starts a read event on a goroutine of its own and reports whether it did; every other event goes to the
// mailbox. The reader keeps the container open (pending), but isn't counted in events: a reader never makes the
// container busy. Events sent before the container started wait in the mailbox and run as writers.
func (c *Container[T, K]) dispatchRead(evt *containerEvent) bool {
	b := c
	if c.rw.base != nil {
		b = c.rw.base
	}
	if b.rw.newBuilderF == nil {
		return false
	}
	ts := b.currentTransitions()
	idx := ts.readIndex()
	if idx == nil || !idx.readers {
		return false
	}
	if r := evt.remote; r != nil {
		// executions on a lent lock bring the caller's entity, they are part of its writer
		if r.entity != nil || r.chain != nil || !idx.executions[evt.name] {
			return false
		}
	} else if !idx.direct[evt.name] {
		return false
	}

	p := b.containerProcessing
	// a writer which began before the container had read-only transitions took no copy: the event waits for it in the
	// mailbox instead of blocking a goroutine (it might wait for a call which waits for the event)
	p.pub.mu.Lock()
	missing := p.pub.writing && !p.pub.fresh && b.initialized()
	p.pub.mu.Unlock()
	if missing {
		return false
	}
	p.lifecycleMu.Lock()
	if p.runner == nil {
		p.lifecycleMu.Unlock()
		return false
	}
	p.events--
	p.lifecycleMu.Unlock()

	go b.runRead(ts, evt)
	return true
}

// runRead runs a read event on a reader view, like processEventLocked runs a writer. ts are the transitions the event
// was classified with.
func (c *Container[T, K]) runRead(ts *publishedTransitions, evt *containerEvent) {
	// the reader isn't counted in events (dispatchRead)
	defer c.donePending()
	if s := c.rw.slots; s != nil {
		// beyond the cap a reader waits here for a running one to end (WithMaxReaders)
		s <- struct{}{}
		defer func() { <-s }()
	}
	if m := c.metrics; m != nil {
		start := time.Now()
		defer func() {
			m.events.Add(1)
			m.eventTime.observe(time.Since(start))
		}()
	}

	if debugOn(c.logger) {
		debugf(c.logger, "received event %s, running it as a reader", evt.name)
	}

	v := c.readerView(ts, c.readEntity())
	evt.refs = 1
	if evt.remote != nil {
		v.processRemote(c.ctx, evt)
	} else {
		v.runEvent(c.ctx, evt)
	}
	v.endRead(evt)
}

// readerView returns the view a reader runs on: the container with its own builder holding the entity, and the
// transitions ts. Its errors go to the container's error queue; it has no lifecycle funcs, lock token or lent lock,
// and isn't reentrant: a waited Emit blocks only the reader.
func (c *Container[T, K]) readerView(ts *publishedTransitions, entity T) *Container[T, K] {
	b := c.rw.newBuilderF()
	if !isNilMessage(entity) {
		b.Set(entity)
	}
	return &Container[T, K]{
		containerProcessing: c.containerProcessing,
		id:                  c.id,
		builder:             b,
		bitmap:              c.bitmap,
		bitmapP:             c.bitmapP,
		transitions:         ts,
		deps:                c.deps,
		getBitmapByTypeF:    c.getBitmapByTypeF,
		targetF:             c.targetF,
		nodeErrF:            c.nodeErrF,
		traceF:              c.traceF,
		metrics:             c.metrics,
		entityName:          c.entityName,
		ctx:                 c.ctx,
		status:              containerStatusRunning,
		logger:              c.logger,
		rw:                  rwContainer[T, K]{base: c, immutable: c.rw.immutable},
	}
}

// endRead ends a read event like releaseEvent: the caller gets the errors and the reply, no entity (a reader changes
// nothing).
func (c *Container[T, K]) endRead(evt *containerEvent) {
	evt.refs--
	if evt.refs != 0 {
		return
	}
	if r := evt.remote; r != nil {
		r.err = errors.Join(r.errs...)
	}
	if evt.closeAfter {
		c.containerProcessing.requestCallClose()
	}
	if evt.doneC != nil {
		evt.doneC <- struct{}{}
	}
}

// checkReadOnlyEmit returns ErrReadOnly for an Emit without target of a reader, unless the event is a read event:
// it runs on the reader's view.
func (c *Container[T, K]) checkReadOnlyEmit(eventName string) error {
	if c.rw.base == nil {
		return nil
	}
	if idx := c.currentTransitions().readIndex(); idx != nil && idx.direct[eventName] {
		return nil
	}
	return fmt.Errorf("emitting event %s: %w", eventName, ErrReadOnly)
}

// busy shadows containerProcessing.busy for the node's busy checks of calls (WithOnBusy): an execution of a read
// event never waits. Running readers aren't counted in events (dispatchRead), so they don't make writers wait either.
func (c *Container[T, K]) busy(r reentrance, eventName string) bool {
	if c.readsCall(eventName) {
		return false
	}
	return c.containerProcessing.busy(r, eventName)
}

// readsCall reports whether a call of the event from another container (on the container's own lock) runs as a
// reader: it never waits for the container's writer, so the lock table lets it join without a deadlock check
// (callContainer).
func (c *Container[T, K]) readsCall(eventName string) bool {
	if c.rw.newBuilderF == nil {
		return false
	}
	idx := c.currentTransitions().readIndex()
	return idx != nil && idx.executions[eventName]
}

// errNotRead is returned by Event for a call joined as a read (withReadCall) which doesn't run as a reader (the
// transitions changed meanwhile, or a writer took no copy): callContainer calls it again as a writer.
var errNotRead = errors.New("the call doesn't run as a reader")

// withReadCall marks a call the lock table let join as a read: it runs as a reader, or Event returns errNotRead.
func withReadCall() EventOption {
	return newFuncEventOption(func(options *eventOptions) {
		options.readCall = true
	})
}
