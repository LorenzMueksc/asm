package asm

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"rumue.io/asm/service"
)

type (
	GetBitmapByTypeFunc                              func(entity any, bitMap *Bitmap) *Bitmap
	CreateBuilderFunc[T proto.Message, K Builder[T]] func() K
	lifecycleFunc[T proto.Message, K Builder[T]]     func(context.Context, *Container[T, K]) error
	transitionFunc[T proto.Message, K Builder[T]]    func(context.Context, *Container[T, K], any) (bool, error)
	errorFunc[T proto.Message, K Builder[T]]         func(context.Context, *Container[T, K], error) error
	// forwardFunc sends events to nodes owning the matched remote transitions. The receiving nodes decide
	// which of their transitions run. With wait it lends the lock and returns what the nodes returned,
	// otherwise it returns once the work was sent: a hand-over failing later goes to handOverErrF, from another
	// goroutine, after the transitions of an unreachable owner were covered again. It returns the results which came
	// back even if sending to another node failed.
	// chain names the nodes lending the lock the container runs on, nil if it holds the lock itself.
	// waitPath holds the lock tokens of the containers waiting for the answer, only with wait.
	// trace is nil unless an event is traced.
	forwardFunc func(ctx context.Context, eventName, containerId string, chain, waitPath, matched []string, entity *anypb.Any, payloads []*anypb.Any, wait bool, trace *forwardTrace, handOverErrF func(error)) ([]forwardResult, error)
	// targetFunc runs an event in the container of another id of the same entity on its own lock (WithTarget).
	// With wait it blocks until the transitions ran and returns their reply and errors, otherwise it hands the event over.
	targetFunc func(ctx context.Context, id, eventName string, payload any, wait bool, waitPath []string, trace *service.TraceRef) (any, error)
	// forwardTrace is the FORWARD span of forwarded events: its parents in the traces of the events and the trace of
	// every payload for the receiver, with the span as parent
	forwardTrace struct {
		spanId   string
		parents  []*service.TraceRef
		payloads []*service.TraceRef
	}
	// forwardResult is what a node returned for forwarded events: the bits of the properties it changed,
	// or its entity when bits alone don't describe its changes.
	forwardResult struct {
		entity  proto.Message
		changed Bits
		// replies of the receiver's transitions (Container.Reply)
		replies []any
	}
	containerAccessor interface {
		Event(string, any, ...EventOption) error
		Close()
	}
	Dependencies             interface{}
	Builder[T proto.Message] interface {
		Build() T
		Set(T)
		IsSet() bool
	}
	Container[T proto.Message, K Builder[T]] struct {
		*containerProcessing

		id      string
		builder K
		states  map[string]Bits
		// bitmap and transitions are used when the container is not managed by a node,
		// otherwise the committed bitmap and the current transitions of the node are used.
		bitmap           *Bitmap
		transitions      *publishedTransitions
		bitmapP          *atomic.Pointer[Bitmap]
		transitionsP     *atomic.Pointer[publishedTransitions]
		deps             Dependencies
		getBitmapByTypeF GetBitmapByTypeFunc

		saveF, initF, closeF lifecycleFunc[T, K]
		errorF               errorFunc[T, K]
		// removeF is called once the container is closed, right before signaling done.
		// It is set by the node to remove the container from its container map.
		removeF func()
		// forwardF is set by the node to forward events of remote transitions.
		forwardF forwardFunc
		// targetF is set by the node to run events emitted with WithTarget.
		targetF targetFunc
		// nodeErrF is set by the node and returns the error the node failed with.
		nodeErrF func() error
		// traceF is set by the node when its event trace is enabled. It is called under mu, except for errors.
		traceF func(traceRecord)
		// metrics are the metrics of the node, nil for containers without a node
		metrics *nodeMetrics
		// entityName is the name of the root entity bitmap of the container's entity, set by the node
		entityName string
		// lent holds the chain of nodes lending the lock while the container executes a waited event of another node,
		// nil while it holds the lock itself. Transitions emitting waited events lend the lock on.
		lent atomic.Pointer[[]string]
		// lockToken is the token of the container's own lock, set by the node. Empty on a lent lock or without raft.
		lockToken string
		// slot reports whether the container holds a slot of the node's max container count
		slot bool
		// ctx is the context the container runs on (start), stopCtxWatch stops sending the close signal when it
		// ends, closedF runs once the container closed
		ctx          context.Context
		stopCtxWatch func() bool
		closedF      func()
		// keepAlive is how long the running goroutine waits for more work before it ends (WithContainerKeepAlive)
		keepAlive time.Duration
		// callEvents are the events the running func transition executes, for the wait path of events it emits
		callEvents []*containerEvent
		// snapshot holds the entity, states and batches when processing of the current event started and after it,
		// while the trace is enabled, so dev tools can show them while the container is busy
		snapshot atomic.Pointer[containerSnapshot]
		// routing caches the routing transitions by event name for the transitions in routingFor
		routing    map[string]*Transition
		routingFor *publishedTransitions

		// mu serializes the processing of events and the execution of batch timers
		mu       sync.Mutex
		initOnce sync.Once
		status   containerStatus

		logger *zap.Logger
	}
	containerEvent struct {
		payload    interface{}
		name       string
		opts       []EventOption
		doneC      chan<- struct{}
		waitRemote bool
		closeAfter bool
		remote     *remoteExecution
		// parent is the forwarded event a payload of it belongs to, it is released once the payload is
		parent *containerEvent
		// waitPath holds the lock tokens of the containers waiting for the event, nil if nobody does
		waitPath []string
		// trace is the trace of the event with the span it is part of, spanId the span of its processing
		// (the RECEIVED span for payloads of forwarded events); both only with tracing
		trace  *service.TraceRef
		spanId string
		// remoteMatches names the remote transitions the event matched, the event is routed to their owners
		remoteMatches []string
		// call is set for a waited event emitted by a transition: it collects what the forwards return to Emit
		call *emitCall
		// refs counts the processing pass and every batch still holding the event.
		// The done channel is signaled once it drops to zero.
		refs int
	}
	// emitCall collects the errors and replies of the waited forwards of an emitted event while Emit runs. A batched
	// event forwarded after Emit returned reports its errors to the error func as before.
	emitCall struct {
		replyF   func(any)
		errs     []error
		returned bool
	}
	// containerProcessing holds the state of one life of a container.
	// Resetting a container replaces it instead of modifying it, so holders of the previous one only reach the closed
	// life (containers of a node are not reused, ResetContainer is for containers built with NewContainer).
	containerProcessing struct {
		// eventsC queues the events, the mailbox of the container
		eventsC chan *containerEvent
		// runner runs the container (Container.run), started by schedule while work is queued
		runner interface{ run() }
		// wakeC (capacity 1) wakes a running goroutine waiting for more work (keep-alive): queued errors, the close
		// signal, a batch timer leaving a closing container idle. Events arrive through eventsC.
		wakeC chan struct{}

		// errs queues the errors for the running goroutine.
		// A queue instead of a buffered channel of errors: no memory per container, and no error is dropped.
		errMu sync.Mutex
		errs  []queuedError

		// batches waiting for more events, by transition name
		transitionQueues map[string]*transitionQueue
		transitionTimers map[string]*time.Timer

		// lifecycleMu guards pending, accepting, closeRequested, running, closed and doneC.
		// The container only stops accepting events once pending dropped to zero after a close signal.
		lifecycleMu    sync.Mutex
		pending        int
		accepting      bool
		closeRequested bool
		// running is set while a goroutine runs the container
		running bool
		// doneC is closed once the container closed; it is created when the first one waits for it (done)
		doneC  chan struct{}
		closed bool
	}
	transitionQueue struct {
		t      *Transition
		events []*containerEvent
	}
	// queuedError is an error of the container with the traces of the events it belongs to
	queuedError struct {
		err    error
		traces []*service.TraceRef
	}
	// containerSnapshot is the state of a container for dev tools, taken cheaply while processing (a copy of the
	// entity, the states and the batch sizes) and encoded only when the container is inspected
	containerSnapshot struct {
		entity proto.Message
		// encoded right away instead of entity: entities which are nil or have no protobuf reflection (fakes of tests)
		entityType, entityJSON string
		states                 map[string]Bits
		batches                map[string]uint32
	}
	containerStatus string
)

const (
	containerStatusRunning containerStatus = "running"
	containerStatusClosed  containerStatus = "closed"
)

// ErrContainerClosed is returned when sending an event to a container which stopped accepting events.
var ErrContainerClosed = errors.New("container is closed")

func newContainerProcessing(maxInFlight int) *containerProcessing {
	p := &containerProcessing{}
	p.init(maxInFlight)
	return p
}

// init prepares a new state of a container life.
func (p *containerProcessing) init(maxInFlight int) {
	// the batch maps are created when a transition batches the first time, the done channel when someone waits
	p.eventsC = make(chan *containerEvent, maxInFlight)
	p.wakeC = make(chan struct{}, 1)
	p.accepting = true
}

// closedC is a closed channel, the done channel of a container which closed before anyone waited for it
var closedC = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// scheduleEvent is schedule for an event put into eventsC: a running goroutine waiting for more work receives it
// from eventsC, it isn't woken.
func (p *containerProcessing) scheduleEvent() {
	p.lifecycleMu.Lock()
	if p.running || p.closed || p.runner == nil {
		p.lifecycleMu.Unlock()
		return
	}
	p.running = true
	p.lifecycleMu.Unlock()
	go p.runner.run()
}

// schedule starts a goroutine running the container, unless the container closed or wasn't started yet; a running
// one is woken in case it waits for more work. Whoever makes work visible calls it afterwards: an event in eventsC,
// an error, the close signal, a batch timer leaving a closing container idle. The running goroutine only ends after
// checking for work under lifecycleMu (stopRunning), so no work is left behind without a goroutine.
func (p *containerProcessing) schedule() {
	p.lifecycleMu.Lock()
	if p.closed || p.runner == nil {
		p.lifecycleMu.Unlock()
		return
	}
	if p.running {
		p.lifecycleMu.Unlock()
		select {
		case p.wakeC <- struct{}{}:
		default:
		}
		return
	}
	p.running = true
	p.lifecycleMu.Unlock()
	go p.runner.run()
}

// hasWork reports whether work is queued: events, errors, or an idle container to close.
func (p *containerProcessing) hasWork() bool {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	return p.hasWorkLocked()
}

func (p *containerProcessing) hasWorkLocked() bool {
	if len(p.eventsC) > 0 || (p.closeRequested && p.pending == 0) {
		return true
	}
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return len(p.errs) > 0
}

// stopRunning ends the running goroutine if no work is queued.
func (p *containerProcessing) stopRunning() bool {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if p.hasWorkLocked() {
		return false
	}
	p.running = false
	return true
}

// done returns a channel which is closed once the container closed.
func (p *containerProcessing) done() <-chan struct{} {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if p.doneC == nil {
		if p.closed {
			return closedC
		}
		p.doneC = make(chan struct{})
	}
	return p.doneC
}

// markDone signals the waiters of done that the container closed.
func (p *containerProcessing) markDone() {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	p.closed = true
	if p.doneC != nil {
		close(p.doneC)
	}
}

// addPending registers work the container has to finish before it may close.
// Events are only accepted while the container is accepting. Internal work (eg. batch timers)
// is always accepted because it is added while an event is being processed.
func (p *containerProcessing) addPending(event bool) bool {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if event && !p.accepting {
		return false
	}
	p.pending++

	return true
}

func (p *containerProcessing) donePending() {
	p.lifecycleMu.Lock()
	p.pending--
	// a closing container closes once it is idle
	idle := p.pending == 0 && p.closeRequested
	p.lifecycleMu.Unlock()

	if idle {
		p.schedule()
	}
}

// stopAccepting stops accepting events if the container got the close signal and is idle, and reports whether it
// did.
func (p *containerProcessing) stopAccepting() bool {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if !p.closeRequested || p.pending > 0 {
		return false
	}
	p.accepting = false
	return true
}

// closing reports whether the container got the close signal.
func (p *containerProcessing) closing() bool {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	return p.closeRequested
}

// takeErrors returns the queued errors.
func (p *containerProcessing) takeErrors() []queuedError {
	p.errMu.Lock()
	defer p.errMu.Unlock()

	errs := p.errs
	p.errs = nil
	return errs
}

func (p *containerProcessing) isAccepting() bool {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	return p.accepting
}

func (c *Container[T, K]) currentBitmap() *Bitmap {
	if c.bitmapP != nil {
		return c.bitmapP.Load()
	}
	return c.bitmap
}

// setState sets the state of a root entity bitmap; the map is created on the first state, a container which
// closes without events doesn't need one.
func (c *Container[T, K]) setState(name string, bits Bits) {
	if c.states == nil {
		c.states = make(map[string]Bits)
	}
	c.states[name] = bits
}

// currentTransitions returns the transitions of the node, or the container's own without a node.
func (c *Container[T, K]) currentTransitions() *publishedTransitions {
	if c.transitionsP != nil {
		return c.transitionsP.Load()
	}
	return c.transitions
}

// start lets the container run on ctx: queued work starts a goroutine running it (schedule), ctx ending sends the
// close signal. closedF runs once the container closed. An idle container has no goroutine.
func (c *Container[T, K]) start(ctx context.Context, closedF func()) {
	p := c.containerProcessing
	c.ctx, c.closedF = ctx, closedF
	c.stopCtxWatch = context.AfterFunc(ctx, p.requestClose)
	p.lifecycleMu.Lock()
	p.runner = c
	p.lifecycleMu.Unlock()
	// work queued before
	p.schedule()
}

// run processes the queued events and errors of the container and closes it after the close signal, once it has
// no events in the pipeline and is not doing any work (closeIdle). With nothing queued it waits keepAlive for more
// work, then returns: starting a goroutine for every burst of work costs more than keeping it a few milliseconds.
func (c *Container[T, K]) run() {
	p := c.containerProcessing
	// the keep-alive timer runs from the first wait on and isn't reset per event: when it fires after work came
	// meanwhile, it is armed again for the rest of the keep-alive
	var keepAlive *time.Timer
	lastWork := time.Now()
	defer func() {
		if keepAlive != nil {
			keepAlive.Stop()
		}
	}()
	for {
		select {
		case evt := <-p.eventsC:
			c.processEvent(c.ctx, evt)
			lastWork = time.Now()
		default:
		}
		for _, e := range p.takeErrors() {
			c.reportError(c.ctx, p, e)
		}

		if p.stopAccepting() {
			c.closeIdle(c.ctx, p)
			c.stopCtxWatch()
			if c.closedF != nil {
				c.closedF()
			}
			return
		}
		if c.keepAlive > 0 && !p.hasWork() {
			if keepAlive == nil {
				keepAlive = time.NewTimer(c.keepAlive)
			}
			select {
			case evt := <-p.eventsC:
				c.processEvent(c.ctx, evt)
				lastWork = time.Now()
				continue
			case <-p.wakeC:
				continue
			case <-keepAlive.C:
				if rest := c.keepAlive - time.Since(lastWork); rest > 0 {
					keepAlive.Reset(rest)
					continue
				}
			}
		}
		if p.stopRunning() {
			return
		}
	}
}

// reportError logs an error of the container, traces it and passes it to the error func.
func (c *Container[T, K]) reportError(ctx context.Context, p *containerProcessing, e queuedError) {
	// %+v prints the stack trace for errors that carry one
	c.logger.Error(fmt.Sprintf("error happened in container: %+v", e.err))
	if c.traceF != nil {
		c.traceF(traceRecord{kind: service.TraceEvent_ERROR, err: e.err, spanId: newTraceId(), traces: e.traces})
	}
	c.handleError(ctx, p, e.err)
}

// handleError passes an error of the container to the error func.
// It holds mu like a transition, so the error func can use the container safely.
// A returned error sends the container the close signal.
// Errors of the close func arrive after the container closed, so the error func can't send events then.
func (c *Container[T, K]) handleError(ctx context.Context, p *containerProcessing, err error) {
	if c.errorF == nil {
		return
	}

	c.mu.Lock()
	closeErr := c.errorF(ctx, c, err)
	c.mu.Unlock()

	if closeErr != nil {
		c.logger.Info(fmt.Sprintf("error func returned an error, closing container: %+v", closeErr))
		p.requestClose()
	}
}

// closeIdle closes the container, which stopped accepting events.
func (c *Container[T, K]) closeIdle(ctx context.Context, p *containerProcessing) {
	// execute batches which are still waiting for more events
	c.flushBatches(ctx)

	c.logger.Debug("all events processed, executing close func if present")

	if c.closeF != nil {
		err := c.closeF(ctx, c)
		if err != nil {
			c.sendError(err)
		}
	}

	close(p.eventsC)

	c.logger.Debug("all channels closed, done closing container")
	c.status = containerStatusClosed

	if c.removeF != nil {
		c.removeF()
	}

	// signal waiter that container is done closing
	p.markDone()

	// errors of the last batches and the close func arrive after the container closed
	for _, e := range p.takeErrors() {
		c.reportError(ctx, p, e)
	}
}

func (c *Container[T, K]) processEvent(ctx context.Context, evt *containerEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if m := c.metrics; m != nil {
		start := time.Now()
		defer func() {
			m.events.Add(1)
			m.eventTime.observe(time.Since(start))
		}()
	}

	// the event itself is done after this pass, batches holding it keep a reference
	defer c.donePending()
	evt.refs = 1
	defer c.releaseEvent(evt)

	debugf(c.logger, "received event %s", evt.name)

	if c.initF != nil {
		c.initOnce.Do(func() {
			c.logger.Debug("executing container init func")
			err := c.initF(ctx, c)
			if err != nil {
				c.logger.Debug("container init func failed", zap.Error(err))
				c.sendError(err)
			}
		})
	}

	// events forwarded by another node run the own func transitions of this node
	if evt.remote != nil {
		c.processRemote(ctx, evt)
		return
	}

	c.runEvent(ctx, evt)
}

// runEvent runs the transitions of an event: the own ones in priority order, remote ones are routed at their place.
// The caller holds mu.
func (c *Container[T, K]) runEvent(ctx context.Context, evt *containerEvent) {
	start := time.Now()
	c.EvaluateEntityWithEvents([]any{evt.payload})
	if c.traceF != nil {
		evt.spanId = newTraceId()
		if evt.trace == nil {
			evt.trace = &service.TraceRef{TraceId: newTraceId()}
		}
		c.storeSnapshot()
	}

	// matching remote transitions are forwarded at their place in the priority order: the ones collected
	// are forwarded together before the next local transition of the event is evaluated, so with wait it sees
	// their result. Batched forwarding can't return a result in time, so the event joins the batch at the end.
	rt := c.routingTransition(evt.name)
	batched := rt.Batching.Count > 1
	saveAfter := false
	var remoteMatches, ran, routed []string
	route := func() {
		if len(remoteMatches) == 0 {
			return
		}
		evt.remoteMatches = remoteMatches
		remoteMatches = nil
		saveAfter = c.executeOrQueue(ctx, rt, evt, saveAfter)
	}

	for _, t := range c.currentTransitions().forEvent(evt.name) {
		// the receiving nodes decide which of their transitions run
		if t.Remote {
			if c.matchesRemote(t, evt.name) {
				remoteMatches = append(remoteMatches, t.Name)
				if c.traceF != nil {
					routed = append(routed, t.Name)
				}
			}
			continue
		}
		if !batched {
			route()
		}
		if !c.matches(t, evt.name) {
			continue
		}
		debugf(c.logger, "transition %s matches conditions", t.Name)

		if c.traceF != nil {
			ran = append(ran, t.Name)
		}
		saveAfter = c.executeOrQueue(ctx, t, evt, saveAfter)
	}
	route()

	// execute the save event
	if saveAfter {
		c.save(ctx, evt.name)
	}

	if c.traceF != nil {
		c.storeSnapshot()
		c.traceF(traceRecord{
			kind:        service.TraceEvent_EVENT,
			event:       evt.name,
			payloads:    []any{evt.payload},
			transitions: ran,
			remote:      routed,
			wait:        evt.waitRemote,
			duration:    time.Since(start),
			spanId:      evt.spanId,
			traces:      []*service.TraceRef{evt.trace},
		})
	}
}

// spanOrParent returns the span of the event, or the span it is part of if this node doesn't trace,
// so the spans of the next nodes attach to the last traced one.
func (evt *containerEvent) spanOrParent() string {
	if evt.spanId != "" {
		return evt.spanId
	}
	return evt.trace.GetParentSpanId()
}

// ErrNotInTransition is returned by Emit when it isn't called with the ctx of a transition of the container.
var ErrNotInTransition = errors.New("emit is only possible with the ctx of a transition of the container")

// maxEmitDepth limits events emitted by transitions of events emitted by transitions, eg. a transition emitting its own event.
const maxEmitDepth = 16

type (
	transitionCtxKey struct{}
	// transitionCtx marks the ctx a container passes to its func transitions
	transitionCtx struct {
		container any
		depth     int
	}
)

// withTransition marks the ctx of a func transition of the container, keeping the depth of an emitted event.
func (c *Container[T, K]) withTransition(ctx context.Context) context.Context {
	if tc, ok := ctx.Value(transitionCtxKey{}).(transitionCtx); ok && tc.container == any(c) {
		return ctx
	}
	return context.WithValue(ctx, transitionCtxKey{}, transitionCtx{container: c})
}

// Emit processes an event as part of the transition calling it, with the ctx the transition got.
// The transitions of this container matching the event run right away, remote ones are routed.
// With WithWaitRemote the transition waits for the receiving nodes: a container holding the lock lends it to them,
// and a container running on a lent lock lends it on. Without it the work is handed over: the receiving nodes
// get the lock once the node holding it released it.
// Called with another ctx it returns ErrNotInTransition.
func (c *Container[T, K]) Emit(ctx context.Context, eventName string, event any, opts ...EventOption) error {
	tc, ok := ctx.Value(transitionCtxKey{}).(transitionCtx)
	if !ok || tc.container != any(c) {
		return ErrNotInTransition
	}
	if tc.depth >= maxEmitDepth {
		return fmt.Errorf("emitting event %s: more than %d nested events", eventName, maxEmitDepth)
	}

	var eventOptions eventOptions
	for _, opt := range opts {
		opt.apply(&eventOptions)
	}
	evt := &containerEvent{
		payload:    event,
		name:       eventName,
		opts:       opts,
		doneC:      eventOptions.doneC,
		waitRemote: eventOptions.waitRemote,
		// whoever waits for the calling transition waits for the emitted event too
		waitPath: c.waitPathOf(c.callEvents),
		refs:     1,
	}
	// the emitted event is part of the trace of the calling transition's event
	for _, call := range c.callEvents {
		if call.trace != nil {
			evt.trace = &service.TraceRef{TraceId: call.trace.GetTraceId(), ParentSpanId: call.spanOrParent()}
			break
		}
	}
	if eventOptions.target != "" {
		return c.emitToTarget(ctx, evt, eventOptions)
	}
	debugf(c.logger, "emitted event %s", eventName)
	if evt.waitRemote {
		evt.call = &emitCall{replyF: eventOptions.replyF}
	}

	c.runEvent(context.WithValue(ctx, transitionCtxKey{}, transitionCtx{container: c, depth: tc.depth + 1}), evt)
	c.releaseEvent(evt)
	if evt.call == nil {
		return nil
	}
	evt.call.returned = true
	return errors.Join(evt.call.errs...)
}

// ErrNoCaller is returned by Reply when no waited call of another container runs the transition's event.
var ErrNoCaller = errors.New("no waiting caller to reply to")

// emitToTarget runs an emitted event in the container of the target id (WithTarget). The calling transition holds the
// container, so the target can't be this container.
func (c *Container[T, K]) emitToTarget(ctx context.Context, evt *containerEvent, options eventOptions) error {
	if options.target == c.id {
		return fmt.Errorf("emitting event %s: the target is the emitting container %s", evt.name, c.id)
	}
	if c.targetF == nil {
		return fmt.Errorf("emitting event %s to container %s: only containers of a node can call other containers", evt.name, options.target)
	}
	debugf(c.logger, "emitted event %s to container %s", evt.name, options.target)

	reply, err := c.targetF(ctx, options.target, evt.name, evt.payload, evt.waitRemote, evt.waitPath, evt.trace)
	if err != nil {
		return fmt.Errorf("event %s in container %s: %w", evt.name, options.target, err)
	}
	if reply != nil && options.replyF != nil {
		options.replyF(reply)
	}
	return nil
}

// Reply sets the reply of a transition to the container waiting for its event (Emit with WithWaitRemote: a target
// call, or a waited forward, also from another entity), with the ctx the transition got. A later reply replaces an
// earlier one. A reply to another node must be a protobuf message. It returns ErrNotInTransition with another ctx and
// ErrNoCaller if nobody waits for the event.
func (c *Container[T, K]) Reply(ctx context.Context, reply any) error {
	if tc, ok := ctx.Value(transitionCtxKey{}).(transitionCtx); !ok || tc.container != any(c) {
		return ErrNotInTransition
	}
	replied := false
	for _, evt := range c.callEvents {
		if evt.parent != nil && evt.parent.remote != nil && evt.parent.remote.waited {
			evt.parent.remote.reply = reply
			replied = true
		}
	}
	if !replied {
		return ErrNoCaller
	}
	return nil
}

// waitPathOf returns the wait path of transitions running for the events: the containers waiting for one of them,
// and this container if it holds its own lock.
func (c *Container[T, K]) waitPathOf(events []*containerEvent) []string {
	var path []string
	for _, evt := range events {
		for _, t := range evt.waitPath {
			if !slices.Contains(path, t) {
				path = append(path, t)
			}
		}
	}
	if c.lockToken != "" && !slices.Contains(path, c.lockToken) {
		path = append(path, c.lockToken)
	}
	return path
}

// lendChain returns the nodes lending the lock the container runs on, nil if it holds the lock itself.
func (c *Container[T, K]) lendChain() []string {
	if chain := c.lent.Load(); chain != nil {
		return *chain
	}
	return nil
}

// matchesRemote reports whether a remote transition matches the event. Conditions on the bitmap of another entity
// than the container's are left to the receiving node, which has that entity.
func (c *Container[T, K]) matchesRemote(t *Transition, eventName string) bool {
	if t.Entity == "" || t.Entity == c.entityName {
		return c.matches(t, eventName)
	}
	if t.EventName != eventName {
		return false
	}
	conditions := make([]*Condition, 0, len(t.Conditions))
	for _, cond := range t.Conditions {
		if _, ok := cond.States[t.Entity]; !ok {
			conditions = append(conditions, cond)
		}
	}
	return len(conditions) == 0 || evaluateConditions(conditions, c.states)
}

// receives reports whether the container runs the transition for forwarded events: its own func transitions,
// and for events of another entity also the state transitions of its entity, which the sender can't run.
func (c *Container[T, K]) receives(t *Transition, r *remoteExecution) bool {
	if isOwnFuncTransition[T, K](t) {
		return true
	}
	return r.otherEntity && !t.Remote && (t.Entity == "" || t.Entity == c.entityName)
}

// matches reports whether the transition listens to the event and its conditions are satisfied.
func (c *Container[T, K]) matches(t *Transition, eventName string) bool {
	return t.EventName == eventName && (len(t.Conditions) == 0 || evaluateConditions(t.Conditions, c.states))
}

// executeOrQueue executes the transition with the event, or queues the event if the transition batches.
// The executed actions release the event again.
func (c *Container[T, K]) executeOrQueue(ctx context.Context, t *Transition, evt *containerEvent, saveAfter bool) bool {
	evt.refs++

	// if a transitions batch is configured to be only one event,
	// then we can just send the event to the transition without needing to queue
	if t.Batching.Count <= 1 {
		return c.executeActions(ctx, t, []*containerEvent{evt}, saveAfter)
	}

	if c.transitionQueues == nil {
		c.transitionQueues = make(map[string]*transitionQueue)
	}
	queue, ok := c.transitionQueues[t.Name]
	if !ok {
		queue = &transitionQueue{}
		c.transitionQueues[t.Name] = queue
	}
	queue.t = t
	queue.events = append(queue.events, evt)

	if len(queue.events) >= t.Batching.Count {
		delete(c.transitionQueues, t.Name)

		saveAfter = c.executeActions(ctx, t, queue.events, saveAfter)

		if transitionTimer, ok := c.transitionTimers[t.Name]; ok {
			// if the timer could be stopped, its pending work is done.
			// if not, it already fired and its func returns without work because it is not the current timer anymore
			if transitionTimer.Stop() {
				c.donePending()
			}
			delete(c.transitionTimers, t.Name)
		}
	} else if t.Batching.Period > 0 {
		if transitionTimer, ok := c.transitionTimers[t.Name]; !ok {
			c.startBatchTimer(ctx, t)
		} else if transitionTimer.Stop() {
			transitionTimer.Reset(t.Batching.Period)
		}
		// if the timer could not be stopped, it already fired and its func
		// is waiting for the lock, so it will execute this event as well
	}

	return saveAfter
}

// routingPrefix keys the batches of events routed to other nodes. User transition names don't contain NUL.
const routingPrefix = "\x00route:"

// routingTransition batches the events of a name which match remote transitions before they are forwarded.
// It uses the batching config of the remote transitions listening to the name:
// the one with the largest count wins, then the one with the longest period.
func (c *Container[T, K]) routingTransition(eventName string) *Transition {
	// published transitions are replaced as a whole on every change, so the cache is valid as long as they are the same
	ts := c.currentTransitions()
	if ts != c.routingFor {
		c.routing, c.routingFor = nil, ts
	}
	if rt, ok := c.routing[eventName]; ok {
		return rt
	}

	rt := &Transition{Name: routingPrefix + eventName, EventName: eventName, Remote: true}
	defer func() {
		if c.routing == nil {
			c.routing = make(map[string]*Transition)
		}
		c.routing[eventName] = rt
	}()
	for _, t := range ts.forEvent(eventName) {
		if !t.Remote {
			continue
		}
		if b := t.Batching; b.Count > rt.Batching.Count || (b.Count == rt.Batching.Count && b.Period > rt.Batching.Period) {
			rt.Batching = b
		}
	}
	return rt
}

// isOwnFuncTransition reports whether the transition is a func transition of this node.
func isOwnFuncTransition[T proto.Message, K Builder[T]](t *Transition) bool {
	if t.Remote {
		return false
	}
	return slices.ContainsFunc(t.Actions, func(a any) bool {
		_, ok := a.(transitionFunc[T, K])
		return ok
	})
}

// processRemote runs the own func transitions of this node for events another node forwarded,
// except the ones the sender excluded. Every payload passes the transitions like a local event,
// so batching transitions batch them and the others run once per payload, in order.
// A forwarded container gets the close signal after this pass, so batches still waiting run when it closes.
func (c *Container[T, K]) processRemote(ctx context.Context, evt *containerEvent) {
	r := evt.remote
	if evt.closeAfter {
		defer c.containerProcessing.requestClose()
	}

	if r.entity != nil {
		entity, ok := r.entity.(T)
		if !ok {
			err := fmt.Errorf("remote entity of type %T does not match the entity type of the container", r.entity)
			r.errs = append(r.errs, err)
			c.sendError(err)
			return
		}
		c.builder.Set(entity)
	}
	// the container runs on the lent lock until the forwarded events are released, batches included
	if r.chain != nil {
		chain := r.chain
		c.lent.Store(&chain)
	}

	debugf(c.logger, "processing %d events %s forwarded by another node", len(r.payloads), evt.name)

	start := time.Now()
	// the payloads are part of their traces, the RECEIVED span is the span of all of them
	var receivedSpan string
	var untraced *service.TraceRef
	if c.traceF != nil {
		receivedSpan = newTraceId()
		c.EvaluateEntityWithEvents(nil)
		c.storeSnapshot()
	}
	saveAfter := false
	var ran []string
	subs := make([]*containerEvent, 0, len(r.payloads))
	for i, payload := range r.payloads {
		// every payload holds the forwarded event until its transitions ran
		sub := &containerEvent{payload: payload, name: evt.name, parent: evt, waitPath: r.waitPath, refs: 1, spanId: receivedSpan}
		switch {
		case i < len(r.traces):
			sub.trace = r.traces[i]
		case receivedSpan != "":
			// the sender doesn't trace: the forwarded events start a trace here
			if untraced == nil {
				untraced = &service.TraceRef{TraceId: newTraceId()}
			}
			sub.trace = untraced
		}
		subs = append(subs, sub)
		evt.refs++

		c.EvaluateEntityWithEvents([]any{payload})
		for _, t := range c.currentTransitions().forEvent(evt.name) {
			if !c.receives(t, r) || slices.Contains(r.exclude, t.Name) || !c.matches(t, evt.name) {
				continue
			}
			debugf(c.logger, "transition %s matches conditions of a forwarded event", t.Name)

			if c.traceF != nil && !slices.Contains(ran, t.Name) {
				ran = append(ran, t.Name)
			}
			saveAfter = c.executeOrQueue(ctx, t, sub, saveAfter)
		}
		c.releaseEvent(sub)
	}

	if saveAfter {
		c.save(ctx, evt.name)
	}

	if c.traceF != nil {
		c.storeSnapshot()
		c.traceF(traceRecord{
			kind:        service.TraceEvent_RECEIVED,
			event:       evt.name,
			payloads:    r.payloads,
			transitions: ran,
			excluded:    r.exclude,
			from:        r.from,
			wait:        r.entity != nil,
			duration:    time.Since(start),
			spanId:      receivedSpan,
			traces:      receivedTraces(subs),
		})
	}
}

// receivedTraces returns the traces of the payloads of forwarded events with their parents, every trace once.
func receivedTraces(subs []*containerEvent) []*service.TraceRef {
	var refs []*service.TraceRef
	for _, sub := range subs {
		if sub.trace != nil && !slices.ContainsFunc(refs, func(r *service.TraceRef) bool { return r.GetTraceId() == sub.trace.GetTraceId() }) {
			refs = append(refs, sub.trace)
		}
	}
	return refs
}

// readInfo fills the entity, states and batches of the container into info. The caller holds mu.
func (c *Container[T, K]) readInfo(info *service.ContainerInfo) {
	info.EntityType, info.EntityJson = toJSON(c.builder.Build())
	info.States = make(map[string]*service.Bits, len(c.states))
	for name, bits := range c.states {
		info.States[name] = bits.toProto()
	}
	info.Batches = make(map[string]uint32, len(c.transitionQueues))
	for name, q := range c.transitionQueues {
		if event, ok := strings.CutPrefix(name, routingPrefix); ok {
			name = "route:" + event
		}
		info.Batches[name] = uint32(len(q.events))
	}
}

// storeSnapshot keeps a copy of the entity, the states and the batch sizes for dev tools. The caller holds mu.
func (c *Container[T, K]) storeSnapshot() {
	snap := &containerSnapshot{
		states:  maps.Clone(c.states),
		batches: make(map[string]uint32, len(c.transitionQueues)),
	}
	if e := proto.Message(c.builder.Build()); isNilMessage(e) || e.ProtoReflect() == nil {
		snap.entityType, snap.entityJSON = toJSON(e)
	} else {
		snap.entity = proto.Clone(e)
	}
	for name, q := range c.transitionQueues {
		snap.batches[name] = uint32(len(q.events))
	}
	c.snapshot.Store(snap)
}

// info fills the container info from the snapshot, encoding the entity.
func (s *containerSnapshot) info(info *service.ContainerInfo) {
	info.EntityType, info.EntityJson = s.entityType, s.entityJSON
	if s.entity != nil {
		info.EntityType, info.EntityJson = toJSON(s.entity)
	}
	info.States = make(map[string]*service.Bits, len(s.states))
	for name, bits := range s.states {
		info.States[name] = bits.toProto()
	}
	info.Batches = make(map[string]uint32, len(s.batches))
	for name, size := range s.batches {
		if event, ok := strings.CutPrefix(name, routingPrefix); ok {
			name = "route:" + event
		}
		info.Batches[name] = size
	}
}

// startBatchTimer starts a timer which executes the queued events of a transition
// when not enough events arrived within the batching period.
func (c *Container[T, K]) startBatchTimer(ctx context.Context, t *Transition) {
	// the running timer is pending work, so the container does not close before it executed
	c.addPending(false)

	var transitionTimer *time.Timer
	transitionTimer = time.AfterFunc(t.Batching.Period, func() {
		defer c.donePending()

		c.mu.Lock()
		defer c.mu.Unlock()

		// if this is not the current timer, a full batch already executed its events
		if c.transitionTimers[t.Name] != transitionTimer {
			return
		}
		delete(c.transitionTimers, t.Name)

		queue, ok := c.transitionQueues[t.Name]
		if !ok || len(queue.events) == 0 {
			return
		}
		delete(c.transitionQueues, t.Name)

		if c.executeActions(ctx, queue.t, queue.events, false) {
			c.save(ctx, t.EventName)
		}
	})
	if c.transitionTimers == nil {
		c.transitionTimers = make(map[string]*time.Timer)
	}
	c.transitionTimers[t.Name] = transitionTimer
}

// flushBatches executes all batches still waiting for more events.
func (c *Container[T, K]) flushBatches(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// execute in the order of the transitions, batches of transitions which are gone afterwards
	names := make([]string, 0, len(c.transitionQueues))
	for _, t := range c.currentTransitions().list() {
		if _, ok := c.transitionQueues[t.Name]; ok && !slices.Contains(names, t.Name) {
			names = append(names, t.Name)
		}
	}
	rest := make([]string, 0)
	for name := range c.transitionQueues {
		if !slices.Contains(names, name) {
			rest = append(rest, name)
		}
	}
	slices.Sort(rest)
	names = append(names, rest...)

	saveAfter := false
	for _, name := range names {
		queue := c.transitionQueues[name]
		delete(c.transitionQueues, name)
		if len(queue.events) == 0 {
			continue
		}

		saveAfter = c.executeActions(ctx, queue.t, queue.events, saveAfter)
	}

	if saveAfter {
		c.save(ctx, "close")
	}
}

func (c *Container[T, K]) executeActions(ctx context.Context, t *Transition, events []*containerEvent, saveAfter bool) bool {
	defer func() {
		for _, evt := range events {
			c.releaseEvent(evt)
		}
	}()

	payloads := mapSlice(events, func(e *containerEvent) any { return e.payload })

	if t.Remote {
		c.forward(ctx, t, events, payloads)
		return saveAfter
	}

	for _, action := range t.Actions {
		switch stateOrFunc := action.(type) {
		case Bits:
			debugf(c.logger, "executing state transition %s", t.Name)

			entity := c.builder.Build()
			entityBitmap := c.getBitmapByTypeF(entity, c.currentBitmap())
			if entityBitmap == nil {
				c.actionFailed(fmt.Errorf("bitmap for entity of state transition %s not found", t.Name), events)
				continue
			}

			// replace the bits of every property the state action sets
			c.setState(entityBitmap.Name, mergeState(entityBitmap, c.states[entityBitmap.Name], stateOrFunc))

			// update entity with state
			ApplyStateToEntity(entity, c.states[entityBitmap.Name], entityBitmap)

			if t.SaveOnChange {
				saveAfter = true
			}
		case transitionFunc[T, K]:
			var err error

			skipSave := false
			prevEvents := c.callEvents
			c.callEvents = events

			debugf(c.logger, "executing func transition %s", t.Name)

			if t.Batching.Count == 1 {
				skipSave, err = stateOrFunc(c.withTransition(ctx), c, payloads[0])
				if err != nil {
					debugf(c.logger, "func transition %s failed: %v", t.Name, err)
					c.actionFailed(err, events)
				}
			} else {
				skipSave, err = stateOrFunc(c.withTransition(ctx), c, payloads)
				if err != nil {
					debugf(c.logger, "func transition %s failed: %v", t.Name, err)
					c.actionFailed(err, events)
				}
			}
			c.callEvents = prevEvents

			// the func may have changed the entity (also when it failed), so the following transitions
			// of the event are evaluated against its new state
			c.EvaluateEntityWithEvents(nil)

			// only save if no error is returned and skipSave is false
			if t.SaveOnChange && !skipSave {
				saveAfter = true
			}
		}
	}

	return saveAfter
}

// forward routes the events of a name to the nodes owning the remote transitions they matched.
// The receiving nodes decide which of their transitions run.
//
// If one of the events waits for remote transitions, the container lends its lock to the nodes,
// blocks until they have finished and merges the entities they returned into the builder:
// the changes of every node to the sent entity are applied field by field.
// Otherwise the work is handed over: the nodes accept it right away and execute it in their own containers
// once this container released the lock, with the entity loaded by their init funcs.
func (c *Container[T, K]) forward(ctx context.Context, t *Transition, events []*containerEvent, payloads []any) {
	if c.forwardF == nil {
		c.sendError(fmt.Errorf("event %s can not be forwarded without a node", t.EventName))
		return
	}

	matched := make([]string, 0)
	for _, evt := range events {
		for _, name := range evt.remoteMatches {
			if !slices.Contains(matched, name) {
				matched = append(matched, name)
			}
		}
	}
	wait := slices.ContainsFunc(events, func(e *containerEvent) bool { return e.waitRemote })

	// marshal before sending, so later changes to the entity or the events do not race with sending.
	// a handed over execution loads the entity on the receiver, so it isn't sent.
	var sent proto.Message
	var entity any
	if wait {
		if e := proto.Message(c.builder.Build()); !isNilMessage(e) {
			sent, entity = e, e
		}
	}
	entityAny, remotePayloads, err := marshalRemote(entity, payloads)
	if err != nil {
		c.sendError(fmt.Errorf("forwarding event %s: %w", t.EventName, err))
		return
	}

	if wait {
		debugf(c.logger, "forwarding %d events %s and lending the lock", len(events), t.EventName)
	} else {
		debugf(c.logger, "forwarding %d events %s", len(events), t.EventName)
	}

	var waitPath []string
	if wait {
		waitPath = c.waitPathOf(events)
	}
	var handOverErrF func(error)
	if !wait {
		handOverErrF = func(err error) {
			c.lateError(fmt.Errorf("error forwarding event %s: %w", t.EventName, err))
		}
	}
	results, err := c.forwardF(ctx, t.EventName, c.id, c.lendChain(), waitPath, matched, entityAny, remotePayloads, wait, c.forwardTraceOf(events), handOverErrF)
	// Emits waiting for the events get the errors and replies, the error func gets the rest
	var calls []*emitCall
	for _, evt := range events {
		if evt.call != nil && !evt.call.returned && !slices.Contains(calls, evt.call) {
			calls = append(calls, evt.call)
		}
	}
	if err != nil {
		err = fmt.Errorf("error forwarding event %s: %w", t.EventName, err)
		if len(calls) == 0 {
			c.sendError(err)
		}
		for _, call := range calls {
			call.errs = append(call.errs, err)
		}
	}
	if !wait || len(results) == 0 {
		return
	}
	for _, result := range results {
		for _, reply := range result.replies {
			for _, call := range calls {
				if call.replyF != nil {
					call.replyF(reply)
				}
			}
		}
	}

	var base, merged protoreflect.Message
	if sent != nil {
		base = sent.ProtoReflect()
	}
	// one result is merged into the sent entity, the builder's: nothing else changes it while the container waits.
	// Several results are merged into a copy, each compared with the sent entity.
	mergeInto := func(base protoreflect.Message) protoreflect.Message {
		if len(results) == 1 {
			return base
		}
		return proto.Clone(base.Interface()).ProtoReflect()
	}
	for _, result := range results {
		if result.entity == nil && result.changed == nil {
			// only replies
			continue
		}
		if result.entity == nil {
			// bits of the changed properties, a node only sends them for a sent entity
			if base == nil {
				continue
			}
			if merged == nil {
				merged = mergeInto(base)
			}
			if err := c.applyChangedState(merged.Interface(), result.changed); err != nil {
				c.sendError(fmt.Errorf("forwarded event %s: %w", t.EventName, err))
			}
			continue
		}

		updated, ok := result.entity.(T)
		if !ok {
			c.sendError(fmt.Errorf("forwarded event %s returned entity of type %T", t.EventName, result.entity))
			continue
		}
		u := updated.ProtoReflect()
		if base == nil {
			// without an entity the changes are relative to an empty one
			base = u.New()
		}
		if merged == nil {
			merged = mergeInto(base)
		}
		mergeChangedFields(merged, base, u)
	}
	if merged == nil {
		return
	}
	c.builder.Set(merged.Interface().(T))

	// the following events see the state of the updated entity
	c.EvaluateEntityWithEvents(nil)
}

// forwardTraceOf returns the FORWARD span of the events, nil if none of them is traced. Without tracing on this node
// the traces only pass through: the payloads keep the spans their events are part of.
func (c *Container[T, K]) forwardTraceOf(events []*containerEvent) *forwardTrace {
	if !slices.ContainsFunc(events, func(e *containerEvent) bool { return e.trace != nil }) {
		return nil
	}
	ft := &forwardTrace{payloads: make([]*service.TraceRef, len(events))}
	if c.traceF != nil {
		ft.spanId = newTraceId()
		ft.parents = traceRefs(events)
	}
	for i, evt := range events {
		if evt.trace == nil {
			continue
		}
		parent := ft.spanId
		if parent == "" {
			parent = evt.spanOrParent()
		}
		ft.payloads[i] = &service.TraceRef{TraceId: evt.trace.GetTraceId(), ParentSpanId: parent}
	}
	return ft
}

// applyChangedState replaces the properties of the entity which the changed bits set.
func (c *Container[T, K]) applyChangedState(entity proto.Message, changed Bits) error {
	bm := c.getBitmapByTypeF(entity, c.currentBitmap())
	if bm == nil {
		return fmt.Errorf("bitmap for entity of type %T not found", entity)
	}
	ApplyStateToEntity(entity, mergeState(bm, EvaluateEntityProperties(entity, bm), changed), bm)
	return nil
}

// mergeChangedFields applies the fields which differ between base and changed to dst.
// Nested messages set on all three are merged recursively, lists and maps are replaced as a whole.
func mergeChangedFields(dst, base, changed protoreflect.Message) {
	fields := changed.Descriptor().Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		if fieldEqual(base, changed, fd) {
			continue
		}
		if fd.Message() != nil && !fd.IsList() && !fd.IsMap() && base.Has(fd) && changed.Has(fd) && dst.Has(fd) {
			mergeChangedFields(dst.Mutable(fd).Message(), base.Get(fd).Message(), changed.Get(fd).Message())
			continue
		}
		if changed.Has(fd) {
			dst.Set(fd, changed.Get(fd))
		} else {
			dst.Clear(fd)
		}
	}
}

// fieldEqual compares one field of two messages of the same type.
func fieldEqual(a, b protoreflect.Message, fd protoreflect.FieldDescriptor) bool {
	if a.Has(fd) != b.Has(fd) {
		return false
	}
	if !a.Has(fd) {
		return true
	}
	// compare messages holding only this field, proto.Equal handles every kind of field
	x, y := a.New(), b.New()
	x.Set(fd, a.Get(fd))
	y.Set(fd, b.Get(fd))
	return proto.Equal(x.Interface(), y.Interface())
}

// marshalRemote converts the entity and the payloads of forwarded events to Any.
// They have to be protobuf messages.
func marshalRemote(entity any, payloads []any) (*anypb.Any, []*anypb.Any, error) {
	var entityAny *anypb.Any
	if !isNilValue(entity) {
		a, err := messageToAny(entity)
		if err != nil {
			return nil, nil, fmt.Errorf("entity: %w", err)
		}
		entityAny = a
	}

	payloadAnys := make([]*anypb.Any, 0, len(payloads))
	for _, payload := range payloads {
		a, err := messageToAny(payload)
		if err != nil {
			return nil, nil, fmt.Errorf("payload: %w", err)
		}
		payloadAnys = append(payloadAnys, a)
	}

	return entityAny, payloadAnys, nil
}

func messageToAny(v any) (*anypb.Any, error) {
	m, ok := v.(proto.Message)
	if !ok || isNilMessage(m) || m.ProtoReflect() == nil {
		return nil, fmt.Errorf("%T is not a protobuf message", v)
	}
	return anypb.New(m)
}

func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Pointer && rv.IsNil()
}

func isNilMessage(m proto.Message) bool {
	return isNilValue(m)
}

// releaseEvent drops one reference of the event. Once neither the processing pass nor a batch holds it anymore,
// it signals the done channel and sends the close signal if the event requested it.
func (c *Container[T, K]) releaseEvent(evt *containerEvent) {
	evt.refs--
	if evt.refs != 0 {
		return
	}
	// a payload of a forwarded event releases the forwarded event
	if evt.parent != nil {
		c.releaseEvent(evt.parent)
		return
	}
	// all transitions of the forwarded events ran, the receiver returns the entity and the errors
	if r := evt.remote; r != nil {
		// the lent lock goes back with the answer
		if r.chain != nil {
			c.lent.Store(nil)
		}
		r.err = errors.Join(r.errs...)
		if entity := proto.Message(c.builder.Build()); !isNilMessage(entity) {
			r.result = proto.Clone(entity)
		}
	}
	if evt.closeAfter {
		c.containerProcessing.requestClose()
	}
	if evt.doneC != nil {
		evt.doneC <- struct{}{}
	}
}

func (c *Container[T, K]) save(ctx context.Context, source string) {
	if c.saveF == nil {
		return
	}

	c.logger.Debug("executing save action", zap.String("source", source))
	err := c.saveF(ctx, c)
	if err != nil {
		c.logger.Debug("save action failed", zap.Error(err))
		c.sendError(err)
	}
}

// actionFailed reports the error of an action and collects it for the forwarded events the events belong to.
func (c *Container[T, K]) actionFailed(err error, events []*containerEvent) {
	seen := make([]*remoteExecution, 0, 1)
	for _, evt := range events {
		if evt.parent == nil || evt.parent.remote == nil || slices.Contains(seen, evt.parent.remote) {
			continue
		}
		seen = append(seen, evt.parent.remote)
		evt.parent.remote.errs = append(evt.parent.remote.errs, err)
	}
	if c.traceF != nil {
		c.sendTracedError(err, traceRefs(events))
		return
	}
	c.sendError(err)
}

func (c *Container[T, K]) sendError(err error) {
	c.sendTracedError(err, nil)
}

// lateError reports an error another goroutine found after the work it belongs to was done: to the error func while
// the container is open, logged once it closed. closeIdle reports the errors queued before the container closed.
func (c *Container[T, K]) lateError(err error) {
	p := c.containerProcessing
	p.lifecycleMu.Lock()
	closed := p.closed
	if !closed {
		p.errMu.Lock()
		p.errs = append(p.errs, queuedError{err: err})
		p.errMu.Unlock()
	}
	p.lifecycleMu.Unlock()

	if closed {
		c.logger.Error(fmt.Sprintf("error after container %s closed: %v", c.id, err))
		return
	}
	p.schedule()
}

// sendTracedError queues an error of the events of the traces.
func (c *Container[T, K]) sendTracedError(err error, traces []*service.TraceRef) {
	p := c.containerProcessing
	p.errMu.Lock()
	p.errs = append(p.errs, queuedError{err: err, traces: traces})
	p.errMu.Unlock()

	p.schedule()
}

// Event sends an event to the container.
// It returns ErrContainerClosed if the container stopped accepting events,
// or the error the node failed with.
func (c *Container[T, K]) Event(eventName string, event any, opts ...EventOption) error {
	if c.nodeErrF != nil {
		if err := c.nodeErrF(); err != nil {
			return err
		}
	}

	if !c.addPending(true) {
		return ErrContainerClosed
	}

	var eventOptions eventOptions
	for _, opt := range opts {
		opt.apply(&eventOptions)
	}

	evt := &containerEvent{
		payload:    event,
		name:       eventName,
		opts:       opts,
		doneC:      eventOptions.doneC,
		waitRemote: eventOptions.waitRemote,
		closeAfter: eventOptions.closeAfter,
		remote:     eventOptions.remote,
	}
	// an event sent to the container starts a trace, events of other nodes come with theirs
	if c.traceF != nil && evt.remote == nil {
		evt.trace = &service.TraceRef{TraceId: newTraceId()}
	}
	c.eventsC <- evt
	c.containerProcessing.scheduleEvent()

	return nil
}

// requestClose sends the close signal to the container without waiting.
// The container keeps accepting events until it has no events in the pipeline and is not doing any work.
func (p *containerProcessing) requestClose() {
	p.lifecycleMu.Lock()
	requested := p.closeRequested
	p.closeRequested = true
	p.lifecycleMu.Unlock()

	if !requested {
		p.schedule()
	}
}

// close sends the close signal and blocks until the container is closed.
func (p *containerProcessing) close() {
	p.requestClose()

	<-p.done()
}

// Close sends the close signal and blocks until the container is closed.
func (c *Container[T, K]) Close() {
	c.containerProcessing.close()
}

func (c *Container[T, K]) Builder() K {
	return c.builder
}

func (c *Container[T, K]) Id() string {
	return c.id
}

func (c *Container[T, K]) Deps() Dependencies {
	return c.deps
}

// EvaluateEntityWithEvents sets the entity and events to the container and
// evaluates their properties to set a Bit state.
func (c *Container[T, K]) EvaluateEntityWithEvents(events []any) {
	bitmap := c.currentBitmap()
	for _, event := range events {
		if bm := c.getBitmapByTypeF(event, bitmap); bm != nil {
			c.setState(bm.Name, mergeState(bm, c.states[bm.Name], EvaluateEntityProperties(event, bm)))
		}
	}

	entity := c.builder.Build()
	entityBitmap := c.getBitmapByTypeF(entity, bitmap)
	if entityBitmap == nil {
		return
	}
	// the entity is merged last, so its values win over events using the same Bitmap.
	// merging replaces the bits per property, so a property never holds more than one value.
	c.setState(entityBitmap.Name, mergeState(entityBitmap, c.states[entityBitmap.Name], EvaluateEntityProperties(entity, entityBitmap)))
}

func NewContainer[T proto.Message, K Builder[T]](
	id string,
	bitmap *Bitmap,
	transitions []*Transition,
	getBitmapByTypeF GetBitmapByTypeFunc,
	createBuilderF CreateBuilderFunc[T, K],
	opts ...ContainerOption,
) *Container[T, K] {
	var containerOpts containerOptions
	for _, opt := range opts {
		opt.apply(&containerOpts)
	}

	// set default if maxInFlight is lower than one
	if containerOpts.maxInFlight < 1 {
		containerOpts.maxInFlight = 10
	}

	c := &Container[T, K]{
		containerProcessing: newContainerProcessing(containerOpts.maxInFlight),
		id:                  id,
		builder:             createBuilderF(),
		bitmap:              bitmap,
		transitions:         newPublishedTransitions(transitions),
		getBitmapByTypeF:    getBitmapByTypeF,
		initOnce:            sync.Once{},
		status:              containerStatusRunning,
		logger:              zap.NewNop(),
	}

	if containerOpts.saveF != nil {
		c.saveF = containerOpts.saveF.(lifecycleFunc[T, K])
	}
	if containerOpts.initF != nil {
		c.initF = containerOpts.initF.(lifecycleFunc[T, K])
	}
	if containerOpts.closeF != nil {
		c.closeF = containerOpts.closeF.(lifecycleFunc[T, K])
	}
	if containerOpts.errorF != nil {
		c.errorF = containerOpts.errorF.(errorFunc[T, K])
	}
	if containerOpts.dependencies != nil {
		c.deps = containerOpts.dependencies
	}
	if containerOpts.logger != nil {
		c.logger = containerLogger(containerOpts.logger, id)
	}

	return c
}
