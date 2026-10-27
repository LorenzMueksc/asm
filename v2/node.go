package asm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"rumue.io/asm/service"
)

type (
	Node[T proto.Message, K Builder[T]] struct {
		sem chan struct{}
		// keepAlive is how long a container's goroutine waits for more work (WithContainerKeepAlive)
		keepAlive time.Duration

		containerMap *nodeContainerMap[T, K]
		logger       *zap.Logger
		id           int
		listenAddr   string

		getBitmapByTypeF     GetBitmapByTypeFunc
		createBuilderF       CreateBuilderFunc[T, K]
		saveF, initF, closeF lifecycleFunc[T, K]
		errorF               errorFunc[T, K]
		containerIdF         ContainerIdFunc
		deps                 Dependencies

		// localBitmap is the bitmap of the local config. It is proposed to the cluster on start.
		localBitmap *Bitmap
		// layout is the committed bitmap shared by all nodes of the cluster
		layout atomic.Pointer[Bitmap]

		registry transitionRegistry
		// forwarded holds the executions other nodes handed over, queued per container id
		forwarded forwardQueues
		// pending holds the asynchronous acquires of handed over work (acquireAsync)
		pending pendingAcquires
		// streams carry the forwarded events to the other nodes (node_stream.go)
		streams forwardStreams
		// skipRaft: no cluster and no locks (WithSkipRaft). client: no raft, the lock table through a member
		// (WithClientMode); clientState is its side, clients the clients attached to this member (node_clients.go)
		skipRaft    bool
		client      bool
		clientState clientState
		clients     clientSessions
		// trace keeps the last events for dev tools, nil unless WithEventTrace is set
		trace *eventTrace
		// entity caches the name of the root entity bitmap of T, see entityName
		entity atomic.Pointer[string]
		// hooks are the container options of the node's funcs, built once (withNodeHooks), hookOptions the same
		// options applied: a container started without options of its own uses them as they are
		hooks       []ContainerOption
		hookOptions containerOptions
		hooksOnce   sync.Once
		// forwardF and failureF are n.forward and n.failure for the containers, bound once: a method value
		// allocates every time it is taken
		forwardF forwardFunc
		failureF func() error
		// targetF is n.callTarget for the containers, bound once like forwardF
		targetF targetFunc
		// metrics are the node's counters and latency histograms, metricsServer serves them (WithMetricsAddr)
		metrics       nodeMetrics
		metricsServer *http.Server
		// tls holds the CA and the certificate of the node (WithTLS), nil without TLS
		tls *nodeTLS
		// publicServer serves the Event rpc on the public listener (WithPublicAddr)
		publicServer *grpc.Server

		// ctx is the context containers run on. It is cancelled once the node stopped or failed
		// and its containers are closed.
		ctx    context.Context
		cancel context.CancelFunc

		// leaving is set once the node leaves the cluster, new containers are rejected then.
		// It is set under containerMap.mu, so no container is added after Leave captured them.
		leaving atomic.Bool

		readyC    chan struct{}
		readyOnce sync.Once
		failedC   chan struct{}
		failOnce  sync.Once
		// failErr is the failure of the node, read on hot paths (atomic, no lock)
		failErr atomic.Pointer[error]
		errC    chan error

		nodeRaft
	}
	nodeContainerMap[T proto.Message, K Builder[T]] struct {
		m  map[string]*Container[T, K]
		mu sync.RWMutex
	}
)

// ErrNodeStopped is returned when the node stopped before it was ready,
// or when starting a container on a node which leaves the cluster.
var ErrNodeStopped = errors.New("node stopped")

// newCondition is the internal func for creating a condition in a transition.
// The entities are compiled to states against the committed bitmap,
// immediately if it is available and again whenever it changes.
func (n *Node[T, K]) newCondition(operator Operator, entities ...any) *Condition {
	c := &Condition{
		Operator: operator,
		entities: entities,
	}
	if layout := n.layout.Load(); layout != nil {
		c.States = n.evaluateEntities(entities, layout)
	}
	return c
}

func (n *Node[T, K]) evaluateEntities(entities []any, layout *Bitmap) map[string][]Bits {
	entityStates := make(map[string][]Bits)
	for _, e := range entities {
		if bm := n.getBitmapByTypeF(e, layout); bm != nil {
			entityStates[bm.Name] = append(entityStates[bm.Name], EvaluateEntityProperties(e, bm))
		}
	}
	return entityStates
}

// Id returns current node id
func (n *Node[T, K]) Id() string {
	return strconv.Itoa(n.id)
}

// Transitions returns the number of transitions known to the node, local and synced ones.
func (n *Node[T, K]) Transitions() int {
	return len(n.registry.published.Load().list())
}

// Bitmap returns the committed bitmap shared by all nodes of the cluster.
func (n *Node[T, K]) Bitmap() *Bitmap {
	return n.layout.Load()
}

// Ready is closed once the node is ready: its bitmap is committed, its transitions are compiled
// and exchanged with the other nodes of the cluster.
func (n *Node[T, K]) Ready() <-chan struct{} {
	return n.readyC
}

// Err receives the error the node failed with, eg. a bitmap or transition conflict.
// A failed node stops.
func (n *Node[T, K]) Err() <-chan error {
	return n.errC
}

func (n *Node[T, K]) markReady() {
	n.readyOnce.Do(func() {
		n.logger.Info(fmt.Sprintf("node %d is ready", n.id))
		close(n.readyC)
	})
}

// failure returns the error the node failed with.
func (n *Node[T, K]) failure() error {
	if err := n.failErr.Load(); err != nil {
		return *err
	}
	return nil
}

// fail stops the node with the given error and reports it on Err.
func (n *Node[T, K]) fail(err error) {
	n.failOnce.Do(func() {
		n.failErr.Store(&err)

		n.logger.Error(fmt.Sprintf("node %d failed: %+v", n.id, err))
		n.errC <- err
		close(n.failedC)

		go func() {
			n.stopRaft()
			n.Shutdown()
		}()
	})
}

// waitReady blocks until the node is ready, failed or the context is done.
func (n *Node[T, K]) waitReady(ctx context.Context) error {
	if err := n.failure(); err != nil {
		return err
	}
	// once ready: a select with several cases locks all their channels, which every caller shares
	select {
	case <-n.readyC:
		return n.failure()
	default:
	}

	select {
	case <-n.readyC:
		return n.failure()
	case <-n.failedC:
		return n.failure()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown closes all containers of the node.
// Every container closes once it has no events in the pipeline and is not doing any work.
func (n *Node[T, K]) Shutdown() {
	n.closeContainers()
}

// closeContainers sends every container the close signal and returns their lives, so callers can wait for them.
func (n *Node[T, K]) closeContainers() []*containerProcessing {
	// capture the current life of every container under the lock, a container closing and being reused
	// meanwhile must not be reached through its new life
	n.containerMap.mu.RLock()
	lives := make([]*containerProcessing, 0, len(n.containerMap.m))
	for _, c := range n.containerMap.m {
		lives = append(lives, c.containerProcessing)
	}
	n.containerMap.mu.RUnlock()

	// closing outside the lock, because closing containers remove themselves from the map
	for _, p := range lives {
		p.close()
	}
	return lives
}

// AtLeastOne adds a condition which returns true when matching at least one of the entities.
func (n *Node[T, K]) AtLeastOne(entities ...any) *Condition {
	return n.newCondition(AtLeastOne, entities...)
}

// All adds a condition which returns true when matching all of the entities.
func (n *Node[T, K]) All(entities ...any) *Condition {
	return n.newCondition(All, entities...)
}

// None adds a condition which returns true when matching none of the entities.
func (n *Node[T, K]) None(entities ...any) *Condition {
	return n.newCondition(None, entities...)
}

// OneOf adds a condition which returns true when matching exactly one of the entities.
func (n *Node[T, K]) OneOf(entities ...any) *Condition {
	return n.newCondition(OneOf, entities...)
}

// StartContainer Starts or gets a container with the given id.
// Safe to use by multiple go routines.
// It blocks until the node is ready and returns the error the node failed with.
// A container id can only be open on one node of the cluster: while another node holds the lock of the id,
// it waits until the lock is released or expired.
// If the container with the id is closing, it waits until it is closed and starts a new one.
// ctx only bounds the waiting. The container runs on the context of the node and lives until it is closed
// with Close, DeleteContainer or Shutdown.
// It returns the container and a bool indicating if the container was loaded
func (n *Node[T, K]) StartContainer(
	ctx context.Context,
	id string,
	opts ...ContainerOption,
) (*Container[T, K], bool, error) {
	return n.startContainer(ctx, id, false, opts...)
}

// startContainer starts or gets a container. A borrowed container runs on the lock another node lends,
// so it neither acquires nor releases the lock.
func (n *Node[T, K]) startContainer(
	ctx context.Context,
	id string,
	borrowed bool,
	opts ...ContainerOption,
) (*Container[T, K], bool, error) {
	if err := n.waitReady(ctx); err != nil {
		return nil, false, err
	}
	if n.leaving.Load() {
		return nil, false, ErrNodeStopped
	}
	opts = n.withNodeHooks(opts)

	for {
		n.containerMap.mu.Lock()
		if container, ok := n.containerMap.m[id]; ok {
			if container.isAccepting() {
				n.containerMap.mu.Unlock()

				debugf(n.logger, "loaded container %s", id)
				return container, true, nil
			}

			// the container stopped accepting events, wait until it is closed and removed and start a new one
			doneC := container.done()
			n.containerMap.mu.Unlock()

			debugf(n.logger, "waiting for closing container %s", id)
			<-doneC
			continue
		}
		n.containerMap.mu.Unlock()

		// a borrowed start runs on the lock another node lends, it doesn't wait for this node acquiring the lock:
		// that may wait for the very lock the borrowed execution runs on (eg. work of the same container
		// handed over to this node before), which the lending node only releases after the execution
		if borrowed {
			container, err := n.openContainer(ctx, id, "", true, true, opts)
			if err != nil {
				return nil, false, err
			}
			return container, false, nil
		}

		// only one goroutine of this node acquires the lock of an id, the others wait and load its container
		n.startingMu.Lock()
		if startingC, ok := n.starting[id]; ok {
			n.startingMu.Unlock()
			select {
			case <-startingC:
				continue
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
		}
		startingC := make(chan struct{})
		n.starting[id] = startingC
		n.startingMu.Unlock()

		container, err := n.createContainer(ctx, id, opts)

		n.startingMu.Lock()
		delete(n.starting, id)
		close(startingC)
		n.startingMu.Unlock()

		if err != nil {
			return nil, false, err
		}
		return container, false, nil
	}
}

// withNodeHooks puts the node's funcs in front of the options, so options passed by the caller override them.
func (n *Node[T, K]) withNodeHooks(opts []ContainerOption) []ContainerOption {
	n.hooksOnce.Do(func() {
		n.hooks = []ContainerOption{
			WithContainerLogger(n.logger),
			WithContainerDependencies(n.deps),
			WithContainerInitFunc(n.initF),
			WithContainerCloseFunc(n.closeF),
			WithContainerSaveFunc(n.saveF),
			WithContainerErrorFunc(n.errorF),
		}
		n.hookOptions = applyContainerOptions(n.hooks)
	})
	if len(opts) == 0 {
		return n.hooks
	}
	return append(slices.Clip(n.hooks), opts...)
}

// containerOptions applies the options; the node's hooks alone were applied once.
func (n *Node[T, K]) containerOptions(opts []ContainerOption) containerOptions {
	if len(opts) > 0 && len(opts) == len(n.hooks) && &opts[0] == &n.hooks[0] {
		return n.hookOptions
	}
	return applyContainerOptions(opts)
}

func applyContainerOptions(opts []ContainerOption) containerOptions {
	var o containerOptions
	for _, opt := range opts {
		opt.apply(&o)
	}
	return o
}

// callContainer runs a waited call of another entity in the container of the id, on the container's own lock,
// and waits until its event is done. The lock table learns that the containers on the wait path wait for the
// container: the call joins the open container of the id through the lock table, or acquires the lock.
// It returns ErrWouldDeadlock if the container waits for one of the containers on the path.
// The call waits nowhere else, so every wait for a container which may wait for the caller is known.
func (n *Node[T, K]) callContainer(ctx context.Context, id string, waitPath []string, eventName string, opts ...EventOption) error {
	if err := n.waitReady(ctx); err != nil {
		return err
	}

	for {
		if n.leaving.Load() {
			return ErrNodeStopped
		}

		// an open container holding its own lock may be busy with work waiting for the caller, it is joined
		// through the lock table
		join := ""
		n.containerMap.mu.RLock()
		if c, ok := n.containerMap.m[id]; ok && c.lockToken != "" && c.isAccepting() {
			join = c.lockToken
		}
		n.containerMap.mu.RUnlock()

		token, joined, err := n.acquireLock(ctx, id, waitPath, join)
		if err != nil {
			return err
		}

		var c *Container[T, K]
		var p *containerProcessing
		if joined {
			n.containerMap.mu.RLock()
			if existing, ok := n.containerMap.m[id]; ok && existing.lockToken == join {
				c, p = existing, existing.containerProcessing
			}
			n.containerMap.mu.RUnlock()
		} else {
			// openContainer releases the lock if it fails
			if c, err = n.openContainer(ctx, id, token, false, true, n.withNodeHooks(nil)); err != nil {
				return err
			}
		}

		doneC := make(chan struct{}, 1)
		err = ErrContainerClosed
		if c != nil {
			err = c.Event(eventName, nil, append(opts, WithDoneChannel(doneC), withCloseAfter())...)
		}
		if err == nil {
			err = waitDone(ctx, doneC)
			n.lockDone(id, token, joined)
			return err
		}
		if !joined || !errors.Is(err, ErrContainerClosed) {
			n.lockDone(id, token, joined)
			return err
		}

		// the joined container closes: wait for it as one of its calls, then call again
		if p != nil {
			select {
			case <-p.done():
			case <-ctx.Done():
				n.lockDone(id, token, joined)
				return ctx.Err()
			}
		}
		n.lockDone(id, token, joined)
	}
}

// callTarget runs an event emitted with WithTarget in the container of the id on this node, on that container's own
// lock, like a call of another entity: the container runs its own func transitions and its state transitions matching
// the event. A waited call carries the caller's wait path, so the lock table refuses a call which would close a cycle.
// Otherwise the event is handed over: queued for the id and run once this node got the lock.
func (n *Node[T, K]) callTarget(ctx context.Context, id, eventName string, payload any, wait bool, waitPath []string, trace *service.TraceRef) (any, error) {
	r := &remoteExecution{
		payloads:    []any{payload},
		from:        n.Id(),
		otherEntity: true,
	}
	if trace != nil {
		r.traces = []*service.TraceRef{trace}
	}
	if !wait {
		_, err := n.acceptHandOver("", []handedOver{{id: id, item: forwardedExecution{eventName: eventName, r: r}}}, nil)
		return nil, err
	}

	r.waited = true
	// the target is open on another node, or kept there: the call runs there instead of moving the lock here
	if req := n.targetRequest(id, eventName, payload, waitPath, trace); req != nil {
		if result, ok, err := n.redirectToHolder(ctx, id, req); ok {
			if err != nil {
				return nil, err
			}
			if len(result.replies) > 0 {
				return result.replies[len(result.replies)-1], nil
			}
			return nil, nil
		}
	}
	var err error
	if len(waitPath) == 0 || n.skipRaft {
		// without locks, or no container waits (the caller runs on a lent lock of a hand-over): nothing to check
		doneC := make(chan struct{}, 1)
		if err = n.sendToContainer(ctx, id, false, eventName, nil,
			withRemoteExecution(r), WithDoneChannel(doneC), withCloseAfter()); err == nil {
			err = waitDone(ctx, doneC)
		}
	} else {
		r.waitPath = waitPath
		err = n.callContainer(ctx, id, waitPath, eventName, withRemoteExecution(r))
	}
	if err != nil {
		return nil, err
	}
	// set by the target's goroutine before it signaled done
	return r.reply, r.err
}

// createContainer acquires the lock of the id, or takes the one the node kept, and starts the container.
func (n *Node[T, K]) createContainer(ctx context.Context, id string, opts []ContainerOption) (*Container[T, K], error) {
	if token, ok := n.reuseRetained(id); ok {
		return n.openContainer(ctx, id, token, false, false, opts)
	}
	token, _, err := n.acquireLock(ctx, id, nil, "")
	if err != nil {
		return nil, err
	}
	return n.openContainer(ctx, id, token, false, false, opts)
}

// openContainer starts the container of the id on the lock with the token, or on a lent lock if borrowed.
// It releases the lock if it fails.
// A start nobody waits for blocks for a slot of the max container count. Waited starts (borrowed ones and waited calls
// of another entity) take a slot if one is free and run without one otherwise: the containers holding the slots may
// wait for them.
func (n *Node[T, K]) openContainer(ctx context.Context, id, token string, borrowed, waited bool, opts []ContainerOption) (*Container[T, K], error) {
	slot := true
	if waited {
		select {
		case n.sem <- struct{}{}:
		default:
			slot = false
		}
	} else {
		select {
		case n.sem <- struct{}{}:
		case <-ctx.Done():
			n.releaseLock(id, token)
			return nil, ctx.Err()
		}
	}
	freeSlot := func() {
		if slot {
			<-n.sem
		}
	}

	// there is at most one container of an id. A borrowed start loads an open one. A start which just got
	// the lock waits until a borrowed container of the previous holder closed, it closes once its work is done.
	n.containerMap.mu.Lock()
	for {
		existing, ok := n.containerMap.m[id]
		if !ok {
			break
		}
		if borrowed && existing.isAccepting() {
			n.containerMap.mu.Unlock()
			freeSlot()
			return existing, nil
		}
		doneC := existing.done()
		n.containerMap.mu.Unlock()
		select {
		case <-doneC:
		case <-ctx.Done():
			freeSlot()
			n.releaseLock(id, token)
			return nil, ctx.Err()
		}
		n.containerMap.mu.Lock()
	}
	defer n.containerMap.mu.Unlock()

	if n.leaving.Load() {
		freeSlot()
		n.releaseLock(id, token)
		return nil, ErrNodeStopped
	}

	// every life of a container is a new one, so a caller still holding a closed container only reaches its closed
	// life and gets ErrContainerClosed, never another container
	life := &struct {
		c Container[T, K]
		p containerProcessing
	}{}
	container := &life.c
	n.resetContainer(container, &life.p, id, opts)
	n.metrics.containersStarted.Add(1)
	container.lockToken = token
	container.slot = slot
	container.removeF = func() {
		n.removeContainer(container)
		n.endLock(id, token)
		n.recordTrace(id, traceRecord{kind: service.TraceEvent_CONTAINER_CLOSED})
	}

	n.containerMap.m[id] = container
	n.recordTrace(id, traceRecord{kind: service.TraceEvent_CONTAINER_STARTED})

	// every container runs on its own context, cancelled once it closed. It has a goroutine only while work is
	// queued; the slot of the max container count is freed once it closed
	ctx, cancel := context.WithCancel(n.ctx)
	container.start(ctx, func() {
		cancel()
		if container.slot {
			<-n.sem
		}
	})

	debugf(n.logger, "started new container with id %s", id)
	return container, nil
}

// hasContainer reports whether a container of the id is open or closing on the node.
func (n *Node[T, K]) hasContainer(id string) bool {
	n.containerMap.mu.RLock()
	defer n.containerMap.mu.RUnlock()
	_, ok := n.containerMap.m[id]
	return ok
}

// removeContainer removes the closed container from the container map.
func (n *Node[T, K]) removeContainer(c *Container[T, K]) {
	n.containerMap.mu.Lock()
	if n.containerMap.m[c.id] == c {
		delete(n.containerMap.m, c.id)
	}
	n.containerMap.mu.Unlock()
}

// ResetContainer prepares a container built with NewContainer for a new life with the id on this node.
func (n *Node[T, K]) ResetContainer(c *Container[T, K], id string, opts ...ContainerOption) {
	n.resetContainer(c, nil, id, opts)
	c.removeF = func() {
		n.removeContainer(c)
	}
}

// resetContainer prepares the container for a life with the id; p is the state of the new life, nil allocates it.
func (n *Node[T, K]) resetContainer(c *Container[T, K], p *containerProcessing, id string, opts []ContainerOption) {
	containerOpts := n.containerOptions(opts)

	// set default if maxInFlight is lower than one
	if containerOpts.maxInFlight < 1 {
		containerOpts.maxInFlight = 10
	}

	c.id = id
	c.builder = n.createBuilderF()
	c.status = containerStatusRunning

	c.initOnce = sync.Once{}
	// created on the first state (setState)
	clear(c.states)

	if p == nil {
		p = &containerProcessing{}
	}
	p.init(containerOpts.maxInFlight)
	c.containerProcessing = p
	c.forwardF = n.forwardF
	c.targetF = n.targetF
	c.nodeErrF = n.failureF
	c.keepAlive = n.keepAlive
	c.metrics = &n.metrics
	c.entityName = n.entityName()
	c.traceF = nil
	c.snapshot.Store(nil)
	c.lent.Store(nil)
	c.lockToken = ""
	c.slot = false
	c.callEvents = nil
	if n.trace != nil {
		c.traceF = func(r traceRecord) { n.recordTrace(id, r) }
	}

	// containers of a node always use the committed bitmap and the current transitions
	c.bitmap = nil
	c.transitions = nil
	c.bitmapP = &n.layout
	c.transitionsP = &n.registry.published
	c.getBitmapByTypeF = n.getBitmapByTypeF

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
}

// DeleteContainer closes the container with the given id and blocks until it is closed.
// The container closes once it has no events in the pipeline and is not doing any work.
func (n *Node[T, K]) DeleteContainer(id string) {
	n.containerMap.mu.RLock()
	c, ok := n.containerMap.m[id]
	var p *containerProcessing
	if ok {
		p = c.containerProcessing
	}
	n.containerMap.mu.RUnlock()

	if ok {
		p.close()
	}
}

// AddFuncTransition adds a transition executing f.
// The transition is synced to the other nodes of the cluster, which forward matching events to this node.
func (n *Node[T, K]) AddFuncTransition(name string, event string, f transitionFunc[T, K], opts ...TransitionOption) *Node[T, K] {
	t := &Transition{
		Name:       name,
		Conditions: make([]*Condition, 0),
		EventName:  event,
		Actions:    []any{f},
		Batching: Batching{
			Count:  1,
			Period: 0,
		},
	}

	for _, opt := range opts {
		opt.apply(t)
	}

	if t.funcKey == "" {
		t.funcKey = funcName(f)
	}

	n.addLocalTransition(t)

	return n
}

// AddStateTransition adds a transition setting the state of toState.
// Zero values of toState are applied too if they are values in the bitmap.
// The transition is synced to the other nodes of the cluster, which execute it themselves.
func (n *Node[T, K]) AddStateTransition(name string, event string, toState T, opts ...TransitionOption) *Node[T, K] {
	t := &Transition{
		Name:      name,
		EventName: event,
		toState:   toState,
		Batching: Batching{
			Count:  1,
			Period: 0,
		},
	}

	for _, opt := range opts {
		opt.apply(t)
	}

	n.addLocalTransition(t)

	return n
}

func NewNode[T proto.Message, K Builder[T]](
	id int,
	getBitmapByType GetBitmapByTypeFunc,
	createBuilderF func() K,
	listenAddr string,
	opts ...NodeSetupOption,
) *Node[T, K] {
	l, _ := zap.NewDevelopment()

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node[T, K]{
		ctx:              ctx,
		cancel:           cancel,
		logger:           l,
		getBitmapByTypeF: getBitmapByType,
		containerMap: &nodeContainerMap[T, K]{
			m: make(map[string]*Container[T, K]),
		},
		id:             id,
		createBuilderF: createBuilderF,
		listenAddr:     listenAddr,
		registry: transitionRegistry{
			remote: make(map[string]*Transition),
		},
		forwarded: forwardQueues{
			queues: make(map[string]*forwardQueue),
		},
		pending: pendingAcquires{
			byId:  make(map[string]*pendingAcquire),
			kickC: make(chan struct{}, 1),
		},
		streams: forwardStreams{
			streams: make(map[uint64]*forwardStream),
		},
		clientState: clientState{
			watches: make(map[string]int),
		},
		clients: clientSessions{
			sessions: make(map[uint64]*clientSession),
		},
		readyC:  make(chan struct{}),
		failedC: make(chan struct{}),
		errC:    make(chan error, 1),
		nodeRaft: nodeRaft{
			storage:                raft.NewMemoryStorage(),
			errorC:                 make(chan error),
			raftStopC:              make(chan struct{}),
			peers:                  make(map[uint64]string),
			peerClients:            make(map[uint64]service.NodeClient),
			removed:                make(map[uint64]bool),
			addedAt:                make(map[uint64]uint64),
			senders:                make(map[uint64]*peerSender),
			bitmapCommittedC:       make(chan struct{}),
			pendingBitmapNames:     make(map[string]bool),
			locks:                  newLockTable(),
			starting:               make(map[string]chan struct{}),
			lockTokens:             make(map[string]bool),
			refusedTokens:          make(map[string]bool),
			retained:               make(map[string]string),
			proposeWakeC:           make(chan struct{}, 1),
			leaseTimeout:           defaultLeaseTimeout,
			renewInterval:          defaultRenewInterval,
			snapshotCount:          defaultSnapshotCount,
			snapshotCatchUpEntries: defaultSnapshotCatchUpEntries,
			snapshotInterval:       defaultSnapshotInterval,
		},
	}

	n.forwardF, n.failureF, n.targetF = n.forward, n.failure, n.callTarget
	n.keepAlive = defaultContainerKeepAlive

	var nodeSetupOptions nodeSetupOptions
	for _, opt := range opts {
		opt.apply(&nodeSetupOptions)
	}

	if nodeSetupOptions.logger != nil {
		n.logger = nodeSetupOptions.logger
	}
	if nodeSetupOptions.bitmap != nil {
		n.localBitmap = nodeSetupOptions.bitmap
	}
	if nodeSetupOptions.deps != nil {
		n.deps = nodeSetupOptions.deps
	}
	if nodeSetupOptions.initF != nil {
		n.initF = nodeSetupOptions.initF.(lifecycleFunc[T, K])
	}
	if nodeSetupOptions.saveF != nil {
		n.saveF = nodeSetupOptions.saveF.(lifecycleFunc[T, K])
	}
	if nodeSetupOptions.closeF != nil {
		n.closeF = nodeSetupOptions.closeF.(lifecycleFunc[T, K])
	}
	if nodeSetupOptions.errorF != nil {
		n.errorF = nodeSetupOptions.errorF.(errorFunc[T, K])
	}
	if nodeSetupOptions.containerIdF != nil {
		n.containerIdF = nodeSetupOptions.containerIdF
	}
	if nodeSetupOptions.maxContainerCount > 0 {
		n.sem = make(chan struct{}, nodeSetupOptions.maxContainerCount)
	} else {
		n.sem = make(chan struct{}, 100)
	}
	if nodeSetupOptions.maxHandOvers > 0 {
		n.streams.slots = make(chan struct{}, nodeSetupOptions.maxHandOvers)
	} else {
		n.streams.slots = make(chan struct{}, defaultMaxPendingHandOvers)
	}
	if nodeSetupOptions.leaseTimeout > 0 && nodeSetupOptions.renewInterval > 0 {
		n.leaseTimeout = nodeSetupOptions.leaseTimeout
		n.renewInterval = nodeSetupOptions.renewInterval
	}
	if nodeSetupOptions.traceCapacity > 0 {
		n.trace = newEventTrace(nodeSetupOptions.traceCapacity)
	}
	if nodeSetupOptions.snapshotCount > 0 {
		n.snapshotCount = nodeSetupOptions.snapshotCount
		n.snapshotCatchUpEntries = min(nodeSetupOptions.snapshotCatchUp, nodeSetupOptions.snapshotCount)
	}
	if nodeSetupOptions.snapshotInterval != nil {
		n.snapshotInterval = *nodeSetupOptions.snapshotInterval
	}
	if nodeSetupOptions.keepAlive != nil {
		n.keepAlive = max(*nodeSetupOptions.keepAlive, 0)
	}
	n.stopC = make(chan struct{})
	if nodeSetupOptions.stopC != nil {
		go n.leaveOnStop(nodeSetupOptions.stopC)
	}

	// the TLS options first: the metrics endpoint uses them
	n.setSecurityOptions(&nodeSetupOptions)
	if nodeSetupOptions.metricsAddr != "" {
		n.serveMetrics(nodeSetupOptions.metricsAddr)
	}

	// once the node stopped or failed, close its containers and cancel the context they run on
	go func() {
		select {
		case <-n.stopC:
		case <-n.failedC:
		}
		n.Shutdown()
		n.cancel()
		if n.metricsServer != nil {
			_ = n.metricsServer.Close()
		}
		if n.publicServer != nil {
			n.publicServer.Stop()
		}
	}()

	n.skipRaft = nodeSetupOptions.skipRaft
	n.client = nodeSetupOptions.clientMode && !n.skipRaft
	n.dataDir = nodeSetupOptions.dataDir
	// n.tls was set by setSecurityOptions, before the metrics server started reading it
	if n.tls == nil && !n.skipRaft {
		n.logger.Warn("no TLS: any process reaching the node port can act as any node of the cluster")
	}
	if !n.skipRaft {
		if err := n.checkOwnCertificate(); err != nil {
			n.fail(err)
			return n
		}
	}
	if nodeSetupOptions.publicAddr != "" {
		if err := n.servePublic(nodeSetupOptions.publicAddr); err != nil {
			n.fail(err)
			return n
		}
	}
	// lock tokens are unique over restarts of the node: the log may still hold locks of its previous life
	n.lockSeq.Store(uint64(time.Now().UnixMicro()))
	// so are hand-over ids: a receiver remembers the ones it ran for a while
	n.streams.handOverSeq.Store(uint64(time.Now().UnixMicro()))
	if nodeSetupOptions.skipRaft {
		// without a cluster the local bitmap is the committed one and the node is ready right away
		if n.localBitmap != nil {
			n.layout.Store(n.localBitmap)
		}
		if err := n.compileTransitions(); err != nil {
			n.fail(err)
		} else {
			n.markReady()
		}
	} else if n.client {
		go n.startClient(nodeSetupOptions.joinAddr)
	} else {
		go n.startRaft(nodeSetupOptions.joinAddr)
	}

	return n
}
