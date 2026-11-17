package asm

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
)

// A round is a phase of the work of a standalone node. Node.Round is its barrier: it delivers the deferred events of
// every outbox (outbox.go) sorted by (sender id, sequence) into the inboxes of their targets, runs every container
// which received some, concurrently across containers, and returns once all of them ran. Deferred events emitted
// meanwhile stay in the outboxes for the next round. Every container runs its inbox in order on its own goroutine, so
// what it receives, and in which order, doesn't depend on the scheduling of the goroutines.

// ErrNotStandalone is returned by Node.Round on a node of a cluster: rounds need a standalone node (WithSkipRaft).
var ErrNotStandalone = errors.New("rounds need a standalone node (WithSkipRaft)")

// roundEventName is the name of the event the barrier sends a container to run its inbox; its payload is the
// *roundRun
const roundEventName = "\x00round"

type (
	// roundRun is a round in progress, the payload of the round event of every container taking part.
	roundRun struct {
		name string
		// runAll: every open container runs its round batches, also without events (WithRunAll)
		runAll bool
		// left counts the containers which didn't run their inbox yet, plus one while the barrier sends round events;
		// doneC is closed once it dropped to zero
		left  atomic.Int64
		doneC chan struct{}
		// mu guards errs
		mu   sync.Mutex
		errs []roundError
	}
	// roundError is the first error of the transitions of a container in a round
	roundError struct {
		id  string
		err error
	}

	// RoundOption is an option of Node.Round.
	RoundOption interface {
		apply(*roundOptions)
	}
	roundOptions struct {
		runAll bool
	}
	funcRoundOption struct {
		f func(*roundOptions)
	}
)

func (o *funcRoundOption) apply(options *roundOptions) {
	o.f(options)
}

func newFuncRoundOption(f func(*roundOptions)) *funcRoundOption {
	return &funcRoundOption{f: f}
}

// runAllOption is the option WithRunAll returns, allocated once
var runAllOption RoundOption = newFuncRoundOption(func(o *roundOptions) {
	o.runAll = true
})

// WithRunAll makes the round run every container open on the node, not only those which received deferred events:
// each one runs its round batch transitions (WithBatchOnRound) once, also those without events in the round, with an
// empty []Message, if their conditions match the entity (evaluated after the container ran the round's events). The
// containers need no event to take part, eg. a simulation stepping every container in every round. The round batches
// run as without it: after the container's events, by priority, then name. Containers which are closing don't take
// part. A container spawned in a round (Spawn) starts at its end: it takes part from the next round on, not in the one
// that spawned it; one spawned between rounds takes part in the next round. Without round batch transitions on the
// node it changes nothing.
func WithRunAll() RoundOption {
	return runAllOption
}

// Round runs a round of the node's containers, the barrier between two phases of their work:
//
//  1. it takes the outbox of every container: the deferred events emitted since the last round (WithDeferred), and
//     the containers spawned between the rounds (Spawn in live events), and checks them: every spawned id is free and
//     spawned once, every target is open on the node and accepting events, or spawned now. If a check fails it
//     delivers nothing, starts nothing, keeps the outboxes and spawns for the next round (or DropOutboxes) and returns
//     ErrContainerExists or ErrUnknownTarget;
//  2. it starts those spawned containers in id order and delivers the events into the inboxes of their targets,
//     sorted by (sender id, sequence);
//  3. every container which received events runs them in that order, concurrently with the other containers: the
//     transitions matching them, as for events sent with Event, then its round batches (WithBatchOnRound);
//  4. once every one of them ran, it closes the containers which asked for it (CloseAfterRound), then starts the
//     containers spawned during the round in id order, checked like in step 1 (an id open after the closes, or spawned
//     twice: none starts, they stay queued, Round returns ErrContainerExists), and returns: the containers of the node
//     are those after the round. Deferred events emitted meanwhile are delivered by the next round, not by this one;
//     a container spawned in the round receives events from the next round on.
//
// With WithRunAll every open container takes part in step 3, also without events.
//
// After a successful check it returns the first error of a transition of the container with the lowest id (transition
// errors still go to the error func as well), else the error of the spawns of step 4, else ErrRoundOverlap if a
// container started work during the round which runs on its own, or such work still runs after it: a hand-over without
// wait (WithTarget without WithWaitRemote) or a batch timer (WithBatching with a period), which Round doesn't wait for.
// Rounds run one at a time; ctx bounds waiting for the node, for the round before and for this one: if it ends first,
// Round returns its error and the round goes on (step 4 included: spawns failing their check then stay queued for the
// next round), the next one starts once it is done. What Round waits for is the work of the delivered events,
// including what their transitions run synchronously (nested and waited Emits). Transitions see the round's name with
// RoundOf and their deferred event with MessageOf. Only for a standalone node (WithSkipRaft), else it returns
// ErrNotStandalone.
func (n *Node[T, K]) Round(ctx context.Context, name string, opts ...RoundOption) error {
	var o roundOptions
	if len(opts) > 0 {
		// on the heap only with options: a round without them allocates as little as possible
		p := new(roundOptions)
		for _, opt := range opts {
			opt.apply(p)
		}
		o = *p
	}
	if err := n.waitReady(ctx); err != nil {
		return err
	}
	if !n.skipRaft {
		return ErrNotStandalone
	}
	r := &n.rounds
	if err := r.acquire(ctx); err != nil {
		return err
	}
	// work started on its own from now on fails the round (ErrRoundOverlap)
	r.started.Store(0)
	r.name.Store(&name)

	// the containers with something for the barrier, by id: the order of the senders is the order of their events
	r.mu.Lock()
	marked := r.marked
	r.marked = r.spare
	r.mu.Unlock()
	slices.SortStableFunc(marked, compareContainerIds[T, K])
	for _, c := range marked {
		// what a container queues from now on marks it again, for the next round
		c.round.marked.Store(false)
	}

	// nothing is delivered or started before everything checked out
	spawning := n.takeSpawning()
	spawns := n.collectSpawns(spawning)
	if err := n.checkRound(marked, spawns, name); err != nil {
		// kept for the next barrier, or DropOutboxes
		n.keepSpawns(spawning, spawns)
		r.mu.Lock()
		for _, c := range marked {
			if c.round.marked.CompareAndSwap(false, true) {
				r.marked = append(r.marked, c)
			}
		}
		r.mu.Unlock()
		clear(marked)
		r.spare = marked[:0]
		r.name.Store(nil)
		r.release()
		return err
	}

	run := &roundRun{name: name, runAll: o.runAll, doneC: make(chan struct{})}
	// the barrier's own count: the round can't end while round events are being sent
	run.left.Store(1)

	var barrierErr error
	fail := func(err error) {
		if barrierErr == nil {
			barrierErr = err
		}
	}

	// the containers spawned between the rounds start before the events are delivered, so they receive what was
	// deferred to them
	n.startSpawns(ctx, spawning, spawns, name, fail)
	targets := n.deliverRound(marked, name, fail)
	delivered := len(targets)
	clear(marked)
	r.spare = marked[:0]
	if o.runAll && len(n.registry.published.Load().roundBatches()) > 0 {
		targets = n.allContainers(targets)
	}

	run.left.Add(int64(len(targets)))
	var d *roundDispatch[T, K]
	workers := 0
	if len(targets) > 0 {
		d = &roundDispatch[T, K]{targets: targets, delivered: delivered, seq: r.seq, run: run}
		workers = d.start()
	}
	run.done()

	if !d.wait(run, workers, ctx.Done()) {
		// the round goes on without the caller; the next one waits until it is done. Spawns failing their check stay
		// queued: the next round reports them.
		go func() {
			d.wait(run, workers, nil)
			n.finishRound(ctx, targets)
		}()
		return ctx.Err()
	}
	started, spawnErr := n.finishRound(ctx, targets)

	if barrierErr != nil {
		return barrierErr
	}
	if err := d.err(); err != nil {
		return err
	}
	if err := run.firstError(); err != nil {
		return err
	}
	if spawnErr != nil {
		return spawnErr
	}
	if left := r.background.Load(); left > 0 || started > 0 {
		return fmt.Errorf("round %s: %d hand-overs without wait or batch timers started during the round, %d running after it: %w",
			name, started, left, ErrRoundOverlap)
	}
	return nil
}

// ErrRoundOverlap is returned by Node.Round when work of a container which runs on its own, a hand-over without wait
// or a batch timer, started during the round or still runs after it. Rounds don't wait for it, so it belongs to no
// round: use deferred events and round batches.
var ErrRoundOverlap = errors.New("work of a container runs on its own during a round")

// allContainers appends every container open on the node to the targets of a round which runs all (WithRunAll), in
// the order of the node's map: the order the containers get their round events in doesn't change what they run.
// The delivered targets are among them again: the dispatch skips them there (listed). Containers spawned during the
// round start at its end, after this. It doesn't touch the containers, the barrier runs alone here.
func (n *Node[T, K]) allContainers(targets []*Container[T, K]) []*Container[T, K] {
	n.containerMap.mu.RLock()
	defer n.containerMap.mu.RUnlock()
	targets = slices.Grow(targets, len(n.containerMap.m))
	for _, c := range n.containerMap.m {
		targets = append(targets, c)
	}
	return targets
}

// roundWorkerTargets is the number of targets per worker a round starts for its dispatch (roundDispatch), roundStall
// how long its workers may take no target before the barrier sends the targets left their round events
const (
	roundWorkerTargets = 64
	roundStall         = 5 * time.Millisecond
)

// roundDispatch runs the rounds of the targets of a round. With many targets, workers (one per CPU) take them in order
// (next) and run each container's round on their own goroutine while no goroutine of the container runs and nothing
// waits in its mailbox (Container.runRound), else they send it the round event: no goroutine per container and round.
// The order in which the containers run doesn't change what they run. A worker waiting in a transition doesn't stop
// the others; if no worker takes a target for roundStall (eg. transitions waiting for each other's rounds), the
// barrier sends the targets left their round events, so they run on their own goroutines as without workers.
type roundDispatch[T proto.Message, K Builder[T]] struct {
	// targets are the delivered targets by id (the first delivered), then, for WithRunAll, every open container: those
	// listed with seq are delivered targets, skipped there
	targets   []*Container[T, K]
	delivered int
	seq       uint64
	run       *roundRun
	next      atomic.Int64
	// mu guards failed and failErr: the target with the lowest index which stopped accepting events since the check
	// with events in its inbox
	mu      sync.Mutex
	failed  int
	failErr error
}

// start starts the workers for many targets, else it sends the round events itself.
func (d *roundDispatch[T, K]) start() (workers int) {
	workers = min(runtime.GOMAXPROCS(0), len(d.targets)/roundWorkerTargets)
	if workers < 2 {
		d.drain()
		return 0
	}
	for range workers {
		go d.work()
	}
	return workers
}

// take returns the next target and its index, false once all are taken. A container counted twice (delivered, and
// among all) is done once.
func (d *roundDispatch[T, K]) take() (*Container[T, K], int, bool) {
	for {
		i := int(d.next.Add(1) - 1)
		if i >= len(d.targets) {
			return nil, i, false
		}
		c := d.targets[i]
		if i >= d.delivered && c.round.listed == d.seq {
			d.run.done()
			continue
		}
		return c, i, true
	}
}

// work runs the rounds of the targets it takes.
func (d *roundDispatch[T, K]) work() {
	for {
		c, i, ok := d.take()
		if !ok {
			return
		}
		if err := c.runRound(d.run); err != nil {
			d.dropped(i, c, err)
		}
	}
}

// drain sends the targets no worker took their round events.
func (d *roundDispatch[T, K]) drain() {
	for {
		c, i, ok := d.take()
		if !ok {
			return
		}
		if err := c.postRound(d.run); err != nil {
			d.dropped(i, c, err)
		}
	}
}

// dropped drops the inbox of a target which stopped accepting events since the check (closed meanwhile) and counts
// it as done.
func (d *roundDispatch[T, K]) dropped(i int, c *Container[T, K], err error) {
	if len(c.round.inbox) > 0 {
		d.mu.Lock()
		if d.failErr == nil || i < d.failed {
			d.failed = i
			d.failErr = fmt.Errorf("round %s: delivering to container %s: %w: %w", d.run.name, c.id, ErrUnknownTarget, err)
		}
		d.mu.Unlock()
	}
	clear(c.round.inbox)
	c.round.inbox = c.round.inbox[:0]
	d.run.done()
}

// wait waits until every container ran the round run, or ctxDone; with workers it drains the targets left once they
// stall. It reports whether the round is done. Nil (a round without targets) only waits.
func (d *roundDispatch[T, K]) wait(run *roundRun, workers int, ctxDone <-chan struct{}) bool {
	var stall <-chan time.Time
	if d != nil && workers > 0 && int(d.next.Load()) < len(d.targets) {
		ticker := time.NewTicker(roundStall)
		defer ticker.Stop()
		stall = ticker.C
	}
	last := int64(-1)
	for {
		select {
		case <-run.doneC:
			return true
		case <-ctxDone:
			return false
		case <-stall:
			cur := d.next.Load()
			switch {
			case int(cur) >= len(d.targets):
				stall = nil
			case cur == last:
				d.drain()
				stall = nil
			default:
				last = cur
			}
		}
	}
}

// err returns the error of the first target dropped, by index; nil without targets.
func (d *roundDispatch[T, K]) err() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.failErr
}

// runRound runs the round of run in the container on the caller's goroutine, as the container's goroutine would, if
// no goroutine runs the container and nothing waits in its mailbox; else it sends it the round event (postRound).
func (c *Container[T, K]) runRound(run *roundRun) error {
	if c.nodeErrF != nil {
		if err := c.nodeErrF(); err != nil {
			return err
		}
	}
	p := c.containerProcessing
	if !p.claimRun() {
		return c.postRound(run)
	}
	c.processEvent(c.ctx, c.round.eventOf(run))
	p.releaseRun()
	return nil
}

// eventOf returns the container's round event for run: one per container, reused by every round, since the round
// before is done with it (processRoundEvent).
func (cr *containerRound[T, K]) eventOf(run *roundRun) *containerEvent {
	if cr.event == nil {
		cr.event = &containerEvent{}
	}
	*cr.event = containerEvent{payload: run, name: roundEventName}
	return cr.event
}

// postRound sends the container the round event of run, like Event without options.
func (c *Container[T, K]) postRound(run *roundRun) error {
	if c.nodeErrF != nil {
		if err := c.nodeErrF(); err != nil {
			return err
		}
	}
	if !c.addPending(true) {
		return ErrContainerClosed
	}
	c.eventsC <- c.round.eventOf(run)
	c.containerProcessing.scheduleEvent()
	return nil
}

// checkRound checks what the barrier is going to start and deliver: every spawned id is free and spawned once, the
// target of every deferred event is open and accepting events, or spawned now. It returns the first failure, the
// spawns by id first, then the events in delivery order. What is queued after the check waits for the next barrier.
func (n *Node[T, K]) checkRound(marked []*Container[T, K], spawns []spawnRequest[T], name string) error {
	r := &n.rounds
	r.seq++
	n.containerMap.mu.RLock()
	defer n.containerMap.mu.RUnlock()

	if err := n.checkSpawnsLocked(spawns, name); err != nil {
		return err
	}
	for _, c := range marked {
		cr := &c.round
		cr.mu.Lock()
		cr.checked = len(cr.outbox)
		for i := range cr.checked {
			e := &cr.outbox[i]
			t, ok := n.containerMap.m[e.to]
			switch {
			case ok && t.round.seen == r.seq:
				continue
			case ok && t.isAccepting():
				t.round.seen = r.seq
				continue
			case !ok && spawned(spawns, e.to):
				continue
			}
			cr.mu.Unlock()
			return fmt.Errorf("round %s: event %s of container %s to container %s: %w", name, e.event, c.id, e.to, ErrUnknownTarget)
		}
		cr.mu.Unlock()
	}
	return nil
}

// deliverRound takes the checked part of the outboxes of the marked containers, sorted by id, and appends their events
// to the inboxes of the targets. It returns the targets by id, listed with the barrier's sequence (listed). An event to a
// target closed since the check is dropped and reported to fail.
func (n *Node[T, K]) deliverRound(marked []*Container[T, K], name string, fail func(error)) []*Container[T, K] {
	targets := n.rounds.targets
	n.containerMap.mu.RLock()
	defer n.containerMap.mu.RUnlock()

	for _, c := range marked {
		cr := &c.round
		cr.mu.Lock()
		for i := range cr.checked {
			e := &cr.outbox[i]
			t, ok := n.containerMap.m[e.to]
			if !ok {
				fail(fmt.Errorf("round %s: event %s of container %s to container %s: %w", name, e.event, c.id, e.to, ErrUnknownTarget))
				continue
			}
			if t.round.listed != n.rounds.seq {
				t.round.listed = n.rounds.seq
				targets = append(targets, t)
			}
			t.round.inbox = append(t.round.inbox, envelope{from: c.id, event: e.event, seq: e.seq, payload: e.payload})
		}
		// events emitted since the check (a live event's transition) wait for the next barrier, numbered from 0 again
		rest := copy(cr.outbox, cr.outbox[cr.checked:])
		clear(cr.outbox[rest:])
		cr.outbox = cr.outbox[:rest]
		for i := range cr.outbox {
			cr.outbox[i].seq = uint64(i)
		}
		cr.checked = 0
		cr.mu.Unlock()
	}

	slices.SortStableFunc(targets, compareContainerIds[T, K])
	return targets
}

// ErrUnknownTarget is returned by Node.Round for a deferred event to a container which isn't open on the node, or is
// closing.
var ErrUnknownTarget = errors.New("target container is not open")

// finishRound ends a round once every target ran it: their buffer is used again by the next round (endRound).
func (n *Node[T, K]) finishRound(ctx context.Context, targets []*Container[T, K]) (int64, error) {
	clear(targets)
	n.rounds.targets = targets[:0]
	return n.endRound(ctx)
}

// endRound closes the containers which asked for it (CloseAfterRound), then starts the containers spawned during the
// round (startRoundSpawns), ends the current round and lets the next one start. It returns how many hand-overs without
// wait and batch timers started during the round (runsOn), and the error of the spawns.
func (n *Node[T, K]) endRound(ctx context.Context) (int64, error) {
	n.closeRoundContainers()
	err := n.startRoundSpawns(ctx, n.RoundName())
	started := n.rounds.started.Load()
	n.rounds.name.Store(nil)
	n.rounds.release()
	return started, err
}

// RoundName returns the name of the round the node runs right now, "" between rounds.
func (n *Node[T, K]) RoundName() string {
	if name := n.rounds.name.Load(); name != nil {
		return *name
	}
	return ""
}

// RoundOf returns the name of the round a transition runs in, given the ctx the transition got, while it runs: the
// round whose barrier delivered the event, or which runs the round batch. It returns "" for transitions of events sent
// outside of rounds (Event, hand-overs, calls of other containers), also those run while a transition of the round
// waits.
func RoundOf(ctx context.Context) string {
	if tc, ok := ctx.Value(transitionCtxKey{}).(*transitionCtx); ok {
		if c, ok := tc.container.(messager); ok {
			return c.roundName()
		}
	}
	return ""
}

// roundName returns the name of the round the container runs right now, "" if none (RoundOf).
func (c *Container[T, K]) roundName() string {
	if run := c.round.run.Load(); run != nil {
		return run.name
	}
	return ""
}

// processRoundEvent runs the round event the barrier sent the container (postRound): the init func before the first
// event, then the round (processRound). The next round reuses the event and may start as soon as this container is
// done with this one: it says so last (run.done). The caller holds mu.
func (c *Container[T, K]) processRoundEvent(ctx context.Context, run *roundRun) {
	defer run.done()
	if m := c.metrics; m != nil {
		start := time.Now()
		defer func() {
			m.events.Add(1)
			m.eventTime.observe(time.Since(start))
		}()
	}
	defer c.endPending(true)

	if debugOn(c.logger) {
		debugf(c.logger, "running round %s", run.name)
	}
	c.initFirst(ctx)
	c.processRound(ctx, run)
}

// processRound runs the events the barrier delivered to the container, in order, like events sent with Event, then
// its round batches. The caller holds mu.
func (c *Container[T, K]) processRound(ctx context.Context, run *roundRun) {
	cr := &c.round
	cr.run.Store(run)
	cr.running, cr.err = true, nil

	for i := range cr.inbox {
		m := &cr.inbox[i]
		// the container's event, unless a batch (WithBatching) still holds it: then it gets a new one
		evt := cr.delivered
		if evt == nil || evt.refs != 0 {
			evt = &containerEvent{}
			cr.delivered = evt
		}
		*evt = containerEvent{payload: m.payload, name: m.event, refs: 1}
		// round batches take the sender and the sequence of the entry (queueRoundBatch)
		cr.current, cr.currentMsg = evt, m
		c.runEvent(ctx, evt)
		cr.current, cr.currentMsg = nil, nil
		c.releaseEvent(evt)
	}
	clear(cr.inbox)
	cr.inbox = cr.inbox[:0]

	// the round batches run once the container ran all its events
	if c.runRoundBatches(ctx, false, run.runAll) {
		c.save(ctx, "round batch")
	}

	cr.running = false
	cr.run.Store(nil)
	if cr.err != nil {
		run.fail(c.id, cr.err)
		cr.err = nil
	}
}

// failed keeps the first error of the container's transitions while it runs a round. The caller holds the
// container's mu.
func (cr *containerRound[T, K]) failed(err error) {
	if cr.running && cr.err == nil {
		cr.err = err
	}
}

// acquire waits until no other round runs.
func (r *nodeRounds[T, K]) acquire(ctx context.Context) error {
	r.semOnce.Do(func() {
		r.sem = make(chan struct{}, 1)
	})
	select {
	case r.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release lets the next round start.
func (r *nodeRounds[T, K]) release() {
	<-r.sem
}

// runsOn counts work of the node's containers which runs on its own, a hand-over without wait or a batch timer, by
// delta: a round during which some started, or ending while some runs, fails with ErrRoundOverlap. Nil (a container
// without a node) counts nothing.
func (r *nodeRounds[T, K]) runsOn(delta int64) {
	if r == nil {
		return
	}
	r.background.Add(delta)
	if delta > 0 && r.name.Load() != nil {
		r.started.Add(delta)
	}
}

// done counts a container as done with the round.
func (r *roundRun) done() {
	if r.left.Add(-1) == 0 {
		close(r.doneC)
	}
}

// fail keeps the first error of a container's transitions in the round.
func (r *roundRun) fail(id string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, roundError{id: id, err: err})
}

// firstError returns the error of the container with the lowest id, nil if no transition failed.
func (r *roundRun) firstError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.errs) == 0 {
		return nil
	}
	first := slices.MinFunc(r.errs, func(a, b roundError) int { return cmp.Compare(a.id, b.id) })
	return fmt.Errorf("round %s: container %s: %w", r.name, first.id, first.err)
}

func compareContainerIds[T proto.Message, K Builder[T]](a, b *Container[T, K]) int {
	return strings.Compare(a.id, b.id)
}
