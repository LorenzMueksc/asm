package asm

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

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
	// roundCtxKey is the ctx key of the *roundRun the transitions of a round run in (RoundOf)
	roundCtxKey struct{}
)

// Round runs a round of the node's containers, the barrier between two phases of their work:
//
//  1. it takes the outbox of every container: the deferred events emitted since the last round (WithDeferred);
//  2. it delivers them into the inboxes of their targets, sorted by (sender id, sequence);
//  3. every container which received events runs them in that order, concurrently with the other containers: the
//     transitions matching them, as for events sent with Event;
//  4. it returns once every one of them ran its inbox. Deferred events emitted meanwhile are delivered by the next
//     round, not by this one.
//
// It returns the first error: a target which isn't open on the node (its events are dropped), else the first error of
// a transition of the container with the lowest id. Transition errors still go to the error func as well. Rounds run
// one at a time; ctx bounds waiting for the node, for the round before and for this one: if it ends first, Round
// returns its error and the round goes on, the next one starts once it is done.
// What Round waits for is the work of the delivered events, including what their transitions run synchronously
// (nested and waited Emits); work handed over without waiting (WithTarget without WithWaitRemote) and batch timers
// run on their own. Transitions see the round's name with RoundOf. Only for a standalone node (WithSkipRaft), else it
// returns ErrNotStandalone.
func (n *Node[T, K]) Round(ctx context.Context, name string) error {
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
	r.name.Store(&name)

	run := &roundRun{name: name, doneC: make(chan struct{})}
	// the barrier's own count: the round can't end while round events are being sent
	run.left.Store(1)

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

	var barrierErr error
	fail := func(err error) {
		if barrierErr == nil {
			barrierErr = err
		}
	}

	targets := n.deliverRound(marked, name, fail)
	clear(marked)
	r.spare = marked[:0]

	for _, c := range targets {
		run.left.Add(1)
		if err := c.Event(roundEventName, run); err != nil {
			// the container stopped accepting events: its inbox is dropped
			clear(c.round.inbox)
			c.round.inbox = c.round.inbox[:0]
			fail(fmt.Errorf("round %s: delivering to container %s: %w", name, c.id, err))
			run.done()
		}
	}
	clear(targets)
	r.targets = targets[:0]
	run.done()

	select {
	case <-run.doneC:
	case <-ctx.Done():
		// the round goes on without the caller; the next one waits until it is done
		go func() {
			<-run.doneC
			n.endRound()
		}()
		return ctx.Err()
	}
	n.endRound()

	if barrierErr != nil {
		return barrierErr
	}
	return run.firstError()
}

// deliverRound takes the outboxes of the marked containers, sorted by id, and appends their events to the inboxes of
// the targets. It returns the targets by id. Events to a target which isn't open are dropped and reported to fail.
func (n *Node[T, K]) deliverRound(marked []*Container[T, K], name string, fail func(error)) []*Container[T, K] {
	targets := n.rounds.targets
	n.containerMap.mu.RLock()
	defer n.containerMap.mu.RUnlock()

	for _, c := range marked {
		cr := &c.round
		cr.mu.Lock()
		for i := range cr.outbox {
			e := &cr.outbox[i]
			t, ok := n.containerMap.m[e.to]
			if !ok {
				fail(fmt.Errorf("round %s: event %s of container %s to container %s: %w", name, e.event, c.id, e.to, ErrUnknownTarget))
				continue
			}
			if !t.round.queued {
				t.round.queued = true
				targets = append(targets, t)
			}
			t.round.inbox = append(t.round.inbox, envelope{from: c.id, event: e.event, seq: e.seq, payload: e.payload})
		}
		clear(cr.outbox)
		cr.outbox = cr.outbox[:0]
		cr.mu.Unlock()
	}

	slices.SortStableFunc(targets, compareContainerIds[T, K])
	for _, t := range targets {
		t.round.queued = false
	}
	return targets
}

// ErrUnknownTarget is returned by Node.Round for a deferred event to a container which isn't open on the node.
var ErrUnknownTarget = errors.New("target container is not open")

// endRound ends the current round and lets the next one start.
func (n *Node[T, K]) endRound() {
	n.rounds.name.Store(nil)
	n.rounds.release()
}

// RoundName returns the name of the round the node runs right now, "" between rounds.
func (n *Node[T, K]) RoundName() string {
	if name := n.rounds.name.Load(); name != nil {
		return *name
	}
	return ""
}

// RoundOf returns the name of the round a transition runs in, given the ctx the transition got: the round whose
// barrier delivered the event. It returns "" for transitions of events sent outside of rounds (Event, hand-overs,
// calls of other containers).
func RoundOf(ctx context.Context) string {
	if run, ok := ctx.Value(roundCtxKey{}).(*roundRun); ok {
		return run.name
	}
	return ""
}

// processRound runs the events the barrier delivered to the container, in order, like events sent with Event. The
// caller holds mu.
func (c *Container[T, K]) processRound(ctx context.Context, run *roundRun) {
	defer run.done()

	cr := &c.round
	ctx = context.WithValue(ctx, roundCtxKey{}, run)
	cr.running, cr.err = true, nil

	var evt *containerEvent
	for i := range cr.inbox {
		m := &cr.inbox[i]
		// an event no batch holds anymore is used again for the next one
		if evt == nil || evt.refs != 0 {
			evt = &containerEvent{}
		}
		*evt = containerEvent{payload: m.payload, name: m.event, refs: 1}
		c.runEvent(ctx, evt)
		c.releaseEvent(evt)
	}
	clear(cr.inbox)
	cr.inbox = cr.inbox[:0]

	cr.running = false
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
