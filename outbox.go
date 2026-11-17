package asm

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"
)

// Deferred events (WithDeferred) are the messages containers send each other within the rounds of a node: a
// transition appends them to its container's outbox, and nothing is delivered until the node's next round barrier,
// which delivers every outbox sorted by (sender id, sequence). What a container receives in a round therefore doesn't
// depend on how the goroutines of the containers were scheduled.

type (
	// envelope is a deferred event. In an outbox to names the target; in an inbox from names the sender. seq is the
	// position of the event in the sender's outbox between two barriers.
	envelope struct {
		from, to string
		event    string
		seq      uint64
		payload  any
	}
	// containerRound is the part of a container taking part in the rounds of its node.
	containerRound[T proto.Message, K Builder[T]] struct {
		// node is set by the node (resetContainer), nil for containers built with NewContainer and not reset by a node
		node *nodeRounds[T, K]
		// mu guards outbox and spawns. Transitions append to them under the container's mu, the barrier takes them
		// from another goroutine.
		mu sync.Mutex
		// outbox holds the deferred events emitted since the last barrier, in emit order. It is emptied, not
		// reallocated, by the barrier.
		outbox []envelope
		// spawns holds the containers to start (Spawn, spawn.go): at the end of the round, or at the start of the
		// next one for those queued between rounds
		spawns []spawnRequest[T]
		// checked and spawnsTaken count the outbox entries and the spawns the barrier checked and collected; what is
		// queued after waits for the next barrier. seen is the barrier's sequence (nodeRounds.seq) which checked the
		// container as a target. Only used by the barrier.
		checked     int
		spawnsTaken int
		seen        uint64
		// closing is set once the container asked to be closed at the end of the round (CloseAfterRound)
		closing atomic.Bool
		// marked is set while the container is in the node's list of containers the next barrier visits, spawning
		// while it is in the node's list of containers with spawns
		marked   atomic.Bool
		spawning atomic.Bool

		// inbox holds the deferred events the barrier delivered to the container, in delivery order. The barrier fills
		// it while no round runs, the container runs and empties it on its goroutine (processRound); the round event
		// and the end of the round order the two.
		inbox []envelope
		// listed is the sequence of the barrier (nodeRounds.seq) which lists the container as a target, only used by the
		// barrier and its dispatch
		listed uint64
		// running is set while the container runs a round, err holds the first error of its transitions meanwhile;
		// both under the container's mu
		running bool
		err     error
		// run is the round the container runs right now (RoundOf), set and cleared under the container's mu
		run atomic.Pointer[roundRun]
		// event is the round event the barrier sends the container (postRound), delivered the event its inbox entries
		// run as (processRound), both reused by every round, made by the first one
		event     *containerEvent
		delivered *containerEvent

		// batches holds the events of the round batch transitions (WithBatchOnRound), batchOrder is a buffer of
		// runRoundBatches; current is the event of the inbox entry currentMsg running right now (MessageOf). All under
		// the container's mu.
		batches    []*roundBatch
		batchOrder []*roundBatch
		current    *containerEvent
		currentMsg *envelope
	}
	// nodeRounds is the part of a node running rounds.
	nodeRounds[T proto.Message, K Builder[T]] struct {
		// mu guards marked, spawning and closing
		mu sync.Mutex
		// marked lists the containers the next barrier visits, each once: they have deferred events in their outbox
		marked []*Container[T, K]
		// spawning lists the containers which spawned containers, each once: the end of the round, or the start of the
		// next one, starts them
		spawning []*Container[T, K]
		// closing lists the containers to close at the end of the round (CloseAfterRound)
		closing []*Container[T, K]

		// sem (capacity 1) lets one round run at a time, created by the first round
		sem     chan struct{}
		semOnce sync.Once
		// name is the name of the running round, nil between rounds
		name atomic.Pointer[string]
		// seq numbers the barriers (checkRound), only used by the barrier
		seq uint64
		// background counts the work of the node's containers which runs on its own: hand-overs without wait and
		// batch timers; started counts what started of it during the running round (ErrRoundOverlap)
		background atomic.Int64
		started    atomic.Int64
		// spare, spareSpawning and targets are buffers of the barrier, reused by the next round
		spare         []*Container[T, K]
		spareSpawning []*Container[T, K]
		targets       []*Container[T, K]
		spawns        []spawnRequest[T]
	}
)

// WithDeferred queues an event a transition emits (Container.Emit) for the container WithTarget names, which is
// required: the event goes to the emitting container's outbox and is delivered at the node's next round barrier
// (Node.Round), sorted by (sender id, sequence) with the other deferred events. The target may be the emitting
// container itself. Emit returns once the event is queued; WithWaitRemote, WithReply and WithDoneChannel don't apply
// and make Emit fail.
func WithDeferred() EventOption {
	return deferredOption
}

// emitDeferred appends an event emitted with WithDeferred to the container's outbox. The caller runs a transition of
// the container.
func (c *Container[T, K]) emitDeferred(eventName string, payload any, o *eventOptions) error {
	switch {
	case o.target == "":
		return fmt.Errorf("emitting deferred event %s: a deferred event needs a target (WithTarget)", eventName)
	case o.waitRemote || o.replyF != nil || o.doneC != nil:
		return fmt.Errorf("emitting deferred event %s: WithWaitRemote, WithReply and WithDoneChannel don't apply to deferred events", eventName)
	case c.rw.base != nil:
		// a reader runs next to the writer: its place in the outbox would depend on the scheduling
		return fmt.Errorf("emitting deferred event %s: %w", eventName, ErrReadOnly)
	case c.round.node == nil:
		return fmt.Errorf("emitting deferred event %s: only containers of a node emit deferred events", eventName)
	}
	if debugOn(c.logger) {
		debugf(c.logger, "deferred event %s to container %s", eventName, o.target)
	}

	r := &c.round
	r.mu.Lock()
	r.outbox = append(r.outbox, envelope{to: o.target, event: eventName, seq: uint64(len(r.outbox)), payload: payload})
	r.mu.Unlock()
	r.node.mark(c)
	return nil
}

// reset prepares the round part of a container for a life on the node with the rounds r.
func (cr *containerRound[T, K]) reset(r *nodeRounds[T, K]) {
	cr.mu.Lock()
	cr.node = r
	clear(cr.outbox)
	cr.outbox = cr.outbox[:0]
	clear(cr.spawns)
	cr.spawns = cr.spawns[:0]
	cr.mu.Unlock()
	cr.checked, cr.spawnsTaken = 0, 0
	cr.closing.Store(false)
	cr.marked.Store(false)
	cr.spawning.Store(false)
	cr.batches = nil
}

// DropOutboxes drops what the node's containers queued for the next round barrier: the deferred events in their
// outboxes (WithDeferred) and the containers they spawned (Spawn) which didn't start. It returns how many events and
// spawns it dropped. A failed round the caller rolls back must not leave its events for the next one, nor a barrier
// which failed its checks (Round, ErrUnknownTarget and ErrContainerExists keep everything queued). It waits until a
// running round is done, ctx bounds the wait. Closes (CloseAfterRound) stay requested.
func (n *Node[T, K]) DropOutboxes(ctx context.Context) (int, error) {
	r := &n.rounds
	if err := r.acquire(ctx); err != nil {
		return 0, err
	}
	defer r.release()

	r.mu.Lock()
	defer r.mu.Unlock()
	dropped := 0
	for _, c := range r.marked {
		cr := &c.round
		// unmarked before its outbox is emptied: what it queues meanwhile is either dropped, or marks it again once
		// r.mu is free
		cr.marked.Store(false)
		cr.mu.Lock()
		dropped += len(cr.outbox)
		clear(cr.outbox)
		cr.outbox = cr.outbox[:0]
		cr.mu.Unlock()
	}
	clear(r.marked)
	r.marked = r.marked[:0]
	for _, c := range r.spawning {
		cr := &c.round
		cr.spawning.Store(false)
		cr.mu.Lock()
		dropped += len(cr.spawns)
		clear(cr.spawns)
		cr.spawns = cr.spawns[:0]
		cr.mu.Unlock()
	}
	clear(r.spawning)
	r.spawning = r.spawning[:0]
	return dropped, nil
}

// mark adds the container to the containers the next barrier visits, unless it is in the list already.
func (r *nodeRounds[T, K]) mark(c *Container[T, K]) {
	if !c.round.marked.CompareAndSwap(false, true) {
		return
	}
	r.mu.Lock()
	r.marked = append(r.marked, c)
	r.mu.Unlock()
}

// markSpawning adds the container to the containers with spawns, unless it is in the list already.
func (r *nodeRounds[T, K]) markSpawning(c *Container[T, K]) {
	if !c.round.spawning.CompareAndSwap(false, true) {
		return
	}
	r.mu.Lock()
	r.spawning = append(r.spawning, c)
	r.mu.Unlock()
}
