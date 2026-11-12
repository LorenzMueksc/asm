package asm

import (
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
		// mu guards outbox. Transitions append to it under the container's mu, the barrier takes it from another
		// goroutine.
		mu sync.Mutex
		// outbox holds the deferred events emitted since the last barrier, in emit order. It is emptied, not
		// reallocated, by the barrier.
		outbox []envelope
		// marked is set while the container is in the node's list of containers the next barrier visits
		marked atomic.Bool
	}
	// nodeRounds is the part of a node running rounds.
	nodeRounds[T proto.Message, K Builder[T]] struct {
		// mu guards marked
		mu sync.Mutex
		// marked lists the containers the next barrier visits, each once: they have deferred events in their outbox
		marked []*Container[T, K]
	}
)

// deferredOption is the option WithDeferred returns, allocated once
var deferredOption EventOption = newFuncEventOption(func(o *eventOptions) {
	o.deferred = true
})

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
	case c.round.node == nil:
		return fmt.Errorf("emitting deferred event %s: only containers of a node emit deferred events", eventName)
	}
	debugf(c.logger, "deferred event %s to container %s", eventName, o.target)

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
	cr.mu.Unlock()
	cr.marked.Store(false)
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
