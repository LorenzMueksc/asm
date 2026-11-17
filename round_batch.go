package asm

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"unsafe"
)

// Round batches (WithBatchOnRound) collect the deferred events a transition matches in a round and run it once with
// all of them, sorted, after the container ran its inbox: eg. a market collecting the orders of a round and clearing
// them at once. A round batch is exactly the round's delivered events matching it; live events never join one.

type (
	// Message is a deferred event the barrier delivered: its sender, its sequence in the sender's outbox and its
	// payload. Round batches (WithBatchOnRound) get them as payload, other transitions of a delivered event with
	// MessageOf.
	Message struct {
		// From is the id of the container which sent the event deferred
		From string
		// Seq is the position of the event in the outbox of From between two barriers
		Seq     uint64
		Payload any
	}
	// roundBatch holds the events of a round batch transition. msgs collects them, taken holds the ones running; both
	// are reused, like evt, the event the batch runs with, and boxed, taken as its payload.
	roundBatch struct {
		t     *Transition
		msgs  []Message
		taken []Message
		evt   containerEvent
		boxed any
	}
	// roundBatchIndex lists the round batch transitions of published transitions by priority, then name, built on
	// first use (roundBatches)
	roundBatchIndex struct {
		once sync.Once
		ts   []*Transition
	}
	// messager is a container running a delivered event (MessageOf) or a round (RoundOf)
	messager interface {
		deliveredMessage() (Message, bool)
		roundName() string
	}
)

// WithBatchOnRound runs a func transition once per round with all the deferred events of the round matching it,
// instead of once per event: its payload is a []Message sorted by (From, Seq), the order the barrier delivered them.
// The batch runs after the container ran the other transitions of all its events of the round. Live events (Event,
// calls, hand-overs, an Emit without WithDeferred) never join a round batch: they run only their other transitions, so
// a container which needs both registers both. The slice is reused: it is only valid until the transition returns.
// Batches need a container of a standalone node running rounds (Node.Round); elsewhere (a container built without a
// node) the transition runs at once per event with a one-message batch. A round batch transition is a writer. It
// replaces WithBatching.
func WithBatchOnRound() TransitionOption {
	return newFuncTransitionOption(func(t *Transition) {
		t.roundBatch = true
		t.Batching = Batching{Count: 1}
	})
}

// checkRoundBatch refuses the options a round batch transition can't have, called when the transition is compiled with
// readOnly = Transition.ReadOnly: a round batch is a writer, so it can't be read-only (WithReadOnly).
func checkRoundBatch(t *Transition, readOnly bool) error {
	if t.roundBatch && readOnly {
		return fmt.Errorf("transition %s: WithBatchOnRound runs as a writer, it can't be read-only", t.Name)
	}
	return nil
}

// queueRoundBatch adds a delivered event to the next batch of a round batch transition, with its sender and sequence.
// A live event doesn't join it: the transition doesn't run for it. A container without rounds (built without a node)
// doesn't collect events it would never run: the transition runs at once with the event alone. Readers never get here:
// a round batch is a writer (readOnlyFunc). The caller holds mu.
func (c *Container[T, K]) queueRoundBatch(ctx context.Context, t *Transition, evt *containerEvent, saveAfter bool) bool {
	cr := &c.round
	if cr.node == nil {
		return c.runBatch(ctx, t, []Message{{Payload: evt.payload}}, nil, saveAfter)
	}
	if evt != cr.current || cr.currentMsg == nil {
		if debugOn(c.logger) {
			debugf(c.logger, "live event %s doesn't join round batch %s", evt.name, t.Name)
		}
		return saveAfter
	}

	b := cr.batchOf(t)
	// the transition as published now
	b.t = t
	b.msgs = append(b.msgs, Message{From: cr.currentMsg.from, Seq: cr.currentMsg.seq, Payload: evt.payload})
	return saveAfter
}

// MessageOf returns the deferred event a transition runs for, given the ctx the transition got: its sender, its
// sequence and its payload. It returns false for transitions of live events (Event, calls, hand-overs, an Emit without
// WithDeferred, also those a transition of a delivered event emits) and in round batches, whose payload holds their
// messages.
func MessageOf(ctx context.Context) (Message, bool) {
	tc, ok := ctx.Value(transitionCtxKey{}).(*transitionCtx)
	if !ok || tc.depth != 0 {
		return Message{}, false
	}
	if c, ok := tc.container.(messager); ok {
		return c.deliveredMessage()
	}
	return Message{}, false
}

// deliveredMessage returns the delivered event the container runs now. The caller runs a transition of it (mu).
func (c *Container[T, K]) deliveredMessage() (Message, bool) {
	m := c.round.currentMsg
	if m == nil {
		return Message{}, false
	}
	return Message{From: m.from, Seq: m.seq, Payload: m.payload}, true
}

// runRoundBatches runs the round batch transitions which have events, by priority, then by name; with all (WithRunAll)
// also the ones without events, with an empty batch, if their conditions match the entity. The caller holds mu.
func (c *Container[T, K]) runRoundBatches(ctx context.Context, saveAfter, all bool) bool {
	cr := &c.round
	// the batches with events, taken before any of them runs
	taken := cr.batchOrder[:0]
	for _, b := range cr.batches {
		if len(b.msgs) == 0 {
			continue
		}
		b.taken, b.msgs = b.msgs, b.taken[:0]
		taken = append(taken, b)
	}
	if all {
		taken = c.takeEmptyBatches(taken)
	}
	slices.SortFunc(taken, func(a, b *roundBatch) int {
		if p := cmp.Compare(b.t.Priority, a.t.Priority); p != 0 {
			return p
		}
		return strings.Compare(a.t.Name, b.t.Name)
	})

	for _, b := range taken {
		if !slices.IsSortedFunc(b.taken, compareMessages) {
			slices.SortStableFunc(b.taken, compareMessages)
		}
		if debugOn(c.logger) {
			debugf(c.logger, "running round batch %s with %d events", b.t.Name, len(b.taken))
		}
		saveAfter = c.runTaken(ctx, b, saveAfter)
		clear(b.taken)
		b.taken = b.taken[:0]
	}
	clear(taken)
	cr.batchOrder = taken[:0]
	return saveAfter
}

// takeEmptyBatches adds the round batch transitions of the container without events this round to taken (WithRunAll),
// those whose conditions match the entity, with an empty batch. The caller holds mu.
func (c *Container[T, K]) takeEmptyBatches(taken []*roundBatch) []*roundBatch {
	cr := &c.round
	evaluated := false
	for _, t := range c.currentTransitions().roundBatches() {
		b := cr.batchOf(t)
		if len(b.taken) > 0 {
			// it has events
			continue
		}
		if len(t.Conditions) > 0 {
			// against the entity as it is after the round's events, like the events matched it when delivered
			if !evaluated {
				c.EvaluateEntityWithEvents(nil)
				evaluated = true
			}
			if !evaluateConditions(t.Conditions, c.states) {
				continue
			}
		}
		b.t = t
		if b.taken == nil {
			b.taken = make([]Message, 0, 1)
		}
		taken = append(taken, b)
	}
	return taken
}

// batchOf returns the batch of the round batch transition, created on first use. The caller holds mu.
func (cr *containerRound[T, K]) batchOf(t *Transition) *roundBatch {
	for _, b := range cr.batches {
		if b.t.Name == t.Name {
			return b
		}
	}
	b := &roundBatch{t: t}
	cr.batches = append(cr.batches, b)
	return b
}

// runBatch runs a round batch transition with the messages, on the event evt (nil: a new one). The caller holds mu.
func (c *Container[T, K]) runBatch(ctx context.Context, t *Transition, msgs any, evt *containerEvent, saveAfter bool) bool {
	if t.Batching.Count != 1 {
		// WithBatching given after WithBatchOnRound
		cp := *t
		cp.Batching = Batching{Count: 1}
		t = &cp
	}
	if evt == nil {
		evt = &containerEvent{}
	}
	// one event with all the messages: the func of a transition batching one event gets its payload
	*evt = containerEvent{payload: msgs, name: t.EventName, refs: 1}
	return c.executeActions(ctx, t, evt.one(), saveAfter)
}

// runTaken runs the batch with the messages it took, on its own event: executeActions released it when the batch ran
// last. The caller holds mu.
func (c *Container[T, K]) runTaken(ctx context.Context, b *roundBatch, saveAfter bool) bool {
	// the payload is boxed again only when the slice changed: its backing array is reused
	if old, ok := b.boxed.([]Message); !ok || len(old) != len(b.taken) || cap(old) != cap(b.taken) ||
		unsafe.SliceData(old) != unsafe.SliceData(b.taken) {
		b.boxed = b.taken
	}
	return c.runBatch(ctx, b.t, b.boxed, &b.evt, saveAfter)
}

func compareMessages(a, b Message) int {
	if c := strings.Compare(a.From, b.From); c != 0 {
		return c
	}
	return cmp.Compare(a.Seq, b.Seq)
}

// roundBatches returns the round batch transitions (WithBatchOnRound) of this node by priority, then name, nil
// without transitions.
func (p *publishedTransitions) roundBatches() []*Transition {
	if p == nil {
		return nil
	}
	p.rounds.once.Do(func() {
		for _, t := range p.all {
			if t.roundBatch && !t.Remote {
				p.rounds.ts = append(p.rounds.ts, t)
			}
		}
		slices.SortStableFunc(p.rounds.ts, func(a, b *Transition) int {
			if c := cmp.Compare(b.Priority, a.Priority); c != 0 {
				return c
			}
			return strings.Compare(a.Name, b.Name)
		})
	})
	return p.rounds.ts
}
