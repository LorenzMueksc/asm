package asm

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Events forwarded without waiting are processed by the receiving node in its own container: it acquires the lock
// of the container id once the sender released it, loads the entity with its init func, runs its own func transitions
// matching the events, saves on change and closes the container.
// The forwarded events of a container id are queued, so they run in the order they arrived. While the lock is held by
// another node no goroutine waits: the drain acquires it asynchronously (acquireAsync) and resumes once it is granted.
//
// The sender keeps a hand-over until its work ran: the node answers it once its executions ran (executed), also when
// their transitions failed. A hand-over this node can't run anymore (it leaves, the container doesn't open) is failed
// back to the sender, which hands it to another owner. Hand-overs are known by their id while queued or running and
// for doneHandOverTTL after they ran (at most maxDoneHandOvers): one sent again isn't run twice.

const (
	// doneHandOverTTL is how long a node remembers the id of a hand-over it ran
	doneHandOverTTL = time.Minute
	// maxDoneHandOvers limits the ids of hand-overs a node remembers after they ran, the oldest are forgotten first
	maxDoneHandOvers = 65536
)

type (
	forwardQueues struct {
		mu     sync.Mutex
		queues map[string]*forwardQueue
		// handOvers holds the accepted hand-overs by id: queued or running ones, and the done ones listed in done
		handOvers map[string]*acceptedHandOver
		// done holds the hand-overs which ran, in the order they finished
		done []*acceptedHandOver
	}
	forwardQueue struct {
		items   []forwardedExecution
		running bool
	}
	forwardedExecution struct {
		eventName string
		r         *remoteExecution
		// handOver is the hand-over the execution belongs to, nil if it came without id
		handOver *acceptedHandOver
	}
	// acceptedHandOver is a hand-over this node accepted. Events of another entity may run in several containers,
	// one execution each.
	acceptedHandOver struct {
		id string
		// left is the number of executions which didn't run yet
		left int
		// replies get the outcome, one for every call which sent the hand-over: nil once all executions ran,
		// else the error the hand-over was failed back with
		replies []func(error)
		done    bool
		failed  bool
		doneAt  time.Time
	}
	// handedOver is an execution of a hand-over in the container of the id
	handedOver struct {
		id   string
		item forwardedExecution
	}
)

// acceptHandOver queues the executions of a hand-over. A hand-over with a known id isn't queued again: a done one
// returns executed, a queued or running one gets reply added. reply gets the outcome later: nil once all executions
// ran, else the error the hand-over is failed back with. A hand-over without id is queued without being tracked.
// While the node leaves or stopped, new hand-overs are refused with Unavailable.
func (n *Node[T, K]) acceptHandOver(id string, executions []handedOver, reply func(error)) (executed bool, err error) {
	n.forwarded.mu.Lock()
	defer n.forwarded.mu.Unlock()

	if known, ok := n.forwarded.handOvers[id]; ok && id != "" {
		if known.done {
			return true, nil
		}
		if reply != nil {
			known.replies = append(known.replies, reply)
		}
		return false, nil
	}
	if n.leaving.Load() || n.ctx.Err() != nil {
		return false, status.Errorf(codes.Unavailable, "node %s is leaving the cluster", n.Id())
	}
	if len(executions) == 0 {
		return true, nil
	}

	var h *acceptedHandOver
	if id != "" {
		h = &acceptedHandOver{id: id, left: len(executions)}
		if reply != nil {
			h.replies = []func(error){reply}
		}
		if n.forwarded.handOvers == nil {
			n.forwarded.handOvers = make(map[string]*acceptedHandOver)
		}
		n.forwarded.handOvers[id] = h
	}
	for _, e := range executions {
		e.item.handOver = h
		n.enqueueForwardedLocked(e.id, e.item)
	}
	return false, nil
}

// enqueueForwardedLocked queues a forwarded execution of a container id and starts draining the queue if needed.
// The caller holds forwarded.mu.
func (n *Node[T, K]) enqueueForwardedLocked(id string, item forwardedExecution) {
	q, ok := n.forwarded.queues[id]
	if !ok {
		q = &forwardQueue{}
		n.forwarded.queues[id] = q
	}
	q.items = append(q.items, item)

	if !q.running {
		q.running = true
		go n.drainForwarded(id, q)
	}
}

// handOverRan counts an execution of a hand-over as run; once all ran, the senders get executed.
func (n *Node[T, K]) handOverRan(h *acceptedHandOver) {
	if h == nil {
		return
	}
	n.forwarded.mu.Lock()
	h.left--
	if h.left > 0 || h.failed || h.done {
		n.forwarded.mu.Unlock()
		return
	}
	h.done, h.doneAt = true, time.Now()
	replies := h.replies
	h.replies = nil
	n.forwarded.done = append(n.forwarded.done, h)
	n.forgetDoneLocked(h.doneAt)
	n.forwarded.mu.Unlock()

	for _, reply := range replies {
		reply(nil)
	}
}

// forgetDoneLocked forgets the ids of hand-overs done longer than doneHandOverTTL ago, and the oldest beyond
// maxDoneHandOvers. The caller holds forwarded.mu.
func (n *Node[T, K]) forgetDoneLocked(now time.Time) {
	done := n.forwarded.done
	for len(done) > 0 && (len(done) > maxDoneHandOvers || now.Sub(done[0].doneAt) > doneHandOverTTL) {
		if n.forwarded.handOvers[done[0].id] == done[0] {
			delete(n.forwarded.handOvers, done[0].id)
		}
		done[0] = nil
		done = done[1:]
	}
	n.forwarded.done = done
}

// failHandOver fails a hand-over back to its senders, which hand it to another owner on Unavailable. Its id is
// forgotten: sent again it runs. It returns false if nobody gets the failure (no id, or no caller to reply to).
func (n *Node[T, K]) failHandOver(h *acceptedHandOver, err error) bool {
	if h == nil {
		return false
	}
	n.forwarded.mu.Lock()
	if h.failed || h.done {
		n.forwarded.mu.Unlock()
		return true
	}
	h.failed = true
	if n.forwarded.handOvers[h.id] == h {
		delete(n.forwarded.handOvers, h.id)
	}
	replies := h.replies
	h.replies = nil
	n.forwarded.mu.Unlock()

	for _, reply := range replies {
		reply(err)
	}
	return len(replies) > 0
}

// giveUpForwarded gives up an execution which can't run on this node: its hand-over is failed back to the senders,
// errors without a gRPC code as Unavailable, so they hand it to another owner. Without a sender to tell it is dropped.
func (n *Node[T, K]) giveUpForwarded(id string, item forwardedExecution, err error) {
	if c := status.Code(err); c == codes.Unknown || c == codes.OK {
		err = status.Errorf(codes.Unavailable, "node %s can't run events %s of container %s: %v", n.Id(), item.eventName, id, err)
	}
	if n.failHandOver(item.handOver, err) {
		debugf(n.logger, "failed forwarded events %s of container %s back to the sender: %v", item.eventName, id, err)
		return
	}
	n.logger.Error(fmt.Sprintf("dropping forwarded events %s of container %s: %v", item.eventName, id, err))
}

// drainForwarded sends the queued executions of a container id to its container one after another.
// Starting the container waits for the lock. Every execution closes the container once it is idle,
// so the lock is only held while there is work. Once the node leaves or stopped, the queued executions are given up:
// their hand-overs go back to the senders.
func (n *Node[T, K]) drainForwarded(id string, q *forwardQueue) {
	for {
		n.forwarded.mu.Lock()
		if len(q.items) == 0 || n.leaving.Load() || n.ctx.Err() != nil {
			items := q.items
			q.items = nil
			q.running = false
			delete(n.forwarded.queues, id)
			n.forwarded.mu.Unlock()
			for _, item := range items {
				n.giveUpForwarded(id, item, status.Errorf(codes.Unavailable, "node %s is leaving the cluster", n.Id()))
			}
			return
		}
		item := q.items[0]
		n.forwarded.mu.Unlock()

		// without a container of the id the lock is acquired without waiting here; the drain resumes once it is
		// granted, giving the item up if the container didn't open. If a start of the id runs on this node, or a
		// container of the id closes, the item waits for it below like any start.
		if !n.skipRaft && !n.hasContainer(id) && n.acquireAsync(id, func(err error) {
			if err != nil {
				n.forwarded.mu.Lock()
				q.items = q.items[1:]
				n.forwarded.mu.Unlock()
				n.giveUpForwarded(id, item, err)
			}
			n.drainForwarded(id, q)
		}) {
			return
		}

		n.forwarded.mu.Lock()
		q.items = q.items[1:]
		n.forwarded.mu.Unlock()

		doneC := make(chan struct{}, 1)
		err := n.sendToContainer(n.ctx, id, false, item.eventName, nil,
			withRemoteExecution(item.r), WithDoneChannel(doneC), withCloseAfter())
		if err != nil {
			n.giveUpForwarded(id, item, err)
			continue
		}

		go func() {
			<-doneC
			if item.r.err != nil {
				n.logger.Error(fmt.Sprintf("forwarded events %s of container %s failed: %+v", item.eventName, id, item.r.err))
			}
			// failed transitions stay here: the work ran
			n.handOverRan(item.handOver)
		}()
	}
}
