package asm

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"rumue.io/asm/service"
)

// Forwarded events go over one stream per pair of nodes (ForwardEvents) instead of a unary call each.
// On the sending node a goroutine per stream sends all calls waiting at that moment in one message and another one
// dispatches the replies. The receiving node runs every waited call in its own goroutine, like a unary call, and sends
// the replies back in batches. A call whose context ends is cancelled on the receiver too.
// A broken stream fails its pending calls with Unavailable (the sender covers their transitions with other owners),
// the next call opens a new stream.
//
// Hand-overs (no wait) are kept by the sender until their work ran: the receiver replies twice, with the accept and
// once the work ran (executed). A kept hand-over fails, and its transitions go to other owners, when the stream breaks,
// the receiver replies with an error (it refused the hand-over or left before running it), the receiver is removed
// (closeForwardStream) or its lease in the lock table ended (watchLease). Nothing else times out: work waiting long for
// its lock is no failure. At most WithMaxPendingHandOvers hand-overs are kept, sending more waits for a free slot.

const (
	// maxForwardBatch limits the calls or replies sent in one message
	maxForwardBatch = 256
	// defaultMaxPendingHandOvers is the default limit of the hand-overs a node keeps until they ran
	defaultMaxPendingHandOvers = 10000
)

type (
	forwardStream struct {
		stream service.Node_ForwardEventsClient
		cancel context.CancelFunc
		// sendC queues the calls and cancellations for the sending goroutine
		sendC  chan *service.ForwardCalls
		nextId atomic.Uint64

		// mu guards pending, handOvers and err. doneC is closed once the stream broke, err holds why.
		mu      sync.Mutex
		pending map[uint64]chan *service.ForwardReply
		// handOvers holds the calls nobody waits for (handOver) until their work ran, with the func their outcome goes
		// to: nil once the receiver ran the work, else the failure
		handOvers map[uint64]func(error)
		err       error
		doneC     chan struct{}
	}
	// forwardStreams holds the streams to the other nodes by node id
	forwardStreams struct {
		mu      sync.Mutex
		streams map[uint64]*forwardStream
		// slots limits the hand-overs this node keeps (WithMaxPendingHandOvers): a hand-over holds a slot from sending
		// until its work ran or it failed. Nil means no limit.
		slots chan struct{}
		// handOverSeq numbers the hand-overs of this node. It starts at the start time, so the ids of a restarted node
		// don't collide with the ones of its previous life a receiver may still know.
		handOverSeq atomic.Uint64
	}
)

// forwardStreamTo returns the stream to the node, opening a new one if there is none or it broke.
func (n *Node[T, K]) forwardStreamTo(nodeId uint64) (*forwardStream, error) {
	n.streams.mu.Lock()
	defer n.streams.mu.Unlock()

	if s, ok := n.streams.streams[nodeId]; ok && s.alive() {
		return s, nil
	}
	if n.streams.streams == nil {
		return nil, status.Error(codes.Unavailable, "node stopped")
	}
	client := n.getPeerClient(nodeId)
	if client == nil {
		return nil, status.Errorf(codes.Unavailable, "node %d not found in peers", nodeId)
	}
	ctx, cancel := context.WithCancel(n.ctx)
	st, err := client.ForwardEvents(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	s := &forwardStream{
		stream:    st,
		cancel:    cancel,
		sendC:     make(chan *service.ForwardCalls, maxForwardBatch),
		pending:   make(map[uint64]chan *service.ForwardReply),
		handOvers: make(map[uint64]func(error)),
		doneC:     make(chan struct{}),
	}
	n.streams.streams[nodeId] = s
	go s.sendLoop()
	go s.recvLoop()
	if !n.skipRaft {
		go n.watchLease(nodeId, s)
	}
	return s, nil
}

// closeForwardStream closes the stream to a node removed from the cluster.
func (n *Node[T, K]) closeForwardStream(nodeId uint64) {
	n.streams.mu.Lock()
	s := n.streams.streams[nodeId]
	delete(n.streams.streams, nodeId)
	n.streams.mu.Unlock()
	if s != nil {
		s.fail(status.Errorf(codes.Unavailable, "node %d was removed from the cluster", nodeId))
	}
}

// closeForwardStreams closes all streams once the node stops, no new ones are opened afterwards.
func (n *Node[T, K]) closeForwardStreams() {
	n.streams.mu.Lock()
	streams := n.streams.streams
	n.streams.streams = nil
	n.streams.mu.Unlock()
	for _, s := range streams {
		s.fail(status.Error(codes.Unavailable, "node stopped"))
	}
}

// newHandOverId returns a new id for the hand-overs of one forward: "<node id>-<seq>".
func (n *Node[T, K]) newHandOverId() string {
	return fmt.Sprintf("%d-%d", n.id, n.streams.handOverSeq.Add(1))
}

// takeHandOverSlot waits for a free slot of the limit of kept hand-overs, or until ctx is done.
func (n *Node[T, K]) takeHandOverSlot(ctx context.Context) error {
	if n.streams.slots == nil {
		return nil
	}
	select {
	case n.streams.slots <- struct{}{}:
		return nil
	default:
	}
	debugf(n.logger, "%d hand-overs wait for their receivers, waiting for one to finish", cap(n.streams.slots))
	select {
	case n.streams.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return status.Errorf(status.FromContextError(ctx.Err()).Code(),
			"waiting for one of %d hand-overs to finish (WithMaxPendingHandOvers): %v", cap(n.streams.slots), ctx.Err())
	}
}

// freeHandOverSlot frees the slot of a hand-over which ran, failed or wasn't sent.
func (n *Node[T, K]) freeHandOverSlot() {
	if n.streams.slots != nil {
		<-n.streams.slots
	}
}

// pendingHandOvers returns the number of hand-overs this node keeps until their receivers ran them.
func (n *Node[T, K]) pendingHandOvers() int {
	return len(n.streams.slots)
}

// watchLease fails the hand-overs kept on the stream to the node whenever the node's lease in the lock table ended,
// until the stream broke. The stream stays open: waited calls run on a lent lock and don't need the receiver's lease.
func (n *Node[T, K]) watchLease(nodeId uint64, s *forwardStream) {
	node := strconv.FormatUint(nodeId, 10)
	for {
		select {
		case <-n.locks.expiry(node):
		case <-s.doneC:
			return
		}
		if n.receiverLeaseEnded(node) {
			err := status.Errorf(codes.Unavailable, "the lease of node %s ended", node)
			if count := s.failHandOvers(err); count > 0 {
				n.logger.Warn(fmt.Sprintf("the lease of node %s ended, handing its %d hand-overs to other owners", node, count))
			}
		}
		// the expiry watcher closes the channel of a node without a live lease on every check, look again after that
		select {
		case <-time.After(proposeRetryInterval):
		case <-s.doneC:
			return
		}
	}
}

// receiverLeaseEnded reports whether the lease of the node, a receiver of hand-overs, ended. Not while the lease of
// this node ended too: then this node's view of the lock table is stale (it is cut off), not the receiver dead.
func (n *Node[T, K]) receiverLeaseEnded(node string) bool {
	if n.skipRaft {
		return false
	}
	now := time.Now()
	return n.locks.leaseEnded(node, now) && !n.locks.leaseEnded(n.Id(), now)
}

// leaseEnded reports whether the node has a lease in the table which ended before the given time. A node the table
// doesn't know (not yet, or removed: removals close the streams to it themselves) has none.
func (lt *lockTable) leaseEnded(node string, at time.Time) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	l, ok := lt.nodes[node]
	return ok && l.lease > 0 && l.last.Add(l.lease).Before(at)
}

func (s *forwardStream) alive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err == nil
}

// call sends the request over the stream and waits for its reply, or until ctx is done.
func (s *forwardStream) call(ctx context.Context, req *service.ForwardEventRequest) (*service.ForwardEventResponse, error) {
	id := s.nextId.Add(1)
	replyC := make(chan *service.ForwardReply, 1)
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return nil, s.err
	}
	s.pending[id] = replyC
	s.mu.Unlock()

	calls := &service.ForwardCalls{Calls: []*service.ForwardCall{{Id: id, Request: req}}}
	// try without waiting first: a select with several cases locks all their channels, doneC is shared by every call
	select {
	case s.sendC <- calls:
	default:
		select {
		case s.sendC <- calls:
		case <-ctx.Done():
			s.forget(id)
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-s.doneC:
		}
	}

	select {
	case r := <-replyC:
		if r.GetCode() != uint32(codes.OK) {
			return nil, status.Error(codes.Code(r.GetCode()), r.GetMessage())
		}
		return r.GetResponse(), nil
	case <-ctx.Done():
		s.forget(id)
		// the receiver cancels the call's context, unless the stream broke meanwhile
		select {
		case s.sendC <- &service.ForwardCalls{Cancel: []uint64{id}}:
		default:
		}
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// handOver sends the call without waiting for its replies and keeps it until the receiver ran its work. done gets the
// outcome: nil once the work ran (on the goroutine dispatching the replies: it must not block), the failure on another
// goroutine: an error reply (refused, or failed back by a leaving receiver), the stream breaking, or the receiver's
// lease ending (failHandOvers). If handOver returns an error the call wasn't sent and done is never called.
func (s *forwardStream) handOver(ctx context.Context, req *service.ForwardEventRequest, done func(error)) error {
	id := s.nextId.Add(1)
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return s.err
	}
	s.handOvers[id] = done
	s.mu.Unlock()

	calls := &service.ForwardCalls{Calls: []*service.ForwardCall{{Id: id, Request: req}}}
	select {
	case s.sendC <- calls:
	default:
		select {
		case s.sendC <- calls:
		case <-ctx.Done():
			s.mu.Lock()
			_, kept := s.handOvers[id]
			delete(s.handOvers, id)
			s.mu.Unlock()
			if !kept {
				// the stream broke or the lease ended meanwhile: done got the failure
				return nil
			}
			return status.FromContextError(ctx.Err()).Err()
		case <-s.doneC:
			// the stream broke: fail handed the call to done
		}
	}
	return nil
}

func (s *forwardStream) forget(id uint64) {
	s.mu.Lock()
	delete(s.pending, id)
	s.mu.Unlock()
}

// failHandOvers fails the kept hand-overs with the error, eg. once the lease of the receiver ended. The stream stays
// open, later replies to them are ignored. It returns the number of failed hand-overs.
func (s *forwardStream) failHandOvers(err error) int {
	s.mu.Lock()
	if s.err != nil || len(s.handOvers) == 0 {
		s.mu.Unlock()
		return 0
	}
	handOvers := s.handOvers
	s.handOvers = make(map[uint64]func(error))
	s.mu.Unlock()

	for _, done := range handOvers {
		go done(err)
	}
	return len(handOvers)
}

// fail marks the stream broken and fails its pending calls and kept hand-overs with Unavailable.
func (s *forwardStream) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return
	}
	if status.Code(err) != codes.Unavailable {
		err = status.Errorf(codes.Unavailable, "forward stream broke: %v", err)
	}
	s.err = err
	close(s.doneC)
	st := status.Convert(err)
	for id, replyC := range s.pending {
		replyC <- &service.ForwardReply{Id: id, Code: uint32(st.Code()), Message: st.Message()}
	}
	s.pending = nil
	// the receiver may still run a hand-over whose executed reply is lost: sending it to another owner may run it
	// twice, as with a waited call
	for _, done := range s.handOvers {
		go done(err)
	}
	s.handOvers = nil
	s.cancel()
}

// sendLoop sends the queued calls and cancellations, all waiting at that moment in one message.
func (s *forwardStream) sendLoop() {
	for {
		var batch *service.ForwardCalls
		select {
		case batch = <-s.sendC:
		case <-s.doneC:
			return
		}
	more:
		for len(batch.Calls)+len(batch.Cancel) < maxForwardBatch {
			select {
			case next := <-s.sendC:
				batch.Calls = append(batch.Calls, next.Calls...)
				batch.Cancel = append(batch.Cancel, next.Cancel...)
			default:
				break more
			}
		}
		if err := s.stream.Send(batch); err != nil {
			s.fail(err)
			return
		}
	}
}

// recvLoop hands the replies to the waiting calls and ends kept hand-overs until the stream breaks.
func (s *forwardStream) recvLoop() {
	for {
		replies, err := s.stream.Recv()
		if err != nil {
			s.fail(err)
			return
		}
		s.mu.Lock()
		for _, r := range replies.GetReplies() {
			if replyC, ok := s.pending[r.GetId()]; ok {
				delete(s.pending, r.GetId())
				replyC <- r
			} else if done, ok := s.handOvers[r.GetId()]; ok {
				switch {
				case r.GetCode() != uint32(codes.OK):
					delete(s.handOvers, r.GetId())
					go done(status.Error(codes.Code(r.GetCode()), r.GetMessage()))
				case r.GetExecuted():
					delete(s.handOvers, r.GetId())
					done(nil)
				}
				// accepted: kept until its work ran
			}
		}
		s.mu.Unlock()
	}
}

// ForwardEvents receives forwarded events of another node over a stream: every waited call runs in its own goroutine
// like a ForwardEvent rpc, the replies go back in batches. Hand-overs (no wait) are queued right here, so they keep
// the order of the stream: the sender doesn't wait for their accept. Their second reply (executed, or the error of a
// hand-over failed back) comes from the goroutine which ran them. Calls still running when the stream ends are
// cancelled; hand-overs stay queued, their replies are dropped.
func (n *Node[T, K]) ForwardEvents(stream service.Node_ForwardEventsServer) error {
	ctx := stream.Context()
	replyC := make(chan *service.ForwardReply, maxForwardBatch)
	endC := make(chan struct{})
	sentC := make(chan struct{})
	go func() {
		defer close(sentC)
		sendReplies(stream, replyC, endC)
	}()
	// replies of hand-overs may come after the stream ended, so replyC is never closed
	reply := func(r *service.ForwardReply) {
		select {
		case replyC <- r:
		case <-ctx.Done():
		}
	}
	replyOf := func(id uint64, resp *service.ForwardEventResponse, err error) *service.ForwardReply {
		r := &service.ForwardReply{Id: id, Response: resp}
		if err != nil {
			st := status.Convert(err)
			r.Code, r.Message = uint32(st.Code()), st.Message()
		}
		return r
	}

	var (
		mu      sync.Mutex
		cancels = make(map[uint64]context.CancelFunc)
		wg      sync.WaitGroup
	)
	for {
		calls, err := stream.Recv()
		if err != nil {
			break
		}
		mu.Lock()
		for _, id := range calls.GetCancel() {
			if cancel, ok := cancels[id]; ok {
				cancel()
			}
		}
		mu.Unlock()

		for _, call := range calls.GetCalls() {
			if !call.GetRequest().GetWait() {
				id := call.GetId()
				resp, executed, err := n.forwardEvent(ctx, call.GetRequest(), func(err error) {
					r := replyOf(id, nil, err)
					r.Executed = err == nil
					reply(r)
				})
				r := replyOf(id, resp, err)
				r.Executed = executed
				reply(r)
				continue
			}

			callCtx, cancel := context.WithCancel(ctx)
			mu.Lock()
			cancels[call.GetId()] = cancel
			mu.Unlock()

			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := n.ForwardEvent(callCtx, call.GetRequest())
				mu.Lock()
				delete(cancels, call.GetId())
				mu.Unlock()
				cancel()
				reply(replyOf(call.GetId(), resp, err))
			}()
		}
	}

	// the sender closed the stream or it broke: the calls still running are cancelled
	mu.Lock()
	for _, cancel := range cancels {
		cancel()
	}
	mu.Unlock()
	wg.Wait()
	close(endC)
	<-sentC
	return nil
}

// sendReplies sends the replies, all waiting at that moment in one message, until endC is closed; then the ones still
// queued. After a failed send it only drains replyC.
func sendReplies(stream service.Node_ForwardEventsServer, replyC <-chan *service.ForwardReply, endC <-chan struct{}) {
	broken := false
	for {
		var r *service.ForwardReply
		select {
		case r = <-replyC:
		case <-endC:
			select {
			case r = <-replyC:
			default:
				return
			}
		}
		if broken {
			continue
		}
		batch := &service.ForwardReplies{Replies: []*service.ForwardReply{r}}
	more:
		for len(batch.Replies) < maxForwardBatch {
			select {
			case next := <-replyC:
				batch.Replies = append(batch.Replies, next)
			default:
				break more
			}
		}
		if err := stream.Send(batch); err != nil {
			broken = true
		}
	}
}
