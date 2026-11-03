package asm

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"rumue.io/asm/service"
)

// Raft messages are sent to each peer by its own goroutine, so a slow or unreachable peer doesn't
// stall the ready loop or the other peers. The queue of a peer keeps the order of its messages.
// They go over one SyncStream per peer: the sender doesn't wait for the acks, except for batches with a snapshot,
// whose outcome raft has to know. A failed send reopens the stream and is retried a few times, then the peer is
// reported unreachable and the messages are dropped. Raft sends them again later (probing, heartbeats).

const (
	peerQueueSize = 128
	sendRetries   = 3
	// sendRetryBackoff is multiplied by the attempt
	sendRetryBackoff = 100 * time.Millisecond
	// maxSyncBytes limits merging queued batches into one Sync request
	maxSyncBytes = 2 * 1024 * 1024
	// maxUnacked limits the batches sent on a stream and not acked yet
	maxUnacked = 256
)

type (
	peerSender struct {
		client service.NodeClient
		queueC chan peerBatch
		stopC  chan struct{}
		// stream is the stream to the peer, owned by the sender goroutine, nil until opened or after it broke
		stream *syncStream
	}
	// syncStream is a SyncStream to a peer. unacked holds one entry per batch sent and not acked yet, in order:
	// the channel of the sender waiting for the ack, or nil.
	syncStream struct {
		stream  service.Node_SyncStreamClient
		cancel  context.CancelFunc
		unacked chan chan error
		// doneC is closed once the stream broke, err holds why
		doneC chan struct{}
		err   error
		// deniedF is called if the peer rejected this node as removed
		deniedF func()
	}
	peerBatch struct {
		msgs     [][]byte
		size     int
		snapshot bool
	}
)

// sendMessages hands the messages of a Ready to the senders of the peers.
func (n *Node[T, K]) sendMessages(messages []raftpb.Message) {
	batches := make(map[uint64]*peerBatch)
	for _, msg := range messages {
		if msg.To == uint64(n.id) {
			continue
		}

		data, err := msg.Marshal()
		if err != nil {
			n.logger.Error(fmt.Sprintf("failed to marshal message: %v", err))
			continue
		}

		b := batches[msg.To]
		if b == nil {
			b = &peerBatch{}
			batches[msg.To] = b
		}
		b.msgs = append(b.msgs, data)
		b.size += len(data)
		b.snapshot = b.snapshot || msg.Type == raftpb.MsgSnap
	}

	for id, b := range batches {
		s := n.peerSender(id)
		if s == nil {
			n.reportSendFailure(id, b.snapshot)
			continue
		}

		select {
		case s.queueC <- *b:
		default:
			debugf(n.logger, "send queue of node %d is full, dropping messages", id)
			n.reportSendFailure(id, b.snapshot)
		}
	}
}

func (n *Node[T, K]) reportSendFailure(id uint64, snapshot bool) {
	n.metrics.sendFailures.Add(1)
	n.node.ReportUnreachable(id)
	if snapshot {
		n.node.ReportSnapshot(id, raft.SnapshotFailure)
	}
}

// peerSender returns the sender of the peer and starts it on first use.
// It returns nil if the peer is unknown or the node stopped.
func (n *Node[T, K]) peerSender(id uint64) *peerSender {
	n.sendersMu.Lock()
	defer n.sendersMu.Unlock()

	if s, ok := n.senders[id]; ok {
		return s
	}
	// stopSenders ran already
	if n.senders == nil {
		return nil
	}

	client := n.getPeerClient(id)
	if client == nil {
		return nil
	}

	s := &peerSender{
		client: client,
		queueC: make(chan peerBatch, peerQueueSize),
		stopC:  make(chan struct{}),
	}
	n.senders[id] = s
	go n.runPeerSender(id, s)

	return s
}

func (n *Node[T, K]) runPeerSender(id uint64, s *peerSender) {
	defer func() {
		if s.stream != nil {
			s.stream.cancel()
		}
	}()
	for {
		select {
		case <-s.stopC:
			return
		case b := <-s.queueC:
			// merge the batches queued meanwhile into one request, keeping their order
		merge:
			for b.size < maxSyncBytes {
				select {
				case next := <-s.queueC:
					b.msgs = append(b.msgs, next.msgs...)
					b.size += next.size
					b.snapshot = b.snapshot || next.snapshot
				default:
					break merge
				}
			}

			n.sendBatch(id, s, b)
		}
	}
}

// sendBatch sends the messages to the peer over its stream, reopening it and retrying with backoff.
// A batch with a snapshot waits for its ack.
func (n *Node[T, K]) sendBatch(id uint64, s *peerSender, b peerBatch) {
	var err error
	for attempt := range sendRetries + 1 {
		if attempt > 0 {
			select {
			case <-s.stopC:
				return
			case <-time.After(sendRetryBackoff * time.Duration(attempt)):
			}
		}

		if err = n.sendOnStream(s, b); err == nil {
			n.metrics.messagesSent.Add(uint64(len(b.msgs)))
			if b.snapshot {
				n.node.ReportSnapshot(id, raft.SnapshotFinish)
			}
			return
		}
		if s.stream != nil {
			s.stream.cancel()
			s.stream = nil
		}
		if status.Code(err) == codes.PermissionDenied {
			n.logger.Warn(fmt.Sprintf("node %d rejected this node as removed from the cluster, stopping", id))
			n.requestStop()
			return
		}
		debugf(n.logger, "failed to sync with node %d (attempt %d): %v", id, attempt+1, err)
	}

	n.reportSendFailure(id, b.snapshot)
}

// sendOnStream sends the batch on the stream to the peer, opening it if needed. A batch with a snapshot waits until
// it is acked; other batches only fail if the stream broke before.
func (n *Node[T, K]) sendOnStream(s *peerSender, b peerBatch) error {
	if s.stream != nil {
		select {
		case <-s.stream.doneC:
			err := s.stream.err
			s.stream.cancel()
			s.stream = nil
			// batches don't wait for their acks: a peer rejecting this node as removed broke the stream after an
			// earlier batch, sendBatch stops the node instead of reopening it
			if status.Code(err) == codes.PermissionDenied {
				return err
			}
		default:
		}
	}
	if s.stream == nil {
		st, err := openSyncStream(s.client, func() {
			n.logger.Warn("a peer rejected this node as removed from the cluster, stopping")
			n.requestStop()
		})
		if err != nil {
			return err
		}
		s.stream = st
	}
	st := s.stream

	var ackC chan error
	if b.snapshot {
		ackC = make(chan error, 1)
	}
	select {
	case st.unacked <- ackC:
	case <-st.doneC:
		return st.err
	case <-s.stopC:
		return ErrNodeStopped
	}
	if err := st.stream.Send(&service.SyncRequest{Messages: b.msgs}); err != nil {
		// the reading goroutine gets the cause
		select {
		case <-st.doneC:
			return st.err
		case <-time.After(sendTimeout):
			return err
		}
	}
	if ackC == nil {
		return nil
	}
	select {
	case err := <-ackC:
		return err
	case <-time.After(sendTimeout):
		return status.Error(codes.DeadlineExceeded, "no ack for the snapshot")
	case <-s.stopC:
		return ErrNodeStopped
	}
}

// openSyncStream opens a SyncStream and starts reading its acks.
// deniedF is called when the peer rejects this node as removed (PermissionDenied): batches don't wait for their
// acks, so the rejection would only be seen with the next batch, an election timeout later.
func openSyncStream(client service.NodeClient, deniedF func()) (*syncStream, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.SyncStream(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	st := &syncStream{stream: stream, cancel: cancel, unacked: make(chan chan error, maxUnacked), doneC: make(chan struct{}), deniedF: deniedF}
	go st.readAcks()
	return st, nil
}

// readAcks hands every ack to the sender waiting for it, until the stream breaks. Then it fails the waiting senders.
func (st *syncStream) readAcks() {
	for {
		_, err := st.stream.Recv()
		if err != nil {
			st.err = err
			close(st.doneC)
			if status.Code(err) == codes.PermissionDenied && st.deniedF != nil {
				st.deniedF()
			}
			for {
				select {
				case ackC := <-st.unacked:
					if ackC != nil {
						ackC <- err
					}
				default:
					return
				}
			}
		}
		if ackC := <-st.unacked; ackC != nil {
			ackC <- nil
		}
	}
}

// stopSender stops the sender of a removed peer.
func (n *Node[T, K]) stopSender(id uint64) {
	n.sendersMu.Lock()
	defer n.sendersMu.Unlock()

	if s, ok := n.senders[id]; ok {
		close(s.stopC)
		delete(n.senders, id)
	}
}

// stopSenders stops all senders once the node stops, no new ones are started afterwards.
func (n *Node[T, K]) stopSenders() {
	n.sendersMu.Lock()
	defer n.sendersMu.Unlock()

	for _, s := range n.senders {
		close(s.stopC)
	}
	n.senders = nil
}
