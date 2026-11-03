package asm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"rumue.io/asm/service"
)

// Proposals are batched: one proposer goroutine per node takes all log entries waiting at that moment and proposes
// them together, as one raft entry (NodeLogs). A follower sends the batch to the leader in one Propose rpc.
// While a batch is in flight the next one fills up, so batches grow with the load without delaying single proposals.
// Every raft entry costs every node, so a further batch is only sent while others are in flight once a whole batch
// (maxProposalBatch) waits, up to maxProposalsInFlight: with smaller batches in parallel the throughput dropped
// (start+close with WAL ~95k/s with one, ~81k/s with two, ~65k/s with four in flight).
// Batches in flight together may reach the leader in any order: only independent proposals are in flight together,
// a caller proposes what depends on an entry after propose returned, so after the leader appended it.
// The proposer answers every proposal it queued, also when the node stops (stopProposer): a proposing goroutine only
// waits on its own channel, not on the node's stop channels, which all of them would share.
//
// Applying is idempotent (tokens), but every duplicate costs every node. So a proposal is only made again when
// proposing failed, the term changed since (the entry may have been dropped with the old leader), or it wasn't
// applied for proposeTimeout (see proposalState).

const (
	// maxProposalBatch limits the log entries proposed together
	maxProposalBatch = 1024
	// maxProposalsInFlight limits the batches proposed at the same time (parallel Propose rpcs of a follower)
	maxProposalsInFlight = 4
	// proposeTimeout is how long a proposal the leader accepted may take until it is applied before it is
	// proposed again, a safety net: accepted entries of a term are committed unless the leader changes
	proposeTimeout = 10 * time.Second
	// maxUncommittedSize limits the uncommitted entries the leader accepts, further proposals fail and are retried
	maxUncommittedSize = 64 << 20
)

type (
	proposal struct {
		log  *service.NodeLog
		errC chan error
	}
	// proposalState tracks the last proposal of an entry, see needsPropose
	proposalState struct {
		at   time.Time
		term uint64
		err  error
	}
)

// propose proposes the log entry and returns once the leader accepted it, together with the entries
// waiting at that moment. It doesn't wait until the entry is committed.
func (n *Node[T, K]) propose(l *service.NodeLog) error {
	if n.skipRaft {
		return nil
	}

	errC := make(chan error, 1)
	if err := n.queueProposals(proposal{log: l, errC: errC}); err != nil {
		return err
	}
	return <-errC
}

// proposeMany proposes the log entries together and returns the first error of their batches.
func (n *Node[T, K]) proposeMany(logs []*service.NodeLog) error {
	ps := make([]proposal, len(logs))
	for i, l := range logs {
		ps[i] = proposal{log: l, errC: make(chan error, 1)}
	}
	if err := n.queueProposals(ps...); err != nil {
		return err
	}
	var first error
	for _, p := range ps {
		if err := <-p.errC; err != nil && first == nil {
			first = err
		}
	}
	return first
}

// queueProposals queues the proposals for the proposer, or returns why it stopped.
func (n *Node[T, K]) queueProposals(ps ...proposal) error {
	n.proposeMu.Lock()
	if n.proposeStopped {
		n.proposeMu.Unlock()
		return n.stopError()
	}
	n.proposals = append(n.proposals, ps...)
	n.proposeMu.Unlock()
	n.wakeProposer()
	return nil
}

func (n *Node[T, K]) wakeProposer() {
	select {
	case n.proposeWakeC <- struct{}{}:
	default:
	}
}

// queuedProposals returns the number of proposals waiting for a batch.
func (n *Node[T, K]) queuedProposals() int {
	n.proposeMu.Lock()
	defer n.proposeMu.Unlock()
	return len(n.proposals)
}

// takeProposals moves up to maxProposalBatch queued proposals into batch and wakes the proposer again if more wait.
func (n *Node[T, K]) takeProposals(batch []proposal) []proposal {
	n.proposeMu.Lock()
	defer n.proposeMu.Unlock()
	k := min(len(n.proposals), maxProposalBatch)
	batch = append(batch, n.proposals[:k]...)
	rest := copy(n.proposals, n.proposals[k:])
	clear(n.proposals[rest:])
	n.proposals = n.proposals[:rest]
	if rest > 0 {
		n.wakeProposer()
	}
	return batch
}

// stopProposer answers the queued proposals with the reason the node stopped and refuses later ones.
func (n *Node[T, K]) stopProposer() {
	err := n.stopError()
	n.proposeMu.Lock()
	n.proposeStopped = true
	queued := n.proposals
	n.proposals = nil
	n.proposeMu.Unlock()
	for _, p := range queued {
		p.errC <- err
	}
}

// stopError is the failure of a failed node, else ErrNodeStopped.
func (n *Node[T, K]) stopError() error {
	if err := n.failure(); err != nil {
		return err
	}
	return ErrNodeStopped
}

// runProposer proposes the waiting log entries in batches until the node stops.
func (n *Node[T, K]) runProposer() {
	defer n.stopProposer()
	// the batches in flight are answered before the queued ones get the stop error
	var inFlight sync.WaitGroup
	defer inFlight.Wait()

	slots := make(chan struct{}, maxProposalsInFlight)
	for {
		select {
		case <-n.proposeWakeC:
		case <-n.stopC:
			return
		case <-n.failedC:
			return
		}
		// while a batch is in flight the next one fills up, unless a whole batch waits already. An answered batch
		// wakes the proposer again
		if len(slots) > 0 && n.queuedProposals() < maxProposalBatch {
			continue
		}
		select {
		case slots <- struct{}{}:
		default:
			continue
		}
		batch := n.takeProposals(nil)
		if len(batch) == 0 {
			<-slots
			continue
		}

		inFlight.Add(1)
		go func() {
			defer inFlight.Done()
			logs := make([]*service.NodeLog, len(batch))
			for i, p := range batch {
				logs[i] = p.log
			}
			err := n.proposeBatch(logs)
			for _, p := range batch {
				p.errC <- err
			}
			<-slots
			n.wakeProposer()
		}()
	}
}

// proposeBatch proposes the log entries as one raft entry: on the leader directly, on a follower through
// the Propose rpc of the leader.
func (n *Node[T, K]) proposeBatch(logs []*service.NodeLog) error {
	n.metrics.proposals.Add(uint64(len(logs)))
	n.metrics.proposalBatches.Add(1)

	leaderId := n.leader.Load()
	switch leaderId {
	case uint64(n.id):
		return n.proposeLogs(logs)
	case 0:
		// no leader yet, the callers retry
		return errors.New("no leader available")
	}

	leaderClient := n.getPeerClient(leaderId)
	if leaderClient == nil {
		return fmt.Errorf("leader %d not found in peers", leaderId)
	}
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	n.metrics.proposeRPCs.Add(1)
	resp, err := leaderClient.Propose(ctx, &service.ProposeRequest{Logs: logs})
	if status.Code(err) == codes.PermissionDenied {
		n.logger.Warn("the leader rejected this node as removed from the cluster, stopping")
		n.requestStop()
	}
	if err != nil {
		return fmt.Errorf("proposing to leader %d: %w", leaderId, err)
	}
	if !resp.GetAcked() {
		return errors.New("the leader did not ack the proposal")
	}
	return nil
}

// proposeLogs proposes the log entries as one raft entry on this node, the leader.
func (n *Node[T, K]) proposeLogs(logs []*service.NodeLog) error {
	data, err := (&service.NodeLogs{Logs: logs}).MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling proposal: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	if err := n.node.Propose(ctx, data); err != nil {
		return fmt.Errorf("proposing: %w", err)
	}
	return nil
}

// needsPropose reports whether an entry has to be proposed (again): it never was, proposing failed and the retry
// interval passed, the term changed since, or it wasn't applied for proposeTimeout.
func (n *Node[T, K]) needsPropose(p *proposalState) bool {
	switch {
	case p.at.IsZero():
		return true
	case p.err != nil:
		return time.Since(p.at) >= proposeRetryInterval
	case n.term.Load() != p.term:
		return true
	default:
		return time.Since(p.at) >= proposeTimeout
	}
}

// proposeTracked proposes the entry and records the proposal in p.
func (n *Node[T, K]) proposeTracked(p *proposalState, l *service.NodeLog) error {
	l.Ts = timestamppb.Now()
	p.at, p.term = time.Now(), n.term.Load()
	p.err = n.propose(l)
	return p.err
}
