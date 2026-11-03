package asm

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"rumue.io/asm/service"
)

// Nodes leave the cluster with Leave, which also runs when the stop channel of the node is closed.
// RemoveNode removes another node, eg. one that crashed. Removals are configuration changes in the
// raft log: every node frees the locks of the removed node, drops it as transition owner and rejects
// its messages. A removed node stops; it can join again with the same id, except with TLS (its certificate is
// rejected then).

const (
	// leaveTimeout bounds leaving the cluster when the stop channel is closed
	leaveTimeout = 30 * time.Second
	// transferTimeout bounds waiting for another node to take over the leadership
	transferTimeout = 3 * time.Second
)

// errNotLeader is returned by the rpcs only the leader handles. Callers retry with the current leader.
var errNotLeader = status.Error(codes.Unavailable, "not the leader")

// leaveOnStop leaves the cluster once the stop channel of the user is closed.
// The node stops even if leaving fails.
func (n *Node[T, K]) leaveOnStop(userStopC <-chan struct{}) {
	select {
	case <-userStopC:
	case <-n.stopC:
		return
	case <-n.failedC:
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), leaveTimeout)
	defer cancel()

	if err := n.Leave(ctx); err != nil {
		n.logger.Warn(fmt.Sprintf("failed to leave the cluster, stopping anyway: %+v", err))
	}
	n.requestStop()
}

// Leave removes the node from the cluster and stops it.
// It closes all containers and waits until they are closed, so their locks are released.
// New containers are rejected with ErrNodeStopped from then on.
// A leader hands its leadership to another node first. Then the leader removes this node,
// and the node stops raft, the gRPC server and its containers. Err receives nothing.
// If ctx ends first, Leave returns its error and the node stays a member, still rejecting new containers.
// Work other nodes handed over is refused from then on, and the queued work which didn't run yet goes back to its
// senders, which hand it to other owners; work running already finishes while the containers close.
func (n *Node[T, K]) Leave(ctx context.Context) error {
	if err := n.failure(); err != nil {
		return err
	}

	n.containerMap.mu.Lock()
	n.leaving.Store(true)
	n.containerMap.mu.Unlock()

	// handed over work waiting for its lock: the drains resume, see the node leaving and give their queues back
	n.dropPending(status.Errorf(codes.Unavailable, "node %s is leaving the cluster", n.Id()), true)

	n.logger.Info("leaving the cluster, closing containers")
	for _, p := range n.closeContainers() {
		select {
		case <-p.done():
		case <-ctx.Done():
			return fmt.Errorf("waiting for containers to close: %w", ctx.Err())
		}
	}

	switch {
	case n.client:
		if err := n.leaveAsClient(ctx); err != nil {
			return err
		}
	case !n.skipRaft:
		if err := n.removeSelf(ctx); err != nil {
			return err
		}
	}

	n.logger.Info("left the cluster, stopping")
	n.requestStop()
	return nil
}

// removeSelf asks the leader to remove this node, handing over the leadership first if this node leads.
func (n *Node[T, K]) removeSelf(ctx context.Context) error {
	for {
		lead := n.node.Status().Lead
		switch {
		case lead == uint64(n.id) && !n.hasOtherPeers():
			// the last node of the cluster has nobody to hand over to
			return nil
		case lead == uint64(n.id):
			if err := n.transferLeadership(ctx); err != nil {
				debugf(n.logger, "transferring the leadership failed, retrying: %v", err)
			}
		case lead != 0:
			err := n.removeAtLeader(ctx, lead, uint64(n.id))
			// with TLS a removed node's rpcs are rejected: the removal applied, its answer was lost
			if err == nil || status.Code(err) == codes.PermissionDenied {
				return nil
			}
			debugf(n.logger, "asking the leader to remove this node failed, retrying: %v", err)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("leaving the cluster: %w", ctx.Err())
		case <-n.failedC:
			return n.failure()
		case <-n.stopC:
			// the removal was applied here before the leader answered
			if n.isRemoved(uint64(n.id)) {
				return nil
			}
			return ErrNodeStopped
		case <-time.After(retryDelay(lead)):
		}
	}
}

// retryDelay waits shortly after a failed attempt, longer while there is no leader.
func retryDelay(lead uint64) time.Duration {
	if lead == 0 {
		return proposeRetryInterval
	}
	return 100 * time.Millisecond
}

// RemoveNode removes another node from the cluster, eg. one that crashed and won't come back.
// The removal goes through the leader, so it works on every node. It returns once the leader applied it.
// A node that is still running stops once it learns about its removal.
// Removing this node is the same as Leave.
func (n *Node[T, K]) RemoveNode(ctx context.Context, id int) error {
	if id == n.id {
		return n.Leave(ctx)
	}
	if n.skipRaft {
		return errors.New("a node without raft has no cluster")
	}
	if err := n.waitReady(ctx); err != nil {
		return err
	}

	for {
		lead := n.leader.Load()
		var err error
		switch lead {
		case 0:
			err = errors.New("no leader available")
		case uint64(n.id):
			err = n.removeAsLeader(ctx, uint64(id))
		default:
			err = n.removeAtLeader(ctx, lead, uint64(id))
		}
		if err == nil {
			return nil
		}
		debugf(n.logger, "removing node %d failed, retrying: %v", id, err)

		select {
		case <-ctx.Done():
			return fmt.Errorf("removing node %d: %w (last error: %w)", id, ctx.Err(), err)
		case <-n.failedC:
			return n.failure()
		case <-n.stopC:
			return ErrNodeStopped
		case <-time.After(retryDelay(lead)):
		}
	}
}

// removeAtLeader asks the leader to remove the node.
func (n *Node[T, K]) removeAtLeader(ctx context.Context, lead, id uint64) error {
	client := n.getPeerClient(lead)
	if client == nil {
		return fmt.Errorf("leader %d not found in peers", lead)
	}

	_, err := client.RemovePeer(ctx, &service.RemovePeerRequest{NodeId: id})
	return err
}

// RemovePeer handles the removal of a node on the leader.
// The leader can't remove itself: it hands over the leadership and returns errNotLeader,
// so the caller retries with the new leader.
func (n *Node[T, K]) RemovePeer(ctx context.Context, req *service.RemovePeerRequest) (*service.RemovePeerResponse, error) {
	// a node may remove itself (leave), removing other nodes needs the admin role
	if id, ok := caller(ctx); ok && !id.has(RoleAdmin) {
		if err := ownName(ctx, strconv.FormatUint(req.GetNodeId(), 10)); err != nil {
			return nil, err
		}
	}
	if err := n.removeAsLeader(ctx, req.GetNodeId()); err != nil {
		return nil, err
	}
	return &service.RemovePeerResponse{}, nil
}

// removeAsLeader removes the node from the cluster. It must run on the leader and waits until the removal is applied.
func (n *Node[T, K]) removeAsLeader(ctx context.Context, id uint64) error {
	if n.node == nil || n.node.Status().Lead != uint64(n.id) {
		return errNotLeader
	}
	if n.hasPeer(id) && !n.isMember(id) {
		return n.removeClientAsLeader(ctx, id)
	}
	if id == uint64(n.id) {
		if err := n.transferLeadership(ctx); err != nil {
			return status.Errorf(codes.Unavailable, "handing over the leadership: %v", err)
		}
		return errNotLeader
	}

	// raft drops a configuration change proposed while another one is pending,
	// so removals are serialized with joins and each waits until applied, proposing again if dropped
	n.joinMu.Lock()
	defer n.joinMu.Unlock()

	deadline := time.Now().Add(joinTimeout)
	for n.hasPeer(id) {
		if time.Now().After(deadline) {
			return status.Errorf(codes.DeadlineExceeded, "node %d was not removed from the cluster in time", id)
		}
		if n.node.Status().Lead != uint64(n.id) {
			return errNotLeader
		}

		n.peersMu.Lock()
		confChangeId := n.confChangeCount
		n.confChangeCount++
		n.peersMu.Unlock()

		err := n.node.ProposeConfChange(ctx, raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: id,
			ID:     confChangeId,
		})
		if err != nil {
			return err
		}

		if err := n.waitForPeer(ctx, id, false, joinRetryInterval); err != nil {
			return err
		}
	}

	n.logger.Info(fmt.Sprintf("removed node %d from the cluster", id))
	return nil
}

// removeClientAsLeader removes a client node: it proposes CLIENT_REMOVE until the client is gone.
func (n *Node[T, K]) removeClientAsLeader(ctx context.Context, id uint64) error {
	l := &service.NodeLog{
		NodeId: n.Id(),
		Type:   service.NodeEventType_CLIENT_REMOVE,
		Body:   &service.NodeLog_Client{Client: &service.ClientNode{Id: id}},
	}
	var prop proposalState
	deadline := time.Now().Add(joinTimeout)
	for n.hasPeer(id) {
		if time.Now().After(deadline) {
			return status.Errorf(codes.DeadlineExceeded, "client %d was not removed from the cluster in time", id)
		}
		if n.needsPropose(&prop) {
			if err := n.proposeTracked(&prop, l); err != nil {
				debugf(n.logger, "proposing the removal of client %d failed, retrying: %v", id, err)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	n.logger.Info(fmt.Sprintf("removed client %d from the cluster", id))
	return nil
}

// transferLeadership hands the leadership of this node to the most up to date peer
// and waits until another node leads.
func (n *Node[T, K]) transferLeadership(ctx context.Context) error {
	st := n.node.Status()
	if st.Lead != uint64(n.id) {
		return nil
	}

	var transferee, match uint64
	for id, pr := range st.Progress {
		if id != uint64(n.id) && (transferee == 0 || pr.Match > match) {
			transferee, match = id, pr.Match
		}
	}
	if transferee == 0 {
		return errors.New("no other node to hand over the leadership to")
	}

	n.logger.Info(fmt.Sprintf("handing over the leadership to node %d", transferee))
	n.node.TransferLeadership(ctx, uint64(n.id), transferee)

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(transferTimeout)
	for {
		if lead := n.node.Status().Lead; lead != 0 && lead != uint64(n.id) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("node %d did not take over the leadership in time", transferee)
		case <-ticker.C:
		}
	}
}

// hasOtherPeers reports whether the cluster has nodes besides this one.
func (n *Node[T, K]) hasOtherPeers() bool {
	n.peersMu.RLock()
	defer n.peersMu.RUnlock()

	for id := range n.peers {
		if id != uint64(n.id) {
			return true
		}
	}
	return false
}

// nodeIdOf parses the id of a node as used in the raft log.
func nodeIdOf(id string) (uint64, bool) {
	v, err := strconv.ParseUint(id, 10, 64)
	return v, err == nil
}
