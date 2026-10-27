package asm

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"rumue.io/asm/service"
)

// Client nodes (WithClientMode) don't run raft. A client attaches to a raft member (Attach), which keeps it up to
// date: leader and term, the committed bitmap, the nodes of the cluster, their liveness, and the locks the client
// holds, waits for, calls into or watches, plus its refused acquires. The client keeps these locks in its own lock
// table, so the lock code runs on it unchanged; it proposes through the leader like a follower.
// Clients are entries in the log (CLIENT_ADD, CLIENT_REMOVE), so every node knows them as peers.

const (
	// clientUpdateBuffer limits the updates waiting for a slow client; a client falling further behind is detached
	// and attaches again, getting the full state
	clientUpdateBuffer = 4096
	// livenessInterval is how often a member sends its clients the liveness of the nodes
	livenessInterval = 500 * time.Millisecond
)

type (
	clientSessions struct {
		mu       sync.Mutex
		sessions map[uint64]*clientSession
	}
	// clientSession is a client attached to this member
	clientSession struct {
		id      uint64
		nodeId  string
		updateC chan *service.AttachUpdate
		closeC  chan struct{}
		once    sync.Once

		// mu guards watched, the container ids whose locks the client watches with a count
		mu      sync.Mutex
		watched map[string]int
	}
)

func (s *clientSession) close() {
	s.once.Do(func() { close(s.closeC) })
}

// send queues an update for the client; a client which can't keep up is detached.
func (s *clientSession) send(u *service.AttachUpdate) {
	select {
	case s.updateC <- u:
	case <-s.closeC:
	default:
		s.close()
	}
}

func (s *clientSession) watches(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watched[id] > 0
}

// involves reports whether the lock concerns the client: it holds it, waits for it or calls into its container.
func (s *clientSession) involves(l containerLock) bool {
	if l.holder == s.nodeId {
		return true
	}
	for _, w := range l.queue {
		if w.node == s.nodeId {
			return true
		}
	}
	for _, c := range l.calls {
		if tokenNode(c.token) == s.nodeId {
			return true
		}
	}
	return false
}

// tokenNode returns the node of a lock token ("node-seq").
func tokenNode(token string) string {
	node, _, _ := strings.Cut(token, "-")
	return node
}

// Attach serves a client node: it registers the client in the log, sends the state and then the changes.
func (n *Node[T, K]) Attach(stream service.Node_AttachServer) error {
	if n.client || n.skipRaft {
		return status.Error(codes.FailedPrecondition, "only raft members serve clients")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	id := first.GetClientId()
	if err := ownName(stream.Context(), strconv.FormatUint(id, 10)); err != nil {
		return err
	}
	if n.isRemoved(id) {
		return status.Errorf(codes.PermissionDenied, "node %d was removed from the cluster", id)
	}
	if err := n.addClient(stream.Context(), id, first.GetAddr()); err != nil {
		return err
	}

	s := &clientSession{
		id:      id,
		nodeId:  strconv.FormatUint(id, 10),
		updateC: make(chan *service.AttachUpdate, clientUpdateBuffer),
		closeC:  make(chan struct{}),
		watched: make(map[string]int),
	}
	n.clients.mu.Lock()
	if old, ok := n.clients.sessions[id]; ok {
		old.close()
	}
	n.clients.sessions[id] = s
	n.clients.mu.Unlock()
	defer func() {
		n.clients.mu.Lock()
		if n.clients.sessions[id] == s {
			delete(n.clients.sessions, id)
		}
		n.clients.mu.Unlock()
		s.close()
	}()
	n.logger.Info(fmt.Sprintf("client %d attached", id))

	// the full state first; changes applied meanwhile are queued already and sent after it
	if err := stream.Send(n.fullClientUpdate(s)); err != nil {
		return err
	}

	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				s.close()
				return
			}
			n.changeWatches(s, req)
		}
	}()

	ticker := time.NewTicker(livenessInterval)
	defer ticker.Stop()
	for {
		select {
		case u := <-s.updateC:
			if err := stream.Send(u); err != nil {
				return err
			}
		case <-ticker.C:
			if err := stream.Send(n.livenessUpdate()); err != nil {
				return err
			}
		case <-s.closeC:
			return status.Error(codes.Unavailable, "detached")
		case <-stream.Context().Done():
			return nil
		case <-n.stopC:
			return status.Error(codes.Unavailable, "member stopped")
		case <-n.failedC:
			return status.Error(codes.Unavailable, "member failed")
		}
	}
}

// addClient proposes CLIENT_ADD until the client is a peer with its address.
func (n *Node[T, K]) addClient(ctx context.Context, id uint64, addr string) error {
	l := &service.NodeLog{
		NodeId: n.Id(),
		Type:   service.NodeEventType_CLIENT_ADD,
		Body:   &service.NodeLog_Client{Client: &service.ClientNode{Id: id, Addr: addr}},
	}
	var prop proposalState
	for {
		n.peersMu.RLock()
		known := n.peers[id] == addr
		n.peersMu.RUnlock()
		if known {
			return nil
		}
		if n.needsPropose(&prop) {
			if err := n.proposeTracked(&prop, l); err != nil {
				debugf(n.logger, "proposing client %d failed, retrying: %v", id, err)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopC:
			return status.Error(codes.Unavailable, "member stopped")
		case <-n.failedC:
			return status.Error(codes.Unavailable, "member failed")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// changeWatches adds and removes the watched locks of a client, sending the current state of new ones.
func (n *Node[T, K]) changeWatches(s *clientSession, req *service.AttachRequest) {
	var added []string
	s.mu.Lock()
	for _, id := range req.GetWatch() {
		if s.watched[id] == 0 {
			added = append(added, id)
		}
		s.watched[id]++
	}
	for _, id := range req.GetUnwatch() {
		if s.watched[id]--; s.watched[id] <= 0 {
			delete(s.watched, id)
		}
	}
	s.mu.Unlock()

	if len(added) == 0 {
		return
	}
	u := n.baseClientUpdate()
	for _, id := range added {
		l, _ := n.locks.peek(id)
		u.Locks = append(u.Locks, lockToProto(id, l))
	}
	s.send(u)
}

// fullClientUpdate is the state a client gets when it attaches.
func (n *Node[T, K]) fullClientUpdate(s *clientSession) *service.AttachUpdate {
	u := n.baseClientUpdate()
	u.Full = true
	if layout := n.layout.Load(); layout != nil {
		if pb, err := bitmapToProto(layout); err == nil {
			u.Layout = pb
		}
	}
	n.fillMembership(u)
	u.Liveness = n.livenessUpdate().GetLiveness()

	n.locks.mu.Lock()
	for id, l := range n.locks.locks {
		if s.involves(l) {
			u.Locks = append(u.Locks, lockToProto(id, l))
		}
	}
	n.locks.mu.Unlock()
	return u
}

func (n *Node[T, K]) baseClientUpdate() *service.AttachUpdate {
	return &service.AttachUpdate{Leader: n.leader.Load(), Term: n.term.Load()}
}

// fillMembership sets the nodes of the cluster: peers with addresses, the raft members among them, removed ids.
func (n *Node[T, K]) fillMembership(u *service.AttachUpdate) {
	u.Membership = true
	n.peersMu.RLock()
	u.Peers = maps.Clone(n.peers)
	for id := range n.removed {
		u.Removed = append(u.Removed, id)
	}
	u.Members = slices.Clone(n.members)
	n.peersMu.RUnlock()
}

func (n *Node[T, K]) livenessUpdate() *service.AttachUpdate {
	u := n.baseClientUpdate()
	u.Liveness = make(map[string]*service.NodeSnapshot_Liveness)
	n.locks.mu.Lock()
	for id, l := range n.locks.nodes {
		u.Liveness[id] = &service.NodeSnapshot_Liveness{Last: timestamppb.New(l.last), Lease: durationpb.New(l.lease)}
	}
	n.locks.mu.Unlock()
	return u
}

func lockToProto(id string, l containerLock) *service.NodeSnapshot_Lock {
	lock := &service.NodeSnapshot_Lock{ContainerId: id, Holder: l.holder, Token: l.token}
	for _, w := range l.queue {
		lock.Waiters = append(lock.Waiters, &service.NodeSnapshot_Lock_Waiter{Node: w.node, Token: w.token, WaitPath: w.waitPath})
	}
	for _, c := range l.calls {
		lock.Calls = append(lock.Calls, &service.NodeSnapshot_Lock_Call{Token: c.token, WaitPath: c.waitPath})
	}
	return lock
}

func lockFromProto(l *service.NodeSnapshot_Lock) containerLock {
	lock := containerLock{holder: l.GetHolder(), token: l.GetToken()}
	for _, w := range l.GetWaiters() {
		lock.queue = append(lock.queue, lockWaiter{node: w.GetNode(), token: w.GetToken(), waitPath: w.GetWaitPath()})
	}
	for _, c := range l.GetCalls() {
		lock.calls = append(lock.calls, lockCall{token: c.GetToken(), waitPath: c.GetWaitPath()})
	}
	return lock
}

// sessions returns the attached clients, nil without any (the common case on the hot path).
func (n *Node[T, K]) sessions() []*clientSession {
	n.clients.mu.Lock()
	defer n.clients.mu.Unlock()
	if len(n.clients.sessions) == 0 {
		return nil
	}
	return slices.Collect(maps.Values(n.clients.sessions))
}

// notifyLock sends the change of a lock to the clients it concerns, before or after, or which watch it.
func (n *Node[T, K]) notifyLock(id string, before, after containerLock) {
	for _, s := range n.sessions() {
		if s.involves(before) || s.involves(after) || s.watches(id) {
			u := n.baseClientUpdate()
			u.Locks = []*service.NodeSnapshot_Lock{lockToProto(id, after)}
			s.send(u)
		}
	}
}

// notifyRefused tells a client that the lock table refused its acquire.
func (n *Node[T, K]) notifyRefused(nodeId, id, token string) {
	for _, s := range n.sessions() {
		if s.nodeId == nodeId {
			u := n.baseClientUpdate()
			u.Refused = []*service.ContainerLock{{ContainerId: id, Token: token}}
			s.send(u)
		}
	}
}

// notifyClients sends all clients what changed for everyone: the bitmap and the nodes of the cluster.
func (n *Node[T, K]) notifyClients(layout, membership bool) {
	sessions := n.sessions()
	if len(sessions) == 0 {
		return
	}
	u := n.baseClientUpdate()
	if layout {
		if l := n.layout.Load(); l != nil {
			if pb, err := bitmapToProto(l); err == nil {
				u.Layout = pb
			}
		}
	}
	if membership {
		n.fillMembership(u)
	}
	for _, s := range sessions {
		s.send(u)
	}
}

// notifyLeader sends all clients the new leader and term.
func (n *Node[T, K]) notifyLeader() {
	for _, s := range n.sessions() {
		s.send(n.baseClientUpdate())
	}
}

// applyClientLog applies the addition or removal of a client node.
func (n *Node[T, K]) applyClientLog(l *service.NodeLog) {
	c := l.GetClient()
	if c == nil {
		return
	}
	switch l.GetType() {
	case service.NodeEventType_CLIENT_ADD:
		n.peersMu.Lock()
		changed := n.peers[c.GetId()] != c.GetAddr()
		n.peers[c.GetId()] = c.GetAddr()
		// a client which left can attach again with the same id
		delete(n.removed, c.GetId())
		delete(n.peerClients, c.GetId())
		n.peersMu.Unlock()
		if changed {
			n.logger.Info(fmt.Sprintf("client %d joined the cluster at %s", c.GetId(), c.GetAddr()))
		}
		n.notifyClients(false, true)
	case service.NodeEventType_CLIENT_REMOVE:
		n.removeClientNode(c.GetId())
	}
}

// removeClientNode removes a client from the cluster: like a removed raft member it can't hold locks anymore,
// loses its transitions and its messages are rejected.
func (n *Node[T, K]) removeClientNode(id uint64) {
	n.peersMu.Lock()
	_, known := n.peers[id]
	delete(n.peers, id)
	delete(n.peerClients, id)
	n.removed[id] = true
	n.peersMu.Unlock()

	n.closeForwardStream(id)
	// after the grace the member told the client about its removal (Attach): it stops then
	n.closeRemovedConns(id)
	nodeId := strconv.FormatUint(id, 10)
	for lockId, change := range n.locks.removeNode(nodeId) {
		n.lockChanged(lockId, change[0], change[1], false)
		n.notifyLock(lockId, change[0], change[1])
	}
	n.removeTransitionOwner(nodeId)
	if known {
		n.logger.Info(fmt.Sprintf("client %d has been removed from the cluster", id))
	}
	n.notifyClients(false, true)
}

// isMember reports whether the id is a raft member, as this node sees the configuration.
func (n *Node[T, K]) isMember(id uint64) bool {
	n.peersMu.RLock()
	defer n.peersMu.RUnlock()
	return slices.Contains(n.members, id)
}
