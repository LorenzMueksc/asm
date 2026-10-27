package asm

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"rumue.io/asm/service"
)

// The client side of client nodes (see node_clients.go for the member side): attaching to a member, applying its
// updates, watching locks of other nodes, leaving.

// attachRetry is the pause before attaching again after the stream to a member broke
const attachRetry = 200 * time.Millisecond

type clientState struct {
	// mu guards stream, attached and watches. Sends on the stream are serialized with mu.
	mu       sync.Mutex
	stream   service.Node_AttachClient
	attached string
	// watches counts the watchers of the locks of other nodes by container id
	watches map[string]int
}

// startClient starts a client node: it serves gRPC, attaches to a member and becomes ready like a member.
func (n *Node[T, K]) startClient(joinAddr string) {
	if joinAddr == "" {
		n.fail(errors.New("a client node needs WithJoinAddr, the address of a raft member"))
		return
	}
	if err := n.serveGRPC(); err != nil {
		n.fail(err)
		return
	}

	go n.runProposer()
	go n.attachLoop(joinAddr)
	go func() {
		select {
		case <-n.stopC:
		case <-n.failedC:
		}
		n.stop()
	}()
	go n.becomeReady()
}

// attachLoop keeps the node attached to a member: the join address first, another member when one fails.
func (n *Node[T, K]) attachLoop(joinAddr string) {
	addr := joinAddr
	for {
		err := n.attach(addr)
		select {
		case <-n.stopC:
			return
		case <-n.failedC:
			return
		default:
		}
		if status.Code(err) == codes.PermissionDenied {
			n.logger.Warn("the cluster rejected this client as removed, stopping")
			n.requestStop()
			return
		}
		debugf(n.logger, "stream to member %s broke, attaching again: %v", addr, err)
		addr = n.nextMember(addr, joinAddr)

		select {
		case <-time.After(attachRetry):
		case <-n.stopC:
			return
		case <-n.failedC:
			return
		}
	}
}

// nextMember returns the address of the member to attach to after the one at addr failed: the next member,
// the join address if no member is known.
func (n *Node[T, K]) nextMember(addr, joinAddr string) string {
	n.peersMu.RLock()
	defer n.peersMu.RUnlock()

	var addrs []string
	for _, id := range n.members {
		if a, ok := n.peers[id]; ok {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) == 0 {
		return joinAddr
	}
	slices.Sort(addrs)
	i := slices.Index(addrs, addr)
	return addrs[(i+1)%len(addrs)]
}

// attach attaches to the member at addr and applies its updates until the stream breaks.
func (n *Node[T, K]) attach(addr string) error {
	conn, err := grpc.NewClient(addr, n.dialOptions(0)...)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(n.ctx)
	defer cancel()
	stream, err := service.NewNodeClient(conn).Attach(ctx)
	if err != nil {
		return err
	}

	n.clientState.mu.Lock()
	watched := make([]string, 0, len(n.clientState.watches))
	for id := range n.clientState.watches {
		watched = append(watched, id)
	}
	err = stream.Send(&service.AttachRequest{ClientId: uint64(n.id), Addr: n.listenAddr, Watch: watched})
	if err == nil {
		n.clientState.stream, n.clientState.attached = stream, addr
	}
	n.clientState.mu.Unlock()
	if err != nil {
		return err
	}
	defer func() {
		n.clientState.mu.Lock()
		if n.clientState.stream == stream {
			n.clientState.stream, n.clientState.attached = nil, ""
		}
		n.clientState.mu.Unlock()
	}()

	for {
		u, err := stream.Recv()
		if err != nil {
			return err
		}
		n.applyClientUpdate(u)
	}
}

// applyClientUpdate applies an update of the member.
func (n *Node[T, K]) applyClientUpdate(u *service.AttachUpdate) {
	n.leader.Store(u.GetLeader())
	n.term.Store(u.GetTerm())
	if u.GetLayout() != nil {
		n.applyClientLayout(u.GetLayout())
	}
	if u.GetMembership() {
		n.applyClientMembership(u)
	}
	if len(u.GetLiveness()) > 0 {
		n.locks.setLiveness(u.GetLiveness())
	}
	if len(u.GetLocks()) > 0 || u.GetFull() {
		n.applyClientLocks(u.GetLocks(), u.GetFull())
	}
	for _, r := range u.GetRefused() {
		n.refuseLockToken(r.GetToken())
		n.locks.wake(r.GetContainerId())
	}
}

// applyClientLayout takes the committed bitmap. A local root entity bitmap is committed once merging it into the
// committed bitmap adds nothing; one conflicting with it fails the node, like on a member.
func (n *Node[T, K]) applyClientLayout(pb *service.Bitmap) {
	layout, err := bitmapFromProto(pb)
	if err != nil {
		n.logger.Error(fmt.Sprintf("failed to convert the committed bitmap: %v", err))
		return
	}
	n.layout.Store(layout)
	n.checkPendingBitmaps()
	go n.recompileTransitions()
}

// checkPendingBitmaps marks the local root entity bitmaps committed which the committed bitmap contains. It runs on
// every new committed bitmap and once the bitmaps are pending: the bitmap may contain them already and not change.
func (n *Node[T, K]) checkPendingBitmaps() {
	layout := n.layout.Load()
	if layout == nil {
		return
	}
	n.bitmapMu.Lock()
	for name := range n.pendingBitmapNames {
		merged, err := applyBitmapEntry(layout, n.localBitmap.Children[name])
		if err != nil {
			n.bitmapMu.Unlock()
			n.fail(fmt.Errorf("bitmap conflict: %w", err))
			return
		}
		if bitmapSize(merged) == bitmapSize(layout) {
			delete(n.pendingBitmapNames, name)
		}
	}
	n.bitmapMu.Unlock()
	n.checkBitmapCommitted()
}

// bitmapSize counts the values of the bitmap and its children.
func bitmapSize(b *Bitmap) int {
	if b == nil {
		return 0
	}
	size := len(b.Values)
	for _, c := range b.Children {
		size += bitmapSize(c)
	}
	return size
}

// applyClientMembership takes the nodes of the cluster. A client removed from the cluster stops.
func (n *Node[T, K]) applyClientMembership(u *service.AttachUpdate) {
	n.peersMu.Lock()
	for id, addr := range n.peers {
		if u.GetPeers()[id] != addr {
			delete(n.peerClients, id)
		}
	}
	n.peers = make(map[uint64]string, len(u.GetPeers()))
	for id, addr := range u.GetPeers() {
		n.peers[id] = addr
	}
	n.members = slices.Clone(u.GetMembers())
	var newlyRemoved []uint64
	for _, id := range u.GetRemoved() {
		if !n.removed[id] {
			newlyRemoved = append(newlyRemoved, id)
		}
		n.removed[id] = true
	}
	n.peersMu.Unlock()

	for _, id := range newlyRemoved {
		if id == uint64(n.id) {
			n.logger.Info("this client has been removed from the cluster, stopping")
			go n.requestStop()
			continue
		}
		n.closeForwardStream(id)
		n.removeTransitionOwner(strconv.FormatUint(id, 10))
	}
}

// applyClientLocks takes the locks the member sent. A full update replaces them all: locks not in it don't concern
// this node anymore.
func (n *Node[T, K]) applyClientLocks(locks []*service.NodeSnapshot_Lock, full bool) {
	seen := make(map[string]bool, len(locks))
	for _, pl := range locks {
		id := pl.GetContainerId()
		seen[id] = true
		after := lockFromProto(pl)
		before := n.locks.set(id, after)
		n.lockChanged(id, before, after, n.ownChange(before))
	}
	if !full {
		return
	}
	for _, id := range n.locks.ids() {
		if !seen[id] {
			before := n.locks.set(id, containerLock{})
			n.lockChanged(id, before, containerLock{}, n.ownChange(before))
		}
	}
}

// ownChange reports whether a change of a lock this node held was its own doing: it released or cancelled the token.
func (n *Node[T, K]) ownChange(before containerLock) bool {
	if before.holder != n.Id() {
		return false
	}
	n.startingMu.Lock()
	defer n.startingMu.Unlock()
	return !n.lockTokens[before.token]
}

// watchLock asks the member for the changes of the lock of the id, which concerns another node.
func (n *Node[T, K]) watchLock(id string) {
	n.clientState.mu.Lock()
	defer n.clientState.mu.Unlock()

	n.clientState.watches[id]++
	if n.clientState.watches[id] == 1 && n.clientState.stream != nil {
		if err := n.clientState.stream.Send(&service.AttachRequest{Watch: []string{id}}); err != nil {
			debugf(n.logger, "watching the lock of %s failed: %v", id, err)
		}
	}
}

// unwatchLock stops watching the lock of the id; the local copy is dropped unless the lock concerns this node.
func (n *Node[T, K]) unwatchLock(id string) {
	n.clientState.mu.Lock()
	n.clientState.watches[id]--
	last := n.clientState.watches[id] <= 0
	if last {
		delete(n.clientState.watches, id)
		if n.clientState.stream != nil {
			if err := n.clientState.stream.Send(&service.AttachRequest{Unwatch: []string{id}}); err != nil {
				debugf(n.logger, "unwatching the lock of %s failed: %v", id, err)
			}
		}
	}
	n.clientState.mu.Unlock()

	if last {
		n.locks.dropUnless(id, n.Id())
	}
}

// attachedTo returns the address of the member the client is attached to.
func (n *Node[T, K]) attachedTo() string {
	n.clientState.mu.Lock()
	defer n.clientState.mu.Unlock()
	return n.clientState.attached
}

// leaveAsClient proposes the removal of this client and waits until it is applied.
func (n *Node[T, K]) leaveAsClient(ctx context.Context) error {
	l := &service.NodeLog{
		NodeId: n.Id(),
		Type:   service.NodeEventType_CLIENT_REMOVE,
		Body:   &service.NodeLog_Client{Client: &service.ClientNode{Id: uint64(n.id), Addr: n.listenAddr}},
	}
	var prop proposalState
	for !n.isRemoved(uint64(n.id)) {
		if n.needsPropose(&prop) {
			if err := n.proposeTracked(&prop, l); err != nil {
				debugf(n.logger, "proposing the removal of this client failed, retrying: %v", err)
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("leaving the cluster: %w", ctx.Err())
		case <-n.failedC:
			return n.failure()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return nil
}

// set replaces the lock of the id (removes it without holder), wakes its waiters and returns the lock before.
func (lt *lockTable) set(id string, l containerLock) containerLock {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	before := lt.locks[id]
	lt.storeLocked(id, l)
	return before
}

// dropUnless removes the lock of the id unless the node holds it, waits for it or calls into its container.
func (lt *lockTable) dropUnless(id, nodeId string) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	l, ok := lt.locks[id]
	if !ok {
		return
	}
	s := clientSession{nodeId: nodeId}
	if s.involves(l) {
		return
	}
	lt.storeLocked(id, containerLock{})
}

// ids returns the ids of the locks in the table.
func (lt *lockTable) ids() []string {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	ids := make([]string, 0, len(lt.locks))
	for id := range lt.locks {
		ids = append(ids, id)
	}
	return ids
}

// setLiveness replaces the liveness of the nodes with the one a member sent.
func (lt *lockTable) setLiveness(liveness map[string]*service.NodeSnapshot_Liveness) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.nodes = make(map[string]nodeLiveness, len(liveness))
	for id, l := range liveness {
		lt.nodes[id] = nodeLiveness{last: l.GetLast().AsTime(), lease: l.GetLease().AsDuration()}
	}
	lt.livenessChangedLocked()
}
