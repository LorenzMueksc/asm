package asm

import (
	"context"
	"maps"
	"slices"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"rumue.io/asm/service"
)

// Inspect returns the state of the node as it sees the cluster, for dev tools: raft state, peers,
// transitions, the committed bitmap, the container locks and the open containers.
func (n *Node[T, K]) Inspect(_ context.Context, req *service.InspectRequest) (*service.InspectResponse, error) {
	limit := int(req.GetLimit())
	locks, lockCount := n.inspectLocks(limit)
	containers, containerCount := n.inspectContainers(limit)
	resp := &service.InspectResponse{
		Node:           n.inspectNode(),
		Transitions:    n.inspectTransitions(),
		Locks:          locks,
		Containers:     containers,
		Metrics:        n.collectMetrics(),
		LockCount:      uint32(lockCount),
		ContainerCount: uint32(containerCount),
	}
	if layout := n.layout.Load(); layout != nil {
		if pb, err := bitmapToProto(layout); err == nil {
			resp.Layout = pb
		}
	}
	return resp, nil
}

func (n *Node[T, K]) inspectNode() *service.NodeInfo {
	info := &service.NodeInfo{
		Id:            n.Id(),
		ListenAddr:    n.listenAddr,
		SkipRaft:      n.skipRaft,
		Client:        n.client,
		Entity:        n.entityName(),
		MaxContainers: uint32(cap(n.sem)),
		Peers:         make(map[uint64]string),
		Liveness:      make(map[string]*service.NodeSnapshot_Liveness),
	}
	select {
	case <-n.readyC:
		info.Ready = true
	default:
	}
	if err := n.failure(); err != nil {
		info.Error = err.Error()
	}
	if n.trace != nil {
		info.TraceCapacity = uint32(n.trace.capacity)
	}

	if n.client {
		info.AttachedTo = n.attachedTo()
		info.Leader = n.leader.Load()
		info.Term = n.term.Load()
		info.RaftState = "client"
	}
	if n.node != nil {
		st := n.node.Status()
		info.RaftState = st.RaftState.String()
		info.Leader = st.Lead
		info.Term = st.Term
		info.CommitIndex = st.Commit
		info.AppliedIndex = st.Applied
		if snap, err := n.storage.Snapshot(); err == nil {
			info.SnapshotIndex = snap.Metadata.Index
		}
	}

	n.peersMu.RLock()
	for id, addr := range n.peers {
		info.Peers[id] = addr
	}
	for id := range n.removed {
		info.Removed = append(info.Removed, id)
	}
	n.peersMu.RUnlock()
	slices.Sort(info.Removed)

	n.locks.mu.Lock()
	for id, l := range n.locks.nodes {
		info.Liveness[id] = &service.NodeSnapshot_Liveness{Last: timestamppb.New(l.last), Lease: durationpb.New(l.lease)}
	}
	n.locks.mu.Unlock()

	return info
}

func (n *Node[T, K]) inspectTransitions() []*service.InspectTransition {
	ts := n.registry.published.Load().list()
	if ts == nil {
		return nil
	}

	result := make([]*service.InspectTransition, 0, len(ts))
	for _, t := range ts {
		result = append(result, &service.InspectTransition{
			Transition: t.toProto(n.Id()),
			NodeIds:    t.NodeIds,
			Remote:     t.Remote,
			FuncKey:    t.funcKey,
		})
	}
	return result
}

// inspectLocks returns the first limit locks by container id (all with limit 0) and the number of locks.
func (n *Node[T, K]) inspectLocks(limit int) ([]*service.NodeSnapshot_Lock, int) {
	n.locks.mu.Lock()
	defer n.locks.mu.Unlock()

	ids := firstIds(slices.Collect(maps.Keys(n.locks.locks)), limit)
	result := make([]*service.NodeSnapshot_Lock, 0, len(ids))
	for _, id := range ids {
		l := n.locks.locks[id]
		lock := &service.NodeSnapshot_Lock{ContainerId: id, Holder: l.holder, Token: l.token}
		for _, w := range l.queue {
			lock.Waiters = append(lock.Waiters, &service.NodeSnapshot_Lock_Waiter{Node: w.node, Token: w.token, WaitPath: w.waitPath})
		}
		for _, c := range l.calls {
			lock.Calls = append(lock.Calls, &service.NodeSnapshot_Lock_Call{Token: c.token, WaitPath: c.waitPath})
		}
		result = append(result, lock)
	}
	return result, len(n.locks.locks)
}

// firstIds sorts the ids and returns the first limit of them, all with limit 0.
func firstIds(ids []string, limit int) []string {
	slices.Sort(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return ids
}

// inspectContainers reads the open containers. A container which is processing isn't waited for,
// it is reported busy with the entity, states and batches when its current event started (with WithEventTrace).
// inspectContainers returns the first limit open containers by id (all with limit 0) and the number of open containers.
func (n *Node[T, K]) inspectContainers(limit int) ([]*service.ContainerInfo, int) {
	n.containerMap.mu.RLock()
	defer n.containerMap.mu.RUnlock()

	ids := firstIds(slices.Collect(maps.Keys(n.containerMap.m)), limit)
	result := make([]*service.ContainerInfo, 0, len(ids))
	for _, id := range ids {
		c := n.containerMap.m[id]
		p := c.containerProcessing
		info := &service.ContainerInfo{Id: id}

		p.lifecycleMu.Lock()
		info.Pending = int64(p.pending)
		accepting := p.accepting
		p.lifecycleMu.Unlock()
		switch {
		case accepting:
			info.Status = string(containerStatusRunning)
		case isClosed(p.doneC):
			info.Status = string(containerStatusClosed)
		default:
			info.Status = "closing"
		}

		// trying, because waiting for a busy container while holding the map lock would block the node.
		// A busy container shows the snapshot after its last processed event, if the trace keeps one.
		if !c.mu.TryLock() {
			info.Busy = true
			if snap := c.snapshot.Load(); snap != nil {
				snap.info(info)
			}
			result = append(result, info)
			continue
		}
		c.readInfo(info)
		c.mu.Unlock()

		result = append(result, info)
	}
	return result, len(n.containerMap.m)
}

func isClosed(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
