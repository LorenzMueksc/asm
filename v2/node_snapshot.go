package asm

import (
	"errors"
	"fmt"
	"maps"
	"strconv"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"rumue.io/asm/service"
)

// The raft log is compacted into a snapshot of the state it builds: the bitmap layout, the peers,
// the lock table and the liveness of the nodes. Followers behind the compacted index and joining
// nodes receive the snapshot and restore the state from it.
//
// A snapshot is taken after snapshotCount entries, but at most every snapshotInterval: with 100k locks a snapshot
// is a few MB and takes 50-100ms, more than the log it compacts at a few thousand entries per second.
// A log grown to maxLogBytes since the last snapshot is compacted right away.
//
// Applying only copies the state (snapshotState): the locks are replaced as a whole on every change and the layout
// on every bitmap, so copying the maps is enough. A goroutine builds the snapshot from the copy, marshals it, stores
// it in the MemoryStorage, writes the file and compacts; one at a time.

const (
	defaultSnapshotCount          uint64 = 10000
	defaultSnapshotCatchUpEntries uint64 = 5000
	defaultSnapshotInterval              = 30 * time.Second
	maxLogBytes                          = 64 << 20
)

// snapshotState is a copy of the state the raft log builds, at an applied index.
type snapshotState struct {
	index     uint64
	confState raftpb.ConfState
	layout    *Bitmap
	peers     map[uint64]string
	removed   []uint64
	locks     map[string]containerLock
	nodes     map[string]nodeLiveness
}

// copySnapshotState copies the state at the applied index. The values aren't modified later: locks and the layout
// are replaced as a whole on every change, so are the slices of the conf state.
func (n *Node[T, K]) copySnapshotState() *snapshotState {
	s := &snapshotState{index: n.appliedIndex, confState: n.confState, layout: n.layout.Load()}

	n.peersMu.RLock()
	s.peers = maps.Clone(n.peers)
	for id := range n.removed {
		s.removed = append(s.removed, id)
	}
	n.peersMu.RUnlock()

	n.locks.mu.Lock()
	s.locks = maps.Clone(n.locks.locks)
	s.nodes = maps.Clone(n.locks.nodes)
	n.locks.mu.Unlock()
	return s
}

// marshal serializes the state as a NodeSnapshot.
func (s *snapshotState) marshal() ([]byte, error) {
	snap := &service.NodeSnapshot{
		Peers:   s.peers,
		Removed: s.removed,
		Nodes:   make(map[string]*service.NodeSnapshot_Liveness, len(s.nodes)),
		Locks:   make([]*service.NodeSnapshot_Lock, 0, len(s.locks)),
	}
	if snap.Peers == nil {
		snap.Peers = make(map[uint64]string)
	}

	if s.layout != nil {
		pb, err := bitmapToProto(s.layout)
		if err != nil {
			return nil, err
		}
		snap.Layout = pb
	}
	for id, l := range s.locks {
		lock := &service.NodeSnapshot_Lock{ContainerId: id, Holder: l.holder, Token: l.token}
		for _, w := range l.queue {
			lock.Waiters = append(lock.Waiters, &service.NodeSnapshot_Lock_Waiter{Node: w.node, Token: w.token, WaitPath: w.waitPath})
		}
		for _, c := range l.calls {
			lock.Calls = append(lock.Calls, &service.NodeSnapshot_Lock_Call{Token: c.token, WaitPath: c.waitPath})
		}
		snap.Locks = append(snap.Locks, lock)
	}
	for id, l := range s.nodes {
		snap.Nodes[id] = &service.NodeSnapshot_Liveness{Last: timestamppb.New(l.last), Lease: durationpb.New(l.lease)}
	}

	return snap.MarshalVT()
}

// restoreSnapshot replaces the state with the one of the snapshot.
func (n *Node[T, K]) restoreSnapshot(s raftpb.Snapshot) error {
	var snap service.NodeSnapshot
	if err := snap.UnmarshalVT(s.Data); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	if snap.GetLayout() != nil {
		layout, err := bitmapFromProto(snap.GetLayout())
		if err != nil {
			return fmt.Errorf("failed to convert snapshot layout: %w", err)
		}
		n.layout.Store(layout)
	}

	// the peers of the snapshot replace the current ones, nodes removed meanwhile are dropped
	n.peersMu.Lock()
	gone := make([]uint64, 0)
	for id := range n.peers {
		if _, ok := snap.GetPeers()[id]; !ok {
			gone = append(gone, id)
		}
	}
	n.peers = make(map[uint64]string, len(snap.GetPeers()))
	for id, addr := range snap.GetPeers() {
		n.peers[id] = addr
	}
	for _, id := range gone {
		delete(n.peerClients, id)
	}
	n.removed = make(map[uint64]bool, len(snap.GetRemoved()))
	for _, id := range snap.GetRemoved() {
		n.removed[id] = true
	}
	selfRemoved := n.removed[uint64(n.id)]
	n.peersMu.Unlock()

	for _, id := range gone {
		n.stopSender(id)
		n.removeTransitionOwner(strconv.FormatUint(id, 10))
	}

	n.locks.mu.Lock()
	n.locks.locks = make(map[string]containerLock, len(snap.GetLocks()))
	n.locks.waitedFor = make(map[string]map[string]int)
	for _, l := range snap.GetLocks() {
		lock := containerLock{holder: l.GetHolder(), token: l.GetToken()}
		for _, w := range l.GetWaiters() {
			lock.queue = append(lock.queue, lockWaiter{node: w.GetNode(), token: w.GetToken(), waitPath: w.GetWaitPath()})
		}
		for _, c := range l.GetCalls() {
			lock.calls = append(lock.calls, lockCall{token: c.GetToken(), waitPath: c.GetWaitPath()})
		}
		n.locks.putLocked(l.GetContainerId(), lock)
	}
	n.locks.nodes = make(map[string]nodeLiveness, len(snap.GetNodes()))
	for id, l := range snap.GetNodes() {
		n.locks.nodes[id] = nodeLiveness{last: l.GetLast().AsTime(), lease: l.GetLease().AsDuration()}
	}
	n.locks.changedAllLocked()
	n.locks.mu.Unlock()

	n.confState = s.Metadata.ConfState
	n.setMembers(n.confState.Voters)
	// the attached clients attach again and get the state of the snapshot
	for _, cs := range n.sessions() {
		cs.close()
	}
	n.appliedIndex = s.Metadata.Index
	n.snapshotIndex = s.Metadata.Index

	n.logger.Info(fmt.Sprintf("restored snapshot at index %d", s.Metadata.Index))

	if selfRemoved {
		n.selfRemoved(s.Metadata.Index)
	}

	go n.recompileTransitions()

	return nil
}

// maybeSnapshot compacts the raft log once enough entries were applied since the last snapshot: it copies the state
// and leaves the rest to a goroutine (writeSnapshot), unless the previous snapshot is still being written.
func (n *Node[T, K]) maybeSnapshot() {
	if n.appliedIndex-n.snapshotIndex < n.snapshotCount {
		return
	}
	if time.Since(n.snapshotAt) < n.snapshotInterval && n.logBytes < maxLogBytes {
		return
	}
	if !n.snapshotting.CompareAndSwap(false, true) {
		return
	}

	start := time.Now()
	state := n.copySnapshotState()
	n.snapshotIndex = n.appliedIndex
	n.snapshotAt = time.Now()
	n.logBytes = 0

	// a member with a wal stops once the snapshot is written: the apply goroutine is counted, so Add is safe here
	persist := n.wal != nil
	if persist {
		n.storageWG.Add(1)
	}
	go func() {
		defer n.snapshotting.Store(false)
		if persist {
			defer n.storageWG.Done()
		}
		n.writeSnapshot(state)
		n.metrics.snapshotTime.observe(time.Since(start))
	}()
}

// writeSnapshot stores the snapshot of the state in the MemoryStorage and the data directory and compacts the log.
// A snapshot received from the leader meanwhile replaced the log: the older one is dropped (ErrSnapOutOfDate).
func (n *Node[T, K]) writeSnapshot(state *snapshotState) {
	data, err := state.marshal()
	if err != nil {
		n.logger.Error(fmt.Sprintf("failed to create snapshot data: %v", err))
		return
	}
	n.metrics.snapshots.Add(1)
	n.metrics.snapshotBytes.Store(uint64(len(data)))

	snap, err := n.storage.CreateSnapshot(state.index, &state.confState, data)
	if err != nil {
		if !errors.Is(err, raft.ErrSnapOutOfDate) {
			n.logger.Error(fmt.Sprintf("failed to create snapshot: %v", err))
		}
		return
	}
	if n.wal != nil {
		// without the file the segments stay, the next snapshot tries again
		if err := n.wal.saveSnapshot(snap, false); err != nil && !errors.Is(err, ErrNodeStopped) {
			n.logger.Error(fmt.Sprintf("failed to write snapshot to %s: %v", n.dataDir, err))
		}
	}

	// keep some entries, so slow followers can catch up without a snapshot
	if state.index > n.snapshotCatchUpEntries {
		compactIndex := state.index - n.snapshotCatchUpEntries
		if err := n.storage.Compact(compactIndex); err != nil && !errors.Is(err, raft.ErrCompacted) {
			n.logger.Error(fmt.Sprintf("failed to compact log: %v", err))
			return
		}
		debugf(n.logger, "compacted log up to index %d", compactIndex)
	}
}
