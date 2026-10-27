package asm

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
	"rumue.io/asm/service"
)

type (
	nodeRaft struct {
		service.UnimplementedNodeServer

		errorC chan error
		// stopC is closed once the node stops: after it left the cluster, was removed,
		// or the stop channel of the user was closed and leaving failed
		stopC           chan struct{}
		requestStopOnce sync.Once
		stopOnce        sync.Once

		// peersMu guards peers, peerClients, removed and confChangeCount
		peersMu     sync.RWMutex
		peers       map[uint64]string
		peerClients map[uint64]service.NodeClient
		// removed holds the ids of the nodes removed from the cluster, their messages are rejected
		removed map[uint64]bool
		// addedAt holds the index of the entry which added each node, returned by Join
		addedAt map[uint64]uint64
		// members are the ids of the raft members (the voters), guarded by peersMu; the other peers are clients
		members []uint64

		// selfMu guards joined, joinIndex and selfRemovedIndex.
		// A node joining with the id of a node which left replays the removal of that node,
		// only removals after joinIndex remove this node.
		selfMu           sync.Mutex
		joined           bool
		joinIndex        uint64
		selfRemovedIndex uint64

		// sendersMu guards senders, the queues sending raft messages to each peer
		sendersMu sync.Mutex
		senders   map[uint64]*peerSender

		// proposals queues the log entries the proposer goroutine proposes in batches (node_propose.go), guarded by
		// proposeMu. proposeWakeC wakes the proposer; proposeStopped is set once it answered the queue at stop.
		proposeMu      sync.Mutex
		proposals      []proposal
		proposeStopped bool
		proposeWakeC   chan struct{}
		// leader and term are the raft leader and term as of the last Ready
		leader atomic.Uint64
		term   atomic.Uint64

		confChangeCount uint64
		// joinMu serializes joins and removals, raft only accepts one pending configuration change at a time
		joinMu     sync.Mutex
		node       raft.Node
		grpcServer *grpc.Server

		storage *raft.MemoryStorage
		// dataDir and wal persist the raft state (WithDataDir), restarted is set if the node started from it
		dataDir   string
		wal       *wal
		restarted bool
		startedAt time.Time
		// raftStopC is closed when raft stops: it ends the tick goroutine and the append and apply goroutines of a
		// member with a wal (node_async.go); storageWG waits for the latter
		raftStopC chan struct{}
		storageWG sync.WaitGroup

		appliedIndex uint64
		// confState is the cluster configuration at appliedIndex, stored in snapshots
		confState raftpb.ConfState
		// snapshotIndex is the index of the last snapshot
		snapshotIndex          uint64
		snapshotCount          uint64
		snapshotCatchUpEntries uint64
		// snapshotInterval is the minimum time between snapshots, snapshotAt the time of the last one,
		// logBytes the size of the entries applied since
		snapshotInterval time.Duration
		snapshotAt       time.Time
		logBytes         int
		// snapshotting is set while a goroutine writes a snapshot (writeSnapshot)
		snapshotting atomic.Bool

		// locks is the lock table built by the raft log
		locks         lockTable
		lockSeq       atomic.Uint64
		leaseTimeout  time.Duration
		renewInterval time.Duration
		// starting holds the container ids this node is acquiring the lock for,
		// so only one goroutine of the node acquires the lock of an id at a time
		startingMu sync.Mutex
		starting   map[string]chan struct{}
		// lockTokens holds the tokens of this node's locks in use: acquires in progress and locks held until
		// their release, also guarded by startingMu
		lockTokens map[string]bool
		// refusedTokens holds the tokens of acquires the lock table refused, also guarded by startingMu
		refusedTokens map[string]bool
		// retained holds the tokens of the locks this node keeps after their containers closed, by container id,
		// also guarded by startingMu (see endLock)
		retained map[string]string

		// bitmapMu guards pendingBitmapNames, the names of the local root entity bitmaps not committed yet.
		// bitmapCommittedC is closed once all of them are committed.
		bitmapMu            sync.Mutex
		pendingBitmapNames  map[string]bool
		bitmapCommittedC    chan struct{}
		bitmapCommittedOnce sync.Once
	}
)

const (
	// raftTickInterval is the raft tick: ElectionTick and HeartbeatTick count them
	raftTickInterval     = 100 * time.Millisecond
	proposeRetryInterval = time.Second
	sendTimeout          = 2 * time.Second
	joinTimeout          = 30 * time.Second
	joinRetryInterval    = 2 * time.Second
)

func (n *Node[T, K]) startRaft(joinAddr string) {
	c := &raft.Config{
		ID: uint64(n.id),
		// 2-4s without a heartbeat start an election (100ms ticks). A loaded leader may miss some, and with
		// PreVote a node which only lost the heartbeats can't raise the term while the others still hear the leader
		ElectionTick:              20,
		HeartbeatTick:             1,
		PreVote:                   true,
		Storage:                   n.storage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: maxUncommittedSize,
		Logger:                    newRaftLogger(n.logger),
	}

	n.startedAt = time.Now()
	if n.dataDir != "" {
		if err := n.openWAL(c); err != nil {
			n.fail(err)
			return
		}
		// the ready loop doesn't wait for the disk (node_async.go)
		c.AsyncStorageWrites = true
	}

	n.selfMu.Lock()
	if !n.restarted {
		n.joined = joinAddr == ""
	}
	n.selfMu.Unlock()

	switch {
	case n.restarted:
		// the configuration and the log come from the data directory
		n.node = raft.RestartNode(c)
	case joinAddr != "":
		// a joining node starts without peers and receives the cluster configuration from the leader
		n.node = raft.RestartNode(c)
	default:
		n.node = raft.StartNode(c, []raft.Peer{{ID: uint64(n.id), Context: []byte(n.listenAddr)}})
	}

	if err := n.serveGRPC(); err != nil {
		n.fail(err)
		return
	}
	go n.serveChannels()
	go n.runTicks()
	go n.runProposer()

	if n.restarted {
		go n.becomeReady()
		return
	}

	// join after setting up node
	if joinAddr != "" {
		grpcConn, err := grpc.NewClient(joinAddr, n.dialOptions(0)...)
		if err != nil {
			n.fail(fmt.Errorf("error while connecting to join addr %s: %w", joinAddr, err))
			return
		}

		joinClient := service.NewNodeClient(grpcConn)

		joinCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("node_id", fmt.Sprintf("%d", n.id)))

		joinResp, err := joinClient.Join(joinCtx, &service.JoinRequest{
			ListenAddr: n.listenAddr,
		})
		if err != nil {
			n.fail(fmt.Errorf("failed to join: %w", err))
			return
		}

		n.peersMu.Lock()
		for _, p := range joinResp.GetPeers() {
			n.peers[p.GetId()] = p.GetAddr()
		}
		n.peersMu.Unlock()
		n.joinedAt(joinResp.GetAddedIndex())
	} else {
		err := n.node.Campaign(context.Background())
		if err != nil {
			n.fail(fmt.Errorf("failed to campaign: %w", err))
			return
		}
	}

	go n.becomeReady()
}

// serveGRPC listens and serves the gRPC Node service.
func (n *Node[T, K]) serveGRPC() error {
	listener, err := net.Listen("tcp", n.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", n.listenAddr, err)
	}
	n.grpcServer = grpc.NewServer(n.serverOptions()...)
	service.RegisterNodeServer(n.grpcServer, n)
	go n.serveRaft(listener)
	return nil
}

// becomeReady proposes the local bitmap, waits until it is committed, compiles the local transitions
// against the committed bitmap and exchanges transitions with the peers.
func (n *Node[T, K]) becomeReady() {
	go n.heartbeat()
	go n.watchExpiry()

	// a node stopping meanwhile (it left or was removed) didn't fail
	fail := func(err error) {
		if !errors.Is(err, ErrNodeStopped) {
			n.fail(err)
		}
	}

	if err := n.proposeLocalBitmap(); err != nil {
		fail(err)
		return
	}

	// a restarted node waits for a heartbeat of this life: the log is applied up to it then
	since := time.Time{}
	if n.restarted {
		since = n.startedAt
	}
	if err := n.waitForHeartbeat(since); err != nil {
		fail(err)
		return
	}
	if n.restarted {
		n.endPreviousLife()
	}

	if err := n.compileTransitions(); err != nil {
		fail(err)
		return
	}

	if err := n.syncTransitions(); err != nil {
		fail(err)
		return
	}

	if n.failure() == nil {
		n.markReady()
	}
}

// proposeLocalBitmap proposes every root entity bitmap of the local config
// and retries until all of them are committed.
func (n *Node[T, K]) proposeLocalBitmap() error {
	logs := make(map[string]*service.NodeLog)
	if n.localBitmap != nil {
		for _, name := range sortedChildNames(n.localBitmap) {
			pb, err := bitmapToProto(n.localBitmap.Children[name])
			if err != nil {
				return err
			}
			logs[name] = &service.NodeLog{
				NodeId: n.Id(),
				Type:   service.NodeEventType_BITMAP_ADD,
				Body:   &service.NodeLog_Bitmap{Bitmap: pb},
				Ts:     timestamppb.Now(),
			}
		}
	}

	n.bitmapMu.Lock()
	for name := range logs {
		n.pendingBitmapNames[name] = true
	}
	n.bitmapMu.Unlock()
	n.checkBitmapCommitted()
	if n.client {
		n.checkPendingBitmaps()
	}

	for {
		for name, l := range logs {
			n.bitmapMu.Lock()
			pending := n.pendingBitmapNames[name]
			n.bitmapMu.Unlock()
			if !pending {
				continue
			}

			if err := n.propose(l); err != nil {
				debugf(n.logger, "proposing bitmap %s failed, retrying: %v", name, err)
			}
		}

		select {
		case <-n.bitmapCommittedC:
			return nil
		case <-n.failedC:
			return n.failure()
		case <-n.stopC:
			return ErrNodeStopped
		case <-time.After(proposeRetryInterval):
		}
	}
}

func (n *Node[T, K]) checkBitmapCommitted() {
	n.bitmapMu.Lock()
	defer n.bitmapMu.Unlock()

	if len(n.pendingBitmapNames) == 0 {
		n.bitmapCommittedOnce.Do(func() {
			close(n.bitmapCommittedC)
		})
	}
}

// applyLog applies a committed log entry.
func (n *Node[T, K]) applyLog(l *service.NodeLog) {
	switch l.GetType() {
	case service.NodeEventType_BITMAP_ADD:
		n.applyBitmapLog(l)
	case service.NodeEventType_LOCK_ACQUIRE, service.NodeEventType_LOCK_RELEASE, service.NodeEventType_LOCK_CANCEL,
		service.NodeEventType_LOCK_DONE, service.NodeEventType_NODE_HEARTBEAT:
		// entries a removed node proposed before it noticed its removal must not lock again
		if id, ok := nodeIdOf(l.GetNodeId()); ok && n.isRemoved(id) {
			return
		}
		n.applyLockLog(l)
	case service.NodeEventType_CLIENT_ADD, service.NodeEventType_CLIENT_REMOVE:
		n.applyClientLog(l)
	}
}

// applyBitmapLog merges a committed bitmap into the layout.
// A conflicting bitmap of this node fails the node, one of another node is ignored.
func (n *Node[T, K]) applyBitmapLog(l *service.NodeLog) {
	pb := l.GetBitmap()
	if pb == nil {
		n.logger.Error(fmt.Sprintf("bitmap entry of node %s without bitmap", l.GetNodeId()))
		return
	}
	entry, err := bitmapFromProto(pb)
	if err != nil {
		n.logger.Error(fmt.Sprintf("failed to convert committed bitmap: %v", err))
		return
	}

	own := l.GetNodeId() == n.Id()

	layout, err := applyBitmapEntry(n.layout.Load(), entry)
	if err != nil {
		if own {
			n.fail(fmt.Errorf("bitmap conflict: %w", err))
		} else {
			n.logger.Error(fmt.Sprintf("ignoring conflicting bitmap of node %s: %v", l.GetNodeId(), err))
		}
		return
	}
	n.layout.Store(layout)
	n.logger.Info(fmt.Sprintf("committed bitmap %s of node %s", entry.Name, l.GetNodeId()))
	n.notifyClients(true, false)

	if own {
		n.bitmapMu.Lock()
		delete(n.pendingBitmapNames, entry.Name)
		n.bitmapMu.Unlock()
		n.checkBitmapCommitted()
	}

	go n.recompileTransitions()
}

// serveChannels is the ready loop of a raft member without a wal: it stores, sends and applies each Ready, then
// calls Advance. A member with a wal runs serveChannelsAsync.
func (n *Node[T, K]) serveChannels() {
	if n.wal != nil {
		n.serveChannelsAsync()
		return
	}

	for {
		select {
		case rd := <-n.node.Ready():
			n.updateLeader(rd)
			if !raft.IsEmptySnap(rd.Snapshot) {
				err := n.storage.ApplySnapshot(rd.Snapshot)
				if err != nil {
					log.Fatalf("failed to apply snapshot: %v", err)
				}
				if err := n.restoreSnapshot(rd.Snapshot); err != nil {
					n.fail(err)
				}
			}
			err := n.storage.Append(rd.Entries)
			if err != nil {
				log.Fatalf("failed to append entries: %v", err)
			}

			// the messages go out before applying: the entries are in the storage, so followers may ack them,
			// and replication doesn't wait for the apply
			n.sendMessages(rd.Messages)

			start := time.Now()
			n.publishEntries(n.entriesToApply(rd.CommittedEntries))
			n.metrics.applyNanos.Add(uint64(time.Since(start)))
			n.maybeSnapshot()

			n.node.Advance()
		case <-n.stopC:
			n.stop()
			return
		case <-n.failedC:
			n.stop()
			return
		}
	}
}

// runTicks ticks raft until it stops. The ready loop doesn't tick: a tick due while it processes a Ready would be
// dropped by its ticker.
func (n *Node[T, K]) runTicks() {
	ticker := time.NewTicker(raftTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.node.Tick()
		case <-n.raftStopC:
			return
		}
	}
}

// updateLeader takes the leader and the term of a Ready and tells the clients if they changed.
func (n *Node[T, K]) updateLeader(rd raft.Ready) {
	changed := false
	if rd.SoftState != nil && n.leader.Swap(rd.SoftState.Lead) != rd.SoftState.Lead {
		changed = true
	}
	if !raft.IsEmptyHardState(rd.HardState) && n.term.Swap(rd.HardState.Term) != rd.HardState.Term {
		changed = true
	}
	if changed {
		n.notifyLeader()
	}
}

func (n *Node[T, K]) stop() {
	n.stopOnce.Do(func() {
		close(n.errorC)
		close(n.raftStopC)
		n.stopProposer()
		n.stopSenders()
		n.closeForwardStreams()
		if n.node != nil {
			// a storage goroutine waiting in Step returns once raft stopped
			n.node.Stop()
		}
		if n.grpcServer != nil {
			n.grpcServer.Stop()
		}
		// the append and apply goroutines are done with the wal before it closes
		n.storageWG.Wait()
		if n.wal != nil {
			if err := n.wal.close(); err != nil {
				n.logger.Error(fmt.Sprintf("closing the wal: %v", err))
			}
		}
	})
}

// requestStop stops the node without an error: raft, the gRPC server and the containers.
func (n *Node[T, K]) requestStop() {
	n.requestStopOnce.Do(func() {
		close(n.stopC)
	})
}

// applyRemoveNode applies the removal of a node from the cluster on every node: the node can't
// hold locks anymore, its transitions lose it as owner and its messages are rejected.
// With TLS its certificate is rejected and its connections to this node are closed.
// A removed node stops.
func (n *Node[T, K]) applyRemoveNode(id, index uint64) {
	n.peersMu.Lock()
	delete(n.peers, id)
	delete(n.peerClients, id)
	n.removed[id] = true
	n.peersMu.Unlock()

	n.stopSender(id)
	n.closeForwardStream(id)
	n.closeRemovedConns(id)
	nodeId := strconv.FormatUint(id, 10)
	for lockId, change := range n.locks.removeNode(nodeId) {
		n.lockChanged(lockId, change[0], change[1], false)
		n.notifyLock(lockId, change[0], change[1])
	}
	n.removeTransitionOwner(nodeId)
	n.notifyClients(false, true)

	if id == uint64(n.id) {
		n.selfRemoved(index)
		return
	}
	n.logger.Info(fmt.Sprintf("node %d has been removed from the cluster", id))
}

// selfRemoved stops the node once the removal of its id at the index applied,
// unless the removal is of an earlier node with the same id.
func (n *Node[T, K]) selfRemoved(index uint64) {
	n.selfMu.Lock()
	defer n.selfMu.Unlock()

	// the join response tells which removals are of an earlier node
	if !n.joined {
		n.selfRemovedIndex = max(n.selfRemovedIndex, index)
		return
	}
	if index <= n.joinIndex {
		return
	}

	if n.wal != nil {
		n.logger.Info(fmt.Sprintf("this node has been removed from the cluster, stopping; delete %s to join again", n.dataDir))
	} else {
		n.logger.Info("this node has been removed from the cluster, stopping")
	}
	go n.requestStop()
}

// joinedAt records the index of the entry which added this node and stops it,
// if it was removed after that index meanwhile.
func (n *Node[T, K]) joinedAt(index uint64) {
	n.selfMu.Lock()
	n.joined = true
	n.joinIndex = index
	removedIndex := n.selfRemovedIndex
	n.selfMu.Unlock()

	if n.wal != nil {
		if err := n.wal.setJoinIndex(index); err != nil {
			n.logger.Error(fmt.Sprintf("writing the join index to the wal: %v", err))
		}
	}

	if removedIndex > index {
		n.selfRemoved(removedIndex)
	}
}

// setMembers sets the ids of the raft members.
func (n *Node[T, K]) setMembers(voters []uint64) {
	n.peersMu.Lock()
	n.members = slices.Clone(voters)
	n.peersMu.Unlock()
}

// isRemoved reports whether the node was removed from the cluster.
func (n *Node[T, K]) isRemoved(id uint64) bool {
	n.peersMu.RLock()
	defer n.peersMu.RUnlock()

	return n.removed[id]
}

// stopRaft stops raft and the gRPC server of a failed node.
func (n *Node[T, K]) stopRaft() {
	n.stop()
}

func (n *Node[T, K]) serveRaft(listener net.Listener) {
	n.logger.Info(fmt.Sprintf("listening on %s", n.listenAddr))

	err := n.grpcServer.Serve(listener)
	select {
	case <-n.stopC:
	case <-n.failedC:
	default:
		n.fail(fmt.Errorf("failed to serve: %w", err))
	}
}

func (n *Node[T, K]) entriesToApply(entries []raftpb.Entry) []raftpb.Entry {
	if len(entries) == 0 {
		return nil
	}

	entriesToApply := make([]raftpb.Entry, 0)
	firstIdx := entries[0].Index
	if firstIdx > n.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, n.appliedIndex)
	}
	if n.appliedIndex-firstIdx+1 < uint64(len(entries)) {
		entriesToApply = entries[n.appliedIndex-firstIdx+1:]
	}
	return entriesToApply
}

func (n *Node[T, K]) publishEntries(entries []raftpb.Entry) bool {
	if len(entries) == 0 {
		return false
	}

	for i := range entries {
		switch entries[i].Type {
		case raftpb.EntryNormal:
			if len(entries[i].Data) == 0 {
				break
			}
			n.metrics.entriesApplied.Add(1)
			n.logBytes += len(entries[i].Data)

			var batch service.NodeLogs
			if err := batch.UnmarshalVT(entries[i].Data); err != nil {
				n.logger.Error(fmt.Sprintf("failed to unmarshal committed log: %v", err))
				break
			}
			for _, l := range batch.GetLogs() {
				n.metrics.logsApplied.Add(1)
				n.applyLog(l)
			}
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			err := cc.Unmarshal(entries[i].Data)
			if err != nil {
				log.Fatalf("failed to unmarshal conf change: %v", err)
			}
			n.confState = *n.node.ApplyConfChange(cc)
			n.setMembers(n.confState.Voters)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					n.peersMu.Lock()
					n.peers[cc.NodeID] = string(cc.Context)
					n.addedAt[cc.NodeID] = entries[i].Index
					// a node which left can join again with the same id
					delete(n.removed, cc.NodeID)
					n.peersMu.Unlock()
					n.logger.Info(fmt.Sprintf("node %d joined the cluster at %s", cc.NodeID, string(cc.Context)))
					n.notifyClients(false, true)
				}
			case raftpb.ConfChangeRemoveNode:
				n.applyRemoveNode(cc.NodeID, entries[i].Index)
			}
		}

		n.appliedIndex = entries[len(entries)-1].Index
	}

	return true
}

func (n *Node[T, K]) getPeerClient(peerId uint64) service.NodeClient {
	n.peersMu.Lock()
	defer n.peersMu.Unlock()

	client, ok := n.peerClients[peerId]
	if !ok {
		if listenAddr, ok := n.peers[peerId]; !ok {
			debugf(n.logger, "error peer not found %d", peerId)
			return nil
		} else {
			c, err := grpc.NewClient(listenAddr, n.dialOptions(peerId)...)
			if err != nil {
				n.logger.Error(fmt.Sprintf("error creating connection to peer %d: %v", peerId, err))
				return nil
			}
			client = service.NewNodeClient(c)
			n.peerClients[peerId] = client
		}
	}

	return client
}

func (n *Node[T, K]) hasPeer(id uint64) bool {
	n.peersMu.RLock()
	defer n.peersMu.RUnlock()

	_, ok := n.peers[id]
	return ok
}

// peerClientsSnapshot returns clients for all peers except this node.
func (n *Node[T, K]) peerClientsSnapshot() map[uint64]service.NodeClient {
	n.peersMu.RLock()
	ids := make([]uint64, 0, len(n.peers))
	for id := range n.peers {
		if id != uint64(n.id) {
			ids = append(ids, id)
		}
	}
	n.peersMu.RUnlock()
	slices.Sort(ids)

	clients := make(map[uint64]service.NodeClient, len(ids))
	for _, id := range ids {
		if client := n.getPeerClient(id); client != nil {
			clients[id] = client
		}
	}
	return clients
}

func (n *Node[T, K]) Process(ctx context.Context, m raftpb.Message) error {
	return n.node.Step(ctx, m)
}
func (n *Node[T, K]) IsIDRemoved(id uint64) bool  { return n.isRemoved(id) }
func (n *Node[T, K]) ReportUnreachable(id uint64) { n.node.ReportUnreachable(id) }
func (n *Node[T, K]) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	n.node.ReportSnapshot(id, status)
}
