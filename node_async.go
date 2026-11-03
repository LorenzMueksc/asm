package asm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// Raft members with a data directory use raft's AsyncStorageWrites: the ready loop neither writes nor applies, it
// sends the messages to the peers and hands the local storage messages to two goroutines:
//
//   - MsgStorageAppend goes to the append goroutine. It writes everything queued meanwhile to the wal with one
//     durable write (group commit), puts the entries into the MemoryStorage and then delivers the responses: to
//     raft through Step (stable entries, the leader's own ack, votes), to the peers by sendMessages (a follower's
//     acks).
//   - MsgStorageApply goes to the apply goroutine. It applies the committed entries, takes snapshots and steps the
//     responses.
//
// So raft keeps exchanging messages while the disk writes, and writing, sending and applying overlap.
// A snapshot received from the leader is written and put into the MemoryStorage by the append goroutine, which
// queues its restore for the apply goroutine. raft hands out no committed entries while a snapshot isn't stable,
// so the restore comes after the entries before the snapshot and before the ones after it.
// Members without a data directory keep the synchronous Ready/Advance loop (serveChannels).

// storageQueueSize limits the messages queued for the append and the apply goroutine; a full queue blocks the
// ready loop
const storageQueueSize = 256

type (
	// storageApply is an item of the apply goroutine: committed entries (MsgStorageApply), or a snapshot received
	// from the leader to restore
	storageApply struct {
		msg      raftpb.Message
		snapshot *raftpb.Snapshot
	}
)

// serveChannelsAsync is the ready loop with AsyncStorageWrites.
func (n *Node[T, K]) serveChannelsAsync() {
	appendC := make(chan raftpb.Message, storageQueueSize)
	applyC := make(chan storageApply, storageQueueSize)
	n.storageWG.Add(2)
	go n.runAppends(appendC, applyC)
	go n.runApplies(applyC)

	var network []raftpb.Message
	for {
		select {
		case rd := <-n.node.Ready():
			n.updateLeader(rd)

			// the leader's appends go out at once, its own write is queued meanwhile (raft thesis 10.2.1)
			network = network[:0]
			for _, m := range rd.Messages {
				if !raft.IsLocalMsgTarget(m.To) {
					network = append(network, m)
				}
			}
			n.sendMessages(network)

			for _, m := range rd.Messages {
				if !raft.IsLocalMsgTarget(m.To) {
					continue
				}
				// only the queue of the target is set, a send on a nil channel never proceeds
				var appendQ chan<- raftpb.Message
				var applyQ chan<- storageApply
				if m.To == raft.LocalAppendThread {
					appendQ = appendC
				} else {
					applyQ = applyC
				}
				select {
				case appendQ <- m:
				case applyQ <- storageApply{msg: m}:
				case <-n.stopC:
					n.stop()
					return
				case <-n.failedC:
					n.stop()
					return
				case <-n.raftStopC:
					return
				}
			}
		case <-n.stopC:
			n.stop()
			return
		case <-n.failedC:
			n.stop()
			return
		}
	}
}

// runAppends writes the queued MsgStorageAppend messages and delivers their responses once they are durable.
func (n *Node[T, K]) runAppends(appendC <-chan raftpb.Message, applyC chan<- storageApply) {
	defer n.storageWG.Done()

	batch := make([]raftpb.Message, 0, storageQueueSize)
	for {
		batch = batch[:0]
		select {
		case m := <-appendC:
			batch = append(batch, m)
		case <-n.raftStopC:
			return
		}
		// what was queued while the last write ran goes into this one
	drain:
		for len(batch) < cap(batch) {
			select {
			case m := <-appendC:
				batch = append(batch, m)
			default:
				break drain
			}
		}

		if err := n.appendBatch(batch, applyC); err != nil {
			if !errors.Is(err, ErrNodeStopped) {
				n.fail(fmt.Errorf("writing the raft log to %s: %w", n.dataDir, err))
			}
			return
		}
		if !n.deliverResponses(batch) {
			return
		}
	}
}

// appendBatch writes the snapshots, entries and hard states of the messages to the wal and the MemoryStorage.
func (n *Node[T, K]) appendBatch(batch []raftpb.Message, applyC chan<- storageApply) error {
	appends := make([]walAppend, 0, len(batch))
	for i := range batch {
		m := &batch[i]
		if m.Snapshot != nil {
			// the entries of the messages before go into the wal before the snapshot marker
			if err := n.wal.save(appends...); err != nil {
				return err
			}
			appends = appends[:0]
			if err := n.wal.saveSnapshot(*m.Snapshot, true); err != nil {
				return err
			}
			if err := n.storage.ApplySnapshot(*m.Snapshot); err != nil {
				return fmt.Errorf("applying the snapshot at index %d to the storage: %w", m.Snapshot.Metadata.Index, err)
			}
			select {
			case applyC <- storageApply{snapshot: m.Snapshot}:
			case <-n.raftStopC:
				return ErrNodeStopped
			}
		}
		// the fields of the hard state are only set if it changed
		appends = append(appends, walAppend{
			hardState: raftpb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit},
			entries:   m.Entries,
		})
		// raft reads entries from the storage only once they are stable, so they may go in before the write
		if err := n.storage.Append(m.Entries); err != nil {
			return fmt.Errorf("appending entries to the storage: %w", err)
		}
	}
	return n.wal.save(appends...)
}

// runApplies applies the committed entries and restores the received snapshots, in order.
func (n *Node[T, K]) runApplies(applyC <-chan storageApply) {
	defer n.storageWG.Done()

	batch := make([]storageApply, 0, storageQueueSize)
	applied := make([]raftpb.Message, 0, storageQueueSize)
	for {
		batch = batch[:0]
		select {
		case a := <-applyC:
			batch = append(batch, a)
		case <-n.raftStopC:
			return
		}
	drain:
		for len(batch) < cap(batch) {
			select {
			case a := <-applyC:
				batch = append(batch, a)
			default:
				break drain
			}
		}

		applied = applied[:0]
		var applyTime time.Duration
		for _, a := range batch {
			if a.snapshot != nil {
				if err := n.restoreSnapshot(*a.snapshot); err != nil {
					n.fail(err)
				}
				continue
			}
			start := time.Now()
			n.publishEntries(n.entriesToApply(a.msg.Entries))
			applyTime += time.Since(start)
			applied = append(applied, a.msg)
		}
		n.metrics.applyNanos.Add(uint64(applyTime))
		n.maybeSnapshot()

		if !n.deliverResponses(applied) {
			return
		}
	}
}

// deliverResponses hands the responses of local storage messages to the peers and to raft.
// It returns false once raft stopped.
func (n *Node[T, K]) deliverResponses(msgs []raftpb.Message) bool {
	var network []raftpb.Message
	for i := range msgs {
		for _, r := range msgs[i].Responses {
			if r.To != uint64(n.id) {
				network = append(network, r)
			}
		}
	}
	n.sendMessages(network)

	for i := range msgs {
		for _, r := range msgs[i].Responses {
			if r.To != uint64(n.id) {
				continue
			}
			if err := n.node.Step(context.Background(), r); err != nil {
				return false
			}
		}
	}
	return true
}
