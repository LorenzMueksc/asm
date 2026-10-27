package asm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// Raft members with a data directory (WithDataDir) persist their raft state, so they restart after a crash or a
// shutdown and catch up with the cluster instead of joining again:
//
//	<dir>/wal/<seq>.wal               segments of records: entries, hard states, snapshot markers, the join index
//	<dir>/snap/<term>-<index>.snap    the latest snapshot
//
// A record is [length uint32][crc32c uint32][type byte][data], little endian; length counts type and data, the
// checksum covers them. The append goroutine (node_async.go) writes the records of the raft Readys queued meanwhile
// with one write and delivers their responses once it returned. Segments are opened write-through (walSyncFlag), so
// a write is durable once it returns, without an fsync.
// A segment is closed at walSegmentSize; every segment starts with the current hard state and join index, so older
// segments are deleted once a snapshot covers their entries. A torn record at the end of the last segment (a crash
// while writing) is cut off on restart; any other damaged record fails the start.
//
// A snapshot received from the leader replaces the whole log, so a snapshot marker in the wal drops the entries
// before it. Snapshot files are written to a temporary file and renamed, so the latest one is always complete.

const (
	walSegmentSize = 64 << 20
	// walMaxRecord limits the length a record header may claim, a larger one is damaged
	walMaxRecord = 256 << 20
)

type walRecordType byte

const (
	walEntry walRecordType = iota + 1
	walHardState
	walSnapshot
	walJoinIndex
)

var walCrcTable = crc32.MakeTable(crc32.Castagnoli)

type (
	// wal is the write ahead log of a raft member. It is written by the append goroutine, snapshots by the apply
	// goroutine too, the join index by the joining goroutine, so it has a mutex.
	wal struct {
		mu sync.Mutex
		// snapMu serializes writing snapshot files, which happens outside mu so appends go on meanwhile.
		// snapIndex is the index of the latest snapshot written, guarded by mu.
		snapMu    sync.Mutex
		snapIndex uint64
		dir       string
		segments  []walSegment
		f         *os.File
		w         *bufio.Writer
		size      int64
		// segmentSize is the size at which a new segment starts (walSegmentSize)
		segmentSize int64
		// hardState and joinIndex are written at the start of every segment
		hardState raftpb.HardState
		joinIndex uint64
		metrics   *nodeMetrics
		buf       []byte
		closed    bool
	}
	// walAppend is what one MsgStorageAppend writes: entries and the hard state, empty if unchanged
	walAppend struct {
		hardState raftpb.HardState
		entries   []raftpb.Entry
	}
	// walSegment is a segment file; lastIndex is the highest entry index in it
	walSegment struct {
		seq       uint64
		lastIndex uint64
	}
	// walState is the raft state read from the data directory
	walState struct {
		snapshot  raftpb.Snapshot
		hardState raftpb.HardState
		entries   []raftpb.Entry
		joinIndex uint64
	}
)

// restored reports whether the data directory held raft state.
func (s *walState) restored() bool {
	return !raft.IsEmptySnap(s.snapshot) || len(s.entries) > 0 || !raft.IsEmptyHardState(s.hardState)
}

// openWAL reads the raft state in the directory, creating it if needed, and opens the wal for appending.
func openWAL(dir string, metrics *nodeMetrics) (*wal, *walState, error) {
	for _, sub := range []string{"wal", "snap"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, nil, fmt.Errorf("creating the data directory: %w", err)
		}
	}
	w := &wal{dir: dir, metrics: metrics, segmentSize: walSegmentSize}

	state := &walState{}
	snap, err := w.loadSnapshot()
	if err != nil {
		return nil, nil, err
	}
	state.snapshot = snap
	w.snapIndex = snap.Metadata.Index

	seqs, err := w.segmentSeqs()
	if err != nil {
		return nil, nil, err
	}
	if err := w.readSegments(seqs, state); err != nil {
		return nil, nil, err
	}

	// the hard state and the snapshot may be from different Readys if the node crashed in between: the commit
	// index must lie between the snapshot and the last entry, the term can't be older than the snapshot's
	snapIndex, snapTerm := snap.Metadata.Index, snap.Metadata.Term
	lastIndex := snapIndex
	if len(state.entries) > 0 {
		lastIndex = state.entries[len(state.entries)-1].Index
	}
	if !raft.IsEmptyHardState(state.hardState) || !raft.IsEmptySnap(snap) {
		state.hardState.Commit = min(max(state.hardState.Commit, snapIndex), lastIndex)
		if state.hardState.Term < snapTerm {
			state.hardState.Term, state.hardState.Vote = snapTerm, 0
		}
	}
	w.hardState = state.hardState
	w.joinIndex = state.joinIndex

	if len(w.segments) == 0 {
		if err := w.newSegment(); err != nil {
			return nil, nil, err
		}
	} else if err := w.openLast(); err != nil {
		return nil, nil, err
	}
	return w, state, nil
}

func (w *wal) segmentPath(seq uint64) string {
	return filepath.Join(w.dir, "wal", fmt.Sprintf("%016x.wal", seq))
}

// segmentSeqs returns the sequence numbers of the segment files, in order.
func (w *wal) segmentSeqs() ([]uint64, error) {
	files, err := os.ReadDir(filepath.Join(w.dir, "wal"))
	if err != nil {
		return nil, err
	}
	var seqs []uint64
	for _, f := range files {
		var seq uint64
		if _, err := fmt.Sscanf(f.Name(), "%016x.wal", &seq); err == nil && strings.HasSuffix(f.Name(), ".wal") {
			seqs = append(seqs, seq)
		}
	}
	slices.Sort(seqs)
	return seqs, nil
}

// readSegments reads the records of the segments into the state.
func (w *wal) readSegments(seqs []uint64, state *walState) error {
	floor := state.snapshot.Metadata.Index
	marker := uint64(0)
	for i, seq := range seqs {
		last := i == len(seqs)-1
		seg := walSegment{seq: seq}
		valid, err := readWALFile(w.segmentPath(seq), last, func(t walRecordType, data []byte) error {
			switch t {
			case walEntry:
				var e raftpb.Entry
				if err := e.Unmarshal(data); err != nil {
					return err
				}
				seg.lastIndex = max(seg.lastIndex, e.Index)
				if e.Index <= floor {
					return nil
				}
				switch {
				case len(state.entries) == 0 && e.Index != floor+1:
					return fmt.Errorf("entry %d follows index %d, entries are missing", e.Index, floor)
				case len(state.entries) > 0 && e.Index > state.entries[len(state.entries)-1].Index+1:
					return fmt.Errorf("entry %d follows entry %d, entries are missing", e.Index, state.entries[len(state.entries)-1].Index)
				case len(state.entries) > 0:
					// a later entry with an index already written replaces it and the entries after it
					state.entries = state.entries[:e.Index-state.entries[0].Index]
				}
				state.entries = append(state.entries, e)
			case walHardState:
				var hs raftpb.HardState
				if err := hs.Unmarshal(data); err != nil {
					return err
				}
				state.hardState = hs
			case walSnapshot:
				if len(data) != 16 {
					return errors.New("invalid snapshot marker")
				}
				// raft replaced the log with the snapshot, the entries before don't count anymore
				marker = binary.LittleEndian.Uint64(data)
				if marker > state.snapshot.Metadata.Index {
					return fmt.Errorf("the snapshot at index %d is missing", marker)
				}
				if marker > floor || len(state.entries) > 0 && state.entries[0].Index <= marker {
					state.entries = nil
					floor = max(floor, marker)
				}
			case walJoinIndex:
				if len(data) != 8 {
					return errors.New("invalid join index")
				}
				state.joinIndex = binary.LittleEndian.Uint64(data)
			default:
				return fmt.Errorf("unknown record type %d", t)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("reading %s: %w", w.segmentPath(seq), err)
		}
		if last {
			// cut off a torn record at the end
			if info, err := os.Stat(w.segmentPath(seq)); err == nil && info.Size() > valid {
				if err := os.Truncate(w.segmentPath(seq), valid); err != nil {
					return fmt.Errorf("cutting off the torn end of %s: %w", w.segmentPath(seq), err)
				}
			}
			w.size = valid
		}
		w.segments = append(w.segments, seg)
	}
	return nil
}

// readWALFile calls f with every record of the file and returns the size of the valid records. A damaged record
// ends the file if it is the last one (torn write), otherwise it is an error.
func readWALFile(path string, last bool, f func(t walRecordType, data []byte) error) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	r := bufio.NewReaderSize(file, 1<<20)
	var valid int64
	var header [8]byte
	var data []byte
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if err == io.EOF {
				return valid, nil
			}
			if last {
				return valid, nil
			}
			return 0, fmt.Errorf("damaged record at offset %d: %w", valid, err)
		}
		length := binary.LittleEndian.Uint32(header[0:4])
		sum := binary.LittleEndian.Uint32(header[4:8])
		if length == 0 || length > walMaxRecord {
			if last {
				return valid, nil
			}
			return 0, fmt.Errorf("damaged record at offset %d: invalid length %d", valid, length)
		}
		if cap(data) < int(length) {
			data = make([]byte, length)
		}
		data = data[:length]
		if _, err := io.ReadFull(r, data); err != nil || crc32.Checksum(data, walCrcTable) != sum {
			if last {
				return valid, nil
			}
			return 0, fmt.Errorf("damaged record at offset %d", valid)
		}
		if err := f(walRecordType(data[0]), data[1:]); err != nil {
			return 0, fmt.Errorf("record at offset %d: %w", valid, err)
		}
		valid += 8 + int64(length)
	}
}

// openLast opens the last segment for appending.
func (w *wal) openLast() error {
	seg := w.segments[len(w.segments)-1]
	f, err := os.OpenFile(w.segmentPath(seg.seq), os.O_WRONLY|os.O_APPEND|walSyncFlag, 0o644)
	if err != nil {
		return err
	}
	w.f, w.w = f, bufio.NewWriterSize(f, 1<<20)
	return nil
}

// newSegment closes the current segment and starts the next one with the hard state and the join index.
func (w *wal) newSegment() error {
	if w.f != nil {
		if err := w.w.Flush(); err != nil {
			return err
		}
		if err := w.f.Close(); err != nil {
			return err
		}
	}
	seq := uint64(1)
	if len(w.segments) > 0 {
		seq = w.segments[len(w.segments)-1].seq + 1
	}
	f, err := os.OpenFile(w.segmentPath(seq), os.O_WRONLY|os.O_CREATE|os.O_EXCL|walSyncFlag, 0o644)
	if err != nil {
		return err
	}
	w.f, w.w, w.size = f, bufio.NewWriterSize(f, 1<<20), 0
	w.segments = append(w.segments, walSegment{seq: seq})

	if !raft.IsEmptyHardState(w.hardState) {
		if err := w.writeHardState(w.hardState); err != nil {
			return err
		}
	}
	if w.joinIndex > 0 {
		if err := w.writeJoinIndex(w.joinIndex); err != nil {
			return err
		}
	}
	if err := w.sync(); err != nil {
		return err
	}
	syncDir(filepath.Join(w.dir, "wal"))
	return nil
}

// write appends a record to the current segment, without syncing.
func (w *wal) write(t walRecordType, size int, marshal func([]byte) (int, error)) error {
	need := 9 + size
	if cap(w.buf) < need {
		w.buf = make([]byte, need)
	}
	buf := w.buf[:need]
	buf[8] = byte(t)
	n, err := marshal(buf[9:])
	if err != nil {
		return err
	}
	buf = buf[:9+n]
	binary.LittleEndian.PutUint32(buf[0:4], uint32(1+n))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.Checksum(buf[8:], walCrcTable))
	if _, err := w.w.Write(buf); err != nil {
		return err
	}
	w.size += int64(len(buf))
	w.metrics.walBytes.Add(uint64(len(buf)))
	return nil
}

func (w *wal) writeHardState(hs raftpb.HardState) error {
	return w.write(walHardState, hs.Size(), hs.MarshalTo)
}

func (w *wal) writeJoinIndex(index uint64) error {
	return w.write(walJoinIndex, 8, func(b []byte) (int, error) {
		binary.LittleEndian.PutUint64(b, index)
		return 8, nil
	})
}

// sync writes the buffered records to the segment, durable once it returns (write-through).
func (w *wal) sync() error {
	start := time.Now()
	err := w.w.Flush()
	w.metrics.walSync.observe(time.Since(start))
	return err
}

// save writes the entries of the appends in order and the last hard state with one durable write (write-through).
// The earlier hard states don't need a record: nothing waits for them before the whole write returned.
// Without entries, a hard state which only moved the commit index needs no durable write (raft.MustSync): its
// record stays buffered and goes out with the next write. A lost commit index is learned from the leader again.
func (w *wal) save(appends ...walAppend) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrNodeStopped
	}

	mustSync := false
	var hs raftpb.HardState
	seg := &w.segments[len(w.segments)-1]
	for _, a := range appends {
		for i := range a.entries {
			if err := w.write(walEntry, a.entries[i].Size(), a.entries[i].MarshalTo); err != nil {
				return err
			}
			seg.lastIndex = max(seg.lastIndex, a.entries[i].Index)
			mustSync = true
		}
		if !raft.IsEmptyHardState(a.hardState) {
			hs = a.hardState
		}
	}
	if !raft.IsEmptyHardState(hs) {
		mustSync = mustSync || hs.Term != w.hardState.Term || hs.Vote != w.hardState.Vote
		if err := w.writeHardState(hs); err != nil {
			return err
		}
		w.hardState = hs
	}
	if !mustSync {
		return nil
	}
	if err := w.sync(); err != nil {
		return err
	}
	if w.size >= w.segmentSize {
		return w.newSegment()
	}
	return nil
}

// setJoinIndex records the index of the entry which added this node to the cluster.
func (w *wal) setJoinIndex(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrNodeStopped
	}
	w.joinIndex = index
	if err := w.writeJoinIndex(index); err != nil {
		return err
	}
	return w.sync()
}

func (w *wal) snapPath(term, index uint64) string {
	return filepath.Join(w.dir, "snap", fmt.Sprintf("%016x-%016x.snap", term, index))
}

// saveSnapshot writes the snapshot file, drops older snapshots and the segments the snapshot covers. A snapshot
// received from the leader (replacesLog) also gets a marker in the wal: raft replaced the log with it.
func (w *wal) saveSnapshot(snap raftpb.Snapshot, replacesLog bool) error {
	w.snapMu.Lock()
	defer w.snapMu.Unlock()
	w.mu.Lock()
	closed := w.closed
	w.mu.Unlock()
	if closed {
		return ErrNodeStopped
	}

	data, err := snap.Marshal()
	if err != nil {
		return err
	}
	path := w.snapPath(snap.Metadata.Term, snap.Metadata.Index)
	if err := writeFileSynced(path, data); err != nil {
		return fmt.Errorf("writing snapshot %s: %w", path, err)
	}
	w.metrics.walBytes.Add(uint64(len(data)))

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrNodeStopped
	}
	// a snapshot the apply goroutine took may come after a newer one received from the leader
	if snap.Metadata.Index < w.snapIndex {
		_ = os.Remove(path)
		return nil
	}
	w.snapIndex = snap.Metadata.Index

	if replacesLog {
		err := w.write(walSnapshot, 16, func(b []byte) (int, error) {
			binary.LittleEndian.PutUint64(b[0:8], snap.Metadata.Index)
			binary.LittleEndian.PutUint64(b[8:16], snap.Metadata.Term)
			return 16, nil
		})
		if err != nil {
			return err
		}
		if err := w.sync(); err != nil {
			return err
		}
	}

	// older snapshots and the segments before the first one with entries after the snapshot aren't needed anymore.
	// Only a prefix of the segments goes: a later segment may replace entries of an earlier one
	files, _ := os.ReadDir(filepath.Join(w.dir, "snap"))
	for _, f := range files {
		if p := filepath.Join(w.dir, "snap", f.Name()); p != path {
			_ = os.Remove(p)
		}
	}
	drop := 0
	for drop < len(w.segments)-1 && w.segments[drop].lastIndex <= snap.Metadata.Index {
		drop++
	}
	for _, seg := range w.segments[:drop] {
		if err := os.Remove(w.segmentPath(seg.seq)); err != nil {
			return err
		}
	}
	w.segments = slices.Delete(w.segments, 0, drop)
	return nil
}

// loadSnapshot returns the latest snapshot in the directory, an empty one if there is none.
func (w *wal) loadSnapshot() (raftpb.Snapshot, error) {
	files, err := os.ReadDir(filepath.Join(w.dir, "snap"))
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	var latest string
	var latestIndex uint64
	for _, f := range files {
		var term, index uint64
		if !strings.HasSuffix(f.Name(), ".snap") {
			// a temporary file of a snapshot which wasn't complete
			_ = os.Remove(filepath.Join(w.dir, "snap", f.Name()))
			continue
		}
		if _, err := fmt.Sscanf(f.Name(), "%016x-%016x.snap", &term, &index); err != nil {
			continue
		}
		if latest == "" || index > latestIndex {
			latest, latestIndex = f.Name(), index
		}
	}
	if latest == "" {
		return raftpb.Snapshot{}, nil
	}

	path := filepath.Join(w.dir, "snap", latest)
	data, err := readFileChecked(path)
	if err != nil {
		return raftpb.Snapshot{}, fmt.Errorf("reading snapshot %s: %w", path, err)
	}
	var snap raftpb.Snapshot
	if err := snap.Unmarshal(data); err != nil {
		return raftpb.Snapshot{}, fmt.Errorf("decoding snapshot %s: %w", path, err)
	}
	return snap, nil
}

// close syncs and closes the wal.
func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	err := w.sync()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// writeFileSynced writes [crc32c][data] to a temporary file, syncs it and renames it to the path.
func writeFileSynced(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], crc32.Checksum(data, walCrcTable))
	_, err = f.Write(header[:])
	if err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

// readFileChecked reads a file written by writeFileSynced and verifies its checksum.
func readFileChecked(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) < 4 || crc32.Checksum(b[4:], walCrcTable) != binary.LittleEndian.Uint32(b[:4]) {
		return nil, errors.New("checksum mismatch")
	}
	return b[4:], nil
}

// syncDir syncs a directory, so created and renamed files in it survive a crash. Not every platform supports
// it (Windows), errors are ignored.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

// openWAL opens the data directory of the node and restores the raft state in it, if there is one: the snapshot,
// the hard state and the entries go into the storage, the state the snapshot describes is restored. raft delivers
// the committed entries after the snapshot again, which rebuilds the rest.
func (n *Node[T, K]) openWAL(c *raft.Config) error {
	w, state, err := openWAL(n.dataDir, &n.metrics)
	if err != nil {
		return fmt.Errorf("opening the data directory %s: %w", n.dataDir, err)
	}
	n.wal = w
	if !state.restored() {
		return nil
	}

	// removals of this id up to the join index are of an earlier node with the same id
	n.selfMu.Lock()
	n.joined = true
	n.joinIndex = state.joinIndex
	n.selfMu.Unlock()
	n.restarted = true

	if !raft.IsEmptySnap(state.snapshot) {
		if err := n.storage.ApplySnapshot(state.snapshot); err != nil {
			return err
		}
		if err := n.restoreSnapshot(state.snapshot); err != nil {
			return err
		}
		c.Applied = state.snapshot.Metadata.Index
	}
	if err := n.storage.SetHardState(state.hardState); err != nil {
		return err
	}
	if err := n.storage.Append(state.entries); err != nil {
		return err
	}
	n.logger.Info(fmt.Sprintf("restarting from %s: snapshot at index %d, %d entries, committed up to %d",
		n.dataDir, state.snapshot.Metadata.Index, len(state.entries), state.hardState.Commit))
	return nil
}
