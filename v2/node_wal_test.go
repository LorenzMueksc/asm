package asm

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.etcd.io/raft/v3/raftpb"
)

var _ = Describe("WAL", func() {
	var dir string

	entries := func(term uint64, from, to uint64) []raftpb.Entry {
		var es []raftpb.Entry
		for i := from; i <= to; i++ {
			es = append(es, raftpb.Entry{Term: term, Index: i, Data: []byte{byte(i)}})
		}
		return es
	}
	indexes := func(es []raftpb.Entry) []uint64 {
		var is []uint64
		for _, e := range es {
			is = append(is, e.Index)
		}
		return is
	}
	open := func() (*wal, *walState) {
		GinkgoHelper()
		w, state, err := openWAL(dir, &nodeMetrics{})
		Expect(err).NotTo(HaveOccurred())
		return w, state
	}
	segments := func() []string {
		files, err := filepath.Glob(filepath.Join(dir, "wal", "*.wal"))
		Expect(err).NotTo(HaveOccurred())
		return files
	}

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
	})

	It("should start empty and read back what was written", func() {
		w, state := open()
		Expect(state.restored()).To(BeFalse())

		Expect(w.save(walAppend{raftpb.HardState{Term: 1, Vote: 1, Commit: 2}, entries(1, 1, 3)})).To(Succeed())
		Expect(w.setJoinIndex(7)).To(Succeed())
		Expect(w.save(walAppend{raftpb.HardState{Term: 2, Vote: 2, Commit: 3}, entries(2, 4, 5)})).To(Succeed())
		Expect(w.close()).To(Succeed())

		w, state = open()
		defer w.close()
		Expect(state.restored()).To(BeTrue())
		Expect(indexes(state.entries)).To(Equal([]uint64{1, 2, 3, 4, 5}))
		Expect(state.entries[4].Term).To(Equal(uint64(2)))
		Expect(state.hardState).To(Equal(raftpb.HardState{Term: 2, Vote: 2, Commit: 3}))
		Expect(state.joinIndex).To(Equal(uint64(7)))
	})

	It("should replace entries written again with their index and the ones after them", func() {
		w, _ := open()
		Expect(w.save(walAppend{raftpb.HardState{Term: 1, Commit: 2}, entries(1, 1, 5)})).To(Succeed())
		Expect(w.save(walAppend{raftpb.HardState{Term: 2, Commit: 2}, entries(2, 3, 4)})).To(Succeed())
		Expect(w.close()).To(Succeed())

		w, state := open()
		defer w.close()
		Expect(indexes(state.entries)).To(Equal([]uint64{1, 2, 3, 4}))
		Expect(state.entries[2].Term).To(Equal(uint64(2)))
	})

	It("should cut off a torn record at the end and keep the records before it", func() {
		w, _ := open()
		Expect(w.save(walAppend{raftpb.HardState{Term: 1, Commit: 3}, entries(1, 1, 3)})).To(Succeed())
		Expect(w.close()).To(Succeed())

		// a record whose write was cut off: a header claiming more data than follows
		f, err := os.OpenFile(segments()[0], os.O_WRONLY|os.O_APPEND, 0o644)
		Expect(err).NotTo(HaveOccurred())
		_, err = f.Write([]byte{100, 0, 0, 0, 1, 2, 3, 4, 1, 9, 9})
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Close()).To(Succeed())

		w, state := open()
		Expect(indexes(state.entries)).To(Equal([]uint64{1, 2, 3}))
		// writing goes on after the cut
		Expect(w.save(walAppend{raftpb.HardState{Term: 1, Commit: 4}, entries(1, 4, 4)})).To(Succeed())
		Expect(w.close()).To(Succeed())

		w, state = open()
		defer w.close()
		Expect(indexes(state.entries)).To(Equal([]uint64{1, 2, 3, 4}))
		Expect(state.hardState.Commit).To(Equal(uint64(4)))
	})

	It("should fail on a damaged record in a segment before the last one", func() {
		w, _ := open()
		w.segmentSize = 1
		Expect(w.save(walAppend{raftpb.HardState{Term: 1, Commit: 2}, entries(1, 1, 2)})).To(Succeed())
		Expect(w.save(walAppend{raftpb.HardState{Term: 1, Commit: 3}, entries(1, 3, 3)})).To(Succeed())
		Expect(w.close()).To(Succeed())
		Expect(len(segments())).To(BeNumerically(">=", 2))

		b, err := os.ReadFile(segments()[0])
		Expect(err).NotTo(HaveOccurred())
		b[len(b)-1] ^= 0xff
		Expect(os.WriteFile(segments()[0], b, 0o644)).To(Succeed())

		_, _, err = openWAL(dir, &nodeMetrics{})
		Expect(err).To(MatchError(ContainSubstring("damaged record")))
	})

	It("should start new segments with the hard state and drop the segments a snapshot covers", func() {
		w, _ := open()
		w.segmentSize = 1
		Expect(w.setJoinIndex(3)).To(Succeed())
		for i := uint64(1); i <= 6; i++ {
			Expect(w.save(walAppend{raftpb.HardState{Term: 1, Commit: i}, entries(1, i, i)})).To(Succeed())
		}
		before := len(segments())

		snap := raftpb.Snapshot{Data: []byte("state"), Metadata: raftpb.SnapshotMetadata{Index: 4, Term: 1}}
		Expect(w.saveSnapshot(snap, false)).To(Succeed())
		Expect(len(segments())).To(BeNumerically("<", before))
		Expect(w.close()).To(Succeed())

		w, state := open()
		defer w.close()
		Expect(state.snapshot.Metadata.Index).To(Equal(uint64(4)))
		Expect(state.snapshot.Data).To(Equal([]byte("state")))
		Expect(indexes(state.entries)).To(Equal([]uint64{5, 6}))
		Expect(state.hardState.Commit).To(Equal(uint64(6)))
		Expect(state.joinIndex).To(Equal(uint64(3)))
	})

	It("should drop the entries before a snapshot which replaced the log", func() {
		w, _ := open()
		Expect(w.save(walAppend{raftpb.HardState{Term: 1, Commit: 2}, entries(1, 1, 5)})).To(Succeed())

		// the leader sent a snapshot at 20: the entries 3 to 5 written before don't belong to the log anymore
		snap := raftpb.Snapshot{Data: []byte("leader"), Metadata: raftpb.SnapshotMetadata{Index: 20, Term: 3}}
		Expect(w.saveSnapshot(snap, true)).To(Succeed())
		Expect(w.save(walAppend{raftpb.HardState{Term: 3, Commit: 21}, entries(3, 21, 22)})).To(Succeed())
		Expect(w.close()).To(Succeed())

		w, state := open()
		defer w.close()
		Expect(state.snapshot.Metadata.Index).To(Equal(uint64(20)))
		Expect(indexes(state.entries)).To(Equal([]uint64{21, 22}))
	})

	It("should keep a received snapshot if an older local one is saved after it", func() {
		w, _ := open()
		Expect(w.save(walAppend{raftpb.HardState{Term: 1, Commit: 5}, entries(1, 1, 5)})).To(Succeed())

		// the apply goroutine took a snapshot at 5 and saves it after the append goroutine saved one of the leader
		received := raftpb.Snapshot{Data: []byte("leader"), Metadata: raftpb.SnapshotMetadata{Index: 20, Term: 3}}
		local := raftpb.Snapshot{Data: []byte("local"), Metadata: raftpb.SnapshotMetadata{Index: 5, Term: 1}}
		Expect(w.saveSnapshot(received, true)).To(Succeed())
		Expect(w.saveSnapshot(local, false)).To(Succeed())
		Expect(w.save(walAppend{raftpb.HardState{Term: 3, Commit: 21}, entries(3, 21, 21)})).To(Succeed())
		Expect(w.close()).To(Succeed())

		files, err := filepath.Glob(filepath.Join(dir, "snap", "*"))
		Expect(err).NotTo(HaveOccurred())
		Expect(files).To(HaveLen(1))
		w, state := open()
		defer w.close()
		Expect(state.snapshot.Data).To(Equal([]byte("leader")))
		Expect(indexes(state.entries)).To(Equal([]uint64{21}))
	})

	It("should write several appends with the last hard state", func() {
		w, _ := open()
		Expect(w.save(
			walAppend{raftpb.HardState{Term: 1, Vote: 1, Commit: 1}, entries(1, 1, 2)},
			walAppend{entries: entries(1, 3, 3)},
			walAppend{raftpb.HardState{Term: 1, Vote: 1, Commit: 3}, nil},
		)).To(Succeed())
		Expect(w.close()).To(Succeed())

		w, state := open()
		defer w.close()
		Expect(indexes(state.entries)).To(Equal([]uint64{1, 2, 3}))
		Expect(state.hardState).To(Equal(raftpb.HardState{Term: 1, Vote: 1, Commit: 3}))
	})

	It("should keep a hard state which only moved the commit index buffered until the next write", func() {
		w, _ := open()
		Expect(w.save(walAppend{raftpb.HardState{Term: 1, Vote: 1, Commit: 1}, entries(1, 1, 3)})).To(Succeed())
		size := func() int64 {
			GinkgoHelper()
			info, err := os.Stat(segments()[0])
			Expect(err).NotTo(HaveOccurred())
			return info.Size()
		}
		written := size()

		Expect(w.save(walAppend{hardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 3}})).To(Succeed())
		Expect(size()).To(Equal(written))

		// a new vote is durable at once, with the commit index buffered before
		Expect(w.save(walAppend{hardState: raftpb.HardState{Term: 2, Vote: 2, Commit: 3}})).To(Succeed())
		Expect(size()).To(BeNumerically(">", written))
		Expect(w.close()).To(Succeed())

		w, state := open()
		defer w.close()
		Expect(state.hardState).To(Equal(raftpb.HardState{Term: 2, Vote: 2, Commit: 3}))
	})

	It("should keep the commit index between the snapshot and the last entry", func() {
		w, _ := open()
		Expect(w.save(walAppend{raftpb.HardState{Term: 1, Commit: 9}, entries(1, 1, 3)})).To(Succeed())
		Expect(w.close()).To(Succeed())

		w, state := open()
		Expect(state.hardState.Commit).To(Equal(uint64(3)))
		// a snapshot newer than the hard state: written right before a crash
		snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 10, Term: 4}}
		Expect(w.saveSnapshot(snap, true)).To(Succeed())
		Expect(w.close()).To(Succeed())

		w, state = open()
		defer w.close()
		Expect(state.entries).To(BeEmpty())
		Expect(state.hardState.Commit).To(Equal(uint64(10)))
		Expect(state.hardState.Term).To(Equal(uint64(4)))
		Expect(state.hardState.Vote).To(BeZero())
	})
})
