package asm

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cluster persistence", Label("cluster"), func() {
	var tc *testCluster

	bitmap := func() *Bitmap { return clusterBitmap("pending", "done") }
	holder := func(n *clusterNode, id string) string {
		l, held, _ := n.locks.get(id)
		if !held {
			return ""
		}
		return l.holder
	}
	startThree := func() {
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), nil))
		expectReady(tc.start(3, tc.addr(1), bitmap(), nil))
	}
	startContainer := func(n *clusterNode, id string) {
		GinkgoHelper()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _, err := n.StartContainer(ctx, id)
		Expect(err).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		tc = newTestCluster()
		tc.dataDir = GinkgoT().TempDir()
	})

	AfterEach(func() {
		// the data directories are removed after this, their files must be closed
		tc.killAll()
	})

	It("should restart a member from its data directory and release the locks of its previous life", func() {
		startThree()
		startContainer(tc.node(2), "p-1")
		Eventually(func() string { return holder(tc.node(1), "p-1") }, 5*time.Second).Should(Equal("2"))

		By("killing node 2, its lock stays in the log")
		tc.kill(2)
		Consistently(func() string { return holder(tc.node(1), "p-1") }, time.Second).Should(Equal("2"))

		By("restarting node 2 from its data directory, without joining")
		n := tc.start(2, tc.addr(1), bitmap(), nil)
		expectReady(n)
		Expect(n.restarted).To(BeTrue())
		Expect(peerIds(n)).To(ConsistOf(uint64(1), uint64(2), uint64(3)))
		Expect(n.layout.Load().Children).To(HaveLen(len(tc.node(1).layout.Load().Children)))

		By("releasing the lock of its previous life")
		Eventually(func() string { return holder(tc.node(1), "p-1") }, 10*time.Second).Should(BeEmpty())
		startContainer(tc.node(3), "p-1")
		Eventually(func() string { return holder(n, "p-1") }, 5*time.Second).Should(Equal("3"))
	})

	It("should restart the whole cluster from snapshots and the wal", func() {
		tc.opts = []NodeSetupOption{WithSnapshotCount(20, 5), WithSnapshotInterval(0)}
		startThree()
		for i := range 40 {
			startContainer(tc.node(1), fmt.Sprintf("c-%d", i))
		}
		Eventually(func() string { return holder(tc.node(3), "c-39") }, 5*time.Second).Should(Equal("1"))
		for id := 1; id <= 3; id++ {
			Eventually(func() []string {
				files, _ := filepath.Glob(filepath.Join(tc.dataDir, strconv.Itoa(id), "snap", "*.snap"))
				return files
			}, 10*time.Second).ShouldNot(BeEmpty(), "snapshot of node %d", id)
		}

		By("killing all nodes")
		for id := 1; id <= 3; id++ {
			tc.kill(id)
		}

		By("restarting them from their data directories")
		for id := 1; id <= 3; id++ {
			tc.start(id, "", bitmap(), nil)
		}
		for id := 1; id <= 3; id++ {
			n := tc.node(id)
			expectReady(n)
			Expect(n.restarted).To(BeTrue())
			Expect(peerIds(n)).To(ConsistOf(uint64(1), uint64(2), uint64(3)))
		}
		Eventually(tc.leader, 10*time.Second).ShouldNot(BeZero())

		By("handing the locks node 1 held before on")
		Eventually(func() string { return holder(tc.node(2), "c-0") }, 10*time.Second).Should(BeEmpty())
		startContainer(tc.node(2), "c-0")
		startContainer(tc.node(3), "c-39")
	})

	It("should catch up a restarted member with a snapshot of the leader and restart it from that", func() {
		tc.opts = []NodeSetupOption{WithSnapshotCount(20, 5), WithSnapshotInterval(0)}
		startThree()
		startContainer(tc.node(1), "c-0")
		Eventually(func() string { return holder(tc.node(3), "c-0") }, 5*time.Second).Should(Equal("1"))

		By("killing node 3 while the others compact the log past it")
		tc.kill(3)
		last, err := tc.node(1).storage.LastIndex()
		Expect(err).NotTo(HaveOccurred())
		for i := 1; i <= 40; i++ {
			startContainer(tc.node(1), fmt.Sprintf("c-%d", i))
		}
		Eventually(func() uint64 {
			first, _ := tc.node(1).storage.FirstIndex()
			return first
		}, 10*time.Second).Should(BeNumerically(">", last+1))

		By("restarting node 3: the leader sends a snapshot, which the append goroutine stores and the apply goroutine restores")
		n := tc.start(3, tc.addr(1), bitmap(), nil)
		expectReady(n)
		Eventually(func() string { return holder(n, "c-40") }, 10*time.Second).Should(Equal("1"))
		Expect(holder(n, "c-20")).To(Equal("1"))
		Eventually(func() uint64 {
			first, _ := n.storage.FirstIndex()
			return first
		}, 10*time.Second).Should(BeNumerically(">", last+1))

		By("restarting node 3 again from the received snapshot and the wal")
		tc.kill(3)
		n = tc.start(3, tc.addr(1), bitmap(), nil)
		expectReady(n)
		Expect(n.restarted).To(BeTrue())
		Expect(holder(n, "c-40")).To(Equal("1"))
		startContainer(tc.node(1), "c-41")
		Eventually(func() string { return holder(n, "c-41") }, 5*time.Second).Should(Equal("1"))
	})

	It("should stop a member which left the cluster on restart", func() {
		startThree()

		tc.stop(3)
		Eventually(func() []uint64 { return peerIds(tc.node(1)) }, 10*time.Second).Should(ConsistOf(uint64(1), uint64(2)))
		waitWALClosed(tc.node(3))

		n := tc.start(3, tc.addr(1), bitmap(), nil)
		Eventually(n.stopC, 10*time.Second).Should(BeClosed())
		Consistently(n.Ready(), time.Second).ShouldNot(BeClosed())
		Expect(peerIds(tc.node(1))).To(ConsistOf(uint64(1), uint64(2)))
		tc.forget(3)
	})
})
