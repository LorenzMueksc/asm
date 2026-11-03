package asm

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// testCluster starts raft nodes on localhost and can stop single nodes.
type testCluster struct {
	// opts are added to every node
	opts []NodeSetupOption
	// dataDir, if set, gives every node a data directory of its id, kept over restarts with the node's address
	dataDir string
	// nodeOpts, if set, returns options of a single node (eg. its certificate)
	nodeOpts func(id int) []NodeSetupOption
	mu       sync.Mutex
	nodes    map[int]*clusterNode
	addrs    map[int]string
	stops    map[int]chan struct{}
	stopped  map[int]bool
	crashed  map[int]bool
}

func newTestCluster() *testCluster {
	return &testCluster{
		nodes:   make(map[int]*clusterNode),
		addrs:   make(map[int]string),
		stops:   make(map[int]chan struct{}),
		stopped: make(map[int]bool),
		crashed: make(map[int]bool),
	}
}

// start starts a node. A node without joinAddr campaigns and becomes the leader.
func (tc *testCluster) start(id int, joinAddr string, bm *Bitmap, setup func(n *clusterNode)) *clusterNode {
	addr := freeAddr()
	stopC := make(chan struct{})
	opts := []NodeSetupOption{
		WithBitmap(bm),
		WithLogger(zap.NewNop()),
		WithStopChannel(stopC),
	}
	if joinAddr != "" {
		opts = append(opts, WithJoinAddr(joinAddr))
	}
	if tc.dataDir != "" {
		if used := tc.addr(id); used != "" {
			addr = used
		}
		opts = append(opts, WithDataDir(filepath.Join(tc.dataDir, strconv.Itoa(id))))
	}
	opts = append(opts, tc.opts...)
	if tc.nodeOpts != nil {
		opts = append(opts, tc.nodeOpts(id)...)
	}
	n := NewNode(id, clusterBitmapByType, func() *stringValueBuilder { return &stringValueBuilder{} }, addr, opts...)
	if setup != nil {
		setup(n)
	}

	tc.mu.Lock()
	tc.nodes[id], tc.addrs[id], tc.stops[id] = n, addr, stopC
	// a node can start again with the id of a node which left
	delete(tc.stopped, id)
	delete(tc.crashed, id)
	tc.mu.Unlock()
	return n
}

// forget marks a node which left or was removed as stopped, so leader() and stopAll() skip it.
func (tc *testCluster) forget(id int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.stopped[id] = true
}

// peerIds returns the ids of the peers the node knows, including itself.
func peerIds(n *clusterNode) []uint64 {
	n.peersMu.RLock()
	defer n.peersMu.RUnlock()
	ids := make([]uint64, 0, len(n.peers))
	for id := range n.peers {
		ids = append(ids, id)
	}
	return ids
}

func (tc *testCluster) node(id int) *clusterNode {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.nodes[id]
}

func (tc *testCluster) addr(id int) string {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.addrs[id]
}

// stop stops raft, the gRPC server and the containers of a node.
func (tc *testCluster) stop(id int) {
	tc.mu.Lock()
	n := tc.nodes[id]
	if tc.stopped[id] {
		tc.mu.Unlock()
		return
	}
	tc.stopped[id] = true
	close(tc.stops[id])
	tc.mu.Unlock()

	n.Shutdown()
}

// crash stops raft and the gRPC server of a node without closing its containers,
// so it can't release its locks. Closing the stop channel would close the containers gracefully.
func (tc *testCluster) crash(id int) {
	tc.mu.Lock()
	n := tc.nodes[id]
	if tc.stopped[id] || tc.crashed[id] {
		tc.mu.Unlock()
		return
	}
	tc.crashed[id] = true
	tc.mu.Unlock()

	n.stopRaft()
}

// kill stops a node without leaving the cluster: raft and the gRPC server stop, its containers close but keep their
// locks (nothing can be proposed anymore). It waits until the node's wal is closed, so it can start again.
func (tc *testCluster) kill(id int) {
	GinkgoHelper()
	tc.mu.Lock()
	n := tc.nodes[id]
	if tc.stopped[id] || tc.crashed[id] {
		tc.mu.Unlock()
		return
	}
	tc.stopped[id] = true
	tc.mu.Unlock()

	n.requestStop()
	waitWALClosed(n)
}

// killAll kills every node and waits until all wals are closed, also of nodes which stopped by themselves.
func (tc *testCluster) killAll() {
	GinkgoHelper()
	tc.mu.Lock()
	nodes := make(map[int]*clusterNode, len(tc.nodes))
	for id, n := range tc.nodes {
		nodes[id] = n
	}
	tc.mu.Unlock()

	for id, n := range nodes {
		tc.kill(id)
		n.requestStop()
		waitWALClosed(n)
	}
}

// waitWALClosed waits until the wal of the stopped node is closed.
func waitWALClosed(n *clusterNode) {
	GinkgoHelper()
	Eventually(func() bool {
		if n.wal == nil {
			return true
		}
		n.wal.mu.Lock()
		defer n.wal.mu.Unlock()
		return n.wal.closed
	}, 10*time.Second).Should(BeTrue())
}

func (tc *testCluster) stopAll() {
	tc.mu.Lock()
	ids := make([]int, 0, len(tc.nodes))
	for id := range tc.nodes {
		ids = append(ids, id)
	}
	tc.mu.Unlock()

	for _, id := range ids {
		tc.stop(id)
	}
}

// leader returns the id of the leader as seen by the running nodes, 0 if they don't agree on one.
func (tc *testCluster) leader() int {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	leader := uint64(0)
	for id, n := range tc.nodes {
		if tc.stopped[id] || tc.crashed[id] || n.node == nil {
			continue
		}
		lead := n.node.Status().Lead
		if lead == 0 || (leader != 0 && lead != leader) {
			return 0
		}
		leader = lead
	}
	return int(leader)
}

func expectReady(n *clusterNode) {
	GinkgoHelper()
	select {
	case err := <-n.Err():
		Fail(fmt.Sprintf("node %s failed: %v", n.Id(), err))
	case <-n.Ready():
	case <-time.After(30 * time.Second):
		Fail(fmt.Sprintf("node %s did not become ready", n.Id()))
	}
}

// layoutIndices returns the committed index of every value of the StringValue entity.
func layoutIndices(n *clusterNode) map[string]uint64 {
	indices := make(map[string]uint64)
	bm := n.Bitmap()
	if bm == nil || bm.Children["StringValue"] == nil || bm.Children["StringValue"].Children["value"] == nil {
		return indices
	}
	for _, v := range bm.Children["StringValue"].Children["value"].Values {
		indices[fmt.Sprint(v.Value)] = v.Index
	}
	return indices
}

func publishedTransition(n *clusterNode, name string) *Transition {
	for _, t := range n.registry.published.Load().list() {
		if t.Name == name {
			return t
		}
	}
	return nil
}

// sendWaiting sends an event waiting for remote transitions and returns the container's entity once done.
func sendWaiting(n *clusterNode, containerId, event string, entity *wrapperspb.StringValue) string {
	GinkgoHelper()
	c, _, err := n.StartContainer(context.Background(), containerId)
	Expect(err).NotTo(HaveOccurred())
	c.Builder().Set(entity)

	doneC := make(chan struct{}, 1)
	Expect(c.Event(event, wrapperspb.Int64(1), WithWaitRemote(), WithDoneChannel(doneC))).To(Succeed())
	Eventually(doneC, 15*time.Second).Should(Receive())

	value := c.Builder().Build().GetValue()
	c.Close()
	return value
}

var _ = Describe("Cluster with more nodes", Label("cluster"), func() {
	var tc *testCluster

	// countingTransition owns the transition "work" and counts its calls
	countingTransition := func(calls *atomic.Int32, result string) func(n *clusterNode) {
		return func(n *clusterNode) {
			n.AddFuncTransition("work", "WORK", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				calls.Add(1)
				c.Builder().Set(wrapperspb.String(result))
				return false, nil
			}, WithFuncKey("work"))
		}
	}

	BeforeEach(func() {
		tc = newTestCluster()
	})

	AfterEach(func() {
		tc.stopAll()
	})

	Context("with 3 nodes", func() {
		It("should build one layout from the configs of all nodes", func() {
			expectReady(tc.start(1, "", clusterBitmap("a", "b"), nil))
			expectReady(tc.start(2, tc.addr(1), clusterBitmap("b", "c"), nil))
			expectReady(tc.start(3, tc.addr(1), clusterBitmap("d", "c", "a"), nil))

			expected := map[string]uint64{"a": 0, "b": 1, "c": 2, "d": 3}
			for id := 1; id <= 3; id++ {
				Eventually(func() map[string]uint64 { return layoutIndices(tc.node(id)) }, 10*time.Second).Should(Equal(expected), "node %d", id)
			}
		})

		It("should let every node know every peer", func() {
			expectReady(tc.start(1, "", clusterBitmap("a"), nil))
			expectReady(tc.start(2, tc.addr(1), clusterBitmap("a"), nil))
			expectReady(tc.start(3, tc.addr(1), clusterBitmap("a"), nil))

			for id := 1; id <= 3; id++ {
				Eventually(func() []uint64 {
					n := tc.node(id)
					n.peersMu.RLock()
					defer n.peersMu.RUnlock()
					ids := make([]uint64, 0)
					for peer := range n.peers {
						ids = append(ids, peer)
					}
					return ids
				}, 10*time.Second).Should(ConsistOf(uint64(1), uint64(2), uint64(3)), "node %d", id)
			}
		})

		It("should sync transitions between followers and forward between them", func() {
			var calls atomic.Int32
			expectReady(tc.start(1, "", clusterBitmap("pending", "done"), nil))
			expectReady(tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), countingTransition(&calls, "done")))
			expectReady(tc.start(3, tc.addr(1), clusterBitmap("pending", "done"), nil))

			for id := 1; id <= 3; id++ {
				t := publishedTransition(tc.node(id), "work")
				Expect(t).NotTo(BeNil(), "node %d", id)
				Expect(t.NodeIds).To(Equal([]string{"2"}), "node %d", id)
			}

			Expect(sendWaiting(tc.node(3), "c1", "WORK", wrapperspb.String("pending"))).To(Equal("done"))
			Expect(sendWaiting(tc.node(1), "c2", "WORK", wrapperspb.String("pending"))).To(Equal("done"))
			Expect(calls.Load()).To(Equal(int32(2)))
		})

		It("should skip an unreachable owner", func() {
			var calls2, calls3 atomic.Int32
			expectReady(tc.start(1, "", clusterBitmap("pending", "done"), nil))
			expectReady(tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), countingTransition(&calls2, "done")))
			expectReady(tc.start(3, tc.addr(1), clusterBitmap("pending", "done"), countingTransition(&calls3, "done")))
			Eventually(func() []string { return publishedTransition(tc.node(1), "work").NodeIds }, 10*time.Second).Should(ConsistOf("2", "3"))

			tc.stop(2)

			for i := range 10 {
				Expect(sendWaiting(tc.node(1), fmt.Sprintf("c%d", i), "WORK", wrapperspb.String("pending"))).To(Equal("done"))
			}
			Expect(calls3.Load()).To(Equal(int32(10)))
		})

		It("should keep working after the leader stopped", func() {
			var calls atomic.Int32
			expectReady(tc.start(1, "", clusterBitmap("pending", "done"), nil))
			expectReady(tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), countingTransition(&calls, "done")))
			expectReady(tc.start(3, tc.addr(1), clusterBitmap("pending", "done"), nil))

			tc.stop(1)

			By("electing a new leader")
			Eventually(tc.leader, 20*time.Second).Should(BeElementOf(2, 3))

			By("joining a node with a new bitmap value through the new leader")
			expectReady(tc.start(4, tc.addr(tc.leader()), clusterBitmap("pending", "cancelled"), nil))
			for _, id := range []int{2, 3, 4} {
				Eventually(func() map[string]uint64 { return layoutIndices(tc.node(id)) }, 10*time.Second).
					Should(HaveKeyWithValue("cancelled", uint64(2)), "node %d", id)
			}

			By("forwarding between the remaining nodes")
			Expect(sendWaiting(tc.node(3), "c1", "WORK", wrapperspb.String("pending"))).To(Equal("done"))
			Expect(sendWaiting(tc.node(4), "c2", "WORK", wrapperspb.String("pending"))).To(Equal("done"))
			Expect(calls.Load()).To(Equal(int32(2)))
		})
	})

	Context("with 5 nodes", func() {
		It("should balance a transition owned by several nodes", func() {
			var calls [6]atomic.Int32
			expectReady(tc.start(1, "", clusterBitmap("pending", "done"), nil))
			for id := 2; id <= 4; id++ {
				expectReady(tc.start(id, tc.addr(1), clusterBitmap("pending", "done"), countingTransition(&calls[id], "done")))
			}
			expectReady(tc.start(5, tc.addr(1), clusterBitmap("pending", "done"), nil))
			Expect(publishedTransition(tc.node(5), "work").NodeIds).To(ConsistOf("2", "3", "4"))

			for i := range 30 {
				Expect(sendWaiting(tc.node(5), fmt.Sprintf("c%d", i), "WORK", wrapperspb.String("pending"))).To(Equal("done"))
			}

			Expect(calls[2].Load() + calls[3].Load() + calls[4].Load()).To(Equal(int32(30)))
			// with a random owner per event, the chance that one owner gets nothing is about 1e-5
			for id := 2; id <= 4; id++ {
				Expect(calls[id].Load()).To(BeNumerically(">", 0), "node %d", id)
			}
		})

		It("should accept nodes joining at the same time", func() {
			expectReady(tc.start(1, "", clusterBitmap("v1"), nil))

			var wg sync.WaitGroup
			for id := 2; id <= 5; id++ {
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					tc.start(id, tc.addr(1), clusterBitmap(fmt.Sprintf("v%d", id)), nil)
				}()
			}
			wg.Wait()

			for id := 2; id <= 5; id++ {
				expectReady(tc.node(id))
			}

			expected := layoutIndices(tc.node(1))
			Eventually(func() int { return len(layoutIndices(tc.node(1))) }, 10*time.Second).Should(Equal(5))
			expected = layoutIndices(tc.node(1))
			for id := 2; id <= 5; id++ {
				Eventually(func() map[string]uint64 { return layoutIndices(tc.node(id)) }, 10*time.Second).Should(Equal(expected), "node %d", id)
			}
		})
	})
})
