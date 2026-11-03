package asm

import (
	"context"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"rumue.io/asm/service"
)

// startClient starts a client node (WithClientMode) attached to the member at joinAddr.
func (tc *testCluster) startClient(id int, joinAddr string, bm *Bitmap, setup func(n *clusterNode)) *clusterNode {
	opts := tc.opts
	tc.opts = append(slices.Clone(opts), WithClientMode())
	defer func() { tc.opts = opts }()
	return tc.start(id, joinAddr, bm, setup)
}

var _ = Describe("Cluster with client nodes", Label("cluster"), func() {
	var (
		tc    *testCluster
		calls *callLog
	)
	bitmap := func() *Bitmap { return clusterBitmap("pending", "done") }
	startMembers := func() {
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), func(n *clusterNode) {
			calls.transition(n, "member-work", "MEMBER", "done")
		}))
		expectReady(tc.start(3, tc.addr(1), bitmap(), nil))
	}
	// owners returns the nodes owning the transition as the node sees them
	owners := func(n *clusterNode, name string) []string {
		if t := publishedTransition(n, name); t != nil {
			return t.NodeIds
		}
		return nil
	}
	startWithin := func(n *clusterNode, id string, timeout time.Duration) (*Container[*wrapperspb.StringValue, *stringValueBuilder], error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		c, _, err := n.StartContainer(ctx, id)
		return c, err
	}

	BeforeEach(func() {
		tc = newTestCluster()
		calls = newCallLog()
	})

	AfterEach(func() {
		tc.stopAll()
	})

	It("should use the lock table through a member and forward events both ways", func() {
		startMembers()
		expectReady(tc.startClient(4, tc.addr(1), bitmap(), func(n *clusterNode) {
			calls.transition(n, "client-work", "CLIENT", "done")
		}))
		expectReady(tc.startClient(5, tc.addr(2), bitmap(), nil))

		By("running clients without raft")
		Expect(tc.node(4).node).To(BeNil())
		info, err := tc.node(4).Inspect(context.Background(), &service.InspectRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(info.GetNode().GetClient()).To(BeTrue())
		Expect(info.GetNode().GetAttachedTo()).To(Equal(tc.addr(1)))
		Eventually(func() []uint64 { return peerIds(tc.node(3)) }, 10*time.Second).Should(ContainElements(uint64(4), uint64(5)))
		Eventually(func() []uint64 { return peerIds(tc.node(5)) }, 10*time.Second).Should(ContainElements(uint64(1), uint64(4)))

		By("forwarding a waited event of a member to a transition of a client, which verifies the lender through its member")
		Eventually(func() []string { return owners(tc.node(1), "client-work") }, 10*time.Second).Should(ConsistOf("4"))
		Expect(sendWaiting(tc.node(1), "c1", "CLIENT", wrapperspb.String("pending"))).To(Equal("done"))

		By("forwarding a waited event of a client to a transition of a member")
		Eventually(func() []string { return owners(tc.node(5), "member-work") }, 10*time.Second).Should(ConsistOf("2"))
		Expect(sendWaiting(tc.node(5), "c2", "MEMBER", wrapperspb.String("pending"))).To(Equal("done"))
		Expect(calls.get("4/client-work")).To(Equal([]int{1}))
		Expect(calls.get("2/member-work")).To(Equal([]int{1}))

		By("opening a container id on only one node, clients included")
		c, err := startWithin(tc.node(4), "c3", 10*time.Second)
		Expect(err).NotTo(HaveOccurred())
		_, err = startWithin(tc.node(3), "c3", time.Second)
		Expect(err).To(MatchError(context.DeadlineExceeded))
		c.Close()
		c3, err := startWithin(tc.node(3), "c3", 10*time.Second)
		Expect(err).NotTo(HaveOccurred())
		c3.Close()
	})

	It("should attach to another member when its member stops", func() {
		startMembers()
		expectReady(tc.startClient(4, tc.addr(3), bitmap(), nil))
		Expect(tc.node(4).attachedTo()).To(Equal(tc.addr(3)))

		tc.crash(3)
		Eventually(func() string { return tc.node(4).attachedTo() }, 10*time.Second).Should(Or(Equal(tc.addr(1)), Equal(tc.addr(2))))

		c, err := startWithin(tc.node(4), "c1", 15*time.Second)
		Expect(err).NotTo(HaveOccurred())
		c.Close()
	})

	It("should remove a client which leaves or is removed and free its locks", func() {
		startMembers()
		expectReady(tc.startClient(4, tc.addr(1), bitmap(), nil))
		expectReady(tc.startClient(5, tc.addr(2), bitmap(), nil))

		By("leaving: the client closes its containers and is removed")
		_, err := startWithin(tc.node(5), "c5", 10*time.Second)
		Expect(err).NotTo(HaveOccurred())
		tc.stop(5)
		Eventually(func() []uint64 { return peerIds(tc.node(1)) }, 10*time.Second).ShouldNot(ContainElement(uint64(5)))
		c, err := startWithin(tc.node(1), "c5", 5*time.Second)
		Expect(err).NotTo(HaveOccurred())
		c.Close()

		By("removing a crashed client from a member frees its locks right away")
		_, err = startWithin(tc.node(4), "c4", 10*time.Second)
		Expect(err).NotTo(HaveOccurred())
		tc.crash(4)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		Expect(tc.node(2).RemoveNode(ctx, 4)).To(Succeed())
		tc.forget(4)
		Eventually(func() []uint64 { return peerIds(tc.node(3)) }, 10*time.Second).ShouldNot(ContainElement(uint64(4)))
		c, err = startWithin(tc.node(3), "c4", 5*time.Second)
		Expect(err).NotTo(HaveOccurred())
		c.Close()
	})
})
