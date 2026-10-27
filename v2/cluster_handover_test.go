package asm

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"rumue.io/asm/service"
)

var _ = Describe("Cluster hand-overs kept until they ran", Label("cluster"), func() {
	var (
		tc    *testCluster
		calls *callLog
	)

	bitmap := func() *Bitmap { return clusterBitmap("pending", "ready", "done") }
	owners := func(n *clusterNode, name string) func() []string {
		return func() []string {
			t := publishedTransition(n, name)
			if t == nil {
				return nil
			}
			return t.NodeIds
		}
	}
	// handOver sends the event without waiting for remote transitions and returns its done channel
	handOver := func(c *Container[*wrapperspb.StringValue, *stringValueBuilder], event string) chan struct{} {
		GinkgoHelper()
		doneC := make(chan struct{}, 1)
		Expect(c.Event(event, wrapperspb.Int64(1), WithDoneChannel(doneC))).To(Succeed())
		return doneC
	}
	start := func(n *clusterNode, id string) *Container[*wrapperspb.StringValue, *stringValueBuilder] {
		GinkgoHelper()
		c, _, err := n.StartContainer(context.Background(), id)
		Expect(err).NotTo(HaveOccurred())
		c.Builder().Set(wrapperspb.String("pending"))
		return c
	}
	owner := func(n *clusterNode) { calls.transition(n, "x", "WORK", "") }

	// startWaiting starts nodes 1 and 2: node 2 owns x and accepted a hand-over of node 1 for player-1, whose lock
	// node 1 holds. Then node 3, owning x too, joins. It returns node 1's container.
	startWaiting := func() *Container[*wrapperspb.StringValue, *stringValueBuilder] {
		GinkgoHelper()
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), owner))
		Eventually(owners(tc.node(1), "x"), 10*time.Second).Should(ConsistOf("2"))

		c := start(tc.node(1), "player-1")
		Eventually(handOver(c, "WORK"), 5*time.Second).Should(Receive())
		Eventually(tc.node(2).pendingCount, 5*time.Second).Should(Equal(1))
		Expect(tc.node(1).pendingHandOvers()).To(Equal(1))

		expectReady(tc.start(3, tc.addr(1), bitmap(), owner))
		Eventually(owners(tc.node(1), "x"), 10*time.Second).Should(ConsistOf("2", "3"))
		return c
	}

	BeforeEach(func() {
		tc = newTestCluster()
		calls = newCallLog()
	})

	AfterEach(func() {
		tc.stopAll()
	})

	It("should run a hand-over on another owner once the receiver which accepted it crashed", func() {
		tc.opts = []NodeSetupOption{WithLeaseTimeout(2*time.Second, 300*time.Millisecond)}
		c := startWaiting()

		By("crashing node 2 while it waits for the lock: the stream breaks, node 1 hands the work to node 3")
		tc.crash(2)
		Eventually(tc.node(3).pendingCount, 10*time.Second).Should(Equal(1))
		Expect(tc.node(1).pendingHandOvers()).To(Equal(1))

		By("running it on node 3 once node 1 released the lock and the lease of node 2 ended")
		c.Close()
		Eventually(func() []int { return calls.get("3/x") }, 20*time.Second).Should(Equal([]int{1}))
		Eventually(tc.node(1).pendingHandOvers, 5*time.Second).Should(BeZero())
		Expect(calls.get("2/x")).To(BeEmpty())
	})

	It("should give the queued hand-overs of a leaving node back, they run on another owner", func() {
		c := startWaiting()

		By("node 2 leaving before it got the lock")
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		Expect(tc.node(2).Leave(ctx)).To(Succeed())
		tc.forget(2)
		Eventually(tc.node(3).pendingCount, 10*time.Second).Should(Equal(1))

		c.Close()
		Eventually(func() []int { return calls.get("3/x") }, 20*time.Second).Should(Equal([]int{1}))
		Eventually(tc.node(1).pendingHandOvers, 5*time.Second).Should(BeZero())
		Expect(calls.get("2/x")).To(BeEmpty())
	})

	It("should run a hand-over sent again with the same id once", func() {
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), owner))
		Eventually(owners(tc.node(1), "x"), 10*time.Second).Should(ConsistOf("2"))

		payload, err := anypb.New(wrapperspb.Int64(1))
		Expect(err).NotTo(HaveOccurred())
		req := &service.ForwardEventRequest{
			EventName:    "WORK",
			ContainerId:  "player-1",
			Payloads:     []*anypb.Any{payload},
			SenderNodeId: tc.node(1).Id(),
			SenderEntity: tc.node(1).entityName(),
			HandoverId:   tc.node(1).newHandOverId() + "/x",
		}
		stream, err := tc.node(1).forwardStreamTo(2)
		Expect(err).NotTo(HaveOccurred())
		outcomes := make(chan error, 3)
		send := func() {
			GinkgoHelper()
			Expect(stream.handOver(context.Background(), proto.Clone(req).(*service.ForwardEventRequest), func(err error) {
				outcomes <- err
			})).To(Succeed())
		}

		By("sending it twice right away")
		send()
		send()
		Eventually(outcomes, 10*time.Second).Should(Receive(BeNil()))
		Eventually(outcomes, 10*time.Second).Should(Receive(BeNil()))
		Expect(calls.get("2/x")).To(Equal([]int{1}))

		By("sending it again after it ran")
		send()
		Eventually(outcomes, 10*time.Second).Should(Receive(BeNil()))
		Consistently(func() []int { return calls.get("2/x") }, 500*time.Millisecond).Should(Equal([]int{1}))
	})

	It("should let a container handing work over wait at the limit of kept hand-overs until one ran", func() {
		tc.opts = []NodeSetupOption{WithMaxPendingHandOvers(2)}
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), owner))
		Eventually(owners(tc.node(1), "x"), 10*time.Second).Should(ConsistOf("2"))

		By("handing over two events node 2 can't run while node 1 holds the lock")
		c1 := start(tc.node(1), "player-1")
		Eventually(handOver(c1, "WORK"), 5*time.Second).Should(Receive())
		Eventually(handOver(c1, "WORK"), 5*time.Second).Should(Receive())
		Expect(tc.node(1).pendingHandOvers()).To(Equal(2))

		By("waiting with the third one of another container")
		c2 := start(tc.node(1), "player-2")
		doneC := handOver(c2, "WORK")
		Consistently(doneC, time.Second).ShouldNot(Receive())

		By("sending it once the first two ran")
		c1.Close()
		Eventually(doneC, 10*time.Second).Should(Receive())
		c2.Close()
		Eventually(func() []int { return calls.get("2/x") }, 10*time.Second).Should(HaveLen(3))
		Eventually(tc.node(1).pendingHandOvers, 5*time.Second).Should(BeZero())
	})
})
