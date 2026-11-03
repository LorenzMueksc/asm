package asm

import (
	"context"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var _ = Describe("Cluster membership", Label("cluster"), func() {
	var tc *testCluster

	startThree := func(setups ...func(n *clusterNode)) {
		setup := func(id int) func(n *clusterNode) {
			if id <= len(setups) {
				return setups[id-1]
			}
			return nil
		}
		expectReady(tc.start(1, "", clusterBitmap("pending", "done"), setup(1)))
		expectReady(tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), setup(2)))
		expectReady(tc.start(3, tc.addr(1), clusterBitmap("pending", "done"), setup(3)))
		Eventually(tc.leader, 10*time.Second).ShouldNot(BeZero())
	}
	work := func(calls *atomic.Int32) func(n *clusterNode) {
		return func(n *clusterNode) {
			n.AddFuncTransition("work", "WORK", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				calls.Add(1)
				c.Builder().Set(wrapperspb.String("done"))
				return false, nil
			}, WithFuncKey("work"))
		}
	}
	expectPeers := func(ids []int, peers ...uint64) {
		GinkgoHelper()
		for _, id := range ids {
			Eventually(func() []uint64 { return peerIds(tc.node(id)) }, 10*time.Second).Should(ConsistOf(peers), "node %d", id)
		}
	}
	withTimeout := func(d time.Duration) context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), d)
		DeferCleanup(cancel)
		return ctx
	}

	BeforeEach(func() {
		tc = newTestCluster()
	})

	AfterEach(func() {
		tc.stopAll()
	})

	It("should remove a follower which leaves and drop it as transition owner", func() {
		var calls2, calls3 atomic.Int32
		startThree(nil, work(&calls2), work(&calls3))
		Eventually(func() []string { return publishedTransition(tc.node(1), "work").NodeIds }, 10*time.Second).Should(ConsistOf("2", "3"))

		Expect(tc.node(3).Leave(withTimeout(20 * time.Second))).To(Succeed())
		tc.forget(3)
		Expect(tc.node(3).stopC).To(BeClosed())

		expectPeers([]int{1, 2}, 1, 2)
		for _, id := range []int{1, 2} {
			Eventually(func() []string { return publishedTransition(tc.node(id), "work").NodeIds }, 10*time.Second).
				Should(Equal([]string{"2"}), "node %d", id)
		}

		Expect(sendWaiting(tc.node(1), "c1", "WORK", wrapperspb.String("pending"))).To(Equal("done"))
		Expect(calls2.Load()).To(Equal(int32(1)))
		Expect(calls3.Load()).To(BeZero())
	})

	It("should hand over the leadership when the leader leaves", func() {
		startThree()
		leader := tc.leader()

		Expect(tc.node(leader).Leave(withTimeout(20 * time.Second))).To(Succeed())
		tc.forget(leader)

		Eventually(tc.leader, 10*time.Second).Should(And(Not(BeZero()), Not(Equal(leader))))
		remaining := make([]int, 0, 2)
		for id := 1; id <= 3; id++ {
			if id != leader {
				remaining = append(remaining, id)
			}
		}
		expectPeers(remaining, uint64(remaining[0]), uint64(remaining[1]))

		c, _, err := tc.node(remaining[0]).StartContainer(withTimeout(10*time.Second), "c1")
		Expect(err).NotTo(HaveOccurred())
		c.Close()
	})

	It("should free the locks of a crashed node removed through a follower", func() {
		startThree()
		follower := 2
		if tc.leader() == follower {
			follower = 1
		}

		c3, _, err := tc.node(3).StartContainer(withTimeout(10*time.Second), "player-1")
		Expect(err).NotTo(HaveOccurred())
		_ = c3

		tc.crash(3)
		crashedAt := time.Now()
		Expect(tc.node(follower).RemoveNode(withTimeout(20*time.Second), 3)).To(Succeed())
		expectPeers([]int{1, 2}, 1, 2)

		By("opening the container before the lease of the crashed node expired")
		c, _, err := tc.node(follower).StartContainer(withTimeout(5*time.Second), "player-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(time.Since(crashedAt)).To(BeNumerically("<", defaultLeaseTimeout))
		c.Close()
	})

	It("should stop a running node which was removed by another node", func() {
		startThree()

		Expect(tc.node(1).RemoveNode(withTimeout(20*time.Second), 3)).To(Succeed())
		tc.forget(3)

		Eventually(tc.node(3).stopC, 10*time.Second).Should(BeClosed())
		Expect(tc.node(3).Err()).NotTo(Receive())
		expectPeers([]int{1, 2}, 1, 2)
	})

	It("should leave when the stop channel is closed and join again with the same id", func() {
		startThree()

		tc.stop(3)
		Eventually(tc.node(3).stopC, 20*time.Second).Should(BeClosed())
		expectPeers([]int{1, 2}, 1, 2)

		expectReady(tc.start(3, tc.addr(tc.leader()), clusterBitmap("pending", "done"), nil))
		expectPeers([]int{1, 2, 3}, 1, 2, 3)

		c, _, err := tc.node(3).StartContainer(withTimeout(10*time.Second), "c1")
		Expect(err).NotTo(HaveOccurred())
		c.Close()
	})
})
