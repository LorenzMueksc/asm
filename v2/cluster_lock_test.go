package asm

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"rumue.io/asm/service"
)

var _ = Describe("Cluster locking and compaction", Label("cluster"), func() {
	var tc *testCluster

	startThree := func() {
		expectReady(tc.start(1, "", clusterBitmap("pending", "done"), nil))
		expectReady(tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), nil))
		expectReady(tc.start(3, tc.addr(1), clusterBitmap("pending", "done"), nil))
	}
	// startWithin starts a container, giving up waiting for its lock after the timeout.
	// The context only bounds the waiting, the container lives until it is closed.
	startWithin := func(n *clusterNode, id string, timeout time.Duration) (*Container[*wrapperspb.StringValue, *stringValueBuilder], error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		c, _, err := n.StartContainer(ctx, id)
		return c, err
	}

	BeforeEach(func() {
		tc = newTestCluster()
	})

	AfterEach(func() {
		tc.stopAll()
	})

	Context("container locks", func() {
		It("should open a container id on only one node at a time", func() {
			startThree()

			c2, err := startWithin(tc.node(2), "player-1", 10*time.Second)
			Expect(err).NotTo(HaveOccurred())

			_, err = startWithin(tc.node(3), "player-1", time.Second)
			Expect(err).To(MatchError(context.DeadlineExceeded))

			By("getting the lock once the holder closed the container")
			c2.Close()
			c3, err := startWithin(tc.node(3), "player-1", 10*time.Second)
			Expect(err).NotTo(HaveOccurred())
			c3.Close()
		})

		It("should only lock the container id, not the node", func() {
			startThree()

			held, err := startWithin(tc.node(2), "player-1", 10*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// while node 3 waits for player-1, it starts other players right away
			waitingC := make(chan error, 1)
			go func() {
				_, err := startWithin(tc.node(3), "player-1", 15*time.Second)
				waitingC <- err
			}()

			for _, id := range []string{"player-2", "player-3"} {
				started := time.Now()
				c, err := startWithin(tc.node(3), id, 5*time.Second)
				Expect(err).NotTo(HaveOccurred())
				Expect(time.Since(started)).To(BeNumerically("<", time.Second))

				doneC := make(chan struct{}, 1)
				Expect(c.Event("E", wrapperspb.Int64(1), WithDoneChannel(doneC))).To(Succeed())
				Eventually(doneC).Should(Receive())
				c.Close()
			}

			// the holder can open other players as well
			other, err := startWithin(tc.node(2), "player-4", 5*time.Second)
			Expect(err).NotTo(HaveOccurred())
			other.Close()

			Consistently(waitingC, 200*time.Millisecond).ShouldNot(Receive())
			held.Close()
			Eventually(waitingC, 10*time.Second).Should(Receive(BeNil()))
		})

		It("should wait for a lock and get it once it is released", func() {
			startThree()

			c2, err := startWithin(tc.node(2), "player-1", 10*time.Second)
			Expect(err).NotTo(HaveOccurred())

			startedC := make(chan error, 1)
			go func() {
				_, err := startWithin(tc.node(3), "player-1", 15*time.Second)
				startedC <- err
			}()

			Consistently(startedC, 500*time.Millisecond).ShouldNot(Receive())
			c2.Close()
			Eventually(startedC, 10*time.Second).Should(Receive(BeNil()))
		})

		It("should keep the lock when a container is closed and opened again on the same node", func() {
			startThree()

			for range 3 {
				c, err := startWithin(tc.node(2), "player-1", 10*time.Second)
				Expect(err).NotTo(HaveOccurred())
				c.Close()
			}
			c, err := startWithin(tc.node(2), "player-1", 10*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// the releases of the closed containers must not free the lock of the open one
			_, err = startWithin(tc.node(3), "player-1", 2*time.Second)
			Expect(err).To(MatchError(context.DeadlineExceeded))
			c.Close()
		})

		It("should keep the lock of a closed container until another node asks for it", func() {
			startThree()

			c, err := startWithin(tc.node(2), "player-1", 10*time.Second)
			Expect(err).NotTo(HaveOccurred())
			token := c.lockToken
			c.Close()

			By("keeping the lock after the container closed")
			Consistently(func() string {
				l, _ := tc.node(1).locks.peek("player-1")
				return l.holder + "/" + l.token
			}, 500*time.Millisecond).Should(Equal("2/" + token))

			By("opening the container again without a log entry")
			proposals := tc.node(2).metrics.proposals.Load()
			c, err = startWithin(tc.node(2), "player-1", 10*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(c.lockToken).To(Equal(token))
			Expect(tc.node(2).metrics.lockReuses.Load()).To(Equal(uint64(1)))
			Expect(tc.node(2).metrics.proposals.Load()-proposals).To(BeNumerically("<=", 1), "a heartbeat at most")
			c.Close()

			By("giving it to another node right away when it asks")
			started := time.Now()
			c3, err := startWithin(tc.node(3), "player-1", 10*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(time.Since(started)).To(BeNumerically("<", time.Second))
			Expect(tc.node(2).retainedCount()).To(Equal(0))
			c3.Close()
		})

		It("should take over the lock of a stopped node after its lease expired", func() {
			tc.opts = []NodeSetupOption{WithLeaseTimeout(2*time.Second, 300*time.Millisecond)}
			startThree()

			_, err := startWithin(tc.node(2), "player-1", 10*time.Second)
			Expect(err).NotTo(HaveOccurred())

			tc.crash(2)
			stoppedAt := time.Now()

			c3, err := startWithin(tc.node(3), "player-1", 15*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(time.Since(stoppedAt)).To(BeNumerically(">", time.Second))
			c3.Close()
		})

		It("should reject remote executions from a node not holding the lock", func() {
			startThree()
			tc.node(1).AddFuncTransition("work", "WORK", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				return false, nil
			}, WithFuncKey("work"))
			Eventually(func() *Transition { return publishedTransition(tc.node(1), "work") }).ShouldNot(BeNil())

			payload, err := anypb.New(wrapperspb.Int64(1))
			Expect(err).NotTo(HaveOccurred())
			_, err = tc.node(1).ForwardEvent(context.Background(), &service.ForwardEventRequest{
				EventName:   "WORK",
				ContainerId: "player-1",
				Payloads:    []*anypb.Any{payload},
				LendChain:   []string{"2"},
				// only executions on a lent lock check the holder, handed over ones acquire the lock themselves
				Wait: true,
			})
			Expect(status.Code(err)).To(Equal(codes.FailedPrecondition))
		})
	})

	Context("lock queue", func() {
		type started struct {
			c   *Container[*wrapperspb.StringValue, *stringValueBuilder]
			err error
		}
		startAsync := func(n *clusterNode, id string) chan started {
			resultC := make(chan started, 1)
			go func() {
				c, err := startWithin(n, id, 20*time.Second)
				resultC <- started{c, err}
			}()
			return resultC
		}
		// waiting returns the nodes waiting for the lock as the node sees them
		waiting := func(n *clusterNode, id string) func() []string {
			return func() []string {
				l, _, _ := n.locks.get(id)
				nodes := make([]string, 0, len(l.queue))
				for _, w := range l.queue {
					nodes = append(nodes, w.node)
				}
				return nodes
			}
		}

		It("should hand a released lock to the waiting node before the holder gets it again", func() {
			startThree()
			c2, err := startWithin(tc.node(2), "player-1", 10*time.Second)
			Expect(err).NotTo(HaveOccurred())

			node3C := startAsync(tc.node(3), "player-1")
			Eventually(waiting(tc.node(2), "player-1"), 5*time.Second).Should(ContainElement("3"))

			By("closing and opening the container on the holder right away")
			c2.Close()
			node2C := startAsync(tc.node(2), "player-1")

			var got started
			Eventually(node3C, 10*time.Second).Should(Receive(&got))
			Expect(got.err).NotTo(HaveOccurred())
			Consistently(node2C, 500*time.Millisecond).ShouldNot(Receive())

			got.c.Close()
			Eventually(node2C, 10*time.Second).Should(Receive(&got))
			Expect(got.err).NotTo(HaveOccurred())
			got.c.Close()
		})

		It("should hand the lock to the next waiter when a waiter gave up", func() {
			startThree()
			c2, err := startWithin(tc.node(2), "player-1", 10*time.Second)
			Expect(err).NotTo(HaveOccurred())

			_, err = startWithin(tc.node(3), "player-1", time.Second)
			Expect(err).To(MatchError(context.DeadlineExceeded))

			node1C := startAsync(tc.node(1), "player-1")
			Eventually(waiting(tc.node(2), "player-1"), 5*time.Second).Should(Equal([]string{"1"}))

			c2.Close()
			var got started
			Eventually(node1C, 10*time.Second).Should(Receive(&got))
			Expect(got.err).NotTo(HaveOccurred())
			got.c.Close()
		})
	})

	Context("log compaction", func() {
		It("should compact the log and bring a joining node up to date with the snapshot", func() {
			tc.opts = []NodeSetupOption{
				WithSnapshotCount(20, 5),
				WithSnapshotInterval(0),
				WithLeaseTimeout(5*time.Second, 100*time.Millisecond),
			}
			startThree()

			c, err := startWithin(tc.node(2), "player-1", 10*time.Second)
			Expect(err).NotTo(HaveOccurred())

			By("compacting the log of the leader")
			leader := tc.node(1)
			Eventually(func() uint64 {
				first, _ := leader.storage.FirstIndex()
				return first
			}, 20*time.Second).Should(BeNumerically(">", 10))

			By("joining a node which receives the snapshot")
			joined := tc.start(4, tc.addr(1), clusterBitmap("pending", "cancelled"), nil)
			expectReady(joined)

			snap, err := joined.storage.Snapshot()
			Expect(err).NotTo(HaveOccurred())
			Expect(snap.Metadata.Index).To(BeNumerically(">", 0))

			Eventually(func() map[string]uint64 { return layoutIndices(joined) }, 10*time.Second).
				Should(Equal(map[string]uint64{"pending": 0, "done": 1, "cancelled": 2}))
			for id := 1; id <= 3; id++ {
				Eventually(func() map[string]uint64 { return layoutIndices(tc.node(id)) }, 10*time.Second).
					Should(HaveKeyWithValue("cancelled", uint64(2)), "node %d", id)
			}

			By("knowing the locks and peers from the snapshot")
			lock, held, _ := joined.locks.get("player-1")
			Expect(held).To(BeTrue())
			Expect(lock.holder).To(Equal("2"))
			_, err = startWithin(joined, "player-1", time.Second)
			Expect(err).To(MatchError(context.DeadlineExceeded))
			c.Close()
		})
	})
})
