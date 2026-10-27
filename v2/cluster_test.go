package asm

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// The cluster tests run real raft nodes on localhost.
// The entity is a *wrapperspb.StringValue, a real protobuf message with the json tag "value".

type stringValueBuilder struct {
	mu sync.Mutex
	v  *wrapperspb.StringValue
}

func (b *stringValueBuilder) Build() *wrapperspb.StringValue {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.v
}

func (b *stringValueBuilder) Set(v *wrapperspb.StringValue) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.v = v
}

func (b *stringValueBuilder) IsSet() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.v != nil
}

type (
	clusterNode = Node[*wrapperspb.StringValue, *stringValueBuilder]
	clusterFunc = transitionFunc[*wrapperspb.StringValue, *stringValueBuilder]
)

func clusterBitmap(values ...any) *Bitmap {
	pos := 0
	entity := BuildBitmap(&Property{Name: "StringValue", Properties: []*Property{{Name: "value", Values: values}}}, &pos)
	return &Bitmap{Name: "root", Children: map[string]*Bitmap{"StringValue": entity}}
}

func clusterBitmapByType(entity any, root *Bitmap) *Bitmap {
	if root == nil {
		return nil
	}
	if _, ok := entity.(*wrapperspb.StringValue); ok {
		return root.Children["StringValue"]
	}
	return nil
}

func freeAddr() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	defer l.Close()
	return l.Addr().String()
}

var _ = Describe("Cluster", Label("cluster"), func() {
	var (
		nodes []*clusterNode
		stops []chan struct{}
	)

	// startNode starts a raft node. A node without joinAddr campaigns and becomes the leader.
	startNode := func(id int, joinAddr string, bm *Bitmap, setup func(n *clusterNode)) (*clusterNode, string) {
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
		n := NewNode(id, clusterBitmapByType, func() *stringValueBuilder { return &stringValueBuilder{} }, addr, opts...)
		if setup != nil {
			setup(n)
		}
		nodes = append(nodes, n)
		stops = append(stops, stopC)
		return n, addr
	}
	ready := func(n *clusterNode) {
		select {
		case err := <-n.Err():
			Fail("node failed: " + err.Error())
		case <-n.Ready():
		case <-time.After(20 * time.Second):
			Fail("node did not become ready")
		}
	}
	indexOf := func(n *clusterNode, value string) (uint64, bool) {
		bm := n.Bitmap()
		if bm == nil {
			return 0, false
		}
		v := getExactValue(bm.Children["StringValue"].Children["value"].Values, value)
		if v == nil {
			return 0, false
		}
		return v.Index, true
	}
	transition := func(n *clusterNode, name string) *Transition {
		for _, t := range n.registry.published.Load().list() {
			if t.Name == name {
				return t
			}
		}
		return nil
	}
	sendAndWait := func(n *clusterNode, id, event string, entity *wrapperspb.StringValue, opts ...EventOption) *Container[*wrapperspb.StringValue, *stringValueBuilder] {
		c, _, err := n.StartContainer(context.Background(), id)
		Expect(err).NotTo(HaveOccurred())
		c.Builder().Set(entity)

		doneC := make(chan struct{}, 1)
		Expect(c.Event(event, wrapperspb.Int64(1), append(opts, WithDoneChannel(doneC))...)).To(Succeed())
		Eventually(doneC, 10*time.Second).Should(Receive())
		return c
	}

	BeforeEach(func() {
		nodes = nil
		stops = nil
	})

	AfterEach(func() {
		for _, stopC := range stops {
			close(stopC)
		}
		for _, n := range nodes {
			n.Shutdown()
		}
	})

	Context("bitmap", func() {
		It("should share the committed indices with nodes listing the values in a different order", func() {
			leader, leaderAddr := startNode(1, "", clusterBitmap("pending", "processing", "done"), nil)
			ready(leader)
			follower, _ := startNode(2, leaderAddr, clusterBitmap("done", "pending"), nil)
			ready(follower)

			for _, value := range []string{"pending", "processing", "done"} {
				leaderIndex, ok := indexOf(leader, value)
				Expect(ok).To(BeTrue())
				Eventually(func() uint64 { i, _ := indexOf(follower, value); return i }).Should(Equal(leaderIndex))
			}
		})

		It("should append new values of a joining node on all nodes", func() {
			leader, leaderAddr := startNode(1, "", clusterBitmap("pending", "done"), nil)
			ready(leader)
			follower, _ := startNode(2, leaderAddr, clusterBitmap("pending", "cancelled"), nil)
			ready(follower)

			Eventually(func() bool { _, ok := indexOf(leader, "cancelled"); return ok }, 10*time.Second).Should(BeTrue())
			leaderIndex, _ := indexOf(leader, "cancelled")
			followerIndex, _ := indexOf(follower, "cancelled")
			Expect(leaderIndex).To(Equal(uint64(2)))
			Expect(followerIndex).To(Equal(leaderIndex))
		})

		It("should fail a joining node with a conflicting bitmap", func() {
			leader, leaderAddr := startNode(1, "", clusterBitmap("pending"), nil)
			ready(leader)
			follower, _ := startNode(2, leaderAddr, clusterBitmap(int64(1)), nil)

			Eventually(follower.Err(), 20*time.Second).Should(Receive(MatchError(ContainSubstring("bitmap conflict"))))
			_, _, err := follower.StartContainer(context.Background(), "c1")
			Expect(err).To(MatchError(ContainSubstring("bitmap conflict")))
		})
	})

	Context("transitions", func() {
		var calls atomic.Int32
		promote := func(value string) clusterFunc {
			return func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				calls.Add(1)
				c.Builder().Set(wrapperspb.String(value))
				return false, nil
			}
		}

		BeforeEach(func() {
			calls.Store(0)
		})

		It("should sync transitions and forward remote ones to the owner", func() {
			leader, leaderAddr := startNode(1, "", clusterBitmap("pending", "done"), func(n *clusterNode) {
				n.AddFuncTransition("promote", "PROMOTE", promote("done"), WithFuncKey("promote"),
					WithConditions(n.All(wrapperspb.String("pending"))))
			})
			ready(leader)
			follower, _ := startNode(2, leaderAddr, clusterBitmap("pending", "done"), nil)
			ready(follower)

			t := transition(follower, "promote")
			Expect(t).NotTo(BeNil())
			Expect(t.Remote).To(BeTrue())
			Expect(t.NodeIds).To(Equal([]string{"1"}))

			By("waiting for the owner and applying its entity")
			c := sendAndWait(follower, "c1", "PROMOTE", wrapperspb.String("pending"), WithWaitRemote())
			Expect(calls.Load()).To(Equal(int32(1)))
			Expect(c.Builder().Build().GetValue()).To(Equal("done"))

			By("not forwarding when the synced conditions do not match")
			sendAndWait(follower, "c2", "PROMOTE", wrapperspb.String("done"), WithWaitRemote())
			Expect(calls.Load()).To(Equal(int32(1)))
		})

		It("should forward without waiting by default", func() {
			leader, leaderAddr := startNode(1, "", clusterBitmap("pending", "done"), func(n *clusterNode) {
				n.AddFuncTransition("promote", "PROMOTE", promote("done"), WithFuncKey("promote"))
			})
			ready(leader)
			follower, _ := startNode(2, leaderAddr, clusterBitmap("pending", "done"), nil)
			ready(follower)

			c := sendAndWait(follower, "c1", "PROMOTE", wrapperspb.String("pending"))
			Expect(c.Builder().Build().GetValue()).To(Equal("pending"))

			// the owner executes once the sender released the lock
			Consistently(calls.Load, 500*time.Millisecond).Should(BeZero())
			c.Close()
			Eventually(calls.Load, 10*time.Second).Should(Equal(int32(1)))
		})

		It("should push transitions added at runtime to the other nodes", func() {
			leader, leaderAddr := startNode(1, "", clusterBitmap("pending", "done"), nil)
			ready(leader)
			follower, _ := startNode(2, leaderAddr, clusterBitmap("pending", "done"), nil)
			ready(follower)

			follower.AddFuncTransition("late", "LATE", promote("done"), WithFuncKey("late"))

			Eventually(func() *Transition { return transition(leader, "late") }, 10*time.Second).ShouldNot(BeNil())
			Expect(transition(leader, "late").NodeIds).To(Equal([]string{"2"}))
		})

		It("should run a transition locally when this node owns it too", func() {
			var leaderCalls atomic.Int32
			leader, leaderAddr := startNode(1, "", clusterBitmap("pending", "done"), func(n *clusterNode) {
				n.AddFuncTransition("promote", "PROMOTE", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
					leaderCalls.Add(1)
					return false, nil
				}, WithFuncKey("promote"))
			})
			ready(leader)
			follower, _ := startNode(2, leaderAddr, clusterBitmap("pending", "done"), func(n *clusterNode) {
				n.AddFuncTransition("promote", "PROMOTE", promote("done"), WithFuncKey("promote"))
			})
			ready(follower)

			Expect(transition(follower, "promote").NodeIds).To(ConsistOf("1", "2"))
			Eventually(func() []string { return transition(leader, "promote").NodeIds }, 10*time.Second).Should(ConsistOf("1", "2"))

			sendAndWait(follower, "c1", "PROMOTE", wrapperspb.String("pending"), WithWaitRemote())
			Expect(calls.Load()).To(Equal(int32(1)))
			Expect(leaderCalls.Load()).To(BeZero())
		})

		It("should run synced state transitions locally", func() {
			leader, leaderAddr := startNode(1, "", clusterBitmap("pending", "done"), func(n *clusterNode) {
				n.AddStateTransition("finish", "FINISH", wrapperspb.String("done"))
			})
			ready(leader)
			follower, _ := startNode(2, leaderAddr, clusterBitmap("pending", "done"), nil)
			ready(follower)

			t := transition(follower, "finish")
			Expect(t).NotTo(BeNil())
			Expect(t.Remote).To(BeFalse())

			c := sendAndWait(follower, "c1", "FINISH", wrapperspb.String("pending"))
			Expect(c.Builder().Build().GetValue()).To(Equal("done"))
		})

		It("should fail a joining node with a transition of the same name and a different signature", func() {
			leader, leaderAddr := startNode(1, "", clusterBitmap("pending", "done"), func(n *clusterNode) {
				n.AddFuncTransition("promote", "PROMOTE", promote("done"), WithFuncKey("a"))
			})
			ready(leader)
			follower, _ := startNode(2, leaderAddr, clusterBitmap("pending", "done"), func(n *clusterNode) {
				n.AddFuncTransition("promote", "PROMOTE", promote("done"), WithFuncKey("b"))
			})

			Eventually(follower.Err(), 20*time.Second).Should(Receive(MatchError(ContainSubstring("different signature"))))
			Consistently(leader.Err()).ShouldNot(Receive())
		})
	})
})
