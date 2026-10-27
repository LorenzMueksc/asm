package asm

import (
	"context"
	"errors"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var _ = Describe("Calling another container of the entity", func() {
	var (
		n    *Node[*TestStateEntity, *mockBuilder]
		mu   sync.Mutex
		seen []string
	)
	record := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, s)
	}
	recorded := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}

	BeforeEach(func() {
		seen = nil
		bitmap, _ := createSimpleTestBitmap()
		n = NewNode(1, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "",
			WithSkipRaft(), WithBitmap(bitmap), WithLogger(zap.NewNop()))
	})

	AfterEach(func() {
		n.Shutdown()
	})

	send := func(c *Container[*TestStateEntity, *mockBuilder], event string) {
		GinkgoHelper()
		doneC := make(chan struct{}, 1)
		Expect(c.Event(event, &TestEvent{Value: "v"}, WithDoneChannel(doneC))).To(Succeed())
		Eventually(doneC).Should(Receive())
	}

	It("should run a waited call in the target container and return its reply", func() {
		n.AddFuncTransition("buy", "BUY", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			var reply any
			err := c.Emit(ctx, "SELL", &TestEvent{Value: "10 grain"}, WithTarget("b"), WithWaitRemote(),
				WithReply(func(r any) { reply = r }))
			record(c.Id() + " got " + reply.(*TestEvent).Value)
			return false, err
		})
		n.AddFuncTransition("sell", "SELL", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			record(c.Id() + " sells " + a.(*TestEvent).Value)
			return false, c.Reply(ctx, &TestEvent{Value: "50 money"})
		})

		a, _, err := n.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())
		send(a, "BUY")
		Expect(recorded()).To(Equal([]string{"b sells 10 grain", "a got 50 money"}))
		a.Close()
	})

	It("should return the errors of the target's transitions", func() {
		errC := make(chan error, 1)
		n.AddFuncTransition("buy", "BUY", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			errC <- c.Emit(ctx, "SELL", &TestEvent{}, WithTarget("b"), WithWaitRemote())
			return false, nil
		})
		n.AddFuncTransition("sell", "SELL", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			return false, errors.New("out of grain")
		})

		a, _, err := n.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())
		send(a, "BUY")
		Expect(errC).To(Receive(MatchError(ContainSubstring("out of grain"))))
		a.Close()
	})

	It("should hand the event over to the target without waiting", func() {
		n.AddFuncTransition("gift", "GIFT", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			err := c.Emit(ctx, "RECEIVE", &TestEvent{Value: "food"}, WithTarget("b"))
			record(c.Id() + " gave")
			return false, err
		})
		n.AddFuncTransition("receive", "RECEIVE", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			record(c.Id() + " received " + a.(*TestEvent).Value)
			// nobody waits for a hand-over
			Expect(c.Reply(ctx, "thanks")).To(MatchError(ErrNoCaller))
			return false, nil
		})

		a, _, err := n.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())
		send(a, "GIFT")
		Eventually(recorded).Should(ConsistOf("a gave", "b received food"))
		a.Close()
	})

	It("should refuse the emitting container as target and replies outside of a transition", func() {
		errC := make(chan error, 1)
		n.AddFuncTransition("self", "SELF", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			errC <- c.Emit(ctx, "X", &TestEvent{}, WithTarget(c.Id()), WithWaitRemote())
			return false, nil
		})

		a, _, err := n.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())
		send(a, "SELF")
		Expect(errC).To(Receive(MatchError(ContainSubstring("the target is the emitting container"))))
		Expect(a.Reply(context.Background(), "x")).To(MatchError(ErrNotInTransition))
		a.Close()
	})
})

var _ = Describe("Calling another container of the entity in a cluster", Label("cluster"), func() {
	var tc *testCluster

	BeforeEach(func() {
		tc = newTestCluster()
	})

	AfterEach(func() {
		tc.stopAll()
	})

	It("should call the target on its own lock and refuse a call back closing a cycle", func() {
		errC := make(chan error, 2)
		n := tc.start(1, "", clusterBitmap("pending", "done"), func(n *clusterNode) {
			n.AddFuncTransition("trade", "TRADE", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				var reply any
				err := c.Emit(ctx, "ACCEPT", wrapperspb.String(c.Id()), WithTarget(a.(*wrapperspb.StringValue).GetValue()),
					WithWaitRemote(), WithReply(func(r any) { reply = r }))
				if err == nil && reply.(*wrapperspb.StringValue).GetValue() != "accepted" {
					err = errors.New("unexpected reply")
				}
				errC <- err
				return false, nil
			})
			n.AddFuncTransition("accept", "ACCEPT", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				caller := a.(*wrapperspb.StringValue).GetValue()
				if caller == "b" {
					// the caller of a waits for a: a call back would wait for itself
					return false, c.Emit(ctx, "ACCEPT", wrapperspb.String(c.Id()), WithTarget(caller), WithWaitRemote())
				}
				return false, c.Reply(ctx, wrapperspb.String("accepted"))
			})
		})
		expectReady(n)

		By("a calling b, which accepts")
		a, _, err := n.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(a.Event("TRADE", wrapperspb.String("b"))).To(Succeed())
		Eventually(errC, 10*time.Second).Should(Receive(BeNil()))

		By("b calling a, which calls b back")
		b, _, err := n.StartContainer(context.Background(), "b")
		Expect(err).NotTo(HaveOccurred())
		start := time.Now()
		Expect(b.Event("TRADE", wrapperspb.String("a"))).To(Succeed())
		Eventually(errC, 10*time.Second).Should(Receive(MatchError(ContainSubstring("would deadlock"))))
		Expect(time.Since(start)).To(BeNumerically("<", 5*time.Second))
		a.Close()
		b.Close()
	})

	It("should run a target call on the node holding the target's lock", func() {
		var mu sync.Mutex
		ranOn := map[string]int{}
		setup := func(n *clusterNode) {
			n.AddFuncTransition("trade", "TRADE", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				return false, c.Emit(ctx, "ACCEPT", wrapperspb.String(c.Id()), WithTarget(a.(*wrapperspb.StringValue).GetValue()), WithWaitRemote())
			}, WithFuncKey("trade"))
			id := n.Id()
			n.AddFuncTransition("accept", "ACCEPT", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				mu.Lock()
				ranOn[id]++
				mu.Unlock()
				return false, c.Reply(ctx, wrapperspb.String("accepted on "+id))
			}, WithFuncKey("accept"))
		}
		n1 := tc.start(1, "", clusterBitmap("pending", "done"), setup)
		expectReady(n1)
		n2 := tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), setup)
		expectReady(n2)
		Eventually(func() []string { return publishedTransition(n1, "accept").NodeIds }, 10*time.Second).Should(ConsistOf("1", "2"))

		By("b open on node 2")
		b, _, err := n2.StartContainer(context.Background(), "b")
		Expect(err).NotTo(HaveOccurred())

		By("a on node 1 calling b three times")
		a, _, err := n1.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())
		for range 3 {
			doneC := make(chan struct{}, 1)
			Expect(a.Event("TRADE", wrapperspb.String("b"), WithDoneChannel(doneC))).To(Succeed())
			Eventually(doneC, 10*time.Second).Should(Receive())
		}
		mu.Lock()
		Expect(ranOn).To(Equal(map[string]int{"2": 3}))
		mu.Unlock()
		l, held, _ := n1.locks.get("b")
		Expect(held).To(BeTrue())
		Expect(l.holder).To(Equal("2"))
		Expect(n1.metrics.forwardsRedirected.Load()).To(BeNumerically("==", 3))
		a.Close()
		b.Close()
	})

	It("should return the replies and errors of waited forwards to Emit", func() {
		n1 := tc.start(1, "", clusterBitmap("pending", "done"), nil)
		expectReady(n1)
		expectReady(tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), func(n *clusterNode) {
			n.AddFuncTransition("quote", "QUOTE", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				if a.(*wrapperspb.StringValue).GetValue() == "iron" {
					return false, errors.New("no iron in stock")
				}
				return false, c.Reply(ctx, wrapperspb.String("12.5"))
			}, WithFuncKey("quote"))
		}))
		Eventually(func() *Transition { return publishedTransition(n1, "quote") }, 10*time.Second).ShouldNot(BeNil())

		type outcome struct {
			replies []string
			err     error
		}
		outcomes := make(chan outcome, 2)
		n1.AddFuncTransition("ask", "ASK", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
			var o outcome
			o.err = c.Emit(ctx, "QUOTE", a, WithWaitRemote(), WithReply(func(r any) {
				o.replies = append(o.replies, r.(*wrapperspb.StringValue).GetValue())
			}))
			outcomes <- o
			return false, nil
		})

		c, _, err := n1.StartContainer(context.Background(), "buyer")
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Event("ASK", wrapperspb.String("grain"))).To(Succeed())
		Eventually(outcomes, 10*time.Second).Should(Receive(Equal(outcome{replies: []string{"12.5"}})))

		Expect(c.Event("ASK", wrapperspb.String("iron"))).To(Succeed())
		var o outcome
		Eventually(outcomes, 10*time.Second).Should(Receive(&o))
		Expect(o.err).To(MatchError(ContainSubstring("no iron in stock")))
		Expect(o.replies).To(BeEmpty())
		c.Close()
	})
})
