package asm

import (
	"context"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var _ = Describe("Reentrant containers", func() {
	var (
		n        *Node[*TestStateEntity, *mockBuilder]
		mu       sync.Mutex
		seen     []string
		started  chan struct{}
		releaseC chan struct{}
		release  func()
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
	// newNode adds the transitions: ACT of a container waits for SLOW in b, which works until release; PING and PONG
	// record themselves
	newNode := func(opts ...NodeSetupOption) {
		bitmap, _ := createSimpleTestBitmap()
		n = NewNode(1, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "",
			append([]NodeSetupOption{WithSkipRaft(), WithBitmap(bitmap), WithLogger(zap.NewNop())}, opts...)...)
		n.AddFuncTransition("act", "ACT", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			record(c.Id() + " waits")
			err := c.Emit(ctx, "SLOW", &TestEvent{}, WithTarget("b"), WithWaitRemote())
			record(c.Id() + " goes on")
			return false, err
		})
		n.AddFuncTransition("slow", "SLOW", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			started <- struct{}{}
			<-releaseC
			record(c.Id() + " worked")
			return false, nil
		})
		for _, event := range []string{"PING", "PONG"} {
			n.AddFuncTransition(event, event, func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
				record(c.Id() + " " + event)
				return false, nil
			})
		}
	}
	start := func(id string, opts ...ContainerOption) *Container[*TestStateEntity, *mockBuilder] {
		GinkgoHelper()
		c, _, err := n.StartContainer(context.Background(), id, opts...)
		Expect(err).NotTo(HaveOccurred())
		return c
	}
	// waiting sends ACT to a and returns once its transition waits for b
	waiting := func(a *Container[*TestStateEntity, *mockBuilder]) chan struct{} {
		GinkgoHelper()
		doneC := make(chan struct{}, 1)
		Expect(a.Event("ACT", &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())
		Eventually(started).Should(Receive())
		return doneC
	}

	BeforeEach(func() {
		seen, started, releaseC = nil, make(chan struct{}, 1), make(chan struct{})
		release = sync.OnceFunc(func() { close(releaseC) })
	})

	AfterEach(func() {
		release()
		n.Shutdown()
	})

	It("should run the events arriving while a transition waits, and go on afterwards", func() {
		newNode(WithReentrant())
		a := start("a")
		doneC := waiting(a)

		pingC := make(chan struct{}, 1)
		Expect(a.Event("PING", &TestEvent{}, WithDoneChannel(pingC))).To(Succeed())
		Eventually(pingC).Should(Receive())
		Expect(recorded()).To(Equal([]string{"a waits", "a PING"}))
		Expect(n.metrics.eventsInterleaved.Load()).To(BeNumerically("==", 1))

		release()
		Eventually(doneC).Should(Receive())
		Expect(recorded()).To(Equal([]string{"a waits", "a PING", "b worked", "a goes on"}))
		a.Close()
	})

	It("should block a container whose reentrance is turned off", func() {
		newNode(WithReentrant())
		a := start("a", WithContainerNotReentrant())
		doneC := waiting(a)

		pingC := make(chan struct{}, 1)
		Expect(a.Event("PING", &TestEvent{}, WithDoneChannel(pingC))).To(Succeed())
		Consistently(pingC, 200*time.Millisecond).ShouldNot(Receive())

		release()
		Eventually(doneC).Should(Receive())
		Eventually(pingC).Should(Receive())
		Expect(recorded()).To(Equal([]string{"a waits", "b worked", "a goes on", "a PING"}))
		Expect(n.metrics.eventsInterleaved.Load()).To(BeZero())
		a.Close()
	})

	It("should run only the listed events in between, the others after the waiting transition", func() {
		newNode()
		a := start("a", WithContainerReentrant("PING"))
		doneC := waiting(a)

		pongC, pingC := make(chan struct{}, 1), make(chan struct{}, 1)
		Expect(a.Event("PONG", &TestEvent{}, WithDoneChannel(pongC))).To(Succeed())
		Expect(a.Event("PING", &TestEvent{}, WithDoneChannel(pingC))).To(Succeed())
		Eventually(pingC).Should(Receive())
		Consistently(pongC, 200*time.Millisecond).ShouldNot(Receive())

		release()
		Eventually(doneC).Should(Receive())
		Eventually(pongC).Should(Receive())
		Expect(recorded()).To(Equal([]string{"a waits", "a PING", "b worked", "a goes on", "a PONG"}))
		a.Close()
	})

	It("should run a call back into the waiting container", func() {
		newNode(WithReentrant())
		n.AddFuncTransition("trade", "TRADE", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			err := c.Emit(ctx, "ACCEPT", &TestEvent{}, WithTarget("b"), WithWaitRemote())
			record(c.Id() + " traded")
			return false, err
		})
		n.AddFuncTransition("accept", "ACCEPT", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			// b calls a back while a waits for b
			return false, c.Emit(ctx, "PING", &TestEvent{}, WithTarget("a"), WithWaitRemote())
		})
		a := start("a")
		doneC := make(chan struct{}, 1)
		Expect(a.Event("TRADE", &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())
		Eventually(doneC, 5*time.Second).Should(Receive())
		Expect(recorded()).To(Equal([]string{"a PING", "a traded"}))
		a.Close()
	})

	It("should not count a waiting reentrant container as busy for the events it runs meanwhile", func() {
		newNode(WithReentrant())
		errC := make(chan error, 1)
		n.AddFuncTransition("try", "TRY", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			errC <- c.Emit(ctx, "PING", &TestEvent{}, WithTarget("a"), WithWaitRemote(), WithOnBusy(OnBusyFail))
			return false, nil
		})
		a := start("a")
		doneC := waiting(a)

		buyer := start("buyer")
		Expect(buyer.Event("TRY", &TestEvent{})).To(Succeed())
		Eventually(errC).Should(Receive(BeNil()))
		Expect(recorded()).To(ContainElement("a PING"))

		release()
		Eventually(doneC).Should(Receive())
		a.Close()
		buyer.Close()
	})
})

var _ = Describe("Reentrant containers in a cluster", Label("cluster"), func() {
	var tc *testCluster

	BeforeEach(func() {
		tc = newTestCluster()
	})

	AfterEach(func() {
		tc.stopAll()
	})

	// tradeSetup: TRADE calls the payload's container with ACCEPT, which calls the caller back with CONFIRM
	tradeSetup := func(errC chan error) func(n *clusterNode) {
		return func(n *clusterNode) {
			n.AddFuncTransition("trade", "TRADE", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				errC <- c.Emit(ctx, "ACCEPT", wrapperspb.String(c.Id()), WithTarget(a.(*wrapperspb.StringValue).GetValue()), WithWaitRemote())
				return false, nil
			}, WithFuncKey("trade"))
			n.AddFuncTransition("accept", "ACCEPT", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				return false, c.Emit(ctx, "CONFIRM", wrapperspb.String(c.Id()), WithTarget(a.(*wrapperspb.StringValue).GetValue()), WithWaitRemote())
			}, WithFuncKey("accept"))
			n.AddFuncTransition("confirm", "CONFIRM", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				return false, c.Reply(ctx, wrapperspb.String("confirmed"))
			}, WithFuncKey("confirm"))
		}
	}

	It("should run a call back into a reentrant container instead of refusing it, unless it filters events", func() {
		tc.opts = []NodeSetupOption{WithReentrant()}
		errC := make(chan error, 1)
		n := tc.start(1, "", clusterBitmap("pending", "done"), tradeSetup(errC))
		expectReady(n)

		By("a reentrant for every event")
		a, _, err := n.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(a.Event("TRADE", wrapperspb.String("b"))).To(Succeed())
		Eventually(errC, 10*time.Second).Should(Receive(BeNil()))
		a.Close()

		By("c running only other events in between: the call back is refused")
		c, _, err := n.StartContainer(context.Background(), "c", WithContainerReentrant("OTHER"))
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Event("TRADE", wrapperspb.String("d"))).To(Succeed())
		Eventually(errC, 10*time.Second).Should(Receive(MatchError(ContainSubstring("would deadlock"))))
		c.Close()
	})

	It("should block a reentrant container while its lock is lent", func() {
		tc.opts = []NodeSetupOption{WithReentrant()}
		started, releaseC := make(chan struct{}, 1), make(chan struct{})
		release := sync.OnceFunc(func() { close(releaseC) })
		defer release()
		var mu sync.Mutex
		var seen []string
		record := func(s string) {
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, s)
		}

		n1 := tc.start(1, "", clusterBitmap("pending", "done"), func(n *clusterNode) {
			n.AddFuncTransition("act", "ACT", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				err := c.Emit(ctx, "LENT", a, WithWaitRemote())
				record("goes on")
				return false, err
			}, WithFuncKey("act"))
			n.AddFuncTransition("ping", "PING", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				record("ping")
				return false, nil
			}, WithFuncKey("ping"))
		})
		expectReady(n1)
		expectReady(tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), func(n *clusterNode) {
			n.AddFuncTransition("lent", "LENT", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				started <- struct{}{}
				<-releaseC
				return false, nil
			}, WithFuncKey("lent"))
		}))
		Eventually(func() *Transition { return publishedTransition(n1, "lent") }, 10*time.Second).ShouldNot(BeNil())

		a, _, err := n1.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())
		a.Builder().Set(wrapperspb.String("pending"))
		doneC := make(chan struct{}, 1)
		Expect(a.Event("ACT", wrapperspb.String("x"), WithDoneChannel(doneC))).To(Succeed())
		Eventually(started, 10*time.Second).Should(Receive())

		pingC := make(chan struct{}, 1)
		Expect(a.Event("PING", wrapperspb.String("y"), WithDoneChannel(pingC))).To(Succeed())
		Consistently(pingC, 300*time.Millisecond).ShouldNot(Receive())

		release()
		Eventually(doneC, 10*time.Second).Should(Receive())
		Eventually(pingC, 10*time.Second).Should(Receive())
		mu.Lock()
		Expect(seen).To(Equal([]string{"goes on", "ping"}))
		mu.Unlock()
		a.Close()
	})
})
