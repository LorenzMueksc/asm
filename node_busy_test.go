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
	"rumue.io/asm/service"
)

var _ = Describe("Lock table busy check", func() {
	var (
		lt lockTable
		t0 = time.Now()
	)
	// acquire proposes the acquire of a waited call failing if busy, with the lock tokens waiting for it
	acquire := func(node, id, token, join string, path ...string) (containerLock, error) {
		_, after, refused := lt.applyAcquire(node, t0, id, token, path, join, true)
		return after, refused
	}

	BeforeEach(func() {
		lt = newLockTable()
	})

	It("should refuse a busy acquire only while another node holds the lock or others wait for it", func() {
		By("granting a free lock")
		l, err := acquire("a", "p", "ta", "", "x")
		Expect(err).NotTo(HaveOccurred())
		Expect(l.holder + "/" + l.token).To(Equal("a/ta"))

		By("refusing another node without queueing it")
		l, err = acquire("b", "p", "tb", "", "y")
		Expect(err).To(MatchError(ErrBusy))
		Expect(l.queue).To(BeEmpty())
		cur, _, _ := lt.get("p")
		Expect(cur.queue).To(BeEmpty())

		By("queueing a call which waits")
		_, _, err = lt.applyAcquire("b", t0, "p", "tb2", []string{"y"}, "", false)
		Expect(err).NotTo(HaveOccurred())
		cur, _, _ = lt.get("p")
		Expect(cur.queued("b", "tb2")).To(BeTrue())

		By("refusing the holder's node while others wait, unless it joins its container")
		_, err = acquire("a", "p", "ta2", "", "z")
		Expect(err).To(MatchError(ErrBusy))
		l, err = acquire("a", "p", "ta3", "ta", "z")
		Expect(err).NotTo(HaveOccurred())
		Expect(l.called("ta3")).To(BeTrue())
	})

	It("should queue the holder's node behind its own lock when nobody else waits", func() {
		lt.applyAcquire("a", t0, "q", "tq", nil, "", false)
		l, err := acquire("a", "q", "tq2", "", "x")
		Expect(err).NotTo(HaveOccurred())
		Expect(l.queued("a", "tq2")).To(BeTrue())
	})

	It("should report a deadlock before a busy lock", func() {
		// tp (holding p) waits for m, held by tm
		lt.applyAcquire("a", t0, "p", "tp", nil, "", false)
		lt.applyAcquire("b", t0, "m", "tm", nil, "", false)
		_, _, err := lt.applyAcquire("a", t0, "m", "tx", []string{"tp"}, "", false)
		Expect(err).NotTo(HaveOccurred())

		_, err = acquire("b", "p", "ty", "", "tm")
		Expect(err).To(MatchError(ErrWouldDeadlock))
	})
})

var _ = Describe("Calling a busy container", func() {
	var (
		n       *Node[*TestStateEntity, *mockBuilder]
		started chan struct{}
		release func()
		errC    chan error
		mu      sync.Mutex
		sold    int
	)

	BeforeEach(func() {
		started, errC, sold = make(chan struct{}, 1), make(chan error, 1), 0
		releaseC := make(chan struct{})
		release = sync.OnceFunc(func() { close(releaseC) })
		bitmap, _ := createSimpleTestBitmap()
		n = NewNode(1, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "",
			WithSkipRaft(), WithBitmap(bitmap), WithLogger(zap.NewNop()), WithEventTrace(100))
		n.AddFuncTransition("work", "WORK", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			started <- struct{}{}
			<-releaseC
			return false, nil
		})
		n.AddFuncTransition("try", "TRY", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			errC <- c.Emit(ctx, "SELL", &TestEvent{}, WithTarget("b"), WithWaitRemote(), WithOnBusy(OnBusyFail))
			return false, nil
		})
		n.AddFuncTransition("buy", "BUY", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			errC <- c.Emit(ctx, "SELL", &TestEvent{}, WithTarget("b"), WithWaitRemote())
			return false, nil
		})
		n.AddFuncTransition("sell", "SELL", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			mu.Lock()
			sold++
			mu.Unlock()
			return false, nil
		})
	})

	AfterEach(func() {
		release()
		n.Shutdown()
	})

	soldCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return sold
	}
	// startBusy opens b and lets it work until release
	startBusy := func() *Container[*TestStateEntity, *mockBuilder] {
		GinkgoHelper()
		b, _, err := n.StartContainer(context.Background(), "b")
		Expect(err).NotTo(HaveOccurred())
		Expect(b.Event("WORK", &TestEvent{})).To(Succeed())
		Eventually(started).Should(Receive())
		return b
	}

	It("should fail a call of a busy target with ErrBusy and run it once the target is idle", func() {
		b := startBusy()
		a, _, err := n.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())

		Expect(a.Event("TRY", &TestEvent{})).To(Succeed())
		Eventually(errC).Should(Receive(MatchError(ErrBusy)))
		Expect(soldCount()).To(Equal(0))
		Expect(n.metrics.lockBusy.Load()).To(BeNumerically("==", 1))
		Expect(traceKinds(n.trace.since(0), "b")).To(ContainElement(service.TraceEvent_ERROR))

		release()
		Eventually(func() bool { return b.containerProcessing.busy(b.reentrant, "SELL") }).Should(BeFalse())
		Expect(a.Event("TRY", &TestEvent{})).To(Succeed())
		Eventually(errC).Should(Receive(BeNil()))
		Expect(soldCount()).To(Equal(1))
		a.Close()
		b.Close()
	})

	It("should wait for a busy target by default", func() {
		b := startBusy()
		a, _, err := n.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())

		Expect(a.Event("BUY", &TestEvent{})).To(Succeed())
		Consistently(errC, 200*time.Millisecond).ShouldNot(Receive())
		release()
		Eventually(errC).Should(Receive(BeNil()))
		Expect(soldCount()).To(Equal(1))
		a.Close()
		b.Close()
	})
})

var _ = Describe("Calling a busy container in a cluster", Label("cluster"), func() {
	var (
		tc      *testCluster
		started chan struct{}
		release func()
		errC    chan error
		mu      sync.Mutex
		ranOn   map[string]int
	)

	// setup adds the transitions of a node: TRY calls the payload's container failing if busy, TRADE waits;
	// accept runs ACCEPT when owned (the node's id goes to ranOn), WORK keeps a container busy until release
	setup := func(accept bool, releaseC <-chan struct{}) func(n *clusterNode) {
		return func(n *clusterNode) {
			call := func(opts ...EventOption) func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				return func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
					errC <- c.Emit(ctx, "ACCEPT", wrapperspb.String(c.Id()), append(opts, WithTarget(a.(*wrapperspb.StringValue).GetValue()), WithWaitRemote())...)
					return false, nil
				}
			}
			n.AddFuncTransition("try", "TRY", call(WithOnBusy(OnBusyFail)), WithFuncKey("try"))
			n.AddFuncTransition("trade", "TRADE", call(), WithFuncKey("trade"))
			n.AddFuncTransition("work", "WORK", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				started <- struct{}{}
				<-releaseC
				return false, nil
			}, WithFuncKey("work"))
			if accept {
				id := n.Id()
				n.AddFuncTransition("accept", "ACCEPT", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
					mu.Lock()
					ranOn[id]++
					mu.Unlock()
					return false, nil
				}, WithFuncKey("accept"))
			}
		}
	}
	var releaseC chan struct{}
	ran := func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		out := map[string]int{}
		for k, v := range ranOn {
			out[k] = v
		}
		return out
	}
	// send sends the event to the container and waits for the result of its call
	send := func(c *Container[*wrapperspb.StringValue, *stringValueBuilder], event, target string) error {
		GinkgoHelper()
		Expect(c.Event(event, wrapperspb.String(target))).To(Succeed())
		var err error
		Eventually(errC, 10*time.Second).Should(Receive(&err))
		return err
	}

	BeforeEach(func() {
		tc = newTestCluster()
		started, errC, ranOn = make(chan struct{}, 1), make(chan error, 1), map[string]int{}
		releaseC = make(chan struct{})
		release = sync.OnceFunc(func() { close(releaseC) })
	})

	AfterEach(func() {
		release()
		tc.stopAll()
	})

	It("should fail while another node not running the call holds the target's lock, also kept with its container closed", func() {
		n1 := tc.start(1, "", clusterBitmap("pending", "done"), setup(true, releaseC))
		expectReady(n1)
		n2 := tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), setup(false, releaseC))
		expectReady(n2)
		Eventually(func() []string { return publishedTransition(n2, "accept").NodeIds }, 10*time.Second).Should(ConsistOf("1"))

		b, _, err := n2.StartContainer(context.Background(), "b")
		Expect(err).NotTo(HaveOccurred())
		a, _, err := n1.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())

		By("b open and idle on node 2")
		Expect(send(a, "TRY", "b")).To(MatchError(ErrBusy))
		Expect(n1.metrics.lockBusy.Load()).To(BeNumerically("==", 1))

		By("b closed, its lock kept by node 2")
		b.Close()
		Eventually(func() bool { return n2.hasContainer("b") }, 5*time.Second).Should(BeFalse())
		Expect(send(a, "TRY", "b")).To(MatchError(ErrBusy))

		By("waiting by default: the lock moves to node 1")
		Expect(send(a, "TRADE", "b")).To(Succeed())
		l, _, _ := n1.locks.get("b")
		Expect(l.holder).To(Equal("1"))

		By("the lock kept by node 1 with b closed")
		Eventually(func() bool { return n1.hasContainer("b") }, 5*time.Second).Should(BeFalse())
		Expect(send(a, "TRY", "b")).To(Succeed())
		Expect(ran()).To(Equal(map[string]int{"1": 2}))
		a.Close()
	})

	It("should fail while the target works on the node holding its lock, and run there once it is idle", func() {
		n1 := tc.start(1, "", clusterBitmap("pending", "done"), setup(true, releaseC))
		expectReady(n1)
		n2 := tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), setup(true, releaseC))
		expectReady(n2)
		Eventually(func() []string { return publishedTransition(n1, "accept").NodeIds }, 10*time.Second).Should(ConsistOf("1", "2"))

		b, _, err := n2.StartContainer(context.Background(), "b")
		Expect(err).NotTo(HaveOccurred())
		Expect(b.Event("WORK", wrapperspb.String(""))).To(Succeed())
		Eventually(started, 5*time.Second).Should(Receive())
		a, _, err := n1.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())

		err = send(a, "TRY", "b")
		Expect(errors.Is(err, ErrBusy)).To(BeTrue(), "%v", err)
		Expect(n2.metrics.lockBusy.Load()).To(BeNumerically("==", 1))
		Expect(n1.metrics.forwardsRedirected.Load()).To(BeNumerically("==", 1))

		release()
		Eventually(func() bool { return b.containerProcessing.busy(b.reentrant, "ACCEPT") }, 5*time.Second).Should(BeFalse())
		Expect(send(a, "TRY", "b")).To(Succeed())
		Expect(ran()).To(Equal(map[string]int{"2": 1}))
		a.Close()
		b.Close()
	})

	It("should fail a call of a client while a member not running the call holds the target's lock", func() {
		expectReady(tc.start(1, "", clusterBitmap("pending", "done"), setup(false, releaseC)))
		n2 := tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), setup(false, releaseC))
		expectReady(n2)
		n3 := tc.startClient(3, tc.addr(1), clusterBitmap("pending", "done"), setup(true, releaseC))
		expectReady(n3)
		Eventually(func() []string { return publishedTransition(n2, "accept").NodeIds }, 10*time.Second).Should(ConsistOf("3"))

		b, _, err := n2.StartContainer(context.Background(), "b")
		Expect(err).NotTo(HaveOccurred())
		a, _, err := n3.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())

		Expect(send(a, "TRY", "b")).To(MatchError(ErrBusy))
		Expect(n3.metrics.lockBusy.Load()).To(BeNumerically("==", 1))
		Expect(ran()).To(BeEmpty())
		a.Close()
		b.Close()
	})
})
