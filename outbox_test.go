package asm

import (
	"context"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

var _ = Describe("Deferred events", func() {
	var (
		n    *Node[*TestStateEntity, *mockBuilder]
		mu   sync.Mutex
		errs []error
		got  []string
	)
	record := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err)
	}
	recorded := func() []error {
		mu.Lock()
		defer mu.Unlock()
		return append([]error(nil), errs...)
	}
	received := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), got...)
	}
	send := func(c *Container[*TestStateEntity, *mockBuilder], event string) {
		GinkgoHelper()
		doneC := make(chan struct{}, 1)
		Expect(c.Event(event, &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())
		Eventually(doneC).Should(Receive())
	}
	outbox := func(c *Container[*TestStateEntity, *mockBuilder]) []envelope {
		c.round.mu.Lock()
		defer c.round.mu.Unlock()
		return append([]envelope(nil), c.round.outbox...)
	}

	BeforeEach(func() {
		errs, got = nil, nil
		bitmap, _ := createSimpleTestBitmap()
		n = NewNode(1, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "",
			WithSkipRaft(), WithBitmap(bitmap), WithLogger(zap.NewNop()))
		n.AddFuncTransition("receive", "MSG", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, c.Id()+" got "+a.(*TestEvent).Value)
			return false, nil
		})
	})

	AfterEach(func() {
		n.Shutdown()
	})

	It("should queue the events in the outbox in emit order with a sequence, without delivering them", func() {
		n.AddFuncTransition("send", "SEND", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			for _, to := range []string{"b", "c", "b", "a"} {
				record(c.Emit(ctx, "MSG", &TestEvent{Value: to}, WithTarget(to), WithDeferred()))
			}
			return false, nil
		})
		a, _, err := n.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())
		b, _, err := n.StartContainer(context.Background(), "b")
		Expect(err).NotTo(HaveOccurred())

		send(a, "SEND")
		Expect(recorded()).To(HaveEach(BeNil()))
		Expect(outbox(a)).To(Equal([]envelope{
			{to: "b", event: "MSG", seq: 0, payload: &TestEvent{Value: "b"}},
			{to: "c", event: "MSG", seq: 1, payload: &TestEvent{Value: "c"}},
			{to: "b", event: "MSG", seq: 2, payload: &TestEvent{Value: "b"}},
			{to: "a", event: "MSG", seq: 3, payload: &TestEvent{Value: "a"}},
		}))
		Expect(outbox(b)).To(BeEmpty())

		By("marking the sender once for the next barrier")
		send(a, "SEND")
		Expect(outbox(a)).To(HaveLen(8))
		Expect(outbox(a)[7].seq).To(Equal(uint64(7)))
		n.rounds.mu.Lock()
		Expect(n.rounds.marked).To(Equal([]*Container[*TestStateEntity, *mockBuilder]{a}))
		n.rounds.mu.Unlock()

		By("not running the target's transitions, nor starting a missing target")
		Consistently(received, 100*time.Millisecond).Should(BeEmpty())
		Expect(n.hasContainer("c")).To(BeFalse())
	})

	It("should refuse deferred events without a target or with options which wait", func() {
		n.AddFuncTransition("send", "SEND", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			record(c.Emit(ctx, "MSG", &TestEvent{}, WithDeferred()))
			record(c.Emit(ctx, "MSG", &TestEvent{}, WithDeferred(), WithTarget("b"), WithWaitRemote()))
			record(c.Emit(ctx, "MSG", &TestEvent{}, WithDeferred(), WithTarget("b"), WithReply(func(any) {})))
			record(c.Emit(ctx, "MSG", &TestEvent{}, WithDeferred(), WithTarget("b"), WithDoneChannel(make(chan struct{}, 1))))
			return false, nil
		})
		a, _, err := n.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())

		send(a, "SEND")
		Expect(recorded()).To(HaveExactElements(
			MatchError(ContainSubstring("needs a target")),
			MatchError(ContainSubstring("don't apply")),
			MatchError(ContainSubstring("don't apply")),
			MatchError(ContainSubstring("don't apply")),
		))
		Expect(outbox(a)).To(BeEmpty())
	})

	It("should refuse a deferred event outside of a transition and without a node", func() {
		a, _, err := n.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(a.Emit(context.Background(), "MSG", &TestEvent{}, WithTarget("b"), WithDeferred())).To(MatchError(ErrNotInTransition))

		bitmap, _ := createSimpleTestBitmap()
		var c *Container[*TestStateEntity, *mockBuilder]
		var emitErr error
		c = NewContainer("x", bitmap, []*Transition{{
			Name: "send", EventName: "SEND", Batching: Batching{Count: 1},
			Actions: []any{transitionFunc[*TestStateEntity, *mockBuilder](func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
				emitErr = c.Emit(ctx, "MSG", &TestEvent{}, WithTarget("b"), WithDeferred())
				return false, nil
			})},
		}}, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, newMockBuilder)
		c.start(context.Background(), nil)
		doneC := make(chan struct{}, 1)
		Expect(c.Event("SEND", &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())
		Eventually(doneC).Should(Receive())
		Expect(emitErr).To(MatchError(ContainSubstring("only containers of a node")))
		c.Close()
	})
})
