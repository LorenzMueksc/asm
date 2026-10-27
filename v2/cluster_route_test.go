package asm

import (
	"context"
	"fmt"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"rumue.io/asm/service"
)

// callLog records the calls of func transitions by "node/transition" with the number of payloads of each call.
type callLog struct {
	mu    sync.Mutex
	calls map[string][]int
}

func newCallLog() *callLog {
	return &callLog{calls: make(map[string][]int)}
}

func (l *callLog) get(key string) []int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]int(nil), l.calls[key]...)
}

// transition registers a func transition on the node which records its calls and sets the entity to result, if not empty.
func (l *callLog) transition(n *clusterNode, name, event, result string, opts ...TransitionOption) {
	key := fmt.Sprintf("%s/%s", n.Id(), name)
	n.AddFuncTransition(name, event, func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
		count := 1
		if batch, ok := a.([]any); ok {
			count = len(batch)
		}
		l.mu.Lock()
		l.calls[key] = append(l.calls[key], count)
		l.mu.Unlock()
		if result != "" {
			c.Builder().Set(wrapperspb.String(result))
		}
		return false, nil
	}, append([]TransitionOption{WithFuncKey(name)}, opts...)...)
}

// goroutinesIn counts the goroutines with the function in their stack.
func goroutinesIn(function string) int {
	var b strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	count := 0
	for _, g := range strings.Split(b.String(), "\n\n") {
		if strings.Contains(g, function) {
			count++
		}
	}
	return count
}

var _ = Describe("Cluster routing forwarded events", Label("cluster"), func() {
	var (
		tc    *testCluster
		calls *callLog
	)

	bitmap := func() *Bitmap { return clusterBitmap("pending", "ready", "done") }
	owners := func(n *clusterNode, name string) []string {
		t := publishedTransition(n, name)
		if t == nil {
			return nil
		}
		return t.NodeIds
	}
	sendEvent := func(c *Container[*wrapperspb.StringValue, *stringValueBuilder], event string, opts ...EventOption) chan struct{} {
		GinkgoHelper()
		doneC := make(chan struct{}, 1)
		Expect(c.Event(event, wrapperspb.Int64(1), append(opts, WithDoneChannel(doneC))...)).To(Succeed())
		return doneC
	}

	BeforeEach(func() {
		tc = newTestCluster()
		calls = newCallLog()
	})

	AfterEach(func() {
		tc.stopAll()
	})

	It("should run a transition owned by several nodes once, on the node owning most of the matches", func() {
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), func(n *clusterNode) {
			calls.transition(n, "x", "WORK", "")
		}))
		expectReady(tc.start(3, tc.addr(1), bitmap(), func(n *clusterNode) {
			calls.transition(n, "x", "WORK", "")
			calls.transition(n, "y", "WORK", "done")
		}))
		Eventually(func() []string { return owners(tc.node(1), "x") }, 10*time.Second).Should(ConsistOf("2", "3"))
		Eventually(func() []string { return owners(tc.node(1), "y") }, 10*time.Second).Should(ConsistOf("3"))

		for i := range 10 {
			Expect(sendWaiting(tc.node(1), fmt.Sprintf("c%d", i), "WORK", wrapperspb.String("pending"))).To(Equal("done"))
		}

		Expect(calls.get("3/x")).To(HaveLen(10))
		Expect(calls.get("3/y")).To(HaveLen(10))
		Expect(calls.get("2/x")).To(BeEmpty())
	})

	It("should not let the receiver run transitions the sender runs itself", func() {
		expectReady(tc.start(1, "", bitmap(), func(n *clusterNode) {
			calls.transition(n, "x", "WORK", "")
		}))
		expectReady(tc.start(2, tc.addr(1), bitmap(), func(n *clusterNode) {
			calls.transition(n, "x", "WORK", "")
			calls.transition(n, "y", "WORK", "done")
		}))
		Eventually(func() []string { return owners(tc.node(1), "y") }, 10*time.Second).Should(ConsistOf("2"))

		for i := range 5 {
			Expect(sendWaiting(tc.node(1), fmt.Sprintf("c%d", i), "WORK", wrapperspb.String("pending"))).To(Equal("done"))
		}

		Expect(calls.get("1/x")).To(HaveLen(5))
		Expect(calls.get("2/y")).To(HaveLen(5))
		Expect(calls.get("2/x")).To(BeEmpty())
	})

	It("should only send events to nodes owning a transition which matched on the sender", func() {
		store := newSharedStore()
		tc.opts = store.options()
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), func(n *clusterNode) {
			calls.transition(n, "touch", "FINISH", "")
			calls.transition(n, "finish", "FINISH", "done", WithConditions(n.All(wrapperspb.String("ready"))))
		}))
		expectReady(tc.start(3, tc.addr(1), bitmap(), func(n *clusterNode) {
			calls.transition(n, "other", "FINISH", "done", WithConditions(n.All(wrapperspb.String("ready"))))
		}))
		Eventually(func() []string { return owners(tc.node(1), "other") }, 10*time.Second).Should(ConsistOf("3"))
		Eventually(func() []string { return owners(tc.node(1), "finish") }, 10*time.Second).Should(ConsistOf("2"))

		store.set("player-1", "pending")
		c, _, err := tc.node(1).StartContainer(context.Background(), "player-1")
		Expect(err).NotTo(HaveOccurred())
		Eventually(sendEvent(c, "FINISH"), 5*time.Second).Should(Receive())

		// the entity the receivers load would match the other transitions
		store.set("player-1", "ready")
		c.Close()

		Eventually(func() []int { return calls.get("2/touch") }, 10*time.Second).Should(Equal([]int{1}))
		Consistently(func() []int { return append(calls.get("2/finish"), calls.get("3/other")...) }, time.Second).Should(BeEmpty())
	})

	It("should forward remote transitions at their place in the priority order", func() {
		expectReady(tc.start(1, "", bitmap(), func(n *clusterNode) {
			calls.transition(n, "local", "WORK", "done", WithPriority(5), WithConditions(n.All(wrapperspb.String("ready"))))
		}))
		expectReady(tc.start(2, tc.addr(1), bitmap(), func(n *clusterNode) {
			calls.transition(n, "first", "WORK", "ready", WithPriority(10))
			calls.transition(n, "last", "WORK", "", WithPriority(1), WithConditions(n.All(wrapperspb.String("done"))))
		}))
		Eventually(func() []string { return owners(tc.node(1), "first") }, 10*time.Second).Should(ConsistOf("2"))
		Eventually(func() []string { return owners(tc.node(1), "last") }, 10*time.Second).Should(ConsistOf("2"))

		By("running the remote transition with the higher priority before the local one, which needs its result")
		Expect(sendWaiting(tc.node(1), "c1", "WORK", wrapperspb.String("pending"))).To(Equal("done"))
		Expect(calls.get("2/first")).To(Equal([]int{1}))
		Expect(calls.get("1/local")).To(Equal([]int{1}))

		By("forwarding the remote transition with the lower priority after the local one")
		Expect(calls.get("2/last")).To(Equal([]int{1}))
	})

	It("should return only the changed bits when they describe the changes, otherwise the entity", func() {
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), func(n *clusterNode) {
			calls.transition(n, "known", "KNOWN", "done")
			calls.transition(n, "unknown", "UNKNOWN", "not in the bitmap")
		}))
		Eventually(func() []string { return owners(tc.node(1), "unknown") }, 10*time.Second).Should(ConsistOf("2"))

		c, _, err := tc.node(1).StartContainer(context.Background(), "player-1")
		Expect(err).NotTo(HaveOccurred())
		defer c.Close()

		forward := func(event string) *service.ForwardEventResponse {
			GinkgoHelper()
			entity, err := anypb.New(wrapperspb.String("pending"))
			Expect(err).NotTo(HaveOccurred())
			payload, err := anypb.New(wrapperspb.Int64(1))
			Expect(err).NotTo(HaveOccurred())
			resp, err := tc.node(2).ForwardEvent(context.Background(), &service.ForwardEventRequest{
				EventName:   event,
				ContainerId: "player-1",
				Entity:      entity,
				Payloads:    []*anypb.Any{payload},
				LendChain:   []string{"1"},
				Wait:        true,
			})
			Expect(err).NotTo(HaveOccurred())
			return resp
		}

		known := forward("KNOWN")
		Expect(known.GetEntity()).To(BeNil())
		Expect(bitsFromProto(known.GetChangedState()).Equal(NewBits(layoutIndices(tc.node(2))["done"]))).To(BeTrue())

		unknown := forward("UNKNOWN")
		Expect(unknown.GetChangedState()).To(BeNil())
		Expect(unknown.GetEntity()).NotTo(BeNil())

		By("applying both kinds on the sender")
		Expect(sendWaiting(tc.node(1), "c1", "KNOWN", wrapperspb.String("pending"))).To(Equal("done"))
		Expect(sendWaiting(tc.node(1), "c2", "UNKNOWN", wrapperspb.String("pending"))).To(Equal("not in the bitmap"))
	})

	It("should return the changes of transitions which modify the entity in place", func() {
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), func(n *clusterNode) {
			inPlace := func(value string) func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				return func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
					c.Builder().Build().Value = value
					return false, nil
				}
			}
			n.AddFuncTransition("bits", "BITS", inPlace("done"), WithFuncKey("bits"))
			n.AddFuncTransition("entity", "ENTITY", inPlace("not in the bitmap"), WithFuncKey("entity"))
		}))
		Eventually(func() []string { return owners(tc.node(1), "entity") }, 10*time.Second).Should(ConsistOf("2"))

		Expect(sendWaiting(tc.node(1), "c1", "BITS", wrapperspb.String("pending"))).To(Equal("done"))
		Expect(sendWaiting(tc.node(1), "c2", "ENTITY", wrapperspb.String("pending"))).To(Equal("not in the bitmap"))
	})

	It("should keep a transition owned by several nodes in sync while the bitmap grows", func() {
		shared := func(n *clusterNode) {
			calls.transition(n, "rare", "WORK", "done", WithConditions(n.All(wrapperspb.String("rare"))))
		}
		expectReady(tc.start(1, "", bitmap(), shared))
		expectReady(tc.start(2, tc.addr(1), bitmap(), shared))
		Eventually(func() []string { return owners(tc.node(1), "rare") }, 10*time.Second).Should(ConsistOf("1", "2"))

		By("a node joining with the value of the condition, the owners compile it again")
		expectReady(tc.start(3, tc.addr(1), clusterBitmap("pending", "rare"), nil))
		Eventually(func() map[string]uint64 { return layoutIndices(tc.node(1)) }, 10*time.Second).Should(HaveKey("rare"))
		rare := layoutIndices(tc.node(1))["rare"]

		conditionBits := func(id int) []uint64 {
			t := publishedTransition(tc.node(id), "rare")
			if t == nil || len(t.Conditions) == 0 {
				return nil
			}
			var indices []uint64
			for _, state := range t.Conditions[0].States["StringValue"] {
				indices = append(indices, state.Indices()...)
			}
			return indices
		}
		for id := 1; id <= 3; id++ {
			Eventually(func() []uint64 { return conditionBits(id) }, 10*time.Second).Should(Equal([]uint64{rare}), "node %d", id)
		}
		for id := 1; id <= 2; id++ {
			Consistently(tc.node(id).Err(), 500*time.Millisecond).ShouldNot(Receive(), "node %d", id)
		}
	})

	It("should run a waited event while work of the same container handed over before waits for the lock", func() {
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), func(n *clusterNode) {
			calls.transition(n, "later", "LATER", "")
			calls.transition(n, "now", "NOW", "done")
		}))
		Eventually(func() []string { return owners(tc.node(1), "now") }, 10*time.Second).Should(ConsistOf("2"))
		Eventually(func() []string { return owners(tc.node(1), "later") }, 10*time.Second).Should(ConsistOf("2"))

		c, _, err := tc.node(1).StartContainer(context.Background(), "player-1")
		Expect(err).NotTo(HaveOccurred())
		c.Builder().Set(wrapperspb.String("pending"))

		By("handing work over, node 2 waits for the lock of node 1")
		Eventually(sendEvent(c, "LATER"), 5*time.Second).Should(Receive())
		Eventually(func() bool {
			l, _, _ := tc.node(1).locks.get("player-1")
			for _, w := range l.queue {
				if w.node == "2" {
					return true
				}
			}
			return false
		}, 5*time.Second).Should(BeTrue())

		By("lending the lock to node 2 for a waited event of the same container")
		Eventually(sendEvent(c, "NOW", WithWaitRemote()), 10*time.Second).Should(Receive())
		Expect(c.Builder().Build().GetValue()).To(Equal("done"))
		Expect(calls.get("2/now")).To(Equal([]int{1}))

		c.Close()
		Eventually(func() []int { return calls.get("2/later") }, 10*time.Second).Should(Equal([]int{1}))
	})

	It("should wait for the locks of handed over work without a goroutine each and run the work in order", func() {
		const count = 100
		var mu sync.Mutex
		order := make(map[string][]string)
		record := func(n *clusterNode, name, event string) {
			n.AddFuncTransition(name, event, func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				mu.Lock()
				order[c.Id()] = append(order[c.Id()], name)
				mu.Unlock()
				return false, nil
			}, WithFuncKey(name))
		}
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), func(n *clusterNode) {
			record(n, "first", "FIRST")
			record(n, "second", "SECOND")
		}))
		Eventually(func() []string { return owners(tc.node(1), "second") }, 10*time.Second).Should(ConsistOf("2"))

		containers := make([]*Container[*wrapperspb.StringValue, *stringValueBuilder], count)
		for i := range containers {
			c, _, err := tc.node(1).StartContainer(context.Background(), fmt.Sprintf("h-%d", i))
			Expect(err).NotTo(HaveOccurred())
			c.Builder().Set(wrapperspb.String("pending"))
			containers[i] = c
		}
		By("handing work of every container over while node 1 holds the locks")
		for _, c := range containers {
			Eventually(sendEvent(c, "FIRST"), 5*time.Second).Should(Receive())
			Eventually(sendEvent(c, "SECOND"), 5*time.Second).Should(Receive())
		}
		pending := tc.node(2).pendingCount
		Eventually(pending, 10*time.Second).Should(Equal(count))
		Expect(goroutinesIn("drainForwarded")).To(BeZero())

		By("running the work in order once node 1 released the locks")
		for _, c := range containers {
			c.Close()
		}
		Eventually(func(g Gomega) {
			mu.Lock()
			defer mu.Unlock()
			g.Expect(order).To(HaveLen(count))
			for _, names := range order {
				g.Expect(names).To(Equal([]string{"first", "second"}))
			}
		}, 20*time.Second).Should(Succeed())
		Expect(pending()).To(BeZero())
	})

	Context("lending through several nodes", func() {
		relay := func(name, event, next string, wait bool) func(n *clusterNode) {
			return func(n *clusterNode) {
				n.AddFuncTransition(name, event, func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
					opts := []EventOption{}
					if wait {
						opts = append(opts, WithWaitRemote())
					}
					return false, c.Emit(ctx, next, wrapperspb.Int64(1), opts...)
				}, WithFuncKey(name))
			}
		}

		It("should lend a borrowed lock on to the node a transition emits a waited event to", func() {
			expectReady(tc.start(1, "", bitmap(), nil))
			expectReady(tc.start(2, tc.addr(1), bitmap(), relay("relay", "HOP", "NEXT", true)))
			expectReady(tc.start(3, tc.addr(1), bitmap(), func(n *clusterNode) {
				calls.transition(n, "final", "NEXT", "done")
			}))
			Eventually(func() []string { return owners(tc.node(1), "relay") }, 10*time.Second).Should(ConsistOf("2"))
			Eventually(func() []string { return owners(tc.node(2), "final") }, 10*time.Second).Should(ConsistOf("3"))

			// node 1 lends to node 2, which lends on to node 3 while it waits
			Expect(sendWaiting(tc.node(1), "c1", "HOP", wrapperspb.String("pending"))).To(Equal("done"))
			Expect(calls.get("3/final")).To(Equal([]int{1}))
		})

		It("should follow the event through all nodes in one trace", func() {
			tc.opts = []NodeSetupOption{WithEventTrace(100)}
			expectReady(tc.start(1, "", bitmap(), nil))
			expectReady(tc.start(2, tc.addr(1), bitmap(), relay("relay", "HOP", "NEXT", true)))
			expectReady(tc.start(3, tc.addr(1), bitmap(), func(n *clusterNode) {
				calls.transition(n, "final", "NEXT", "done")
			}))
			Eventually(func() []string { return owners(tc.node(1), "relay") }, 10*time.Second).Should(ConsistOf("2"))
			Eventually(func() []string { return owners(tc.node(2), "final") }, 10*time.Second).Should(ConsistOf("3"))

			Expect(sendWaiting(tc.node(1), "c1", "HOP", wrapperspb.String("pending"))).To(Equal("done"))

			// span returns the trace entry of a node with the kind and event, and its trace
			span := func(node int, kind service.TraceEvent_Kind, event string) (*service.TraceEvent, *service.TraceRef) {
				GinkgoHelper()
				var found *service.TraceEvent
				Eventually(func() *service.TraceEvent {
					for _, e := range tc.node(node).trace.since(0) {
						if e.GetKind() == kind && e.GetEventName() == event && len(e.GetTraces()) > 0 {
							found = e
						}
					}
					return found
				}, 5*time.Second).ShouldNot(BeNil(), "%s %s on node %d", kind, event, node)
				Expect(found.GetSpanId()).NotTo(BeEmpty())
				Expect(found.GetTraces()).To(HaveLen(1))
				return found, found.GetTraces()[0]
			}

			event1, root := span(1, service.TraceEvent_EVENT, "HOP")
			forward1, f1 := span(1, service.TraceEvent_FORWARD, "HOP")
			received2, r2 := span(2, service.TraceEvent_RECEIVED, "HOP")
			event2, e2 := span(2, service.TraceEvent_EVENT, "NEXT")
			forward2, f2 := span(2, service.TraceEvent_FORWARD, "NEXT")
			_, r3 := span(3, service.TraceEvent_RECEIVED, "NEXT")

			By("starting the trace with the event sent to the container")
			Expect(root.GetParentSpanId()).To(BeEmpty())
			for _, ref := range []*service.TraceRef{f1, r2, e2, f2, r3} {
				Expect(ref.GetTraceId()).To(Equal(root.GetTraceId()))
			}

			By("linking every span to the one it is part of")
			Expect(f1.GetParentSpanId()).To(Equal(event1.GetSpanId()))
			Expect(r2.GetParentSpanId()).To(Equal(forward1.GetSpanId()))
			Expect(e2.GetParentSpanId()).To(Equal(received2.GetSpanId()))
			Expect(f2.GetParentSpanId()).To(Equal(event2.GetSpanId()))
			Expect(r3.GetParentSpanId()).To(Equal(forward2.GetSpanId()))
		})

		It("should hand work emitted without waiting over: the next node gets the lock once the holder released it", func() {
			expectReady(tc.start(1, "", bitmap(), nil))
			expectReady(tc.start(2, tc.addr(1), bitmap(), relay("relay", "HOP", "NEXT", false)))
			expectReady(tc.start(3, tc.addr(1), bitmap(), func(n *clusterNode) {
				calls.transition(n, "final", "NEXT", "")
			}))
			Eventually(func() []string { return owners(tc.node(1), "relay") }, 10*time.Second).Should(ConsistOf("2"))
			Eventually(func() []string { return owners(tc.node(2), "final") }, 10*time.Second).Should(ConsistOf("3"))

			c, _, err := tc.node(1).StartContainer(context.Background(), "c1")
			Expect(err).NotTo(HaveOccurred())
			c.Builder().Set(wrapperspb.String("pending"))
			Eventually(sendEvent(c, "HOP", WithWaitRemote()), 10*time.Second).Should(Receive())

			By("queuing node 3 for the lock node 1 still holds")
			Consistently(func() []int { return calls.get("3/final") }, time.Second).Should(BeEmpty())

			c.Close()
			Eventually(func() []int { return calls.get("3/final") }, 10*time.Second).Should(Equal([]int{1}))
		})

		It("should refuse a chain whose last lender doesn't run on the lent lock, and a chain back to the receiver", func() {
			expectReady(tc.start(1, "", bitmap(), nil))
			expectReady(tc.start(2, tc.addr(1), bitmap(), nil))
			expectReady(tc.start(3, tc.addr(1), bitmap(), func(n *clusterNode) {
				calls.transition(n, "final", "NEXT", "done")
			}))
			c, _, err := tc.node(1).StartContainer(context.Background(), "c1")
			Expect(err).NotTo(HaveOccurred())
			defer c.Close()

			payload, err := anypb.New(wrapperspb.Int64(1))
			Expect(err).NotTo(HaveOccurred())
			forward := func(to int, chain ...string) error {
				_, err := tc.node(to).ForwardEvent(context.Background(), &service.ForwardEventRequest{
					EventName: "NEXT", ContainerId: "c1", Payloads: []*anypb.Any{payload}, Wait: true,
					SenderNodeId: chain[len(chain)-1], LendChain: chain,
				})
				return err
			}

			Expect(status.Code(forward(3, "1", "2"))).To(Equal(codes.FailedPrecondition))
			Expect(status.Code(forward(2, "1", "2"))).To(Equal(codes.FailedPrecondition))
			Expect(calls.get("3/final")).To(BeEmpty())
		})
	})

	It("should batch forwarded events per event name and let the receiver batch or queue them", func() {
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), func(n *clusterNode) {
			calls.transition(n, "batch", "BATCH", "", WithBatching(3, 0))
			calls.transition(n, "single", "BATCH", "")
		}))
		Eventually(func() []string { return owners(tc.node(1), "batch") }, 10*time.Second).Should(ConsistOf("2"))
		Eventually(func() []string { return owners(tc.node(1), "single") }, 10*time.Second).Should(ConsistOf("2"))

		c, _, err := tc.node(1).StartContainer(context.Background(), "player-1")
		Expect(err).NotTo(HaveOccurred())

		By("holding the events on the sender until the batch is full")
		first := sendEvent(c, "BATCH")
		Consistently(first, 300*time.Millisecond).ShouldNot(Receive())
		second, third := sendEvent(c, "BATCH"), sendEvent(c, "BATCH")
		for _, doneC := range []chan struct{}{first, second, third} {
			Eventually(doneC, 5*time.Second).Should(Receive())
		}
		c.Close()

		By("running the batching transition once and the other one per event")
		Eventually(func() []int { return calls.get("2/batch") }, 10*time.Second).Should(Equal([]int{3}))
		Eventually(func() []int { return calls.get("2/single") }, 5*time.Second).Should(Equal([]int{1, 1, 1}))
	})

	It("should run a batch which isn't full once a waiting request is processed", func() {
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), func(n *clusterNode) {
			calls.transition(n, "batch", "BATCH", "done", WithBatching(5, 0))
		}))
		Eventually(func() []string { return owners(tc.node(1), "batch") }, 10*time.Second).Should(ConsistOf("2"))

		c, _, err := tc.node(1).StartContainer(context.Background(), "player-1")
		Expect(err).NotTo(HaveOccurred())
		first, second := sendEvent(c, "BATCH", WithWaitRemote()), sendEvent(c, "BATCH", WithWaitRemote())
		Consistently(first, 300*time.Millisecond).ShouldNot(Receive())

		By("forwarding the waiting batch when the sender closes")
		c.Close()
		Eventually(first, 5*time.Second).Should(Receive())
		Eventually(second, 5*time.Second).Should(Receive())
		Expect(calls.get("2/batch")).To(Equal([]int{2}))
	})
})

var _ = Describe("Merging forwarded entities", func() {
	It("should apply the changed fields of every returned entity", func() {
		base := &service.NodeLog{
			NodeId: "1",
			Type:   service.NodeEventType_BITMAP_ADD,
			Ts:     &timestamppb.Timestamp{Seconds: 1, Nanos: 1},
			Body:   &service.NodeLog_Heartbeat{Heartbeat: &service.Heartbeat{}},
		}

		first := proto.Clone(base).(*service.NodeLog)
		first.NodeId = "2"
		first.Ts.Nanos = 5
		second := proto.Clone(base).(*service.NodeLog)
		second.Type = service.NodeEventType_LOCK_ACQUIRE
		second.Ts.Seconds = 9
		second.Body = nil

		merged := proto.Clone(base).(*service.NodeLog)
		mergeChangedFields(merged.ProtoReflect(), base.ProtoReflect(), first.ProtoReflect())
		mergeChangedFields(merged.ProtoReflect(), base.ProtoReflect(), second.ProtoReflect())

		Expect(merged.GetNodeId()).To(Equal("2"))
		Expect(merged.GetType()).To(Equal(service.NodeEventType_LOCK_ACQUIRE))
		Expect(merged.GetTs().GetSeconds()).To(Equal(int64(9)))
		Expect(merged.GetTs().GetNanos()).To(Equal(int32(5)))
		Expect(merged.GetHeartbeat()).To(BeNil())
		// the base is not modified
		Expect(base.GetNodeId()).To(Equal("1"))
	})
})
