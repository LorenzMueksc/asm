package asm

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// a second entity type for the cluster: counters, kept in a map standing in for a database
type (
	int64Builder struct {
		mu sync.Mutex
		v  *wrapperspb.Int64Value
	}
	counterNode = Node[*wrapperspb.Int64Value, *int64Builder]
)

func (b *int64Builder) Build() *wrapperspb.Int64Value {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.v
}

func (b *int64Builder) Set(v *wrapperspb.Int64Value) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.v = v
}

func (b *int64Builder) IsSet() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.v != nil
}

func counterBitmapByType(entity any, root *Bitmap) *Bitmap {
	if root == nil {
		return nil
	}
	switch entity.(type) {
	case *wrapperspb.Int64Value:
		return root.Children["Int64Value"]
	case *wrapperspb.StringValue:
		return root.Children["StringValue"]
	}
	return nil
}

var _ = Describe("Cluster with two entities", Label("cluster"), func() {
	var (
		tc       *testCluster
		counters sync.Map // container id -> int64
		stopC    chan struct{}
		// counterNodes are stopped after every spec, with their stop channels
		counterNodes []struct {
			n     *counterNode
			stopC chan struct{}
		}
		calls *callLog
	)

	// startCounterNode joins a node of the counter entity, which picks the counter of an event by its payload
	startCounterNode := func(id int, setup func(n *counterNode), extra ...NodeSetupOption) *counterNode {
		pos := 0
		bm := &Bitmap{Name: "root", Children: map[string]*Bitmap{
			"Int64Value": BuildBitmap(&Property{Name: "Int64Value", Properties: []*Property{{Name: "value", Values: []any{int64(0), int64(1)}}}}, &pos),
		}}
		stopC = make(chan struct{})
		opts := append([]NodeSetupOption{
			WithBitmap(bm), WithLogger(zap.NewNop()), WithJoinAddr(tc.addr(1)), WithStopChannel(stopC),
			WithContainerIdFunc(func(eventName string, payload proto.Message) string {
				if s, ok := payload.(*wrapperspb.StringValue); ok {
					return s.GetValue()
				}
				return ""
			}),
			WithInitFunc[*wrapperspb.Int64Value, *int64Builder](func(ctx context.Context, c *Container[*wrapperspb.Int64Value, *int64Builder]) error {
				v, _ := counters.LoadOrStore(c.Id(), int64(0))
				c.Builder().Set(wrapperspb.Int64(v.(int64)))
				return nil
			}),
			WithCloseFunc[*wrapperspb.Int64Value, *int64Builder](func(ctx context.Context, c *Container[*wrapperspb.Int64Value, *int64Builder]) error {
				counters.Store(c.Id(), c.Builder().Build().GetValue())
				return nil
			}),
		}, extra...)
		n := NewNode(id, counterBitmapByType, func() *int64Builder { return &int64Builder{} }, freeAddr(), opts...)
		if setup != nil {
			setup(n)
		}
		counterNodes = append(counterNodes, struct {
			n     *counterNode
			stopC chan struct{}
		}{n, stopC})
		Eventually(n.Ready(), 30*time.Second).Should(BeClosed())
		return n
	}
	counter := func(id string) func() int64 {
		return func() int64 {
			v, _ := counters.Load(id)
			n, _ := v.(int64)
			return n
		}
	}
	// tally sends TALLY for a counter from a StringValue container, the counter is the payload
	tally := func(containerId, counterId string, wait bool) {
		GinkgoHelper()
		c, _, err := tc.node(1).StartContainer(context.Background(), containerId)
		Expect(err).NotTo(HaveOccurred())
		c.Builder().Set(wrapperspb.String("pending"))
		opts := []EventOption{}
		if wait {
			opts = append(opts, WithWaitRemote())
		}
		doneC := make(chan struct{}, 1)
		Expect(c.Event("TALLY", wrapperspb.String(counterId), append(opts, WithDoneChannel(doneC))...)).To(Succeed())
		Eventually(doneC, 10*time.Second).Should(Receive())
		// the StringValue entity isn't changed by the counter
		Expect(c.Builder().Build().GetValue()).To(Equal("pending"))
		c.Close()
	}

	// startNotified starts the StringValue nodes 1 and 2, node 2 runs NOTIFY for the StringValue container
	// the payload names
	startNotified := func() {
		GinkgoHelper()
		expectReady(tc.start(1, "", clusterBitmap("pending", "done"), nil))
		expectReady(tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), func(n *clusterNode) {
			n.AddFuncTransition("notified", "NOTIFY", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				calls.mu.Lock()
				calls.calls["2/notified/"+c.Id()] = append(calls.calls["2/notified/"+c.Id()], 1)
				calls.mu.Unlock()
				return false, nil
			}, WithFuncKey("notified"))
		}))
		tc.node(2).containerIdF = func(eventName string, payload proto.Message) string {
			return payload.(*wrapperspb.StringValue).GetValue()
		}
	}
	// errorLog collects the errors of the containers of a counter node
	type errorLog struct {
		mu   sync.Mutex
		errs []string
	}
	collectErrors := func(l *errorLog) NodeSetupOption {
		return WithErrorFunc[*wrapperspb.Int64Value, *int64Builder](func(ctx context.Context, c *Container[*wrapperspb.Int64Value, *int64Builder], err error) error {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.errs = append(l.errs, err.Error())
			return nil
		})
	}
	errorsOf := func(l *errorLog) func() []string {
		return func() []string {
			l.mu.Lock()
			defer l.mu.Unlock()
			return slices.Clone(l.errs)
		}
	}

	BeforeEach(func() {
		tc = newTestCluster()
		counters = sync.Map{}
		calls = newCallLog()
	})

	AfterEach(func() {
		// their containers close with the counters of this spec: wait until they stopped
		for _, c := range counterNodes {
			close(c.stopC)
			Eventually(c.n.stopC, 10*time.Second).Should(BeClosed())
		}
		counterNodes, stopC = nil, nil
		tc.stopAll()
	})

	It("should run transitions of another entity in the containers the receiver picks, on their own locks", func() {
		expectReady(tc.start(1, "", clusterBitmap("pending", "done"), nil))
		expectReady(tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), func(n *clusterNode) {
			// back in the other direction: a counter notifies a StringValue container, picked by the payload
			n.AddFuncTransition("notified", "NOTIFY", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				calls.mu.Lock()
				calls.calls["2/notified/"+c.Id()] = append(calls.calls["2/notified/"+c.Id()], 1)
				calls.mu.Unlock()
				return false, nil
			}, WithFuncKey("notified"))
		}))
		tc.node(2).containerIdF = func(eventName string, payload proto.Message) string {
			return payload.(*wrapperspb.StringValue).GetValue()
		}
		startCounterNode(3, func(n *counterNode) {
			n.AddFuncTransition("count", "TALLY", func(ctx context.Context, c *Container[*wrapperspb.Int64Value, *int64Builder], a any) (bool, error) {
				c.Builder().Set(wrapperspb.Int64(c.Builder().Build().GetValue() + 1))
				// tell the first sender, a container of the other entity, without waiting
				return false, c.Emit(ctx, "NOTIFY", wrapperspb.String("player-1"))
			}, WithFuncKey("count"))
			n.AddStateTransition("reset", "RESET", wrapperspb.Int64(0))
		})
		Eventually(func() *Transition { return publishedTransition(tc.node(1), "count") }, 10*time.Second).ShouldNot(BeNil())
		Eventually(func() *Transition { return publishedTransition(tc.node(1), "reset") }, 10*time.Second).ShouldNot(BeNil())

		By("marking transitions of the other entity remote, also state transitions")
		Expect(publishedTransition(tc.node(1), "count").Entity).To(Equal("Int64Value"))
		Expect(publishedTransition(tc.node(1), "reset").Remote).To(BeTrue())

		By("counting events of two StringValue containers in one counter, the sender waits")
		tally("player-1", "counter-1", true)
		Eventually(counter("counter-1"), 5*time.Second).Should(Equal(int64(1)))
		tally("player-2", "counter-1", true)
		Eventually(counter("counter-1"), 5*time.Second).Should(Equal(int64(2)))

		By("handing events over without waiting")
		tally("player-3", "counter-2", false)
		Eventually(counter("counter-2"), 10*time.Second).Should(Equal(int64(1)))

		By("notifying the StringValue container the counter picked")
		Eventually(func() []int { return calls.get("2/notified/player-1") }, 10*time.Second).Should(HaveLen(3))
	})

	It("should return the reply of a container of another entity to the waiting Emit", func() {
		replies := make(chan any, 1)
		expectReady(tc.start(1, "", clusterBitmap("pending", "done"), func(n *clusterNode) {
			n.AddFuncTransition("ask", "ASK", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				return false, c.Emit(ctx, "TALLY", a, WithWaitRemote(), WithReply(func(r any) { replies <- r }))
			})
		}))
		startCounterNode(3, func(n *counterNode) {
			n.AddFuncTransition("count", "TALLY", func(ctx context.Context, c *Container[*wrapperspb.Int64Value, *int64Builder], a any) (bool, error) {
				c.Builder().Set(wrapperspb.Int64(c.Builder().Build().GetValue() + 1))
				return false, c.Reply(ctx, wrapperspb.Int64(c.Builder().Build().GetValue()))
			}, WithFuncKey("count"))
		})
		Eventually(func() *Transition { return publishedTransition(tc.node(1), "count") }, 10*time.Second).ShouldNot(BeNil())

		c, _, err := tc.node(1).StartContainer(context.Background(), "player-1")
		Expect(err).NotTo(HaveOccurred())
		c.Builder().Set(wrapperspb.String("pending"))
		Expect(c.Event("ASK", wrapperspb.String("counter-1"))).To(Succeed())
		var r any
		Eventually(replies, 10*time.Second).Should(Receive(&r))
		Expect(proto.Equal(r.(proto.Message), wrapperspb.Int64(1))).To(BeTrue())
		c.Close()
	})

	It("should send a waited call of another entity on to the node holding the container's lock", func() {
		var mu sync.Mutex
		ranOn := map[int]int{}
		expectReady(tc.start(1, "", clusterBitmap("pending", "done"), func(n *clusterNode) {
			n.AddFuncTransition("ask", "ASK", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				return false, c.Emit(ctx, "TALLY", a, WithWaitRemote())
			})
		}))
		counting := func(id int) func(n *counterNode) {
			return func(n *counterNode) {
				n.AddFuncTransition("count", "TALLY", func(ctx context.Context, c *Container[*wrapperspb.Int64Value, *int64Builder], a any) (bool, error) {
					mu.Lock()
					ranOn[id]++
					mu.Unlock()
					return false, nil
				}, WithFuncKey("count"))
			}
		}
		startCounterNode(3, counting(3))
		n4 := startCounterNode(4, counting(4))
		Eventually(func() []string { return publishedTransition(tc.node(1), "count").NodeIds }, 10*time.Second).Should(ConsistOf("3", "4"))

		By("counter-1 open on node 4")
		counter1, _, err := n4.StartContainer(context.Background(), "counter-1")
		Expect(err).NotTo(HaveOccurred())

		By("asking ten times: the calls reaching node 3 go on to node 4")
		c, _, err := tc.node(1).StartContainer(context.Background(), "player-1")
		Expect(err).NotTo(HaveOccurred())
		c.Builder().Set(wrapperspb.String("pending"))
		for range 10 {
			doneC := make(chan struct{}, 1)
			Expect(c.Event("ASK", wrapperspb.String("counter-1"), WithDoneChannel(doneC))).To(Succeed())
			Eventually(doneC, 10*time.Second).Should(Receive())
		}
		mu.Lock()
		Expect(ranOn).To(Equal(map[int]int{4: 10}))
		mu.Unlock()
		counter1.Close()
		c.Close()
	})

	It("should report a hand-over the receiver refused to the sender's error func", func() {
		var mu sync.Mutex
		var errs []string
		tc.opts = []NodeSetupOption{WithErrorFunc[*wrapperspb.StringValue, *stringValueBuilder](func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], err error) error {
			mu.Lock()
			defer mu.Unlock()
			errs = append(errs, err.Error())
			return nil
		})}
		expectReady(tc.start(1, "", clusterBitmap("pending", "done"), nil))
		startCounterNode(3, func(n *counterNode) {
			n.AddFuncTransition("count", "TALLY", func(ctx context.Context, c *Container[*wrapperspb.Int64Value, *int64Builder], a any) (bool, error) {
				return false, nil
			}, WithFuncKey("count"))
		})
		Eventually(func() *Transition { return publishedTransition(tc.node(1), "count") }, 10*time.Second).ShouldNot(BeNil())

		c, _, err := tc.node(1).StartContainer(context.Background(), "player-1")
		Expect(err).NotTo(HaveOccurred())
		c.Builder().Set(wrapperspb.String("pending"))
		// handed over without waiting for the accept: the counter node picks no container for an empty counter id
		doneC := make(chan struct{}, 1)
		Expect(c.Event("TALLY", wrapperspb.String(""), WithDoneChannel(doneC))).To(Succeed())
		Eventually(doneC, 5*time.Second).Should(Receive())
		Eventually(func() []string {
			mu.Lock()
			defer mu.Unlock()
			return slices.Clone(errs)
		}, 10*time.Second).Should(ContainElement(ContainSubstring("no container for event TALLY")))
		c.Close()
	})

	It("should refuse a waited call back to a container waiting for it instead of deadlocking", func() {
		startNotified()
		errs := &errorLog{}
		startCounterNode(3, func(n *counterNode) {
			n.AddFuncTransition("count", "TALLY", func(ctx context.Context, c *Container[*wrapperspb.Int64Value, *int64Builder], a any) (bool, error) {
				c.Builder().Set(wrapperspb.Int64(c.Builder().Build().GetValue() + 1))
				// waits for the StringValue container which waits for this counter
				return false, c.Emit(ctx, "NOTIFY", wrapperspb.String("player-1"), WithWaitRemote())
			}, WithFuncKey("count"))
		}, collectErrors(errs))
		Eventually(func() *Transition { return publishedTransition(tc.node(1), "count") }, 10*time.Second).ShouldNot(BeNil())
		Eventually(func() *Transition { return publishedTransition(tc.node(1), "notified") }, 10*time.Second).ShouldNot(BeNil())

		start := time.Now()
		tally("player-1", "counter-1", true)
		Expect(time.Since(start)).To(BeNumerically("<", 5*time.Second))
		Eventually(counter("counter-1"), 5*time.Second).Should(Equal(int64(1)))
		Eventually(errorsOf(errs), 5*time.Second).Should(ContainElement(ContainSubstring("would deadlock")))
		Expect(calls.get("2/notified/player-1")).To(BeEmpty())
	})

	It("should refuse the waited call closing a cycle through a joined container, the others go on", func() {
		startNotified()
		errs := &errorLog{}
		gate, gate2 := make(chan struct{}), make(chan struct{})
		entered, entered2 := make(chan struct{}, 1), make(chan struct{}, 1)
		var first atomic.Bool
		n3 := startCounterNode(3, func(n *counterNode) {
			n.AddFuncTransition("count", "TALLY", func(ctx context.Context, c *Container[*wrapperspb.Int64Value, *int64Builder], a any) (bool, error) {
				c.Builder().Set(wrapperspb.Int64(c.Builder().Build().GetValue() + 1))
				if !first.CompareAndSwap(false, true) {
					// the second call keeps the container open until the first call is known to be done
					entered2 <- struct{}{}
					<-gate2
					return false, nil
				}
				// the first call holds the counter until player-2 waits for it, then waits for player-2
				entered <- struct{}{}
				<-gate
				return false, c.Emit(ctx, "NOTIFY", wrapperspb.String("player-2"), WithWaitRemote())
			}, WithFuncKey("count"))
		}, collectErrors(errs))
		Eventually(func() *Transition { return publishedTransition(tc.node(1), "count") }, 10*time.Second).ShouldNot(BeNil())
		Eventually(func() *Transition { return publishedTransition(tc.node(1), "notified") }, 10*time.Second).ShouldNot(BeNil())

		doneC := make(chan string, 2)
		go func() {
			defer GinkgoRecover()
			tally("player-1", "counter-1", true)
			doneC <- "player-1"
		}()
		Eventually(entered, 10*time.Second).Should(Receive())

		By("player-2 joining the busy counter container")
		go func() {
			defer GinkgoRecover()
			tally("player-2", "counter-1", true)
			doneC <- "player-2"
		}()
		Eventually(func() int {
			l, _, _ := n3.locks.get("counter-1")
			return len(l.calls)
		}, 10*time.Second).Should(Equal(2))

		By("the counter calling player-2")
		close(gate)
		Eventually(doneC, 10*time.Second).Should(Receive(Equal("player-1")))
		Eventually(entered2, 10*time.Second).Should(Receive())

		By("ending the call of player-1 while the container still runs the one of player-2")
		Eventually(func() int {
			l, _, _ := n3.locks.get("counter-1")
			return len(l.calls)
		}, 10*time.Second).Should(Equal(1))
		close(gate2)
		Eventually(doneC, 10*time.Second).Should(Receive(Equal("player-2")))
		Eventually(counter("counter-1"), 5*time.Second).Should(Equal(int64(2)))
		Expect(errorsOf(errs)()).To(ContainElement(ContainSubstring("would deadlock")))
		Expect(calls.get("2/notified/player-2")).To(BeEmpty())
	})

	It("should let a container wait for the same container of another entity again and again", func() {
		startNotified()
		errs := &errorLog{}
		startCounterNode(3, func(n *counterNode) {
			n.AddFuncTransition("count", "TALLY", func(ctx context.Context, c *Container[*wrapperspb.Int64Value, *int64Builder], a any) (bool, error) {
				c.Builder().Set(wrapperspb.Int64(c.Builder().Build().GetValue() + 1))
				// a StringValue container nobody waits for
				return false, c.Emit(ctx, "NOTIFY", wrapperspb.String("lobby"), WithWaitRemote())
			}, WithFuncKey("count"))
		}, collectErrors(errs))
		Eventually(func() *Transition { return publishedTransition(tc.node(1), "count") }, 10*time.Second).ShouldNot(BeNil())
		Eventually(func() *Transition { return publishedTransition(tc.node(1), "notified") }, 10*time.Second).ShouldNot(BeNil())

		c, _, err := tc.node(1).StartContainer(context.Background(), "player-1")
		Expect(err).NotTo(HaveOccurred())
		c.Builder().Set(wrapperspb.String("pending"))
		for range 5 {
			doneC := make(chan struct{}, 1)
			Expect(c.Event("TALLY", wrapperspb.String("counter-1"), WithWaitRemote(), WithDoneChannel(doneC))).To(Succeed())
			Eventually(doneC, 10*time.Second).Should(Receive())
		}
		c.Close()

		Eventually(counter("counter-1"), 5*time.Second).Should(Equal(int64(5)))
		Expect(calls.get("2/notified/lobby")).To(HaveLen(5))
		Expect(errorsOf(errs)()).To(BeEmpty())
	})
})
