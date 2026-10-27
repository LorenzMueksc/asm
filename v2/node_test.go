package asm

import (
	"context"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

var _ = Describe("Node", func() {
	var (
		node   *Node[*TestStateEntity, *mockBuilder]
		bitmap *Bitmap
		deps   *mockDeps
		initF  lifecycleFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder]) error {
			c.Builder().Set(&TestStateEntity{Status: fmt.Sprintf("test-state-%s", c.Id())})
			return nil
		}
		closeF lifecycleFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder]) error {
			return nil
		}
		actionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			return false, nil
		}
	)

	BeforeEach(func() {
		bitmap, _ = createSimpleTestBitmap()
		deps = &mockDeps{Value: "node-dependency"}
		node = NewNode(
			1,
			func(entity any, bitMap *Bitmap) *Bitmap {
				return bitMap
			},
			func() *mockBuilder {
				return &mockBuilder{}
			},
			"TEST",
			WithSkipRaft(),
			WithBitmap(bitmap),
			WithDependencies(deps),
			WithInitFunc(initF),
			WithCloseFunc(closeF),
			WithLogger(zap.NewNop()),
		)
		node.AddFuncTransition(
			"test-transition",
			"TESTEVENT",
			actionF,
		)
	})

	AfterEach(func() {
		node.Shutdown()
	})

	It("should never hand a closed container's pointer another container", func() {
		first, _, err := node.StartContainer(context.Background(), "first")
		Expect(err).NotTo(HaveOccurred())
		first.Close()

		second, _, err := node.StartContainer(context.Background(), "second")
		Expect(err).NotTo(HaveOccurred())
		defer second.Close()
		Expect(second).NotTo(BeIdenticalTo(first))
		Expect(first.Event("E", &TestEvent{Value: "v"})).To(MatchError(ErrContainerClosed))
		Expect(first.Id()).To(Equal("first"))
	})

	It("should run an idle container without a goroutine and start one for new work", func() {
		containers := make([]*Container[*TestStateEntity, *mockBuilder], 0, 50)
		for i := range 50 {
			c, _, err := node.StartContainer(context.Background(), fmt.Sprintf("idle-%d", i))
			Expect(err).NotTo(HaveOccurred())
			doneC := make(chan struct{}, 1)
			Expect(c.Event("TESTEVENT", &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())
			Eventually(doneC).Should(Receive())
			containers = append(containers, c)
		}
		Eventually(func() int { return goroutinesIn("v2.(*Container[") }).Should(BeZero())

		doneC := make(chan struct{}, 1)
		Expect(containers[7].Event("TESTEVENT", &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())
		Eventually(doneC).Should(Receive())
		for _, c := range containers {
			c.Close()
		}
		Eventually(func() int { return goroutinesIn("v2.(*Container[") }).Should(BeZero())
	})

	It("should keep the goroutine of a container for the keep-alive after its last work", func() {
		run := func(keepAlive time.Duration) []*Container[*TestStateEntity, *mockBuilder] {
			GinkgoHelper()
			n := NewNode(2, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "",
				WithSkipRaft(), WithBitmap(bitmap), WithLogger(zap.NewNop()), WithContainerKeepAlive(keepAlive))
			n.AddFuncTransition("test-transition", "TESTEVENT", actionF)
			DeferCleanup(n.Shutdown)
			containers := make([]*Container[*TestStateEntity, *mockBuilder], 0, 10)
			for i := range 10 {
				c, _, err := n.StartContainer(context.Background(), fmt.Sprintf("kept-%d", i))
				Expect(err).NotTo(HaveOccurred())
				doneC := make(chan struct{}, 1)
				Expect(c.Event("TESTEVENT", &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())
				Eventually(doneC).Should(Receive())
				containers = append(containers, c)
			}
			return containers
		}

		By("keeping them for an hour: the goroutines wait and take new events")
		kept := run(time.Hour)
		Consistently(func() int { return goroutinesIn("v2.(*Container[") }, 200*time.Millisecond).Should(Equal(10))
		doneC := make(chan struct{}, 1)
		Expect(kept[3].Event("TESTEVENT", &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())
		Eventually(doneC).Should(Receive())
		Expect(goroutinesIn("v2.(*Container[")).To(Equal(10))
		for _, c := range kept {
			c.Close()
		}
		Eventually(func() int { return goroutinesIn("v2.(*Container[") }).Should(BeZero())

		By("not keeping them")
		run(0)
		Eventually(func() int { return goroutinesIn("v2.(*Container[") }, 100*time.Millisecond).Should(BeZero())
	})

	It("should keep a container running after the context of StartContainer is done", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		c, _, err := node.StartContainer(ctx, "outlives-ctx")
		Expect(err).NotTo(HaveOccurred())
		cancel()
		time.Sleep(100 * time.Millisecond)

		doneC := make(chan struct{}, 1)
		Expect(c.Event("TESTEVENT", &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())
		Eventually(doneC).Should(Receive())
		Expect(c.isAccepting()).To(BeTrue())
		c.Close()
	})

	It("should give up waiting when the context of StartContainer is done", func() {
		failing := NewNode(2, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "",
			WithSkipRaft(), WithLogger(zap.NewNop()), WithMaxContainerCount(1))
		first, _, err := failing.StartContainer(context.Background(), "first")
		Expect(err).NotTo(HaveOccurred())
		defer first.Close()

		// the max container count is reached, so starting another container waits
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, _, err = failing.StartContainer(ctx, "second")
		Expect(err).To(MatchError(context.DeadlineExceeded))
	})

	When("starting and deleting a lot of containers in serial", func() {
		It("should reset the containers before reusing when only using one concurrently", func() {
			for i := 0; i < 1000; i++ {
				cId := fmt.Sprintf("TEST-%d", i)
				c, _, _ := node.StartContainer(context.Background(), cId)

				Expect(c.id).To(Equal(cId))
				Expect(c.status).To(Equal(containerStatusRunning))
				Expect(c.Builder().Build()).To(BeNil())

				c.Event("TESTEVENT", &TestEvent{
					Value:  "test",
					Amount: 1,
				})

				c.Close()

				node.DeleteContainer(cId)
			}
		})

		It("should reset the containers before reusing when using multiple concurrently", func() {
			var wg sync.WaitGroup
			defer wg.Wait()
			for i := 0; i < 5; i++ {
				for x := 0; x < 100; x++ {
					cId := fmt.Sprintf("TEST-%d-%d", i, x)
					c, _, _ := node.StartContainer(context.Background(), cId)
					Expect(c.id).To(Equal(cId))
					Expect(c.status).To(Equal(containerStatusRunning))
					Expect(c.Builder().Build()).To(BeNil())
					c.Event("TESTEVENT", &TestEvent{
						Value:  "test",
						Amount: 1,
					})
					wg.Add(1)
					go func() {
						defer wg.Done()
						c.Close()
						node.DeleteContainer(cId)
					}()
				}
			}
		})
	})
})

var _ = Describe("Node error func and leaving", func() {
	type errorCall struct {
		containerId string
		err         error
	}
	var (
		failing transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			return false, fmt.Errorf("transition failed")
		}
		newTestNode = func(opts ...NodeSetupOption) *Node[*TestStateEntity, *mockBuilder] {
			bitmap, _ := createSimpleTestBitmap()
			opts = append([]NodeSetupOption{WithSkipRaft(), WithBitmap(bitmap), WithLogger(zap.NewNop())}, opts...)
			n := NewNode(1, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "", opts...)
			n.AddFuncTransition("failing", "FAIL", failing)
			return n
		}
		sendEvent = func(c *Container[*TestStateEntity, *mockBuilder], event string) {
			GinkgoHelper()
			doneC := make(chan struct{}, 1)
			Expect(c.Event(event, &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())
			Eventually(doneC).Should(Receive())
		}
	)

	It("should pass the errors of a container to the error func and keep it running when it returns nil", func() {
		callsC := make(chan errorCall, 10)
		n := newTestNode(WithErrorFunc(func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], err error) error {
			callsC <- errorCall{containerId: c.Id(), err: err}
			return nil
		}))
		defer n.Shutdown()

		c, _, err := n.StartContainer(context.Background(), "c1")
		Expect(err).NotTo(HaveOccurred())
		sendEvent(c, "FAIL")

		var call errorCall
		Eventually(callsC).Should(Receive(&call))
		Expect(call.containerId).To(Equal("c1"))
		Expect(call.err).To(MatchError("transition failed"))

		sendEvent(c, "FAIL")
		Eventually(callsC).Should(Receive())
		Expect(c.isAccepting()).To(BeTrue())
		c.Close()
	})

	It("should evaluate the following transitions against the state a func transition changed", func() {
		n := newTestNode()
		defer n.Shutdown()

		ranC := make(chan struct{}, 1)
		n.AddFuncTransition("first", "STEP", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			c.Builder().Set(&TestStateEntity{Status: "processing"})
			return false, nil
		}, WithPriority(10))
		n.AddFuncTransition("second", "STEP", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			ranC <- struct{}{}
			return false, nil
		}, WithPriority(1), WithConditions(n.All(&TestStateEntity{Status: "processing"})))

		c, _, err := n.StartContainer(context.Background(), "c1")
		Expect(err).NotTo(HaveOccurred())
		sendEvent(c, "STEP")

		Expect(ranC).To(Receive())
		c.Close()
	})

	It("should close the container when the error func returns an error", func() {
		n := newTestNode(WithErrorFunc(func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], err error) error {
			return fmt.Errorf("giving up: %w", err)
		}))
		defer n.Shutdown()

		c, _, err := n.StartContainer(context.Background(), "c1")
		Expect(err).NotTo(HaveOccurred())
		p := c.containerProcessing
		sendEvent(c, "FAIL")

		Eventually(p.done()).Should(BeClosed())
	})

	It("should prefer the error func passed to StartContainer over the one of the node", func() {
		nodeCalls, containerCalls := make(chan error, 10), make(chan error, 10)
		n := newTestNode(WithErrorFunc(func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], err error) error {
			nodeCalls <- err
			return nil
		}))
		defer n.Shutdown()

		c, _, err := n.StartContainer(context.Background(), "c1",
			WithContainerErrorFunc(func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], err error) error {
				containerCalls <- err
				return nil
			}))
		Expect(err).NotTo(HaveOccurred())
		sendEvent(c, "FAIL")

		Eventually(containerCalls).Should(Receive())
		Consistently(nodeCalls, 100*time.Millisecond).ShouldNot(Receive())
		c.Close()
	})

	It("should close the containers and reject new ones when leaving", func() {
		var closed sync.WaitGroup
		closed.Add(2)
		n := newTestNode(WithCloseFunc(func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder]) error {
			closed.Done()
			return nil
		}))

		for _, id := range []string{"c1", "c2"} {
			_, _, err := n.StartContainer(context.Background(), id)
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(n.Leave(context.Background())).To(Succeed())
		// Leave returns once the containers are closed
		closed.Wait()
		Expect(n.stopC).To(BeClosed())

		_, _, err := n.StartContainer(context.Background(), "c3")
		Expect(err).To(MatchError(ErrNodeStopped))
	})

	It("should leave when the stop channel is closed", func() {
		stopC := make(chan struct{})
		n := newTestNode(WithStopChannel(stopC))
		_, _, err := n.StartContainer(context.Background(), "c1")
		Expect(err).NotTo(HaveOccurred())

		close(stopC)
		Eventually(n.stopC).Should(BeClosed())
		_, _, err = n.StartContainer(context.Background(), "c2")
		Expect(err).To(MatchError(ErrNodeStopped))
	})
})

var _ = Describe("Emitting events from transitions", func() {
	var n *Node[*TestStateEntity, *mockBuilder]

	BeforeEach(func() {
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

	It("should run the transitions of an emitted event right away, inside the transition", func() {
		var mu sync.Mutex
		order := make([]string, 0)
		record := func(s string) {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, s)
		}
		n.AddFuncTransition("outer", "OUTER", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			record("outer start")
			if err := c.Emit(ctx, "INNER", &TestEvent{Value: "inner"}); err != nil {
				return false, err
			}
			record("outer end")
			return false, nil
		})
		n.AddFuncTransition("inner", "INNER", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			record("inner " + a.(*TestEvent).Value)
			return false, nil
		})

		c, _, err := n.StartContainer(context.Background(), "c1")
		Expect(err).NotTo(HaveOccurred())
		send(c, "OUTER")
		Expect(order).To(Equal([]string{"outer start", "inner inner", "outer end"}))
		c.Close()
	})

	It("should refuse emitting outside of a transition", func() {
		c, _, err := n.StartContainer(context.Background(), "c1")
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Emit(context.Background(), "X", &TestEvent{})).To(MatchError(ErrNotInTransition))
		c.Close()
	})

	It("should stop a transition emitting its own event endlessly", func() {
		errC := make(chan error, 1)
		n.AddFuncTransition("loop", "LOOP", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			if err := c.Emit(ctx, "LOOP", a); err != nil {
				select {
				case errC <- err:
				default:
				}
			}
			return false, nil
		})

		c, _, err := n.StartContainer(context.Background(), "c1")
		Expect(err).NotTo(HaveOccurred())
		send(c, "LOOP")
		Expect(errC).To(Receive(MatchError(ContainSubstring("nested events"))))
		c.Close()
	})
})
