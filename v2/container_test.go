package asm

import (
	"context"
	"errors"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"time"
)

var _ = Describe("Container", func() {
	var (
		ctx         context.Context
		cancel      context.CancelFunc
		container   *Container[*TestStateEntity, *mockBuilder]
		logger      *zap.Logger
		bitmap      *Bitmap
		transitions []*Transition

		actionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			msg := a.(*TestEvent).Value
			c.Builder().Set(&TestStateEntity{Status: msg})
			return false, nil
		}
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		bitmap = &Bitmap{Name: "test"}
		transitions = []*Transition{
			{
				Name:         "TestTransition",
				EventName:    "test-event",
				SaveOnChange: true,
				Conditions:   []*Condition{},
				Batching:     Batching{Count: 1},
				Actions: []interface{}{
					actionF,
				},
			},
		}

		logger = zap.NewNop()

		container = NewContainer(
			"TEST",
			bitmap,
			transitions,
			func(entity any, bitMap *Bitmap) *Bitmap {
				return bitmap
			},
			func() *mockBuilder {
				return &mockBuilder{}
			},
			WithContainerLogger(logger),
		)

		container.start(ctx, nil)
	})

	It("should handle events and execute transition actions", func() {
		tValue := "test"
		container.Event("test-event", &TestEvent{
			Value:  tValue,
			Amount: 1,
		})

		container.Close()

		Expect(container.Builder().IsSet()).To(BeTrue())
		Expect(container.Builder().Build().Status).To(Equal(tValue))
	})

	It("should execute save function after transition if SaveOnChange is true", func() {
		saveCalled := false
		container.saveF = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder]) error {
			saveCalled = true
			return nil
		}

		container.Event("test-event", &TestEvent{})

		container.Close()

		Expect(saveCalled).To(BeTrue())
	})

	It("should invoke lifecycle hooks correctly", func() {
		var initCalled, closeCalled bool
		container.initF = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder]) error {
			initCalled = true
			return nil
		}
		container.closeF = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder]) error {
			closeCalled = true
			return nil
		}

		container.Event("test-event", &TestEvent{})

		container.Close()
		Expect(initCalled).To(BeTrue())
		Expect(closeCalled).To(BeTrue())
		Expect(container.closing()).To(BeTrue())
		Expect(container.eventsC).To(BeClosed())
	})

	It("should properly close all channels upon container closure", func() {
		container.Event("test-event", &TestEvent{})
		container.Close()

		Expect(container.status).To(Equal(containerStatusClosed))
		Expect(container.closing()).To(BeTrue())
		Expect(container.eventsC).To(BeClosed())
	})

	It("should properly close all channels upon context cancellation", func() {
		container.Event("test-event", &TestEvent{})

		cancel()
		Eventually(container.done()).Should(BeClosed())

		Expect(container.status).To(Equal(containerStatusClosed))
		Expect(container.closing()).To(BeTrue())
		Expect(container.eventsC).To(BeClosed())
	})

	It("should wait for both events to finish processing before closing the container", func() {
		var actionCalled, consecutiveActionCalled bool
		var actionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			c.Event("test-event-2", &TestEvent{Value: "for-test-event-2"})
			actionCalled = true
			return false, nil
		}
		var consecutiveActionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			time.Sleep(time.Millisecond * 100)
			consecutiveActionCalled = true
			c.Builder().Set(&TestStateEntity{
				Status: "consecutive-action-processed",
			})
			return false, nil
		}
		container.transitions = newPublishedTransitions([]*Transition{
			{
				Name:         "TestTransition",
				EventName:    "test-event-1",
				SaveOnChange: true,
				Conditions:   []*Condition{},
				Batching:     Batching{Count: 1},
				Actions: []interface{}{
					actionF,
				},
			},
			{
				Name:         "TestTransition",
				EventName:    "test-event-2",
				SaveOnChange: true,
				Conditions:   []*Condition{},
				Batching:     Batching{Count: 1},
				Actions: []interface{}{
					consecutiveActionF,
				},
			},
		})

		container.Event("test-event-1", &TestEvent{Value: "for-test-event-1"})

		container.Close()

		Expect(container.status).To(Equal(containerStatusClosed))
		Expect(actionCalled).To(BeTrue())
		Expect(consecutiveActionCalled).To(BeTrue())
		Expect(container.Builder().Build().Status).To(Equal("consecutive-action-processed"))
		Expect(container.closing()).To(BeTrue())
		Expect(container.eventsC).To(BeClosed())
	})

	It("should handle event batching correctly", func() {
		var batchActionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], value any) (bool, error) {
			msgs := value.([]any)
			c.Builder().Set(&TestStateEntity{Status: "batch-processed", Count: int64(len(msgs))})

			return false, nil
		}
		transitions[0].Batching.Count = 3
		transitions[0].Actions = []interface{}{
			batchActionF,
		}

		container.Event("test-event", &TestEvent{Value: "payload-1"})
		container.Event("test-event", &TestEvent{Value: "payload-2"})
		container.Event("test-event", &TestEvent{Value: "payload-3"})

		container.Close()
		Expect(container.Builder().IsSet()).To(BeTrue())
		Expect(container.Builder().Build().Status).To(Equal("batch-processed"))
		Expect(container.Builder().Build().Count).To(Equal(int64(3)))
		Expect(container.closing()).To(BeTrue())
		Expect(container.eventsC).To(BeClosed())
	})

	It("should execute after time when not enough events were published", func() {
		var batchActionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], value any) (bool, error) {
			msgs := value.([]any)
			c.Builder().Set(&TestStateEntity{Status: "batch-processed", Count: int64(len(msgs))})

			return false, nil
		}
		transitions[0].Batching.Count = 4
		transitions[0].Batching.Period = time.Second
		transitions[0].Actions = []interface{}{
			batchActionF,
		}

		container.Event("test-event", &TestEvent{Value: "payload-1"})
		container.Event("test-event", &TestEvent{Value: "payload-2"})
		container.Event("test-event", &TestEvent{Value: "payload-3"})

		container.Close()
		Expect(container.Builder().IsSet()).To(BeTrue())
		Expect(container.Builder().Build().Status).To(Equal("batch-processed"))
		Expect(container.Builder().Build().Count).To(Equal(int64(3)))
		Expect(container.closing()).To(BeTrue())
		Expect(container.eventsC).To(BeClosed())
	})

	It("should batch and wait for one transition and execute another instantly", func() {
		var batchActionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], value any) (bool, error) {
			msgs := value.([]any)
			c.Builder().Set(&TestStateEntity{Status: "batch-processed", Count: int64(len(msgs))})

			return false, nil
		}
		var normalActionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], value any) (bool, error) {
			c.Builder().Set(&TestStateEntity{Status: "normal-processed"})

			return false, nil
		}
		container.transitions = newPublishedTransitions([]*Transition{
			{
				Name:         "TestTransition",
				EventName:    "test-event",
				SaveOnChange: true,
				Conditions:   []*Condition{},
				Actions: []interface{}{
					normalActionF,
				},
			},
			{
				Name:         "TestTransition",
				EventName:    "test-batch-event",
				SaveOnChange: true,
				Conditions:   []*Condition{},
				Batching:     Batching{Count: 3},
				Actions: []interface{}{
					batchActionF,
				},
			},
		})

		container.Event("test-batch-event", &TestEvent{Value: "payload-1"})
		container.Event("test-batch-event", &TestEvent{Value: "payload-2"})
		container.Event("test-event", &TestEvent{Value: "payload-3"})
		container.Event("test-batch-event", &TestEvent{Value: "payload-3"})

		container.Close()
		Expect(container.Builder().IsSet()).To(BeTrue())
		Expect(container.Builder().Build().Status).To(Equal("batch-processed"))
		Expect(container.Builder().Build().Count).To(Equal(int64(3)))
		Expect(container.closing()).To(BeTrue())
		Expect(container.eventsC).To(BeClosed())
	})

	It("should batch and wait until the timer executes and execute another instantly", func() {
		var batchActionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], value any) (bool, error) {
			msgs := value.([]any)
			c.Builder().Set(&TestStateEntity{Status: "batch-processed", Count: int64(len(msgs))})

			return false, nil
		}
		var normalActionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], value any) (bool, error) {
			c.Builder().Set(&TestStateEntity{Status: "normal-processed"})

			return false, nil
		}
		container.transitions = newPublishedTransitions([]*Transition{
			{
				Name:         "TestTransition",
				EventName:    "test-event",
				SaveOnChange: true,
				Conditions:   []*Condition{},
				Actions: []interface{}{
					normalActionF,
				},
			},
			{
				Name:         "TestTransition",
				EventName:    "test-batch-event",
				SaveOnChange: true,
				Conditions:   []*Condition{},
				Batching:     Batching{Count: 4, Period: time.Millisecond * 100},
				Actions: []interface{}{
					batchActionF,
				},
			},
		})

		container.Event("test-batch-event", &TestEvent{Value: "payload-1"})
		container.Event("test-batch-event", &TestEvent{Value: "payload-2"})
		container.Event("test-batch-event", &TestEvent{Value: "payload-3"})
		container.Event("test-event", &TestEvent{Value: "payload-3"})

		container.Close()
		Expect(container.Builder().IsSet()).To(BeTrue())
		Expect(container.Builder().Build().Status).To(Equal("batch-processed"))
		Expect(container.Builder().Build().Count).To(Equal(int64(3)))
		Expect(container.closing()).To(BeTrue())
		Expect(container.eventsC).To(BeClosed())
	})

	It("should wait until all transitions are finished before closing the container when cancelling context", func() {
		var actionCalled, consecutiveActionCalled bool
		var actionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			actionCalled = true
			c.Event("test-event-2", &TestEvent{Value: "for-test-event-2"})
			return false, nil
		}
		var consecutiveActionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			time.Sleep(time.Millisecond * 100)
			consecutiveActionCalled = true
			return false, nil
		}

		container.transitions = newPublishedTransitions([]*Transition{
			{
				Name:         "TestTransition",
				EventName:    "test-event-1",
				SaveOnChange: true,
				Conditions:   []*Condition{},
				Batching:     Batching{Count: 1},
				Actions: []interface{}{
					actionF,
				},
			},
			{
				Name:         "TestTransition",
				EventName:    "test-event-2",
				SaveOnChange: true,
				Conditions:   []*Condition{},
				Batching:     Batching{Count: 1},
				Actions: []interface{}{
					consecutiveActionF,
				},
			},
		})

		container.Event("test-event-1", &TestEvent{Value: "for-test-event-1"})

		cancel()
		Eventually(container.done()).Should(BeClosed())

		Expect(actionCalled).To(BeTrue())
		Expect(consecutiveActionCalled).To(BeTrue())
		Expect(container.status).To(Equal(containerStatusClosed))
		Expect(container.closing()).To(BeTrue())
		Expect(container.eventsC).To(BeClosed())
	})

	It("should block events when maxInFlight is reached", func() {
		var actionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], value any) (bool, error) {
			time.Sleep(time.Millisecond * 100)

			return false, nil
		}
		transitions[0].Batching.Count = 3
		transitions[0].Actions = []interface{}{
			actionF,
		}

		// newly create container for setting maxInFlight
		container = NewContainer(
			"TEST",
			bitmap,
			transitions,
			func(entity any, bitMap *Bitmap) *Bitmap {
				return bitmap
			},
			func() *mockBuilder {
				return &mockBuilder{}
			},
			WithContainerLogger(logger),
			WithMaxInFlight(1),
		)
		container.start(ctx, nil)

		container.Event("test-event", &TestEvent{})
		startTime := time.Now()
		container.Event("test-event", &TestEvent{})
		Expect(time.Since(startTime)).To(BeNumerically("<", time.Millisecond*100))
		container.Close()
	})

	It("should execute batch transitions correctly when having a lot of events", func() {
		actionCalledCount := 0
		var actionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], value any) (bool, error) {
			time.Sleep(time.Millisecond * 100)
			actionCalledCount += len(value.([]any))
			return false, nil
		}
		transitions[0].Batching.Count = 99
		transitions[0].Batching.Period = time.Millisecond * 200
		transitions[0].Actions = []interface{}{
			actionF,
		}

		for i := 0; i < 1000; i++ {
			container.Event("test-event", &TestEvent{
				Value:  fmt.Sprintf("test-%d", i),
				Amount: int64(i),
			})
		}

		container.Close()

		Expect(actionCalledCount).To(Equal(1000))
	})

	Context("with a done channel event option", func() {
		It("should signal done when finished processing the event", func() {
			// need to have a channel with size 1 to not block sender
			var doneC = make(chan struct{}, 1)

			container.Event("test-event", &TestEvent{}, WithDoneChannel(doneC))

			container.Close()

			Expect(doneC).To(HaveLen(1))
		})

		It("should signal done when the event fails", func() {
			var actionFailedF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
				return false, errors.New("test-error")
			}
			transitions[0].Actions = []interface{}{
				actionFailedF,
			}

			var doneC = make(chan struct{}, 1)
			container.Event("test-event", &TestEvent{}, WithDoneChannel(doneC))

			container.Close()

			Expect(doneC).To(HaveLen(1))
		})

		It("should signal done to all events sent to one container", func() {
			var doneC = make(chan struct{}, 100)

			for i := 0; i < 100; i++ {
				container.Event("test-event", &TestEvent{}, WithDoneChannel(doneC))
			}
			container.Close()

			Expect(doneC).To(HaveLen(100))
		})

		It("should signal done one one channel for all events sent to all containers", func() {
			var doneC = make(chan struct{}, 1000)

			containers := make([]*Container[*TestStateEntity, *mockBuilder], 0)

			cancelCtx, cancel := context.WithCancel(context.Background())
			for i := 0; i < 1000; i++ {
				c := NewContainer(
					fmt.Sprintf("TEST-%d", i),
					bitmap,
					transitions,
					func(entity any, bitMap *Bitmap) *Bitmap {
						return bitmap
					},
					func() *mockBuilder {
						return &mockBuilder{}
					},
					WithContainerLogger(logger),
				)
				containers = append(containers, c)

				c.start(cancelCtx, nil)

				c.Event("test-event", &TestEvent{}, WithDoneChannel(doneC))
			}
			cancel()
			// wait for closing because cancel does not block
			for _, c := range containers {
				Eventually(c.done(), 5*time.Second).Should(BeClosed())
			}

			Expect(doneC).To(HaveLen(1000))
			for _, c := range containers {
				Expect(c.status).To(Equal(containerStatusClosed))
			}
		})

		It("should signal done on every channel for the corresponding event", func() {
			doneCmap := make(map[string]chan struct{})
			containers := make([]*Container[*TestStateEntity, *mockBuilder], 0)

			cancelCtx, cancel := context.WithCancel(context.Background())
			for i := 0; i < 1000; i++ {
				cId := fmt.Sprintf("TEST-%d", i)
				doneCmap[cId] = make(chan struct{}, 1)

				c := NewContainer(
					cId,
					bitmap,
					transitions,
					func(entity any, bitMap *Bitmap) *Bitmap {
						return bitmap
					},
					func() *mockBuilder {
						return &mockBuilder{}
					},
					WithContainerLogger(logger),
				)
				containers = append(containers, c)

				c.start(cancelCtx, nil)

				c.Event("test-event", &TestEvent{}, WithDoneChannel(doneCmap[cId]))
			}
			cancel()
			// wait for closing because cancel does not block
			for _, c := range containers {
				Eventually(c.done(), 5*time.Second).Should(BeClosed())
			}

			for _, c := range containers {
				Expect(c.status).To(Equal(containerStatusClosed))
				Expect(doneCmap[c.id]).To(HaveLen(1))
			}
		})
	})
})
