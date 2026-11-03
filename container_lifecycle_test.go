package asm

import (
	"context"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

var _ = Describe("Container lifecycle", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	newTestContainer := func(bitmap *Bitmap, transitions []*Transition, getBitmapByType GetBitmapByTypeFunc, opts ...ContainerOption) *Container[*TestStateEntity, *mockBuilder] {
		opts = append(opts, WithContainerLogger(zap.NewNop()))
		c := NewContainer(
			"TEST",
			bitmap,
			transitions,
			getBitmapByType,
			func() *mockBuilder {
				return &mockBuilder{}
			},
			opts...,
		)
		c.start(ctx, nil)

		return c
	}
	sameBitmap := func(bitmap *Bitmap) GetBitmapByTypeFunc {
		return func(entity any, bitMap *Bitmap) *Bitmap {
			return bitmap
		}
	}

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		cancel()
	})

	Context("closing", func() {
		It("should keep accepting events after the close signal until it is idle", func() {
			var calls atomic.Int32
			var slowActionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
				time.Sleep(100 * time.Millisecond)
				calls.Add(1)
				return false, nil
			}
			bitmap := &Bitmap{Name: "test"}
			c := newTestContainer(bitmap, []*Transition{
				{Name: "Slow", EventName: "test-event", Batching: Batching{Count: 1}, Actions: []any{slowActionF}},
			}, sameBitmap(bitmap))

			Expect(c.Event("test-event", &TestEvent{})).To(Succeed())
			c.requestClose()

			// the first event is still processing, so the container still accepts events
			Expect(c.Event("test-event", &TestEvent{})).To(Succeed())

			c.Close()
			Expect(calls.Load()).To(Equal(int32(2)))
			Expect(c.status).To(Equal(containerStatusClosed))
		})

		It("should return ErrContainerClosed when sending an event after closing", func() {
			bitmap := &Bitmap{Name: "test"}
			c := newTestContainer(bitmap, []*Transition{}, sameBitmap(bitmap))

			c.Close()

			Expect(c.Event("test-event", &TestEvent{})).To(MatchError(ErrContainerClosed))
		})

		It("should execute a batch without period which is still waiting for events on close", func() {
			var batchSize atomic.Int32
			var batchActionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
				batchSize.Store(int32(len(a.([]any))))
				return false, nil
			}
			bitmap := &Bitmap{Name: "test"}
			c := newTestContainer(bitmap, []*Transition{
				{Name: "Batch", EventName: "test-event", Batching: Batching{Count: 3}, Actions: []any{batchActionF}},
			}, sameBitmap(bitmap))

			Expect(c.Event("test-event", &TestEvent{})).To(Succeed())
			Expect(c.Event("test-event", &TestEvent{})).To(Succeed())

			c.Close()
			Expect(batchSize.Load()).To(Equal(int32(2)))
		})
	})

	Context("done channel", func() {
		It("should signal done once when multiple transitions match the event", func() {
			var actionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
				return false, nil
			}
			bitmap := &Bitmap{Name: "test"}
			c := newTestContainer(bitmap, []*Transition{
				{Name: "First", EventName: "test-event", Batching: Batching{Count: 1}, Actions: []any{actionF}},
				{Name: "Second", EventName: "test-event", Batching: Batching{Count: 1}, Actions: []any{actionF}},
				{Name: "Batch", EventName: "test-event", Batching: Batching{Count: 2}, Actions: []any{actionF}},
			}, sameBitmap(bitmap))

			doneC := make(chan struct{}, 10)
			Expect(c.Event("test-event", &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())

			// the event is held by the batch, so it is not done yet
			Consistently(doneC, 100*time.Millisecond).Should(BeEmpty())

			c.Close()
			Expect(doneC).To(HaveLen(1))
		})

		It("should signal done when no transition matches the event", func() {
			bitmap := &Bitmap{Name: "test"}
			c := newTestContainer(bitmap, []*Transition{}, sameBitmap(bitmap))

			doneC := make(chan struct{}, 1)
			Expect(c.Event("unknown-event", &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())

			Eventually(doneC).Should(HaveLen(1))
			c.Close()
		})
	})

	Context("state transitions", func() {
		var (
			rootBitmap   *Bitmap
			entityBitmap *Bitmap
			stateMap     map[string]int
		)

		BeforeEach(func() {
			entityBitmap, stateMap = createSimpleTestBitmap()
			rootBitmap = &Bitmap{Name: "root", Children: map[string]*Bitmap{entityBitmap.Name: entityBitmap}}
		})

		DescribeTable("should apply the state to the entity",
			func(from, to string) {
				c := newTestContainer(rootBitmap, []*Transition{
					{Name: "State", EventName: "test-event", Batching: Batching{Count: 1}, Actions: []any{mask(stateMap["status_"+to])}},
				}, getTestBitmapByType)
				c.Builder().Set(&TestStateEntity{Status: from, Count: 1})

				Expect(c.Event("test-event", &TestEvent{})).To(Succeed())
				c.Close()

				Expect(c.Builder().Build().Status).To(Equal(to))
				Expect(c.Builder().Build().Count).To(Equal(int64(1)))
				Expect(c.states[entityBitmap.Name]).To(Equal(mask(stateMap["status_"+to] | stateMap["count_1"] | stateMap["active_false"])))
			},
			Entry("moving forward", "pending", "processing"),
			Entry("moving backward", "completed", "pending"),
		)
	})
})
