package asm

import (
	"context"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

var _ = Describe("Hand-overs on a node", func() {
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
	newNode := func(opts ...NodeSetupOption) {
		bitmap, _ := createSimpleTestBitmap()
		n = NewNode(1, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "",
			append([]NodeSetupOption{WithSkipRaft(), WithBitmap(bitmap), WithLogger(zap.NewNop())}, opts...)...)
	}

	BeforeEach(func() {
		seen = nil
	})

	AfterEach(func() {
		n.Shutdown()
	})

	It("should give the transitions and the save func the id of the hand-over they run for", func() {
		newNode(WithSaveFunc[*TestStateEntity, *mockBuilder](func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder]) error {
			record("save " + c.HandOverId())
			return nil
		}))
		n.AddFuncTransition("work", "WORK", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			record("work " + c.HandOverId())
			return false, nil
		}, WithSaveAfter())

		By("a hand-over of another node")
		_, err := n.acceptHandOver("2-7/work", []handedOver{{id: "c", item: forwardedExecution{
			eventName: "WORK", r: &remoteExecution{payloads: []any{&TestEvent{}}, from: "2"},
		}}}, nil)
		Expect(err).NotTo(HaveOccurred())
		Eventually(recorded).Should(Equal([]string{"work 2-7/work", "save 2-7/work"}))
		Eventually(func() bool { return n.hasContainer("c") }).Should(BeFalse())

		By("an event of the container itself")
		c, _, err := n.StartContainer(context.Background(), "c")
		Expect(err).NotTo(HaveOccurred())
		doneC := make(chan struct{}, 1)
		Expect(c.Event("WORK", &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())
		Eventually(doneC).Should(Receive())
		Expect(recorded()[2:]).To(Equal([]string{"work ", "save "}))
		Expect(c.HandOverId()).To(BeEmpty())
		c.Close()
	})

	It("should end waiting for a hand-over slot on the close request of the container handing over", func() {
		newNode(WithMaxPendingHandOvers(1))
		errC := make(chan error, 1)
		n.AddFuncTransition("hand", "HAND", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			errC <- n.takeHandOverSlot(ctx)
			return false, nil
		})
		Expect(n.takeHandOverSlot(context.Background())).To(Succeed())

		c, _, err := n.StartContainer(context.Background(), "c")
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Event("HAND", &TestEvent{})).To(Succeed())
		Consistently(errC, 200*time.Millisecond).ShouldNot(Receive())

		c.Close()
		Eventually(errC).Should(Receive(MatchError(ErrContainerClosed)))
		Eventually(func() bool { return n.hasContainer("c") }).Should(BeFalse())
		n.freeHandOverSlot()
		Expect(n.pendingHandOvers()).To(BeZero())
	})
})
