package asm

import (
	"context"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"rumue.io/asm/service"
)

var _ = Describe("Node Event rpc", func() {
	var (
		node      *Node[*TestStateEntity, *mockBuilder]
		processed atomic.Int32
		received  atomic.Value
	)

	newNode := func(opts ...NodeSetupOption) *Node[*TestStateEntity, *mockBuilder] {
		bitmap, _ := createSimpleTestBitmap()
		opts = append(opts,
			WithSkipRaft(),
			WithBitmap(bitmap),
			WithLogger(zap.NewNop()),
		)
		n := NewNode(
			1,
			func(entity any, bitMap *Bitmap) *Bitmap {
				return bitMap
			},
			func() *mockBuilder {
				return &mockBuilder{}
			},
			"TEST",
			opts...,
		)

		var actionF transitionFunc[*TestStateEntity, *mockBuilder] = func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			time.Sleep(50 * time.Millisecond)
			received.Store(a.(*wrapperspb.StringValue).GetValue())
			processed.Add(1)
			return false, nil
		}
		n.AddFuncTransition("test-transition", "TESTEVENT", actionF)

		return n
	}
	containerIdF := func(eventName string, payload proto.Message) string {
		return payload.(*wrapperspb.StringValue).GetValue()
	}
	newRequest := func(value string, meta map[string]string) *service.EventRequest {
		payload, err := anypb.New(wrapperspb.String(value))
		Expect(err).NotTo(HaveOccurred())

		return &service.EventRequest{
			EventName: "TESTEVENT",
			Payload:   payload,
			Options:   &service.EventRequest_EventOptions{Meta: meta},
		}
	}
	containerCount := func() int {
		node.containerMap.mu.RLock()
		defer node.containerMap.mu.RUnlock()
		return len(node.containerMap.m)
	}

	BeforeEach(func() {
		processed.Store(0)
		received.Store("")
	})

	AfterEach(func() {
		node.Shutdown()
	})

	It("should respond after the event is processed when waiting", func() {
		node = newNode(WithContainerIdFunc(containerIdF))

		resp, err := node.Event(context.Background(), newRequest("C-1", map[string]string{"wait": "true"}))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetContainerId()).To(Equal("C-1"))
		Expect(processed.Load()).To(Equal(int32(1)))
		Expect(received.Load()).To(Equal("C-1"))

		// the container closes after the event
		Eventually(containerCount).Should(BeZero())
	})

	It("should respond right away and close the container after processing when not waiting", func() {
		node = newNode(WithContainerIdFunc(containerIdF))

		resp, err := node.Event(context.Background(), newRequest("C-2", nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetContainerId()).To(Equal("C-2"))
		Expect(processed.Load()).To(BeZero())

		Eventually(processed.Load).Should(Equal(int32(1)))
		Eventually(containerCount).Should(BeZero())
	})

	It("should process all events sent to the same container", func() {
		node = newNode(WithContainerIdFunc(containerIdF))

		for i := 0; i < 10; i++ {
			_, err := node.Event(context.Background(), newRequest("C-3", nil))
			Expect(err).NotTo(HaveOccurred())
		}

		Eventually(processed.Load).Should(Equal(int32(10)))
		Eventually(containerCount).Should(BeZero())
	})

	It("should reject events without a container id func", func() {
		node = newNode()

		_, err := node.Event(context.Background(), newRequest("C-4", nil))
		Expect(status.Code(err)).To(Equal(codes.FailedPrecondition))
	})

	It("should reject payloads of unknown types", func() {
		node = newNode(WithContainerIdFunc(containerIdF))

		_, err := node.Event(context.Background(), &service.EventRequest{
			EventName: "TESTEVENT",
			Payload:   &anypb.Any{TypeUrl: "type.googleapis.com/unknown.Type"},
		})
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
	})

	It("should start a new container when the previous one with the same id is closed", func() {
		node = newNode()

		first, loaded, _ := node.StartContainer(context.Background(), "C-5")
		Expect(loaded).To(BeFalse())
		first.Close()

		second, loaded, _ := node.StartContainer(context.Background(), "C-5")
		Expect(loaded).To(BeFalse())
		Expect(second.isAccepting()).To(BeTrue())
	})
})
