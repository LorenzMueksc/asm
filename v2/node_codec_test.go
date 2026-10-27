package asm

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"rumue.io/asm/service"
)

var _ = Describe("vtCodec", func() {
	roundTrip := func(in, out proto.Message) {
		GinkgoHelper()
		data, err := vtCodec{}.Marshal(in)
		Expect(err).NotTo(HaveOccurred())
		// the wire format is protobuf
		Expect(proto.Unmarshal(data.Materialize(), out)).To(Succeed())
		Expect(proto.Equal(in, out)).To(BeTrue())

		out2 := out.ProtoReflect().New().Interface()
		Expect(vtCodec{}.Unmarshal(data, out2)).To(Succeed())
		Expect(proto.Equal(in, out2)).To(BeTrue())
		data.Free()
	}

	It("should marshal the node's messages with their generated code", func() {
		in := &service.ForwardCalls{
			Calls:  []*service.ForwardCall{{Id: 7, Request: &service.ForwardEventRequest{ContainerId: "p-1", EventName: "WORK", Wait: true}}},
			Cancel: []uint64{3, 4},
		}
		roundTrip(in, &service.ForwardCalls{})
	})

	It("should marshal large messages into pooled buffers", func() {
		in := &service.NodeLogs{}
		for range 2000 {
			in.Logs = append(in.Logs, &service.NodeLog{NodeId: "1", Type: service.NodeEventType_LOCK_ACQUIRE, Body: &service.NodeLog_Lock{Lock: &service.ContainerLock{ContainerId: strings.Repeat("x", 40)}}})
		}
		roundTrip(in, &service.NodeLogs{})
	})

	It("should fall back to proto for other messages", func() {
		roundTrip(wrapperspb.String("entity"), &wrapperspb.StringValue{})
	})
})
