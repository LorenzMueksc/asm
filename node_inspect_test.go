package asm

import (
	"context"
	"errors"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"rumue.io/asm/service"
)

// traceKinds returns the kinds of the trace entries of a container, oldest first.
func traceKinds(entries []*service.TraceEvent, containerId string) []service.TraceEvent_Kind {
	kinds := make([]service.TraceEvent_Kind, 0)
	for _, e := range entries {
		if e.GetContainerId() == containerId {
			kinds = append(kinds, e.GetKind())
		}
	}
	return kinds
}

var _ = Describe("Node introspection", func() {
	It("should inspect a node and trace the events of its containers", func() {
		bitmap, _ := createSimpleTestBitmap()
		n := NewNode(1, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "",
			WithSkipRaft(), WithBitmap(bitmap), WithLogger(zap.NewNop()), WithEventTrace(100))
		defer n.Shutdown()
		n.AddStateTransition("process", "STEP", &TestStateEntity{Status: "processing"})
		n.AddFuncTransition("fail", "FAIL", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			return false, errors.New("broken")
		}, WithFuncKey("fail-key"))

		c, _, err := n.StartContainer(context.Background(), "c1")
		Expect(err).NotTo(HaveOccurred())
		c.Builder().Set(&TestStateEntity{Status: "pending"})
		for _, event := range []string{"STEP", "FAIL"} {
			doneC := make(chan struct{}, 1)
			Expect(c.Event(event, &TestEvent{Value: "v"}, WithDoneChannel(doneC))).To(Succeed())
			Eventually(doneC).Should(Receive())
		}

		By("inspecting the node, its transitions and the container")
		resp, err := n.Inspect(context.Background(), &service.InspectRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetNode().GetSkipRaft()).To(BeTrue())
		Expect(resp.GetNode().GetReady()).To(BeTrue())
		Expect(resp.GetNode().GetTraceCapacity()).To(Equal(uint32(100)))
		Expect(resp.GetLayout()).NotTo(BeNil())

		keys := make(map[string]string)
		for _, t := range resp.GetTransitions() {
			keys[t.GetTransition().GetName()] = t.GetFuncKey()
		}
		Expect(keys).To(Equal(map[string]string{"process": "", "fail": "fail-key"}))

		Expect(resp.GetContainers()).To(HaveLen(1))
		info := resp.GetContainers()[0]
		Expect(info.GetId()).To(Equal("c1"))
		Expect(info.GetStatus()).To(Equal("running"))
		Expect(info.GetEntityJson()).To(ContainSubstring(`"status":"processing"`))
		Expect(bitsFromProto(info.GetStates()[bitmap.Name]).Has(1)).To(BeTrue())

		c.Close()

		By("tracing the start, the events, the error and the close")
		entries := n.trace.since(0)
		Eventually(func() []service.TraceEvent_Kind { return traceKinds(n.trace.since(0), "c1") }).Should(Equal([]service.TraceEvent_Kind{
			service.TraceEvent_CONTAINER_STARTED, service.TraceEvent_EVENT, service.TraceEvent_EVENT,
			service.TraceEvent_ERROR, service.TraceEvent_CONTAINER_CLOSED,
		}))
		entries = n.trace.since(0)
		Expect(entries[1].GetEventName()).To(Equal("STEP"))
		Expect(entries[1].GetTransitions()).To(Equal([]string{"process"}))
		Expect(entries[1].GetPayloads()[0].GetJson()).To(ContainSubstring(`"v"`))
		Expect(entries[3].GetError()).To(Equal("broken"))
	})

	It("should show the state a busy container had when its current event started", func() {
		bitmap, _ := createSimpleTestBitmap()
		n := NewNode(1, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "",
			WithSkipRaft(), WithBitmap(bitmap), WithLogger(zap.NewNop()), WithEventTrace(10))
		defer n.Shutdown()
		releaseC := make(chan struct{})
		// unblock the transition even if an assertion fails, otherwise Shutdown waits forever
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(releaseC) }) }
		defer release()
		n.AddFuncTransition("slow", "SLOW", func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
			<-releaseC
			return false, nil
		})

		c, _, err := n.StartContainer(context.Background(), "c1")
		Expect(err).NotTo(HaveOccurred())
		c.Builder().Set(&TestStateEntity{Status: "pending"})
		doneC := make(chan struct{}, 1)
		Expect(c.Event("SLOW", &TestEvent{}, WithDoneChannel(doneC))).To(Succeed())

		// the container is busy as soon as processing starts, the snapshot follows right after
		var info *service.ContainerInfo
		Eventually(func() string {
			resp, _ := n.Inspect(context.Background(), &service.InspectRequest{})
			if len(resp.GetContainers()) != 1 || !resp.GetContainers()[0].GetBusy() {
				return ""
			}
			info = resp.GetContainers()[0]
			return info.GetEntityJson()
		}).Should(ContainSubstring(`"status":"pending"`))
		Expect(info.GetStates()).To(HaveKey(bitmap.Name))

		release()
		Eventually(doneC).Should(Receive())
		c.Close()
	})

	It("should keep only the last entries of the trace", func() {
		tr := newEventTrace(3)
		for range 5 {
			tr.add(&service.TraceEvent{})
		}
		seqs := make([]uint64, 0)
		for _, e := range tr.since(0) {
			seqs = append(seqs, e.GetSeq())
		}
		Expect(seqs).To(Equal([]uint64{3, 4, 5}))
		Expect(tr.since(4)).To(HaveLen(1))
	})
})

var _ = Describe("Cluster introspection", Label("cluster"), func() {
	var tc *testCluster

	BeforeEach(func() {
		tc = newTestCluster()
		tc.opts = []NodeSetupOption{WithEventTrace(100)}
	})

	AfterEach(func() {
		tc.stopAll()
	})

	It("should stream the forwarded and received events and inspect the cluster over gRPC", func() {
		expectReady(tc.start(1, "", clusterBitmap("pending", "done"), nil))
		expectReady(tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), func(n *clusterNode) {
			n.AddFuncTransition("work", "WORK", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				c.Builder().Set(wrapperspb.String("done"))
				return false, nil
			}, WithFuncKey("work"))
		}))
		Eventually(func() *Transition { return publishedTransition(tc.node(1), "work") }, 10*time.Second).ShouldNot(BeNil())

		conn, err := grpc.NewClient(tc.addr(2), grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
		defer conn.Close()
		client := service.NewNodeClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		stream, err := client.WatchEvents(ctx, &service.WatchEventsRequest{})
		Expect(err).NotTo(HaveOccurred())

		Expect(sendWaiting(tc.node(1), "c1", "WORK", wrapperspb.String("pending"))).To(Equal("done"))

		By("receiving the forwarded events on node 2")
		var received *service.TraceEvent
		for received == nil {
			e, err := stream.Recv()
			Expect(err).NotTo(HaveOccurred())
			if e.GetKind() == service.TraceEvent_RECEIVED {
				received = e
			}
		}
		Expect(received.GetFromNode()).To(Equal("1"))
		Expect(received.GetTransitions()).To(Equal([]string{"work"}))
		Expect(received.GetWait()).To(BeTrue())

		By("tracing the route on node 1")
		Eventually(func() []*service.TraceRoute {
			for _, e := range tc.node(1).trace.since(0) {
				if e.GetKind() == service.TraceEvent_FORWARD {
					return e.GetRoutes()
				}
			}
			return nil
		}).Should(HaveLen(1))

		By("inspecting node 2 over gRPC")
		resp, err := client.Inspect(ctx, &service.InspectRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetNode().GetPeers()).To(HaveLen(2))
		Expect(resp.GetNode().GetLeader()).To(Equal(uint64(1)))
		Expect(resp.GetLayout().GetChildren()).To(HaveKey("StringValue"))
	})
})
