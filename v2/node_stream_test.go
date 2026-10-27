package asm

import (
	"context"
	"io"
	"maps"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"rumue.io/asm/service"
)

// fakeForwardEvents is the client side of a ForwardEvents stream: sent calls go to sent, replies come from replies,
// closing replies breaks the stream
type fakeForwardEvents struct {
	grpc.ClientStream
	sent    chan *service.ForwardCalls
	replies chan *service.ForwardReplies
}

func (f *fakeForwardEvents) Send(c *service.ForwardCalls) error {
	f.sent <- c
	return nil
}

func (f *fakeForwardEvents) Recv() (*service.ForwardReplies, error) {
	r, ok := <-f.replies
	if !ok {
		return nil, io.EOF
	}
	return r, nil
}

// newFakeStream returns a forward stream over a fake ForwardEvents stream, with its goroutines running.
func newFakeStream() (*forwardStream, *fakeForwardEvents) {
	fake := &fakeForwardEvents{sent: make(chan *service.ForwardCalls, 10), replies: make(chan *service.ForwardReplies, 10)}
	s := &forwardStream{
		stream:    fake,
		cancel:    func() {},
		sendC:     make(chan *service.ForwardCalls, maxForwardBatch),
		pending:   make(map[uint64]chan *service.ForwardReply),
		handOvers: make(map[uint64]func(error)),
		doneC:     make(chan struct{}),
	}
	go s.sendLoop()
	go s.recvLoop()
	return s, fake
}

// handOverOutcomes records the outcome of hand-overs by name: nil once ran, else the failure
type handOverOutcomes struct {
	mu       sync.Mutex
	outcomes map[string]error
}

func (o *handOverOutcomes) get() map[string]error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return maps.Clone(o.outcomes)
}

// handOver sends a hand-over named by its container id over the stream and returns its call id.
func (o *handOverOutcomes) handOver(s *forwardStream, fake *fakeForwardEvents, name string) uint64 {
	GinkgoHelper()
	Expect(s.handOver(context.Background(), &service.ForwardEventRequest{ContainerId: name}, func(err error) {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.outcomes[name] = err
	})).To(Succeed())
	var calls *service.ForwardCalls
	Eventually(fake.sent).Should(Receive(&calls))
	return calls.GetCalls()[0].GetId()
}

var _ = Describe("Forward stream", func() {
	It("should keep hand-overs until they ran and fail the ones kept when the stream breaks", func() {
		s, fake := newFakeStream()
		o := &handOverOutcomes{outcomes: make(map[string]error)}

		ran, refused, accepted := o.handOver(s, fake, "ran"), o.handOver(s, fake, "refused"), o.handOver(s, fake, "accepted")
		lost := o.handOver(s, fake, "lost")
		fake.replies <- &service.ForwardReplies{Replies: []*service.ForwardReply{
			{Id: ran},
			{Id: refused, Code: uint32(codes.InvalidArgument), Message: "no container"},
			{Id: accepted},
		}}
		Eventually(o.get).Should(HaveKey("refused"))
		Expect(status.Code(o.get()["refused"])).To(Equal(codes.InvalidArgument))

		By("ending a hand-over once it ran, not on its accept")
		Consistently(o.get, 200*time.Millisecond).ShouldNot(HaveKey("ran"))
		fake.replies <- &service.ForwardReplies{Replies: []*service.ForwardReply{{Id: ran, Executed: true}}}
		Eventually(o.get).Should(HaveKeyWithValue("ran", BeNil()))

		By("breaking the stream: the accepted hand-over and the one without reply fail")
		close(fake.replies)
		Eventually(o.get).Should(HaveKey("lost"))
		Expect(status.Code(o.get()["lost"])).To(Equal(codes.Unavailable))
		Eventually(o.get).Should(HaveKey("accepted"))
		Expect(status.Code(o.get()["accepted"])).To(Equal(codes.Unavailable))
		Expect(o.get()["ran"]).To(BeNil())
		Expect(lost).NotTo(BeZero())
	})

	Context("on a node", Label("cluster"), func() {
		var tc *testCluster

		BeforeEach(func() {
			tc = newTestCluster()
		})

		AfterEach(func() {
			tc.stopAll()
		})

		It("should fail the kept hand-overs once the lease of the receiver ended, the stream stays open", func() {
			expectReady(tc.start(1, "", clusterBitmap("pending", "done"), nil))
			n := tc.node(1)
			s, fake := newFakeStream()
			o := &handOverOutcomes{outcomes: make(map[string]error)}

			// node 9 is only known by its lease, which ends in two seconds
			n.locks.applyHeartbeat("9", time.Now(), 2*time.Second)
			go n.watchLease(9, s)

			id := o.handOver(s, fake, "kept")
			fake.replies <- &service.ForwardReplies{Replies: []*service.ForwardReply{{Id: id}}}
			Consistently(o.get, time.Second).ShouldNot(HaveKey("kept"))

			Eventually(o.get, 5*time.Second).Should(HaveKey("kept"))
			Expect(status.Code(o.get()["kept"])).To(Equal(codes.Unavailable))
			Expect(o.get()["kept"].Error()).To(ContainSubstring("lease of node 9 ended"))
			Expect(s.alive()).To(BeTrue())

			By("refusing to hand over to the node right away")
			err := n.sendHandOver(context.Background(), "9", &service.ForwardEventRequest{}, func(error) {})
			Expect(status.Code(err)).To(Equal(codes.Unavailable))
			Expect(n.pendingHandOvers()).To(BeZero())
		})

		It("should wait for a free slot of the limit of kept hand-overs until ctx is done", func() {
			tc.opts = []NodeSetupOption{WithMaxPendingHandOvers(1)}
			expectReady(tc.start(1, "", clusterBitmap("pending", "done"), nil))
			n := tc.node(1)
			Expect(n.takeHandOverSlot(context.Background())).To(Succeed())

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			err := n.sendHandOver(ctx, "1", &service.ForwardEventRequest{}, func(error) {})
			Expect(status.Code(err)).To(Equal(codes.DeadlineExceeded))
			Expect(err.Error()).To(ContainSubstring("WithMaxPendingHandOvers"))

			n.freeHandOverSlot()
			Expect(n.pendingHandOvers()).To(BeZero())
		})
	})
})
