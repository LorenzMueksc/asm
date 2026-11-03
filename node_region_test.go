package asm

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"rumue.io/asm/service"
)

var _ = Describe("Regions", func() {
	It("should measure nearness by the leading segments regions share", func() {
		Expect(normalizeRegion("/eu//de/fra/")).To(Equal("eu/de/fra"))
		Expect(regionNearness("eu/de/fra", "eu/de/ber")).To(Equal(2))
		Expect(regionNearness("eu/de/fra", "eu/fr")).To(Equal(1))
		Expect(regionNearness("eu/de", "us/ny")).To(BeZero())
		Expect(regionNearness("eu/de", "")).To(BeZero())
	})

	It("should cover with the nearest owners first, the fewest nodes among equally near ones", func() {
		bitmap, _ := createSimpleTestBitmap()
		n := NewNode(1, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "",
			WithSkipRaft(), WithBitmap(bitmap), WithLogger(zap.NewNop()), WithRegion("/eu/de/fra"))
		defer n.Shutdown()
		n.regions.replace(map[string]string{"2": "eu/de/ber", "3": "eu/fr/par", "4": "us/ny", "5": "eu/fr/lyo"})
		owned := func(nodes ...string) *Transition { return &Transition{NodeIds: nodes} }

		By("one transition: the nearest owner, every time")
		for range 20 {
			assigned, order, uncovered := n.coverTransitions(map[string]*Transition{"t": owned("4", "3", "2")}, []string{"t"}, nil)
			Expect(order).To(Equal([]string{"2"}))
			Expect(assigned).To(Equal(map[string][]string{"2": {"t"}}))
			Expect(uncovered).To(BeEmpty())
		}

		By("several: the nearer node first, even if a far one owns more")
		remote := map[string]*Transition{"t1": owned("2", "4"), "t2": owned("4")}
		_, order, _ := n.coverTransitions(remote, []string{"t1", "t2"}, nil)
		Expect(order).To(Equal([]string{"2", "4"}))

		By("equally near: the node owning most")
		remote = map[string]*Transition{"t1": owned("3", "5"), "t2": owned("5")}
		for range 20 {
			_, order, _ = n.coverTransitions(remote, []string{"t1", "t2"}, nil)
			Expect(order).To(Equal([]string{"5"}))
		}

		By("the nearest unreachable: the next nearest")
		_, order, _ = n.coverTransitions(map[string]*Transition{"t": owned("4", "3", "2")}, []string{"t"}, map[string]bool{"2": true})
		Expect(order).To(Equal([]string{"3"}))

		By("keeping the regions in snapshots")
		data, err := n.copySnapshotState().marshal()
		Expect(err).NotTo(HaveOccurred())
		snap := &service.NodeSnapshot{}
		Expect(snap.UnmarshalVT(data)).To(Succeed())
		Expect(snap.GetRegions()).To(Equal(n.regions.all()))
	})

	It("should take the certificate's region and refuse another one", func() {
		Expect(checkRegion("", "eu/de")).To(Equal("eu/de"))
		Expect(checkRegion("eu/de", "")).To(Equal("eu/de"))
		_, err := checkRegion("us", "eu/de")
		Expect(err).To(MatchError(ContainSubstring("differs")))

		id := identity{id: "2", roles: []string{RoleMember, "region:/eu/de/"}}
		Expect(id.region()).To(Equal("eu/de"))
		ctx := context.WithValue(context.Background(), identityKey{}, id)
		region := func(path string) []*service.NodeLog {
			return []*service.NodeLog{{NodeId: "2", Type: service.NodeEventType_NODE_REGION,
				Body: &service.NodeLog_Region{Region: &service.NodeRegion{Path: path}}}}
		}
		Expect(checkProposal(ctx, region("eu/de"))).To(Succeed())
		Expect(status.Code(checkProposal(ctx, region("us")))).To(Equal(codes.PermissionDenied))
	})
})

var _ = Describe("Regions in a cluster", Label("cluster"), func() {
	var (
		tc    *testCluster
		calls *callLog
	)

	BeforeEach(func() {
		tc = newTestCluster()
		calls = newCallLog()
		regions := map[int]string{1: "eu/de/fra", 2: "eu/de/ber", 3: "us/ny", 4: "eu/de/ham"}
		tc.nodeOpts = func(id int) []NodeSetupOption { return []NodeSetupOption{WithRegion(regions[id])} }
	})

	AfterEach(func() {
		tc.stopAll()
	})

	It("should spread the regions through the log, to clients too, and forward to the nearest owner", func() {
		bitmap := func() *Bitmap { return clusterBitmap("pending", "done") }
		n1 := tc.start(1, "", bitmap(), nil)
		expectReady(n1)
		expectReady(tc.start(2, tc.addr(1), bitmap(), func(n *clusterNode) { calls.transition(n, "x", "WORK", "done") }))
		expectReady(tc.start(3, tc.addr(1), bitmap(), func(n *clusterNode) { calls.transition(n, "x", "WORK", "done") }))
		n4 := tc.startClient(4, tc.addr(1), bitmap(), nil)
		expectReady(n4)

		all := map[string]string{"1": "eu/de/fra", "2": "eu/de/ber", "3": "us/ny", "4": "eu/de/ham"}
		for _, n := range []*clusterNode{n1, tc.node(2), tc.node(3), n4} {
			Eventually(n.regions.all, 10*time.Second).Should(Equal(all), "node %s", n.Id())
		}
		info, err := n1.Inspect(context.Background(), &service.InspectRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(info.GetNode().GetRegion()).To(Equal("eu/de/fra"))
		Eventually(func() []string { return publishedTransition(n1, "x").NodeIds }, 10*time.Second).Should(ConsistOf("2", "3"))
		Eventually(func() []string { return publishedTransition(n4, "x").NodeIds }, 10*time.Second).Should(ConsistOf("2", "3"))

		By("forwarding from a member and from a client: always node 2, the owner in eu/de")
		for i := range 5 {
			Expect(sendWaiting(n1, fmt.Sprintf("m%d", i), "WORK", wrapperspb.String("pending"))).To(Equal("done"))
			Expect(sendWaiting(n4, fmt.Sprintf("c%d", i), "WORK", wrapperspb.String("pending"))).To(Equal("done"))
		}
		Expect(calls.get("2/x")).To(HaveLen(10))
		Expect(calls.get("3/x")).To(BeEmpty())
	})
})
