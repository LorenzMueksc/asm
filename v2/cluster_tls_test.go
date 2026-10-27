package asm

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"rumue.io/asm/service"
	"rumue.io/asm/v2/certs"
)

var _ = Describe("Cluster with mutual TLS", Label("cluster"), func() {
	var (
		tc    *testCluster
		ca    *certs.CA
		calls *callLog
	)

	bitmap := func() *Bitmap { return clusterBitmap("pending", "done") }
	// as dials the node with a certificate of the id and roles, issued by the cluster CA
	as := func(node int, id string, roles ...string) service.NodeClient {
		GinkgoHelper()
		cert, err := ca.TLSCertificate(id, roles...)
		Expect(err).NotTo(HaveOccurred())
		conn, err := grpc.NewClient(tc.addr(node), grpc.WithTransportCredentials(ToolCredentials(ca.Pool(), cert)))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(conn.Close)
		return service.NewNodeClient(conn)
	}
	code := func(err error) codes.Code { return status.Code(err) }
	ctx := func() context.Context {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		DeferCleanup(cancel)
		return c
	}
	heartbeat := func(nodeId string) *service.NodeLog {
		return &service.NodeLog{
			NodeId: nodeId,
			Type:   service.NodeEventType_NODE_HEARTBEAT,
			Ts:     timestamppb.Now(),
			Body:   &service.NodeLog_Heartbeat{Heartbeat: &service.Heartbeat{Lease: durationpb.New(time.Second)}},
		}
	}

	BeforeEach(func() {
		var err error
		ca, err = certs.NewCA("test cluster")
		Expect(err).NotTo(HaveOccurred())
		calls = newCallLog()
		tc = newTestCluster()
		// nodes 1-3 are members, node 4 is a client
		tc.nodeOpts = func(id int) []NodeSetupOption {
			role := RoleMember
			if id == 4 {
				role = RoleClient
			}
			cert, err := ca.TLSCertificate(strconv.Itoa(id), role)
			Expect(err).NotTo(HaveOccurred())
			return []NodeSetupOption{WithTLS(ca.Pool(), cert)}
		}
	})

	AfterEach(func() {
		tc.stopAll()
	})

	startMembers := func() {
		expectReady(tc.start(1, "", bitmap(), nil))
		expectReady(tc.start(2, tc.addr(1), bitmap(), func(n *clusterNode) {
			calls.transition(n, "work", "WORK", "done")
		}))
		expectReady(tc.start(3, tc.addr(1), bitmap(), nil))
	}

	It("should run members and a client with certificates: join, attach, locks and forwarded events", func() {
		startMembers()
		expectReady(tc.startClient(4, tc.addr(1), bitmap(), nil))
		Eventually(func() []string {
			if t := publishedTransition(tc.node(4), "work"); t != nil {
				return t.NodeIds
			}
			return nil
		}, 10*time.Second).Should(ConsistOf("2"))

		Expect(sendWaiting(tc.node(4), "player-1", "WORK", wrapperspb.String("pending"))).To(Equal("done"))
		Expect(calls.get("2/work")).To(Equal([]int{1}))
	})

	It("should refuse a node whose certificate names another node or lacks the role", func() {
		cert, err := ca.TLSCertificate("5", RoleMember)
		Expect(err).NotTo(HaveOccurred())
		n := NewNode(6, clusterBitmapByType, func() *stringValueBuilder { return &stringValueBuilder{} }, freeAddr(),
			WithBitmap(bitmap()), WithTLS(ca.Pool(), cert), WithLogger(zap.NewNop()))
		Eventually(n.Err(), 5*time.Second).Should(Receive(MatchError(ContainSubstring("not 6"))))

		cert, err = ca.TLSCertificate("7", RoleClient)
		Expect(err).NotTo(HaveOccurred())
		n = NewNode(7, clusterBitmapByType, func() *stringValueBuilder { return &stringValueBuilder{} }, freeAddr(),
			WithBitmap(bitmap()), WithTLS(ca.Pool(), cert), WithLogger(zap.NewNop()))
		Eventually(n.Err(), 5*time.Second).Should(Receive(MatchError(ContainSubstring("lacks the role member"))))
	})

	It("should reject callers without a certificate of the cluster CA", func() {
		startMembers()

		By("calling without TLS")
		conn, err := grpc.NewClient(tc.addr(1), grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
		defer conn.Close()
		_, err = service.NewNodeClient(conn).Propose(ctx(), &service.ProposeRequest{Logs: []*service.NodeLog{heartbeat("1")}})
		Expect(code(err)).To(Equal(codes.Unavailable))

		By("calling with a certificate of another CA")
		other, err := certs.NewCA("other")
		Expect(err).NotTo(HaveOccurred())
		cert, err := other.TLSCertificate("2", RoleMember)
		Expect(err).NotTo(HaveOccurred())
		conn2, err := grpc.NewClient(tc.addr(1), grpc.WithTransportCredentials(ToolCredentials(ca.Pool(), cert)))
		Expect(err).NotTo(HaveOccurred())
		defer conn2.Close()
		_, err = service.NewNodeClient(conn2).Propose(ctx(), &service.ProposeRequest{Logs: []*service.NodeLog{heartbeat("2")}})
		Expect(code(err)).To(Equal(codes.Unavailable))

		By("joining with a certificate of another CA")
		otherCert, err := other.TLSCertificate("8", RoleMember)
		Expect(err).NotTo(HaveOccurred())
		n := NewNode(8, clusterBitmapByType, func() *stringValueBuilder { return &stringValueBuilder{} }, freeAddr(),
			WithBitmap(bitmap()), WithJoinAddr(tc.addr(1)), WithTLS(other.Pool(), otherCert), WithStopChannel(make(chan struct{})), WithLogger(zap.NewNop()))
		Eventually(n.Err(), 10*time.Second).Should(Receive(MatchError(ContainSubstring("failed to join"))))
		Expect(peerIds(tc.node(1))).To(ConsistOf(uint64(1), uint64(2), uint64(3)))
	})

	It("should check the roles of every rpc", func() {
		startMembers()
		client := as(1, "9", RoleClient)
		observer := as(1, "devtool", RoleObserver)
		member := as(1, "2", RoleMember)

		By("raft rpcs only for members")
		_, err := client.Sync(ctx(), &service.SyncRequest{})
		Expect(code(err)).To(Equal(codes.PermissionDenied))
		joinCtx := ctx()
		_, err = client.Join(joinCtx, &service.JoinRequest{ListenAddr: "127.0.0.1:1"})
		Expect(code(err)).To(Equal(codes.PermissionDenied))

		By("attaching only for clients")
		stream, err := member.Attach(ctx())
		Expect(err).NotTo(HaveOccurred())
		// io.EOF if the node rejected the stream already: Recv returns why
		_ = stream.Send(&service.AttachRequest{ClientId: 2})
		_, err = stream.Recv()
		Expect(code(err)).To(Equal(codes.PermissionDenied))

		By("proposing only for nodes")
		_, err = observer.Propose(ctx(), &service.ProposeRequest{Logs: []*service.NodeLog{heartbeat("devtool")}})
		Expect(code(err)).To(Equal(codes.PermissionDenied))

		By("inspecting only for observers and admins")
		_, err = member.Inspect(ctx(), &service.InspectRequest{})
		Expect(code(err)).To(Equal(codes.PermissionDenied))
		resp, err := observer.Inspect(ctx(), &service.InspectRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetNode().GetId()).To(Equal("1"))
	})

	It("should only let a node act in its own name", func() {
		startMembers()
		leader := tc.leader()
		Expect(leader).NotTo(BeZero())
		// node 2's certificate, used to act for node 3
		node2 := as(leader, "2", RoleMember)

		By("proposing entries of another node")
		_, err := node2.Propose(ctx(), &service.ProposeRequest{Logs: []*service.NodeLog{heartbeat("3")}})
		Expect(code(err)).To(Equal(codes.PermissionDenied))
		_, err = node2.Propose(ctx(), &service.ProposeRequest{Logs: []*service.NodeLog{lockLog("2", service.NodeEventType_LOCK_RELEASE,
			&service.ContainerLock{ContainerId: "c", Token: "3-1"})}})
		Expect(code(err)).To(Equal(codes.PermissionDenied))
		_, err = node2.Propose(ctx(), &service.ProposeRequest{Logs: []*service.NodeLog{heartbeat("2")}})
		Expect(err).NotTo(HaveOccurred())

		By("sending raft messages of another node")
		msg, err := (&raftpb.Message{Type: raftpb.MsgHeartbeatResp, From: 3, To: uint64(leader)}).Marshal()
		Expect(err).NotTo(HaveOccurred())
		_, err = node2.Sync(ctx(), &service.SyncRequest{Messages: [][]byte{msg}})
		Expect(code(err)).To(Equal(codes.PermissionDenied))

		By("adding transitions of another node")
		_, err = node2.AddTransitions(ctx(), &service.AddTransitionsRequest{Transitions: []*service.Transition{{Name: "fake", EventName: "WORK", NodeId: "3"}}})
		Expect(code(err)).To(Equal(codes.PermissionDenied))

		By("forwarding events in the name of another node")
		payload, err := anypb.New(wrapperspb.Int64(1))
		Expect(err).NotTo(HaveOccurred())
		_, err = node2.ForwardEvent(ctx(), &service.ForwardEventRequest{EventName: "WORK", ContainerId: "c", SenderNodeId: "3", Payloads: []*anypb.Any{payload}})
		Expect(code(err)).To(Equal(codes.PermissionDenied))
		_, err = node2.ForwardEvent(ctx(), &service.ForwardEventRequest{EventName: "WORK", ContainerId: "c", SenderNodeId: "2", Wait: true, LendChain: []string{"3"}})
		Expect(code(err)).To(Equal(codes.PermissionDenied))

		By("attaching as another client")
		stream, err := as(1, "9", RoleClient).Attach(ctx())
		Expect(err).NotTo(HaveOccurred())
		Expect(stream.Send(&service.AttachRequest{ClientId: 8})).To(Succeed())
		_, err = stream.Recv()
		Expect(code(err)).To(Equal(codes.PermissionDenied))

		By("removing another node without the admin role")
		_, err = node2.RemovePeer(ctx(), &service.RemovePeerRequest{NodeId: 3})
		Expect(code(err)).To(Equal(codes.PermissionDenied))
		Expect(peerIds(tc.node(1))).To(ConsistOf(uint64(1), uint64(2), uint64(3)))

		By("removing it with the admin role")
		victim := 3
		if leader == 3 {
			victim = 2
		}
		_, err = as(leader, "ops", RoleAdmin).RemovePeer(context.Background(), &service.RemovePeerRequest{NodeId: uint64(victim)})
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() []uint64 { return peerIds(tc.node(leader)) }, 10*time.Second).ShouldNot(ContainElement(uint64(victim)))
		tc.forget(victim)
	})

	It("should serve the Event rpc on the public listener without a client certificate", func() {
		publicAddr := freeAddr()
		tc.opts = []NodeSetupOption{
			WithPublicAddr(publicAddr),
			WithContainerIdFunc(func(eventName string, payload proto.Message) string { return "public-1" }),
		}
		expectReady(tc.start(1, "", bitmap(), func(n *clusterNode) {
			calls.transition(n, "work", "WORK", "done")
		}))

		conn, err := grpc.NewClient(publicAddr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs:    ca.Pool(),
			ServerName: "localhost",
		})))
		Expect(err).NotTo(HaveOccurred())
		defer conn.Close()
		public := service.NewNodeClient(conn)

		payload, err := anypb.New(wrapperspb.Int64(1))
		Expect(err).NotTo(HaveOccurred())
		_, err = public.Event(ctx(), &service.EventRequest{
			EventName: "WORK",
			Payload:   payload,
			Options:   &service.EventRequest_EventOptions{Meta: map[string]string{"wait": "true"}},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(calls.get("1/work")).To(Equal([]int{1}))

		By("serving nothing else there")
		_, err = public.Propose(ctx(), &service.ProposeRequest{})
		Expect(code(err)).To(Equal(codes.Unimplemented))
		_, err = public.Inspect(ctx(), &service.InspectRequest{})
		Expect(code(err)).To(Equal(codes.Unimplemented))
	})

	It("should reject the certificate of a removed node and close its connections", func() {
		startMembers()
		leader := tc.leader()
		Expect(leader).NotTo(BeZero())
		victim := 3
		if leader == 3 {
			victim = 2
		}
		victimId := strconv.Itoa(victim)
		node := as(leader, victimId, RoleMember)

		By("opening a stream before the removal")
		stream, err := node.ForwardEvents(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(stream.Send(&service.ForwardCalls{})).To(Succeed())
		streamErrC := make(chan error, 1)
		go func() {
			_, err := stream.Recv()
			streamErrC <- err
		}()
		Consistently(streamErrC, 300*time.Millisecond).ShouldNot(Receive())

		_, err = as(leader, "ops", RoleAdmin).RemovePeer(context.Background(), &service.RemovePeerRequest{NodeId: uint64(victim)})
		Expect(err).NotTo(HaveOccurred())
		tc.forget(victim)

		By("rejecting its rpcs once the removal applied")
		_, err = node.AddTransitions(ctx(), &service.AddTransitionsRequest{Transitions: []*service.Transition{{Name: "fake", EventName: "WORK", NodeId: victimId}}})
		Expect(code(err)).To(Equal(codes.PermissionDenied))
		payload, err := anypb.New(wrapperspb.Int64(1))
		Expect(err).NotTo(HaveOccurred())
		_, err = node.ForwardEvent(ctx(), &service.ForwardEventRequest{EventName: "WORK", ContainerId: "c", SenderNodeId: victimId, Payloads: []*anypb.Any{payload}})
		Expect(code(err)).To(Equal(codes.PermissionDenied))
		_, err = node.Event(ctx(), &service.EventRequest{EventName: "WORK", Payload: payload})
		Expect(code(err)).To(Equal(codes.PermissionDenied))

		By("closing the stream opened before")
		Eventually(streamErrC, 5*time.Second).Should(Receive(HaveOccurred()))

		By("rejecting it on the other member too, but not tools")
		other := 6 - leader - victim
		Eventually(func() codes.Code {
			_, err := as(other, victimId, RoleMember).AddTransitions(ctx(), &service.AddTransitionsRequest{})
			return code(err)
		}, 10*time.Second).Should(Equal(codes.PermissionDenied))
		_, err = as(leader, "devtool", RoleObserver).Inspect(ctx(), &service.InspectRequest{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("should let a client and a member leave, though a removed id's rpcs are rejected", func() {
		startMembers()
		expectReady(tc.startClient(4, tc.addr(1), bitmap(), nil))
		leaveCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		Expect(tc.node(4).Leave(leaveCtx)).To(Succeed())
		tc.forget(4)
		leader := tc.leader()
		Expect(leader).NotTo(BeZero())
		follower := 1
		for follower == leader {
			follower++
		}
		Expect(tc.node(follower).Leave(leaveCtx)).To(Succeed())
		tc.forget(follower)
		Eventually(func() []uint64 { return peerIds(tc.node(leader)) }, 10*time.Second).
			Should(And(HaveLen(2), Not(ContainElement(uint64(follower))), Not(ContainElement(uint64(4)))))
	})

	It("should check the peers' certificates with the certificate check", func() {
		var checked atomic.Int32
		tc.opts = []NodeSetupOption{WithCertificateCheck(func(c *x509.Certificate) error {
			checked.Add(1)
			if c.Subject.CommonName == "revoked" {
				return errors.New("revoked")
			}
			return nil
		})}
		expectReady(tc.start(1, "", bitmap(), nil))

		By("rejecting rpcs with a refused certificate")
		_, err := as(1, "revoked", RoleObserver).Inspect(ctx(), &service.InspectRequest{})
		Expect(code(err)).To(Equal(codes.PermissionDenied))
		Expect(err.Error()).To(ContainSubstring("revoked"))
		_, err = as(1, "devtool", RoleObserver).Inspect(ctx(), &service.InspectRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(checked.Load()).To(BeNumerically(">=", 2))

		By("failing the handshake when dialing a node whose certificate is refused")
		cert, err := ca.TLSCertificate("2", RoleMember)
		Expect(err).NotTo(HaveOccurred())
		n := NewNode(2, clusterBitmapByType, func() *stringValueBuilder { return &stringValueBuilder{} }, freeAddr(),
			WithBitmap(bitmap()), WithJoinAddr(tc.addr(1)), WithTLS(ca.Pool(), cert), WithStopChannel(make(chan struct{})), WithLogger(zap.NewNop()),
			WithCertificateCheck(func(c *x509.Certificate) error {
				return fmt.Errorf("node %s is not trusted", c.Subject.CommonName)
			}))
		Eventually(n.Err(), 10*time.Second).Should(Receive(MatchError(ContainSubstring("failed to join"))))
		Expect(peerIds(tc.node(1))).To(ConsistOf(uint64(1)))
	})

	It("should serve the metrics over HTTPS to observers and admins only", func() {
		metricsAddr := freeAddr()
		tc.opts = []NodeSetupOption{WithMetricsAddr(metricsAddr)}
		expectReady(tc.start(1, "", bitmap(), nil))

		// get scrapes the metrics with the certificate of the id and roles, without a certificate if id is empty
		get := func(id string, roles ...string) (int, string, error) {
			cfg := &tls.Config{RootCAs: ca.Pool(), ServerName: "localhost"}
			if id != "" {
				cert, err := ca.TLSCertificate(id, roles...)
				Expect(err).NotTo(HaveOccurred())
				cfg.Certificates = []tls.Certificate{cert}
			}
			transport := &http.Transport{TLSClientConfig: cfg}
			defer transport.CloseIdleConnections()
			r, err := (&http.Client{Transport: transport, Timeout: 5 * time.Second}).Get("https://" + metricsAddr + "/metrics")
			if err != nil {
				return 0, "", err
			}
			defer r.Body.Close()
			body, err := io.ReadAll(r.Body)
			return r.StatusCode, string(body), err
		}

		Eventually(func() string {
			_, body, _ := get("devtool", RoleObserver)
			return body
		}, 5*time.Second).Should(ContainSubstring("asm_raft_leader 1\n"))
		httpStatus, _, err := get("ops", RoleAdmin)
		Expect(err).NotTo(HaveOccurred())
		Expect(httpStatus).To(Equal(http.StatusOK))

		By("refusing members and callers without a certificate")
		httpStatus, _, err = get("2", RoleMember)
		Expect(err).NotTo(HaveOccurred())
		Expect(httpStatus).To(Equal(http.StatusForbidden))
		_, _, err = get("")
		Expect(err).To(HaveOccurred())
		r, err := http.Get("http://" + metricsAddr + "/metrics")
		if err == nil {
			_ = r.Body.Close()
			Expect(r.StatusCode).To(Equal(http.StatusBadRequest))
		}
	})

	It("should reject forwarded events with a wait path over the cap", func() {
		expectReady(tc.start(1, "", bitmap(), nil))
		path := make([]string, maxWaitPath+1)
		for i := range path {
			path[i] = fmt.Sprintf("3-%d", i)
		}
		_, err := as(1, "2", RoleMember).ForwardEvent(ctx(), &service.ForwardEventRequest{
			EventName: "WORK", ContainerId: "c", SenderNodeId: "2", Wait: true, WaitPath: path,
		})
		Expect(code(err)).To(Equal(codes.InvalidArgument))
	})
})

var _ = Describe("Wait path cap", func() {
	It("should refuse a lock acquire with a wait path over the cap", func() {
		path := func(n int) []string {
			p := make([]string, n)
			for i := range p {
				p[i] = fmt.Sprintf("9-%d", i)
			}
			return p
		}
		lt := newLockTable()
		_, after, refused := lt.applyAcquire("1", time.Now(), "c", "1-1", path(maxWaitPath+1), "")
		Expect(refused).To(BeTrue())
		Expect(after.holder).To(BeEmpty())
		Expect(checkWaitPath(path(maxWaitPath + 1))).To(HaveOccurred())

		_, after, refused = lt.applyAcquire("1", time.Now(), "c", "1-2", path(maxWaitPath), "")
		Expect(refused).To(BeFalse())
		Expect(after.holder).To(Equal("1"))
		Expect(checkWaitPath(path(maxWaitPath))).To(Succeed())
	})
})
