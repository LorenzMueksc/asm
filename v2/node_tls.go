package asm

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"rumue.io/asm/service"
)

// With WithTLS the node port requires mutual TLS with certificates of the cluster CA. The certificate is the identity:
// its common name is the node id (or a tool's name), its organizational units are the roles. Every rpc of the node
// port checks the caller's roles (methodRoles), and a caller may only act in its own name: log entries, raft
// messages, transitions, forwarded events and Attach carry the caller's id, RemovePeer of another node needs the
// admin role. A node dialing another node verifies the other's certificate the same way: the node id it expects, or
// the member role for join and attach addresses. The Event rpc for callers outside the cluster is served on a
// separate public listener (WithPublicAddr) which doesn't ask for a certificate.
//
// The certificate of a node id removed from the cluster is rejected for every rpc (with TLS a removed id can't join
// again), and the open connections of that id are closed shortly after the removal applied. WithCertificateCheck
// adds an own check of the peers' certificates, on every rpc and when dialing. A connection ends once its client
// certificate expired, and clients move to a new connection after the max connection age (WithMaxConnectionAge), so
// a new handshake checks the certificates again.
//
// Without WithTLS nothing is authenticated: any process reaching the port can act as any node.

const (
	// defaultMaxConnectionAge is how long a client may open rpcs on a connection to the node port before it has to
	// open a new one (WithMaxConnectionAge)
	defaultMaxConnectionAge = time.Hour
	// removedConnGrace is how long the connections of a removed node stay open after its removal applied: answers to
	// its rpcs in flight (eg. its own RemovePeer) still go out. Its new rpcs and stream messages are rejected at once.
	removedConnGrace = time.Second
)

// Roles in the organizational units of a certificate.
const (
	// RoleMember may run raft: join, send raft messages, serve clients.
	RoleMember = "member"
	// RoleClient may attach to a member as client node.
	RoleClient = "client"
	// RoleAdmin may remove other nodes from the cluster.
	RoleAdmin = "admin"
	// RoleObserver may inspect nodes and watch their events (dev tools).
	RoleObserver = "observer"
)

// methodRoles are the roles which may call each rpc of the node port. Event takes any certificate of the cluster.
var methodRoles = map[string][]string{
	service.Node_Event_FullMethodName:          {RoleMember, RoleClient, RoleAdmin, RoleObserver},
	service.Node_Join_FullMethodName:           {RoleMember},
	service.Node_Sync_FullMethodName:           {RoleMember},
	service.Node_SyncStream_FullMethodName:     {RoleMember},
	service.Node_Propose_FullMethodName:        {RoleMember, RoleClient},
	service.Node_Attach_FullMethodName:         {RoleClient},
	service.Node_GetTransitions_FullMethodName: {RoleMember, RoleClient},
	service.Node_AddTransitions_FullMethodName: {RoleMember, RoleClient},
	service.Node_ForwardEvent_FullMethodName:   {RoleMember, RoleClient},
	service.Node_ForwardEvents_FullMethodName:  {RoleMember, RoleClient},
	service.Node_CheckLending_FullMethodName:   {RoleMember, RoleClient},
	service.Node_RemovePeer_FullMethodName:     {RoleMember, RoleClient, RoleAdmin},
	service.Node_Inspect_FullMethodName:        {RoleObserver, RoleAdmin},
	service.Node_WatchEvents_FullMethodName:    {RoleObserver, RoleAdmin},
}

type (
	// identity is who a certificate belongs to: a node id or a tool's name, and its roles
	identity struct {
		id    string
		roles []string
	}
	identityKey struct{}
	// nodeTLS holds the CA and the certificate of the node
	nodeTLS struct {
		ca   *x509.CertPool
		cert tls.Certificate
		// certCheck is the own check of the peers' certificates (WithCertificateCheck), nil if there is none
		certCheck func(*x509.Certificate) error
		// maxConnAge is the max connection age of the node port, 0 = unlimited
		maxConnAge time.Duration
		// conns are the open connections of the node port
		conns connTracker
	}
	// identityStream passes the caller's identity to a stream handler
	identityStream struct {
		grpc.ServerStream
		ctx context.Context
		// removed, if set, reports whether the caller was removed from the cluster: its stream messages are rejected
		removed func() bool
	}
	// connTracker holds the open connections of the node port with the common name of their client certificate, so
	// the connections of a removed node can be closed
	connTracker struct {
		mu    sync.Mutex
		conns map[*trackedConn]struct{}
	}
	// trackedConn is a connection of the node port. raw is the TCP connection under TLS: closing it ends the
	// connection at once, without the TLS close alert, which may block.
	trackedConn struct {
		net.Conn
		raw     net.Conn
		id      string
		tracker *connTracker
		// expiry closes the connection once the client certificate expired
		expiry *time.Timer
		once   sync.Once
	}
	// trackingCredentials are the server credentials of the node port: TLS, and tracking the connections
	trackingCredentials struct {
		credentials.TransportCredentials
		tracker *connTracker
	}
)

func (i identity) has(roles ...string) bool {
	return slices.ContainsFunc(roles, func(r string) bool { return slices.Contains(i.roles, r) })
}

func identityOf(cert *x509.Certificate) identity {
	return identity{id: cert.Subject.CommonName, roles: cert.Subject.OrganizationalUnit}
}

func (s *identityStream) Context() context.Context {
	return s.ctx
}

// RecvMsg receives the next message of the stream. A caller removed from the cluster meanwhile gets PermissionDenied,
// so the handler ends the stream.
func (s *identityStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if s.removed != nil && s.removed() {
		return status.Error(codes.PermissionDenied, "the node was removed from the cluster")
	}
	return nil
}

func (c *trackingCredentials) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, auth, err := c.TransportCredentials.ServerHandshake(raw)
	if err != nil {
		return nil, nil, err
	}
	info, ok := auth.(credentials.TLSInfo)
	if !ok || len(info.State.PeerCertificates) == 0 {
		return conn, auth, nil
	}
	leaf := info.State.PeerCertificates[0]
	tc := &trackedConn{Conn: conn, raw: raw, id: leaf.Subject.CommonName, tracker: c.tracker}
	// an expired certificate ends the connection, its long-lived streams too
	tc.expiry = time.AfterFunc(time.Until(leaf.NotAfter), func() { _ = raw.Close() })
	c.tracker.add(tc)
	return tc, auth, nil
}

func (c *trackingCredentials) Clone() credentials.TransportCredentials {
	return &trackingCredentials{TransportCredentials: c.TransportCredentials.Clone(), tracker: c.tracker}
}

func (c *trackedConn) Close() error {
	c.once.Do(func() {
		c.expiry.Stop()
		c.tracker.remove(c)
	})
	return c.Conn.Close()
}

func (t *connTracker) add(c *trackedConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conns == nil {
		t.conns = make(map[*trackedConn]struct{})
	}
	t.conns[c] = struct{}{}
}

func (t *connTracker) remove(c *trackedConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.conns, c)
}

// close closes the connections of the id. gRPC ends their rpcs and streams and closes them on its side too.
func (t *connTracker) close(id string) {
	t.mu.Lock()
	var raws []net.Conn
	for c := range t.conns {
		if c.id == id {
			raws = append(raws, c.raw)
		}
	}
	t.mu.Unlock()
	for _, raw := range raws {
		_ = raw.Close()
	}
}

// LoadTLSFiles loads the CA certificate, the certificate and its key from PEM files, for WithTLS.
func LoadTLSFiles(caFile, certFile, keyFile string) (*x509.CertPool, tls.Certificate, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	ca := x509.NewCertPool()
	if !ca.AppendCertsFromPEM(caPEM) {
		return nil, tls.Certificate{}, fmt.Errorf("no certificate in %s", caFile)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, tls.Certificate{}, err
	}
	return ca, cert, nil
}

// ToolCredentials are the transport credentials of a tool (eg. a dev tool with the observer role, or an admin
// tool) calling nodes of a cluster with WithTLS: it presents its certificate and accepts nodes with a certificate
// of the CA and the member or client role.
func ToolCredentials(ca *x509.CertPool, cert tls.Certificate) credentials.TransportCredentials {
	t := &nodeTLS{ca: ca, cert: cert}
	return credentials.NewTLS(t.dialConfig(func(id identity) error {
		if !id.has(RoleMember, RoleClient) {
			return fmt.Errorf("%s is not a node of the cluster", id.id)
		}
		return nil
	}))
}

// certificateIdentity returns the identity in the node's own certificate.
func (t *nodeTLS) identity() (identity, error) {
	if len(t.cert.Certificate) == 0 {
		return identity{}, errors.New("no certificate")
	}
	leaf := t.cert.Leaf
	if leaf == nil {
		var err error
		if leaf, err = x509.ParseCertificate(t.cert.Certificate[0]); err != nil {
			return identity{}, err
		}
	}
	return identityOf(leaf), nil
}

func (t *nodeTLS) serverConfig() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{t.cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    t.ca,
		MinVersion:   tls.VersionTLS13,
	}
}

// dialConfig verifies the server's certificate against the CA and checks its identity with expect. Nodes are
// identified by their certificate, not by their address, so the host name isn't verified.
func (t *nodeTLS) dialConfig(expect func(identity) error) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{t.cert},
		MinVersion:   tls.VersionTLS13,
		// the standard verification checks the host name; VerifyConnection checks the chain and the identity instead
		InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("the server sent no certificate")
			}
			intermediates := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				intermediates.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{
				Roots:         t.ca,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			})
			if err != nil {
				return err
			}
			if err := expect(identityOf(cs.PeerCertificates[0])); err != nil {
				return err
			}
			if t.certCheck != nil {
				if err := t.certCheck(cs.PeerCertificates[0]); err != nil {
					return fmt.Errorf("the certificate of %s is refused: %w", cs.PeerCertificates[0].Subject.CommonName, err)
				}
			}
			return nil
		},
	}
}

// setSecurityOptions takes the TLS options. The metrics endpoint needs them, so they are set before it starts.
func (n *Node[T, K]) setSecurityOptions(options *nodeSetupOptions) {
	if options.tls == nil {
		return
	}
	options.tls.certCheck = options.certCheck
	options.tls.maxConnAge = defaultMaxConnectionAge
	if options.maxConnAge != nil {
		options.tls.maxConnAge = max(*options.maxConnAge, 0)
	}
	n.tls = options.tls
}

// checkOwnCertificate fails the start of a node whose certificate doesn't name it with the role of its mode.
func (n *Node[T, K]) checkOwnCertificate() error {
	if n.tls == nil {
		return nil
	}
	id, err := n.tls.identity()
	if err != nil {
		return fmt.Errorf("the TLS certificate: %w", err)
	}
	if id.id != n.Id() {
		return fmt.Errorf("the TLS certificate is for node %q, not %s", id.id, n.Id())
	}
	role := RoleMember
	if n.client {
		role = RoleClient
	}
	if !id.has(role) {
		return fmt.Errorf("the TLS certificate of node %s lacks the role %s", n.Id(), role)
	}
	return nil
}

// serverOptions are the options of the node port: mutual TLS and the role checks, or nothing without TLS.
func (n *Node[T, K]) serverOptions() []grpc.ServerOption {
	if n.tls == nil {
		return []grpc.ServerOption{codecServerOption()}
	}
	opts := []grpc.ServerOption{
		codecServerOption(),
		grpc.Creds(&trackingCredentials{TransportCredentials: credentials.NewTLS(n.tls.serverConfig()), tracker: &n.tls.conns}),
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			ctx, err := n.authorizeMethod(ctx, info.FullMethod)
			if err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			ctx, err := n.authorizeMethod(ss.Context(), info.FullMethod)
			if err != nil {
				return err
			}
			s := &identityStream{ServerStream: ss, ctx: ctx}
			// a removed node's streams take no more messages. Not Attach: the member tells a removed client over that
			// stream, the client stops then.
			if id, _ := caller(ctx); info.FullMethod != service.Node_Attach_FullMethodName {
				if nodeId, ok := nodeIdOf(id.id); ok {
					s.removed = func() bool { return n.isRemoved(nodeId) }
				}
			}
			return handler(srv, s)
		}),
	}
	// GOAWAY after the max age: clients open a new connection with a new handshake for their next rpcs. Streams on
	// the old connection go on (no grace limit), the expiry of the client certificate ends them (trackedConn).
	if n.tls.maxConnAge > 0 {
		opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: n.tls.maxConnAge}))
	}
	return opts
}

// authorizeMethod checks the caller's certificate and roles for the rpc and adds its identity to the context.
func (n *Node[T, K]) authorizeMethod(ctx context.Context, method string) (context.Context, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no peer")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.VerifiedChains[0]) == 0 {
		return nil, status.Error(codes.Unauthenticated, "no verified client certificate")
	}
	leaf := info.State.VerifiedChains[0][0]
	id := identityOf(leaf)
	roles, known := methodRoles[method]
	if !known || !id.has(roles...) {
		return nil, status.Errorf(codes.PermissionDenied, "%s (roles %v) may not call %s", id.id, id.roles, method)
	}
	// a node which left or was removed may join again with its id (Membership [U])
	if err := n.checkPeerCertificate(leaf, id, method == service.Node_Join_FullMethodName); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, identityKey{}, id), nil
}

// checkPeerCertificate checks a verified client certificate beyond its roles: it must not have expired (a connection
// outlives its handshake), its common name must not be a node id removed from the cluster (tools have no node id;
// joining is allowed: a removed id may join again), and the certificate check (WithCertificateCheck) must accept it.
func (n *Node[T, K]) checkPeerCertificate(leaf *x509.Certificate, id identity, joining bool) error {
	if time.Now().After(leaf.NotAfter) {
		return status.Errorf(codes.Unauthenticated, "the certificate of %s expired", id.id)
	}
	if nodeId, ok := nodeIdOf(id.id); ok && !joining && n.isRemoved(nodeId) {
		return status.Errorf(codes.PermissionDenied, "node %d was removed from the cluster", nodeId)
	}
	if n.tls.certCheck != nil {
		if err := n.tls.certCheck(leaf); err != nil {
			return status.Errorf(codes.PermissionDenied, "the certificate of %s is refused: %v", id.id, err)
		}
	}
	return nil
}

// closeRemovedConns closes the connections of a node removed from the cluster after removedConnGrace, so the answers
// to its rpcs in flight go out. Its new rpcs and stream messages are rejected meanwhile (authorizeMethod,
// identityStream).
func (n *Node[T, K]) closeRemovedConns(id uint64) {
	if n.tls == nil {
		return
	}
	time.AfterFunc(removedConnGrace, func() {
		if n.isRemoved(id) {
			n.tls.conns.close(strconv.FormatUint(id, 10))
		}
	})
}

// authorizeHTTP passes requests with a client certificate of the CA and one of the roles on to next. Without TLS it
// is next.
func (n *Node[T, K]) authorizeHTTP(next http.Handler, roles ...string) http.Handler {
	if n.tls == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
			http.Error(w, "no verified client certificate", http.StatusUnauthorized)
			return
		}
		leaf := r.TLS.VerifiedChains[0][0]
		id := identityOf(leaf)
		if !id.has(roles...) {
			http.Error(w, fmt.Sprintf("%s (roles %v) may not read %s", id.id, id.roles, r.URL.Path), http.StatusForbidden)
			return
		}
		if err := n.checkPeerCertificate(leaf, id, false); err != nil {
			code := http.StatusForbidden
			if status.Code(err) == codes.Unauthenticated {
				code = http.StatusUnauthorized
			}
			http.Error(w, status.Convert(err).Message(), code)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// caller returns the identity of the rpc's caller; false without TLS.
func caller(ctx context.Context) (identity, bool) {
	id, ok := ctx.Value(identityKey{}).(identity)
	return id, ok
}

// ownName checks that the caller acts in its own name: nodeId is the node the request claims to come from.
// Without TLS every request is accepted.
func ownName(ctx context.Context, nodeId string) error {
	id, ok := caller(ctx)
	if !ok || id.id == nodeId {
		return nil
	}
	return status.Errorf(codes.PermissionDenied, "%s may not act in the name of node %s", id.id, nodeId)
}

// checkProposal checks that every log entry a caller proposes is in its own name: the node id of the entry, and the
// tokens of lock entries are of that node. Client entries are proposed by members only, except the removal of a
// client by itself (Leave).
func checkProposal(ctx context.Context, logs []*service.NodeLog) error {
	id, ok := caller(ctx)
	if !ok {
		return nil
	}
	for _, l := range logs {
		if err := ownName(ctx, l.GetNodeId()); err != nil {
			return err
		}
		if cl := l.GetLock(); cl != nil {
			if tokenNode(cl.GetToken()) != id.id || (cl.GetJoin() != "" && tokenNode(cl.GetJoin()) != id.id) {
				return status.Errorf(codes.PermissionDenied, "%s may not use lock tokens of other nodes", id.id)
			}
		}
		if c := l.GetClient(); c != nil && !id.has(RoleMember) {
			leaving := l.GetType() == service.NodeEventType_CLIENT_REMOVE && strconv.FormatUint(c.GetId(), 10) == id.id
			if !leaving {
				return status.Errorf(codes.PermissionDenied, "%s may not add or remove clients", id.id)
			}
		}
	}
	return nil
}

// dialOptions return the options to dial a node: the vtCodec and the transport credentials. With TLS the server
// must be the node with the id, or any member if id is 0.
func (n *Node[T, K]) dialOptions(id uint64) []grpc.DialOption {
	return []grpc.DialOption{codecDialOption(), n.transportCredentials(id)}
}

func (n *Node[T, K]) transportCredentials(id uint64) grpc.DialOption {
	if n.tls == nil {
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(n.tls.dialConfig(func(got identity) error {
		switch {
		case id != 0 && got.id != strconv.FormatUint(id, 10):
			return fmt.Errorf("expected node %d, the server is %s", id, got.id)
		case id == 0 && !got.has(RoleMember):
			return fmt.Errorf("the server %s is not a member of the cluster", got.id)
		case id != 0 && !got.has(RoleMember, RoleClient):
			return fmt.Errorf("the server %s is not a node of the cluster", got.id)
		}
		return nil
	})))
}

type (
	// publicServer serves the Event rpc on the public listener, for callers outside the cluster. The other rpcs
	// are unimplemented there.
	publicServer struct {
		service.UnimplementedNodeServer
		event func(context.Context, *service.EventRequest) (*service.EventResponse, error)
	}
)

func (p *publicServer) Event(ctx context.Context, req *service.EventRequest) (*service.EventResponse, error) {
	return p.event(ctx, req)
}

// servePublic serves the Event rpc on the public listener. With TLS the node's certificate identifies the node, the
// callers need none.
func (n *Node[T, K]) servePublic(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	var opts []grpc.ServerOption
	if n.tls != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{n.tls.cert},
			MinVersion:   tls.VersionTLS12,
		})))
	}
	n.publicServer = grpc.NewServer(opts...)
	service.RegisterNodeServer(n.publicServer, &publicServer{event: n.Event})
	go func() {
		if err := n.publicServer.Serve(listener); err != nil {
			n.logger.Error(fmt.Sprintf("public listener on %s: %v", addr, err))
		}
	}()
	n.logger.Info(fmt.Sprintf("serving the Event rpc on %s", addr))
	return nil
}
