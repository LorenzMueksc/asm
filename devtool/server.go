// Package devtool is a web UI for looking into an ASM cluster: nodes, containers, transitions, the bitmap,
// container locks and events. It connects to one node, discovers the other nodes through its peers,
// inspects all of them periodically (Inspect rpc) and merges their event traces (WatchEvents rpc,
// nodes need WithEventTrace). It only reads.
package devtool

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	// the bitmap values in the inspect responses are wrapperspb messages packed in Any
	_ "google.golang.org/protobuf/types/known/wrapperspb"
	"rumue.io/asm/service"
)

const (
	inspectTimeout = 2 * time.Second
	// eventBuffer is the number of events kept for browsers connecting later
	eventBuffer = 5000
	// watchRetry is how long to wait before watching the events of a node again
	watchRetry = 2 * time.Second
	// traceDisabledRetry is how long to wait before asking a node without event trace again
	traceDisabledRetry = 15 * time.Second
)

type (
	Server struct {
		join string
		poll time.Duration
		// limit is the number of locks and containers an inspection returns per node, 0 returns all
		limit uint32
		// creds dial the nodes (WithCredentials)
		creds credentials.TransportCredentials
		// epoch tells the browsers which run of the server the event ids belong to
		epoch string

		mu    sync.Mutex
		nodes map[string]*nodeConn // by address
		// events holds the latest trace events of all nodes, oldest first; lastId is the id of the newest
		events []bufferedEvent
		lastId uint64
		subs   map[chan bufferedEvent]struct{}
	}
	// bufferedEvent is a trace event as JSON with the id browsers resume after (SSE id, Last-Event-ID)
	bufferedEvent struct {
		id  uint64
		raw []byte
	}
	nodeConn struct {
		addr   string
		conn   *grpc.ClientConn
		client service.NodeClient
		cancel context.CancelFunc

		// guarded by Server.mu
		id       string
		inspect  json.RawMessage
		err      string
		traceErr string
		// updated is the time of the last poll, inspected the time of the last successful one
		updated   time.Time
		inspected time.Time
		reachable bool
		// polled is set by the first poll; from then on only changes of reachable are logged
		polled     bool
		lastSeq    uint64
		inspection *service.InspectResponse
	}
	// nodeState is a node in the state served to the browser. An unreachable node keeps its last inspection
	// (taken at Inspected) next to the error.
	nodeState struct {
		Addr      string          `json:"addr"`
		Id        string          `json:"id,omitempty"`
		Error     string          `json:"error,omitempty"`
		TraceErr  string          `json:"traceError,omitempty"`
		Updated   time.Time       `json:"updated"`
		Inspected time.Time       `json:"inspected,omitzero"`
		Inspect   json.RawMessage `json:"inspect,omitempty"`
	}
)

var jsonOptions = protojson.MarshalOptions{}

// Option configures a Server.
type Option func(*Server)

// WithCredentials sets the transport credentials to dial the nodes, eg. asm.ToolCredentials with a certificate
// with the observer role for a cluster with TLS. Without it the nodes are dialed without TLS.
func WithCredentials(creds credentials.TransportCredentials) Option {
	return func(s *Server) {
		s.creds = creds
	}
}

// NewServer returns a server for the cluster of the node at join, inspecting the nodes every poll interval.
// An inspection returns at most limit locks and containers of a node, 0 returns all.
func NewServer(join string, poll time.Duration, limit uint32, opts ...Option) *Server {
	s := &Server{
		join:  join,
		poll:  poll,
		limit: limit,
		creds: insecure.NewCredentials(),
		epoch: strconv.FormatInt(time.Now().UnixNano(), 36),
		nodes: make(map[string]*nodeConn),
		subs:  make(map[chan bufferedEvent]struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Run inspects the nodes and watches their events until ctx is done.
func (s *Server) Run(ctx context.Context) {
	ticker := time.NewTicker(s.poll)
	defer ticker.Stop()

	for {
		s.refresh(ctx)

		select {
		case <-ctx.Done():
			s.mu.Lock()
			for _, n := range s.nodes {
				n.cancel()
				_ = n.conn.Close()
			}
			s.mu.Unlock()
			return
		case <-ticker.C:
		}
	}
}

// refresh inspects all known nodes and updates the nodes from their peers.
func (s *Server) refresh(ctx context.Context) {
	s.mu.Lock()
	if _, ok := s.nodes[s.join]; !ok {
		if err := s.addNodeLocked(ctx, s.join); err != nil {
			log.Printf("connecting to %s: %v", s.join, err)
		}
	}
	nodes := make([]*nodeConn, 0, len(s.nodes))
	for _, n := range s.nodes {
		nodes = append(nodes, n)
	}
	s.mu.Unlock()

	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.inspect(ctx, n)
		}()
	}
	wg.Wait()

	// the cluster is the join node and the peers every reachable node knows
	s.mu.Lock()
	defer s.mu.Unlock()
	peers := map[string]bool{s.join: true}
	for _, n := range s.nodes {
		if n.inspection == nil || !n.reachable {
			continue
		}
		for _, addr := range n.inspection.GetNode().GetPeers() {
			peers[addr] = true
		}
	}
	for addr := range peers {
		if _, ok := s.nodes[addr]; !ok {
			if err := s.addNodeLocked(ctx, addr); err != nil {
				log.Printf("connecting to %s: %v", addr, err)
			}
		}
	}
	for addr, n := range s.nodes {
		// a node nobody knows anymore, which doesn't answer, left the cluster
		if !peers[addr] && !n.reachable {
			log.Printf("%s left the cluster", n.nameLocked())
			n.cancel()
			_ = n.conn.Close()
			delete(s.nodes, addr)
		}
	}
}

func (s *Server) addNodeLocked(ctx context.Context, addr string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(s.creds))
	if err != nil {
		return err
	}
	nodeCtx, cancel := context.WithCancel(ctx)
	n := &nodeConn{addr: addr, conn: conn, client: service.NewNodeClient(conn), cancel: cancel}
	s.nodes[addr] = n
	go s.watch(nodeCtx, n)
	return nil
}

func (s *Server) inspect(ctx context.Context, n *nodeConn) {
	ctx, cancel := context.WithTimeout(ctx, inspectTimeout)
	defer cancel()

	resp, err := n.client.Inspect(ctx, &service.InspectRequest{Limit: s.limit})
	var raw []byte
	if err == nil {
		raw, err = jsonOptions.Marshal(resp)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	n.updated = time.Now()
	// log the first poll and every change between reachable and unreachable, not every failed poll
	logIt := !n.polled || n.reachable != (err == nil)
	n.polled = true
	if err != nil {
		// the last inspection stays, the browser shows it as stale
		n.err = status.Convert(err).Message()
		n.reachable = false
		if logIt {
			log.Printf("%s unreachable: %s", n.nameLocked(), n.err)
		}
		return
	}
	n.err = ""
	n.reachable = true
	n.inspection = resp
	n.inspect = raw
	n.inspected = n.updated
	n.id = resp.GetNode().GetId()
	if logIt {
		log.Printf("%s reachable", n.nameLocked())
	}
}

// nameLocked names the node in log messages: its id once known and its address.
func (n *nodeConn) nameLocked() string {
	if n.id == "" {
		return "node " + n.addr
	}
	return fmt.Sprintf("node %s (%s)", n.id, n.addr)
}

// setTraceErrLocked sets the error of watching the events of the node and logs it when it changes.
func (n *nodeConn) setTraceErrLocked(msg string) {
	if msg == n.traceErr {
		return
	}
	if msg == "" {
		log.Printf("%s: receiving events", n.nameLocked())
	} else {
		log.Printf("%s: watching events: %s", n.nameLocked(), msg)
	}
	n.traceErr = msg
}

// watch streams the event trace of the node, reconnecting after errors, until ctx is done.
func (s *Server) watch(ctx context.Context, n *nodeConn) {
	for ctx.Err() == nil {
		s.mu.Lock()
		after := n.lastSeq
		s.mu.Unlock()

		err := s.stream(ctx, n, after)
		retry := watchRetry
		s.mu.Lock()
		switch {
		case ctx.Err() != nil:
			// the node left the cluster or the server stops: nothing to log
			n.traceErr = ""
		case err == nil:
			n.setTraceErrLocked("")
		case status.Code(err) == codes.FailedPrecondition:
			n.setTraceErrLocked(status.Convert(err).Message())
			retry = traceDisabledRetry
		default:
			n.setTraceErrLocked(status.Convert(err).Message())
		}
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
	}
}

func (s *Server) stream(ctx context.Context, n *nodeConn, after uint64) error {
	stream, err := n.client.WatchEvents(ctx, &service.WatchEventsRequest{AfterSeq: after})
	if err != nil {
		return err
	}
	for {
		e, err := stream.Recv()
		if err != nil {
			return err
		}

		s.mu.Lock()
		n.lastSeq = e.GetSeq()
		n.setTraceErrLocked("")
		s.mu.Unlock()

		s.publish(e)
	}
}

// publish keeps the event with the next id and sends it to the browsers.
func (s *Server) publish(e *service.TraceEvent) {
	raw, err := jsonOptions.Marshal(e)
	if err != nil {
		log.Printf("encoding event: %v", err)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastId++
	be := bufferedEvent{id: s.lastId, raw: raw}
	s.events = append(s.events, be)
	if len(s.events) > eventBuffer {
		s.events = slices.Clone(s.events[len(s.events)-eventBuffer:])
	}
	for sub := range s.subs {
		select {
		case sub <- be:
		default:
		}
	}
}

// Handler serves the web UI and its API:
// GET /api/state returns all nodes with their latest inspection, GET /api/events streams the events
// of all nodes as server-sent events, starting with the buffered ones. Other /api paths are not found.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	// the API only reads; without these the SPA fallback would answer API requests with index.html
	mux.HandleFunc("/api/state", methodNotAllowed)
	mux.HandleFunc("/api/events", methodNotAllowed)
	mux.Handle("/api/", http.NotFoundHandler())
	mux.Handle("/", spaHandler())
	return mux
}

func methodNotAllowed(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Allow", "GET, HEAD")
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func (s *Server) handleState(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	nodes := make([]nodeState, 0, len(s.nodes))
	for _, n := range s.nodes {
		nodes = append(nodes, nodeState{
			Addr: n.addr, Id: n.id, Error: n.err, TraceErr: n.traceErr, Updated: n.updated, Inspected: n.inspected,
			Inspect: n.inspect,
		})
	}
	s.mu.Unlock()
	slices.SortFunc(nodes, func(a, b nodeState) int { return strings.Compare(a.Addr, b.Addr) })

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"join": s.join, "nodes": nodes, "time": time.Now()})
}

// handleEvents streams the events as server-sent events with the id "<epoch>-<counter>". A browser that
// reconnects sends the id of the last event it got (Last-Event-ID) and gets only newer ones. If that id is
// of another epoch (asmdev restarted) or unknown, it gets a "reset" event first and then all buffered events.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")

	ch := make(chan bufferedEvent, eventBuffer)
	s.mu.Lock()
	after, resumed := s.resumeAfterLocked(r.Header.Get("Last-Event-ID"))
	// the buffer is ordered by id; reading it and subscribing under one lock sends no event twice
	start, _ := slices.BinarySearchFunc(s.events, after+1, func(e bufferedEvent, id uint64) int {
		return cmp.Compare(e.id, id)
	})
	buffered := slices.Clone(s.events[start:])
	s.subs[ch] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.subs, ch)
		s.mu.Unlock()
	}()

	if !resumed {
		if _, err := fmt.Fprintf(w, "event: reset\ndata: %s\n\n", s.epoch); err != nil {
			return
		}
	}
	for _, e := range buffered {
		if err := s.writeEvent(w, e); err != nil {
			return
		}
	}
	flusher.Flush()

	keepAlive := time.NewTicker(15 * time.Second)
	defer keepAlive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case e := <-ch:
			if err := s.writeEvent(w, e); err != nil {
				return
			}
			flusher.Flush()
		case <-keepAlive.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) writeEvent(w http.ResponseWriter, e bufferedEvent) error {
	_, err := fmt.Fprintf(w, "id: %s-%d\ndata: %s\n\n", s.epoch, e.id, e.raw)
	return err
}

// resumeAfterLocked returns the id of the last event a browser got, from its Last-Event-ID header. It returns
// false if the header names an event of another epoch or one not sent yet: the browser drops its events.
// Without the header (a new connection) it returns 0 and true.
func (s *Server) resumeAfterLocked(lastEventId string) (uint64, bool) {
	if lastEventId == "" {
		return 0, true
	}
	epoch, counter, ok := strings.Cut(lastEventId, "-")
	if !ok || epoch != s.epoch {
		return 0, false
	}
	id, err := strconv.ParseUint(counter, 10, 64)
	if err != nil || id > s.lastId {
		return 0, false
	}
	return id, true
}

// spaHandler serves the built web UI, falling back to index.html for client side routes.
func spaHandler() http.Handler {
	dist, err := fs.Sub(webDist, "web/dist")
	if err != nil {
		panic(err)
	}
	files := http.FileServerFS(dist)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path != "" {
			if _, err := fs.Stat(dist, path); errors.Is(err, fs.ErrNotExist) {
				r = r.Clone(r.Context())
				r.URL.Path = "/"
			}
		}
		files.ServeHTTP(w, r)
	})
}
