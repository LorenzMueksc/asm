package asm

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"rumue.io/asm/service"
)

// The event trace keeps the last entries of a node for dev tools (WithEventTrace): events processed by its
// containers, events forwarded to other nodes or received from them, container errors, and containers
// starting and closing. WatchEvents streams it. Without WithEventTrace nothing is recorded.

type (
	eventTrace struct {
		mu       sync.Mutex
		capacity int
		// entries is a ring buffer, next is the position of the oldest entry once it is full
		entries []*service.TraceEvent
		next    int
		seq     uint64
		subs    map[chan *service.TraceEvent]struct{}
	}
	// traceRecord is what a container reports to the event trace of its node
	traceRecord struct {
		kind        service.TraceEvent_Kind
		event       string
		payloads    []any
		transitions []string
		remote      []string
		excluded    []string
		from        string
		routes      []*service.TraceRoute
		wait        bool
		err         error
		duration    time.Duration
		spanId      string
		traces      []*service.TraceRef
	}
)

// traceSubscriberBuffer is the number of entries a watcher may lag behind, it misses entries beyond that.
const traceSubscriberBuffer = 1024

func newEventTrace(capacity int) *eventTrace {
	return &eventTrace{
		capacity: capacity,
		entries:  make([]*service.TraceEvent, 0, capacity),
		subs:     make(map[chan *service.TraceEvent]struct{}),
	}
}

func (tr *eventTrace) add(e *service.TraceEvent) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	tr.seq++
	e.Seq = tr.seq
	if len(tr.entries) < tr.capacity {
		tr.entries = append(tr.entries, e)
	} else {
		tr.entries[tr.next] = e
		tr.next = (tr.next + 1) % tr.capacity
	}

	for sub := range tr.subs {
		select {
		case sub <- e:
		default:
		}
	}
}

// since returns the entries with a sequence number higher than after, oldest first.
func (tr *eventTrace) since(after uint64) []*service.TraceEvent {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	ordered := append(append([]*service.TraceEvent(nil), tr.entries[tr.next:]...), tr.entries[:tr.next]...)
	result := make([]*service.TraceEvent, 0, len(ordered))
	for _, e := range ordered {
		if e.Seq > after {
			result = append(result, e)
		}
	}
	return result
}

func (tr *eventTrace) lastSeq() uint64 {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	return tr.seq
}

func (tr *eventTrace) subscribe() chan *service.TraceEvent {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	ch := make(chan *service.TraceEvent, traceSubscriberBuffer)
	tr.subs[ch] = struct{}{}
	return ch
}

func (tr *eventTrace) unsubscribe(ch chan *service.TraceEvent) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	delete(tr.subs, ch)
}

// recordTrace adds an entry of a container to the event trace, if it is enabled.
func (n *Node[T, K]) recordTrace(containerId string, r traceRecord) {
	if n.trace == nil {
		return
	}

	e := &service.TraceEvent{
		Ts:                timestamppb.Now(),
		NodeId:            n.Id(),
		Kind:              r.kind,
		ContainerId:       containerId,
		EventName:         r.event,
		Transitions:       r.transitions,
		RemoteTransitions: r.remote,
		Excluded:          r.excluded,
		FromNode:          r.from,
		Routes:            r.routes,
		Wait:              r.wait,
		SpanId:            r.spanId,
		Traces:            r.traces,
	}
	for _, p := range r.payloads {
		typ, js := toJSON(p)
		e.Payloads = append(e.Payloads, &service.TracePayload{Type: typ, Json: js})
	}
	if r.err != nil {
		e.Error = r.err.Error()
	}
	if r.duration > 0 {
		e.Duration = durationpb.New(r.duration)
	}
	n.trace.add(e)
}

// newTraceId returns a random id for a trace or a span.
func newTraceId() string {
	return strconv.FormatUint(rand.Uint64(), 16)
}

// traceRefs returns the traces of the events with their spans as parents, every trace once.
func traceRefs(events []*containerEvent) []*service.TraceRef {
	var refs []*service.TraceRef
	for _, evt := range events {
		if evt.trace == nil || slices.ContainsFunc(refs, func(r *service.TraceRef) bool { return r.GetTraceId() == evt.trace.GetTraceId() }) {
			continue
		}
		refs = append(refs, &service.TraceRef{TraceId: evt.trace.GetTraceId(), ParentSpanId: evt.spanOrParent()})
	}
	return refs
}

// toJSON returns the type and the JSON of a value for dev tools: protobuf messages (also packed in an Any)
// are encoded with protojson, other values with encoding/json.
func toJSON(v any) (string, string) {
	if isNilValue(v) {
		return fmt.Sprintf("%T", v), "null"
	}
	if a, ok := v.(*anypb.Any); ok {
		m, err := a.UnmarshalNew()
		if err != nil {
			return a.GetTypeUrl(), strconv.Quote(err.Error())
		}
		v = m
	}
	// fake messages of tests return no reflection
	if m, ok := v.(proto.Message); ok && m.ProtoReflect() != nil {
		if b, err := protojson.Marshal(m); err == nil {
			return string(m.ProtoReflect().Descriptor().FullName()), string(b)
		}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%T", v), strconv.Quote(fmt.Sprintf("%+v", v))
	}
	return fmt.Sprintf("%T", v), string(b)
}

// WatchEvents streams the event trace: the entries after the requested sequence number, then new ones.
// A watcher which falls too far behind misses entries.
func (n *Node[T, K]) WatchEvents(req *service.WatchEventsRequest, stream grpc.ServerStreamingServer[service.TraceEvent]) error {
	if n.trace == nil {
		return status.Error(codes.FailedPrecondition, "the event trace is disabled, enable it with WithEventTrace")
	}

	// subscribe before reading the buffer, so no entry is lost in between
	ch := n.trace.subscribe()
	defer n.trace.unsubscribe(ch)

	last := req.GetAfterSeq()
	// a watcher ahead of the trace knew this node before it restarted, it gets the whole trace
	if last > n.trace.lastSeq() {
		last = 0
	}
	for _, e := range n.trace.since(last) {
		if err := stream.Send(e); err != nil {
			return err
		}
		last = e.Seq
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-n.stopC:
			return nil
		case e := <-ch:
			if e.Seq <= last {
				continue
			}
			if err := stream.Send(e); err != nil {
				return err
			}
			last = e.Seq
		}
	}
}
