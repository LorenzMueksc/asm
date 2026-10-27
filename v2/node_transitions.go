package asm

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"rumue.io/asm/service"
)

// transitionRegistry holds the transitions of a node.
// Local transitions are registered on this node, remote ones are synced from other nodes.
// Containers read the published list, which is replaced as a whole on every change.
type transitionRegistry struct {
	mu sync.Mutex
	// local transitions in registration order
	local []*Transition
	// remote transitions synced from other nodes, by name
	remote map[string]*Transition
	// compiled is set once the committed bitmap is ready and the local transitions are compiled
	compiled bool

	published atomic.Pointer[publishedTransitions]
}

// publishedTransitions are the transitions containers use, replaced as a whole on every change: all by priority,
// and the same ones by event name, so an event only looks at its own.
type publishedTransitions struct {
	all     []*Transition
	byEvent map[string][]*Transition
}

func newPublishedTransitions(ts []*Transition) *publishedTransitions {
	p := &publishedTransitions{all: ts, byEvent: make(map[string][]*Transition)}
	for _, t := range ts {
		p.byEvent[t.EventName] = append(p.byEvent[t.EventName], t)
	}
	return p
}

// list returns all transitions by priority, nil before any were published.
func (p *publishedTransitions) list() []*Transition {
	if p == nil {
		return nil
	}
	return p.all
}

// forEvent returns the transitions of the event by priority.
func (p *publishedTransitions) forEvent(eventName string) []*Transition {
	if p == nil {
		return nil
	}
	return p.byEvent[eventName]
}

const syncTimeout = 5 * time.Second

// addLocalTransition registers a transition of this node.
// Once the node is compiled, it is compiled immediately and pushed to all peers.
func (n *Node[T, K]) addLocalTransition(t *Transition) {
	n.registry.mu.Lock()
	n.registry.local = append(n.registry.local, t)
	if !n.registry.compiled {
		n.registry.mu.Unlock()
		return
	}

	if err := n.compileTransitionLocked(t); err != nil {
		n.registry.mu.Unlock()
		n.fail(err)
		return
	}
	n.publishLocked()
	pt := t.toProto(n.Id())
	n.registry.mu.Unlock()

	go n.pushTransitions([]*service.Transition{pt})
}

// compileTransitions compiles all local transitions against the committed bitmap and publishes them.
func (n *Node[T, K]) compileTransitions() error {
	n.registry.mu.Lock()
	defer n.registry.mu.Unlock()

	for _, t := range n.registry.local {
		if err := n.compileTransitionLocked(t); err != nil {
			return err
		}
	}
	n.registry.compiled = true
	n.publishLocked()

	return nil
}

// compileTransitionLocked converts the example entities of the transition to bits
// against the committed bitmap, computes its signature and checks it for conflicts.
func (n *Node[T, K]) compileTransitionLocked(t *Transition) error {
	layout := n.layout.Load()

	for _, c := range t.Conditions {
		if c.entities != nil {
			c.States = n.evaluateEntities(c.entities, layout)
		}
	}

	if t.toState != nil {
		bm := n.getBitmapByTypeF(t.toState, layout)
		if bm == nil {
			return fmt.Errorf("transition %s: bitmap for type %T not found", t.Name, t.toState)
		}
		t.Actions = []any{EvaluateEntityProperties(t.toState, bm)}
		t.stateBitmap = bm.Name
	}

	t.Signature = t.computeSignature()
	t.Entity = n.entityName()

	for _, other := range n.registry.local {
		if other != t && other.Name == t.Name && other.Signature != "" && other.Signature != t.Signature {
			return fmt.Errorf("transition %s is registered twice with different signatures", t.Name)
		}
	}
	if r, ok := n.registry.remote[t.Name]; ok && r.Signature != t.Signature {
		return fmt.Errorf("transition %s conflicts with the transition of nodes %v: different signature", t.Name, r.NodeIds)
	}

	return nil
}

// recompileTransitions compiles the local transitions again after the committed bitmap changed
// and pushes those whose compiled bits changed.
func (n *Node[T, K]) recompileTransitions() {
	n.registry.mu.Lock()
	if !n.registry.compiled {
		n.registry.mu.Unlock()
		return
	}

	changed := make([]*service.Transition, 0)
	for _, t := range n.registry.local {
		before := t.Signature + "/" + t.compiledKey()
		if err := n.compileTransitionLocked(t); err != nil {
			n.registry.mu.Unlock()
			n.fail(err)
			return
		}
		// the signature stays the same when the bitmap grows, but the other nodes need the new bits
		if t.Signature+"/"+t.compiledKey() != before {
			changed = append(changed, t.toProto(n.Id()))
		}
	}
	n.publishLocked()
	n.registry.mu.Unlock()

	if len(changed) > 0 {
		n.pushTransitions(changed)
	}
}

// localTransitionsLocked returns the local transitions without duplicate names.
func (n *Node[T, K]) localTransitionsLocked() []*Transition {
	ts := make([]*Transition, 0, len(n.registry.local))
	for _, t := range n.registry.local {
		if !slices.ContainsFunc(ts, func(o *Transition) bool { return o.Name == t.Name }) {
			ts = append(ts, t)
		}
	}
	return ts
}

// publishLocked replaces the transitions containers see with the merged local and remote transitions.
func (n *Node[T, K]) publishLocked() {
	ts := make([]*Transition, 0, len(n.registry.local)+len(n.registry.remote))
	for _, t := range n.localTransitionsLocked() {
		cp := copyTransition(t)
		cp.Remote = false
		cp.NodeIds = []string{n.Id()}
		if r, ok := n.registry.remote[t.Name]; ok && r.Signature == t.Signature {
			cp.NodeIds = append(cp.NodeIds, r.NodeIds...)
		}
		ts = append(ts, cp)
	}

	names := make([]string, 0, len(n.registry.remote))
	for name := range n.registry.remote {
		names = append(names, name)
	}
	slices.Sort(names)
	entity := n.entityName()
	for _, name := range names {
		if slices.ContainsFunc(ts, func(t *Transition) bool { return t.Name == name }) {
			continue
		}
		cp := copyTransition(n.registry.remote[name])
		// a state transition of another entity can't run on this node's containers, its owners run it
		if cp.Entity != "" && entity != "" && cp.Entity != entity {
			cp.Remote = true
		}
		ts = append(ts, cp)
	}

	sort.SliceStable(ts, func(i, j int) bool {
		return ts[i].Priority > ts[j].Priority
	})

	n.registry.published.Store(newPublishedTransitions(ts))
}

// entityName returns the name of the root entity bitmap of T in the committed layout, empty before it is committed.
func (n *Node[T, K]) entityName() string {
	if name := n.entity.Load(); name != nil {
		return *name
	}

	// a pointer to a new value, so the func mapping types to bitmaps doesn't get a nil pointer
	var entity any
	if rt := reflect.TypeFor[T](); rt.Kind() == reflect.Pointer {
		entity = reflect.New(rt.Elem()).Interface()
	} else {
		var zero T
		entity = zero
	}
	bm := n.getBitmapByTypeF(entity, n.layout.Load())
	if bm == nil {
		return ""
	}
	name := bm.Name
	n.entity.Store(&name)
	return name
}

// copyTransition copies a transition, so containers never see later changes.
func copyTransition(t *Transition) *Transition {
	cp := *t
	cp.NodeIds = slices.Clone(t.NodeIds)
	cp.Actions = slices.Clone(t.Actions)
	cp.Conditions = make([]*Condition, 0, len(t.Conditions))
	for _, c := range t.Conditions {
		states := make(map[string][]Bits, len(c.States))
		for name, list := range c.States {
			states[name] = slices.Clone(list)
		}
		cp.Conditions = append(cp.Conditions, &Condition{States: states, Operator: c.Operator, entities: c.entities})
	}
	return &cp
}

// mergeRemoteLocked merges a transition synced from another node.
// Same name and signature adds the node as owner, a different signature is a conflict.
// With dryRun only the conflict check runs.
func (n *Node[T, K]) mergeRemoteLocked(pt *service.Transition, dryRun bool) error {
	owner := pt.GetNodeId()
	if owner == n.Id() {
		return nil
	}

	t := transitionFromProto(pt)

	for _, local := range n.localTransitionsLocked() {
		if local.Name == t.Name && n.registry.compiled && local.Signature != t.Signature {
			return fmt.Errorf("transition %s of node %s conflicts with the transition of node %s: different signature", t.Name, owner, n.Id())
		}
	}

	existing, ok := n.registry.remote[t.Name]
	if !ok {
		if !dryRun {
			n.registry.remote[t.Name] = t
		}
		return nil
	}

	// a node sending a transition again replaces its previous one
	others := slices.DeleteFunc(slices.Clone(existing.NodeIds), func(id string) bool { return id == owner })
	if len(others) > 0 && existing.Signature != t.Signature {
		return fmt.Errorf("transition %s of node %s conflicts with the transition of nodes %v: different signature", t.Name, owner, others)
	}
	if dryRun {
		return nil
	}

	// the newest push carries the bits compiled against the newest bitmap
	t.NodeIds = append(others, owner)
	n.registry.remote[t.Name] = t
	return nil
}

// addRemoteTransitions merges transitions synced from another node. Either all or none are merged.
func (n *Node[T, K]) addRemoteTransitions(pts []*service.Transition) error {
	n.registry.mu.Lock()
	defer n.registry.mu.Unlock()

	for _, pt := range pts {
		if err := n.mergeRemoteLocked(pt, true); err != nil {
			return err
		}
	}
	for _, pt := range pts {
		if err := n.mergeRemoteLocked(pt, false); err != nil {
			return err
		}
	}
	n.publishLocked()

	return nil
}

// removeTransitionOwner removes a node which left the cluster from the owners of the synced transitions.
func (n *Node[T, K]) removeTransitionOwner(nodeId string) {
	n.registry.mu.Lock()
	defer n.registry.mu.Unlock()

	for name, t := range n.registry.remote {
		t.NodeIds = slices.DeleteFunc(t.NodeIds, func(id string) bool { return id == nodeId })
		if len(t.NodeIds) == 0 {
			delete(n.registry.remote, name)
		}
	}
	n.publishLocked()
}

// localTransitionProtos returns the compiled local transitions in their wire format.
func (n *Node[T, K]) localTransitionProtos() []*service.Transition {
	n.registry.mu.Lock()
	defer n.registry.mu.Unlock()

	if !n.registry.compiled {
		return nil
	}

	pts := make([]*service.Transition, 0, len(n.registry.local))
	for _, t := range n.localTransitionsLocked() {
		pts = append(pts, t.toProto(n.Id()))
	}
	return pts
}

// syncTransitions pulls the transitions of every peer and pushes the local ones to every peer.
// A conflict fails this node.
func (n *Node[T, K]) syncTransitions() error {
	for id, client := range n.peerClientsSnapshot() {
		ctx, cancel := context.WithTimeout(context.Background(), syncTimeout)
		resp, err := client.GetTransitions(ctx, &service.GetTransitionsRequest{})
		cancel()
		if err != nil {
			n.logger.Warn(fmt.Sprintf("failed to get transitions of node %d: %v", id, err))
			continue
		}
		// a node only answers with its own transitions
		owner := strconv.FormatUint(id, 10)
		pts := slices.DeleteFunc(resp.GetTransitions(), func(t *service.Transition) bool {
			if t.GetNodeId() != owner {
				n.logger.Warn(fmt.Sprintf("ignoring transition %s of node %s sent by node %d", t.GetName(), t.GetNodeId(), id))
				return true
			}
			return false
		})
		if err := n.addRemoteTransitions(pts); err != nil {
			return err
		}
		n.logger.Info(fmt.Sprintf("got %d transitions of node %d", len(pts), id))
	}

	return n.pushTransitionsTo(n.localTransitionProtos())
}

// pushTransitions pushes transitions to every peer. A conflict fails this node.
func (n *Node[T, K]) pushTransitions(pts []*service.Transition) {
	if err := n.pushTransitionsTo(pts); err != nil {
		n.fail(err)
	}
}

func (n *Node[T, K]) pushTransitionsTo(pts []*service.Transition) error {
	if len(pts) == 0 {
		return nil
	}

	for id, client := range n.peerClientsSnapshot() {
		ctx, cancel := context.WithTimeout(context.Background(), syncTimeout)
		_, err := client.AddTransitions(ctx, &service.AddTransitionsRequest{Transitions: pts})
		cancel()
		if status.Code(err) == codes.FailedPrecondition {
			return fmt.Errorf("node %d rejected transitions: %w", id, err)
		}
		if err != nil {
			n.logger.Warn(fmt.Sprintf("failed to push transitions to node %d: %v", id, err))
			continue
		}
		n.logger.Info(fmt.Sprintf("pushed %d transitions to node %d", len(pts), id))
	}
	return nil
}

// forward routes events to the owners of the remote transitions they matched and returns the entities
// the receivers returned (with wait). The receivers decide which of their transitions run.
//
// The matched transitions are covered with as few nodes as possible: the node owning most of the uncovered
// ones is picked first, ties are broken randomly, so transitions owned by several nodes are balanced.
// Only nodes owning a matched transition receive the events.
// Every request excludes the func transitions of the event which run on this node or are assigned
// to another request, so no transition runs twice.
// Unreachable nodes are skipped and their transitions covered again by the other owners.
// With wait the lock is lent: the chain of lenders gets this node appended, so a node running on a lent lock lends it on.
// Without wait the work is handed over: the requests are sent without waiting for the accept and kept until the
// receivers ran them (at most WithMaxPendingHandOvers, sending more waits for a free slot), a hand-over failing later
// is handled by handOverFailed.
func (n *Node[T, K]) forward(ctx context.Context, eventName, containerId string, chain, waitPath, matched []string, entity *anypb.Any, payloads []*anypb.Any, wait bool, trace *forwardTrace, handOverErrF func(error)) ([]forwardResult, error) {
	return n.forwardTo(ctx, eventName, containerId, chain, waitPath, matched, entity, payloads, wait, trace, handOverErrF, handOverRetry{})
}

// handOverRetry is the attempt of a hand-over sent again, the node it failed on, which is skipped, and the id of the
// hand-over. The id stays the same over attempts; every request gets it with its transitions appended, so a receiver
// getting the same work again knows it.
type handOverRetry struct {
	attempt int
	skip    string
	id      string
}

// maxHandOverAttempts limits how often a hand-over is sent again after its owner turned out unreachable
const maxHandOverAttempts = 3

// handOverId returns the id of the hand-over of the transitions: the id of the forward with the sorted names.
func handOverId(id string, transitions []string) string {
	return id + "/" + strings.Join(slices.Sorted(slices.Values(transitions)), ",")
}

func (n *Node[T, K]) forwardTo(ctx context.Context, eventName, containerId string, chain, waitPath, matched []string, entity *anypb.Any, payloads []*anypb.Any, wait bool, trace *forwardTrace, handOverErrF func(error), retry handOverRetry) ([]forwardResult, error) {
	if !wait && retry.id == "" {
		retry.id = n.newHandOverId()
	}
	// the func transitions of the event this node knows, its own and the remote ones
	remote := make(map[string]*Transition)
	all := make([]string, 0)
	for _, t := range n.registry.published.Load().forEvent(eventName) {
		if t.Remote {
			remote[t.Name] = t
			all = append(all, t.Name)
		} else if isOwnFuncTransition[T, K](t) {
			all = append(all, t.Name)
		}
	}

	pending := slices.DeleteFunc(slices.Clone(matched), func(name string) bool { return remote[name] == nil })

	start := time.Now()
	var routes []*service.TraceRoute
	var results []forwardResult
	var errs []error
	unreachable := make(map[string]bool)
	if retry.skip != "" {
		unreachable[retry.skip] = true
	}
	for len(pending) > 0 {
		assigned, order, uncovered := n.coverTransitions(remote, pending, unreachable)
		for _, name := range uncovered {
			errs = append(errs, fmt.Errorf("no reachable owner for transition %s", name))
		}
		type outcome struct {
			result *forwardResult
			err    error
		}
		outcomes := make([]outcome, len(order))
		var wg sync.WaitGroup
		for i, node := range order {
			req := &service.ForwardEventRequest{
				EventName:    eventName,
				ContainerId:  containerId,
				Entity:       entity,
				Payloads:     payloads,
				SenderNodeId: n.Id(),
				SenderEntity: n.entityName(),
				Wait:         wait,
				Traces:       trace.payloadRefs(),
				Exclude:      slices.DeleteFunc(slices.Clone(all), func(name string) bool { return slices.Contains(assigned[node], name) }),
			}
			if wait {
				req.LendChain = append(slices.Clone(chain), n.Id())
				req.WaitPath = waitPath
			} else {
				// handed over without waiting for the accept: only errors sending it come back here
				transitions := assigned[node]
				req.HandoverId = handOverId(retry.id, transitions)
				next := handOverRetry{attempt: retry.attempt + 1, skip: node, id: retry.id}
				err := n.sendHandOver(ctx, node, req, func(err error) {
					n.handOverFailed(eventName, containerId, transitions, payloads, trace, handOverErrF, next, err)
				})
				outcomes[i] = outcome{err: err}
				continue
			}
			// a single request runs on this goroutine, several ones in parallel
			if len(order) == 1 {
				result, err := n.sendForward(ctx, node, req)
				outcomes[i] = outcome{result: result, err: err}
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				result, err := n.sendForward(ctx, node, req)
				outcomes[i] = outcome{result: result, err: err}
			}()
		}
		wg.Wait()

		again := pending[:0:0]
		for i, o := range outcomes {
			node := order[i]
			if n.trace != nil {
				route := &service.TraceRoute{NodeId: node, Transitions: assigned[node]}
				if o.err != nil {
					route.Error = o.err.Error()
				}
				routes = append(routes, route)
			}
			switch {
			case o.err == nil:
				if o.result != nil {
					results = append(results, *o.result)
				}
			case status.Code(o.err) == codes.Unavailable:
				// cover the transitions of the node again with the other owners,
				// transitions nobody else owns are reported as uncovered
				debugf(n.logger, "node %s is unreachable, forwarding to other owners: %v", node, o.err)
				unreachable[node] = true
				again = append(again, assigned[node]...)
			default:
				errs = append(errs, fmt.Errorf("node %s: %w", node, o.err))
			}
		}
		pending = again
	}

	err := errors.Join(errs...)
	if n.trace != nil {
		anys := make([]any, len(payloads))
		for i, p := range payloads {
			anys[i] = p
		}
		r := traceRecord{
			kind: service.TraceEvent_FORWARD, event: eventName, payloads: anys, remote: matched, routes: routes,
			wait: wait, err: err, duration: time.Since(start),
		}
		if trace != nil {
			r.spanId, r.traces = trace.spanId, trace.parents
		}
		n.recordTrace(containerId, r)
	}
	return results, err
}

// payloadRefs returns the traces of the payloads, nil without a trace. Payloads without a trace get an empty ref.
func (t *forwardTrace) payloadRefs() []*service.TraceRef {
	if t == nil {
		return nil
	}
	refs := make([]*service.TraceRef, len(t.payloads))
	for i, ref := range t.payloads {
		if ref == nil {
			ref = &service.TraceRef{}
		}
		refs[i] = ref
	}
	return refs
}

// coverTransitions assigns the transitions to as few reachable owners as possible and returns
// the transitions assigned to each picked node, the nodes in the order they were picked
// and the transitions without a reachable owner.
func (n *Node[T, K]) coverTransitions(remote map[string]*Transition, names []string, unreachable map[string]bool) (map[string][]string, []string, []string) {
	// usually a single transition matched: any of its reachable owners
	if len(names) == 1 {
		owners := make([]string, 0, len(remote[names[0]].NodeIds))
		for _, node := range remote[names[0]].NodeIds {
			if node != n.Id() && !unreachable[node] {
				owners = append(owners, node)
			}
		}
		if len(owners) == 0 {
			return map[string][]string{}, nil, names
		}
		best := owners[rand.IntN(len(owners))]
		return map[string][]string{best: names}, []string{best}, nil
	}

	assigned := make(map[string][]string)
	order := make([]string, 0)
	left := slices.Clone(names)
	for len(left) > 0 {
		counts := make(map[string]int)
		for _, name := range left {
			for _, node := range remote[name].NodeIds {
				if node != n.Id() && !unreachable[node] {
					counts[node]++
				}
			}
		}
		if len(counts) == 0 {
			return assigned, order, left
		}

		candidates := make([]string, 0, len(counts))
		for node := range counts {
			candidates = append(candidates, node)
		}
		rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
		best := candidates[0]
		for _, node := range candidates[1:] {
			if counts[node] > counts[best] {
				best = node
			}
		}

		order = append(order, best)
		left = slices.DeleteFunc(left, func(name string) bool {
			if slices.Contains(remote[name].NodeIds, best) {
				assigned[best] = append(assigned[best], name)
				return true
			}
			return false
		})
	}
	return assigned, order, nil
}

// handOverFailed handles a hand-over which failed after it was sent: the transitions of an owner which turned out
// unreachable are covered again by the other owners, up to maxHandOverAttempts; other errors and the last one go to
// errF. It runs on the node's context: the container which handed the work over may have closed.
// retry names the next attempt, the node which failed and the id of the hand-over.
// A stopping node hands nothing on: its streams broke, the receivers may still run the work.
func (n *Node[T, K]) handOverFailed(eventName, containerId string, transitions []string, payloads []*anypb.Any, trace *forwardTrace, errF func(error), retry handOverRetry, err error) {
	select {
	case <-n.raftStopC:
		debugf(n.logger, "node stopped, not handing events %s of container %s to other owners: %v", eventName, containerId, err)
		return
	default:
	}
	n.metrics.forwardErrors.Add(1)
	if status.Code(err) == codes.Unavailable && retry.attempt < maxHandOverAttempts {
		debugf(n.logger, "hand-over to node %s failed, forwarding to other owners: %v", retry.skip, err)
		_, err = n.forwardTo(n.ctx, eventName, containerId, nil, nil, transitions, nil, payloads, false, trace, errF, retry)
		if err == nil {
			return
		}
	} else {
		err = fmt.Errorf("node %s: %w", retry.skip, err)
	}
	if errF != nil {
		errF(err)
	}
}

// sendHandOver sends a hand-over to the node without waiting for its replies; onFail gets a failure found later, on
// another goroutine. The hand-over holds a slot of the limit of kept hand-overs until it ran or failed; without a free
// slot it waits for one, up to ctx. A node which isn't known or reachable, or whose lease ended, returns Unavailable
// right away.
func (n *Node[T, K]) sendHandOver(ctx context.Context, node string, req *service.ForwardEventRequest, onFail func(error)) error {
	id, err := strconv.ParseUint(node, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid node id %s: %w", node, err)
	}
	if n.receiverLeaseEnded(node) {
		return status.Errorf(codes.Unavailable, "the lease of node %s ended", node)
	}
	if err := n.takeHandOverSlot(ctx); err != nil {
		return err
	}
	stream, err := n.forwardStreamTo(id)
	if err != nil {
		n.freeHandOverSlot()
		return err
	}

	n.metrics.forwards.Add(1)
	err = stream.handOver(ctx, req, func(err error) {
		n.freeHandOverSlot()
		if err != nil {
			onFail(err)
		}
	})
	if err != nil {
		n.freeHandOverSlot()
		n.metrics.forwardErrors.Add(1)
		return err
	}
	return nil
}

// sendForward sends the request to the node and returns what it returned, nil if nothing.
// A node which isn't known or reachable returns Unavailable.
func (n *Node[T, K]) sendForward(ctx context.Context, node string, req *service.ForwardEventRequest) (*forwardResult, error) {
	id, err := strconv.ParseUint(node, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid node id %s: %w", node, err)
	}
	stream, err := n.forwardStreamTo(id)
	if err != nil {
		return nil, err
	}

	n.metrics.forwards.Add(1)
	start := time.Now()
	resp, err := stream.call(ctx, req)
	if req.GetWait() {
		n.metrics.forwardWait.observe(time.Since(start))
	}
	if err != nil {
		n.metrics.forwardErrors.Add(1)
		return nil, err
	}
	result := &forwardResult{}
	for _, a := range resp.GetReplies() {
		reply, err := a.UnmarshalNew()
		if err != nil {
			return nil, fmt.Errorf("error decoding reply of node %s: %w", node, err)
		}
		result.replies = append(result.replies, reply)
	}
	switch {
	case resp.GetEntity() != nil:
		entity, err := resp.GetEntity().UnmarshalNew()
		if err != nil {
			return nil, fmt.Errorf("error decoding entity of node %s: %w", node, err)
		}
		result.entity = entity
	case resp.GetChangedState() != nil:
		result.changed = bitsFromProto(resp.GetChangedState())
	case len(result.replies) == 0:
		return nil, nil
	}
	return result, nil
}
