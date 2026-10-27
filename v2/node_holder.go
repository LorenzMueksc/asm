package asm

import (
	"context"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"rumue.io/asm/service"
)

// Holder routing: a waited call of a container on its own lock (a call of another entity, a target call) runs on the
// node holding the container's lock, so the lock doesn't move between nodes for every call. The node that would run
// it asks the lock table for the holder and sends the call on to it once (redirected). A target call goes there
// directly, its sender knows the id; a call of another entity goes to any owner, which picks the container and sends
// it on. The holder must own the transitions the call would run here; a holder that is unreachable, or a lock that is
// free or held by this node, runs the call here as before.

// holderLookupTimeout bounds how long a client waits for its member to tell it the holder of a lock
const holderLookupTimeout = time.Second

// lockHolder returns the node holding the lock of the id, "" if it is free or unknown. A client only knows the locks
// concerning it: it asks its member by watching the lock until the member answered.
func (n *Node[T, K]) lockHolder(ctx context.Context, id string) string {
	if n.skipRaft {
		return ""
	}
	if n.client {
		// a lock it holds itself is known, the others it asks for: the member answers a new watch with the lock,
		// also a free one
		l, held, changedC := n.locks.get(id)
		if held && l.holder == n.Id() {
			return l.holder
		}
		n.watchLock(id)
		defer n.unwatchLock(id)
		timer := time.NewTimer(holderLookupTimeout)
		defer timer.Stop()
		select {
		case <-changedC:
		case <-timer.C:
		case <-ctx.Done():
			return ""
		}
	}
	l, held, _ := n.locks.get(id)
	if !held {
		return ""
	}
	return l.holder
}

// ownsTransitionsOf reports whether the node owns every func transition of this node's entity for the event, except
// the excluded ones: a call sent on to it runs what it would run here.
func (n *Node[T, K]) ownsTransitionsOf(node, eventName string, exclude []string) bool {
	for _, t := range n.registry.published.Load().forEvent(eventName) {
		if !isOwnFuncTransition[T, K](t) || slices.Contains(exclude, t.Name) {
			continue
		}
		if !slices.Contains(t.NodeIds, node) {
			return false
		}
	}
	return true
}

// redirectToHolder sends a waited call of the container id on to the node holding its lock. It reports false if the
// call runs here: the request was redirected already, the lock is free or held by this node, the holder doesn't own
// the transitions or is unreachable.
func (n *Node[T, K]) redirectToHolder(ctx context.Context, id string, req *service.ForwardEventRequest) (*forwardResult, bool, error) {
	if req.GetRedirected() || !req.GetWait() {
		return nil, false, nil
	}
	holder := n.lockHolder(ctx, id)
	if holder == "" || holder == n.Id() || !n.ownsTransitionsOf(holder, req.GetEventName(), req.GetExclude()) {
		return nil, false, nil
	}

	fwd := req.CloneVT()
	fwd.Redirected = true
	// the holder checks the sender against the caller
	fwd.SenderNodeId = n.Id()
	result, err := n.sendForward(ctx, holder, fwd)
	if status.Code(err) == codes.Unavailable {
		debugf(n.logger, "holder %s of container %s is unreachable, running the call here: %v", holder, id, err)
		return nil, false, nil
	}
	n.metrics.forwardsRedirected.Add(1)
	debugf(n.logger, "sent the call %s of container %s on to its holder %s", req.GetEventName(), id, holder)
	if result == nil {
		result = &forwardResult{}
	}
	return result, true, err
}

// forwardTarget runs a waited target call (WithTarget) another node sent: in the container of the target id on its
// own lock, like a call of another entity.
func (n *Node[T, K]) forwardTarget(ctx context.Context, req *service.ForwardEventRequest, payloads []any) (*service.ForwardEventResponse, error) {
	if !req.GetWait() {
		return nil, status.Error(codes.InvalidArgument, "a target call is only sent on when it waits")
	}
	r := &remoteExecution{
		payloads:    payloads,
		exclude:     req.GetExclude(),
		from:        req.GetSenderNodeId(),
		otherEntity: true,
		traces:      payloadTraces(req.GetTraces()),
		waited:      true,
	}
	if err := n.callOtherEntity(ctx, req.GetTargetId(), req, r); err != nil {
		return nil, err
	}
	if r.err != nil {
		return nil, status.Errorf(codes.Aborted, "transitions of event %s failed: %v", req.GetEventName(), r.err)
	}
	resp := &service.ForwardEventResponse{}
	return resp, addReply(resp, r)
}

// targetRequest is the request of a waited target call sent on to the target's holder, nil if the payload can't go
// over the wire.
func (n *Node[T, K]) targetRequest(id, eventName string, payload any, waitPath []string, trace *service.TraceRef) *service.ForwardEventRequest {
	m, ok := payload.(proto.Message)
	if !ok {
		return nil
	}
	_, payloads, err := marshalRemote(nil, []any{m})
	if err != nil {
		return nil
	}
	req := &service.ForwardEventRequest{
		EventName:    eventName,
		TargetId:     id,
		Payloads:     payloads,
		Wait:         true,
		WaitPath:     waitPath,
		SenderNodeId: n.Id(),
		SenderEntity: n.entityName(),
	}
	if trace != nil {
		req.Traces = []*service.TraceRef{trace}
	}
	return req
}
