package asm

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"rumue.io/asm/service"
)

// Event sends an event received from a peer or client to a container.
// The payload is decoded with the global protobuf registry and the container id
// is derived from it with the ContainerIdFunc.
//
// With meta["wait"] = "true" it responds after the event is fully processed,
// otherwise it responds right away. In both cases the container gets a close signal
// and closes once it has no events in the pipeline and is not doing any work.
func (n *Node[T, K]) Event(ctx context.Context, req *service.EventRequest) (*service.EventResponse, error) {
	if n.containerIdF == nil {
		return nil, status.Error(codes.FailedPrecondition, "no container id func configured")
	}

	payload, err := req.GetPayload().UnmarshalNew()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error decoding payload: %v", err)
	}

	id := n.containerIdF(req.GetEventName(), payload)
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "no container id for event")
	}

	wait := req.GetOptions().GetMeta()["wait"] == "true"

	doneC := make(chan struct{}, 1)
	if err := n.sendToContainer(ctx, id, false, req.GetEventName(), payload, WithDoneChannel(doneC), withCloseAfter()); err != nil {
		return nil, err
	}

	if wait {
		if err := waitDone(ctx, doneC); err != nil {
			return nil, err
		}
	}

	return &service.EventResponse{ContainerId: id}, nil
}

// sendToContainer starts or loads the container and sends the event to it.
// ctx bounds waiting for the node and the lock of the container.
// If the container stopped accepting events after it was loaded, a new one is started.
// The container isn't used after the event is sent, it may close any time afterwards.
func (n *Node[T, K]) sendToContainer(ctx context.Context, id string, borrowed bool, eventName string, payload any, opts ...EventOption) error {
	for {
		c, _, err := n.startContainer(ctx, id, borrowed)
		if err != nil {
			return status.Errorf(codes.Unavailable, "error starting container: %v", err)
		}

		err = c.Event(eventName, payload, opts...)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrContainerClosed) {
			return status.Errorf(codes.Internal, "error sending event: %v", err)
		}
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
	}
}

// waitDone blocks until the event is done or ctx is done.
// The event is processed anyway and the container closes itself afterwards.
func waitDone(ctx context.Context, doneC <-chan struct{}) error {
	select {
	case <-doneC:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

func (n *Node[T, K]) Join(ctx context.Context, req *service.JoinRequest) (*service.JoinResponse, error) {
	if n.node == nil {
		return nil, status.Error(codes.FailedPrecondition, "not a raft member")
	}
	leaderId := n.node.Status().Lead
	if leaderId != uint64(n.id) {
		return nil, errors.New("not the leader")
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.New("no metadata found")
	}
	nodeIds := md.Get("node_id")
	if len(nodeIds) == 0 {
		return nil, errors.New("no node_id found in metadata")
	}
	nodeIdString := nodeIds[0]
	if err := ownName(ctx, nodeIdString); err != nil {
		return nil, err
	}

	nodeId, err := strconv.Atoi(nodeIdString)
	if err != nil {
		return nil, err
	}

	// raft drops a configuration change proposed while another one is pending,
	// so joins are serialized and each waits until the node is applied as peer, proposing again if dropped
	n.joinMu.Lock()
	defer n.joinMu.Unlock()

	deadline := time.Now().Add(joinTimeout)
	for !n.hasPeer(uint64(nodeId)) {
		if time.Now().After(deadline) {
			return nil, status.Errorf(codes.DeadlineExceeded, "node %d was not added to the cluster in time", nodeId)
		}

		n.peersMu.Lock()
		confChangeId := n.confChangeCount
		n.confChangeCount++
		n.peersMu.Unlock()

		err = n.node.ProposeConfChange(ctx, raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  uint64(nodeId),
			Context: []byte(req.GetListenAddr()),
			ID:      confChangeId,
		})
		if err != nil {
			return nil, err
		}

		if err := n.waitForPeer(ctx, uint64(nodeId), true, joinRetryInterval); err != nil {
			return nil, err
		}
	}

	peers := make([]*service.JoinResponse_Peer, 0)
	n.peersMu.RLock()
	for id, addr := range n.peers {
		if id == uint64(nodeId) {
			continue
		}
		peers = append(peers, &service.JoinResponse_Peer{
			Id:   id,
			Addr: addr,
		})
	}
	addedIndex := n.addedAt[uint64(nodeId)]
	n.peersMu.RUnlock()

	return &service.JoinResponse{
		Peers:      peers,
		AddedIndex: addedIndex,
	}, nil
}

// waitForPeer waits until the node is applied as peer (or as removed, if present is false) or the timeout passed.
func (n *Node[T, K]) waitForPeer(ctx context.Context, id uint64, present bool, timeout time.Duration) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for n.hasPeer(id) != present {
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
			return nil
		case <-ticker.C:
		}
	}
	return nil
}

func (n *Node[T, K]) Sync(ctx context.Context, req *service.SyncRequest) (*service.SyncResponse, error) {
	if err := n.stepMessages(ctx, req.GetMessages()); err != nil {
		return nil, err
	}
	return &service.SyncResponse{Acked: true}, nil
}

// SyncStream receives the raft messages of a member over a stream and acks every request after stepping its messages.
func (n *Node[T, K]) SyncStream(stream service.Node_SyncStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		if err := n.stepMessages(stream.Context(), req.GetMessages()); err != nil {
			return err
		}
		if err := stream.Send(&service.SyncResponse{Acked: true}); err != nil {
			return err
		}
	}
}

// stepMessages hands raft messages of another member to raft. With TLS they must come from the caller.
func (n *Node[T, K]) stepMessages(ctx context.Context, messages [][]byte) error {
	if n.node == nil {
		return status.Error(codes.FailedPrecondition, "not a raft member")
	}
	for _, entry := range messages {
		var msg raftpb.Message
		if err := msg.Unmarshal(entry); err != nil {
			return status.Errorf(codes.InvalidArgument, "error decoding raft message: %v", err)
		}

		// a removed node which missed its removal must not disturb the cluster, eg. by campaigning.
		// PermissionDenied tells it to stop.
		if n.isRemoved(msg.From) {
			return status.Errorf(codes.PermissionDenied, "node %d was removed from the cluster", msg.From)
		}
		if err := ownName(ctx, strconv.FormatUint(msg.From, 10)); err != nil {
			return err
		}
		// local messages (elections, the storage goroutines' responses) never come from a peer
		if raft.IsLocalMsg(msg.Type) {
			return status.Errorf(codes.InvalidArgument, "raft message %s is local", msg.Type)
		}

		if err := n.node.Step(context.Background(), msg); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node[T, K]) Propose(ctx context.Context, req *service.ProposeRequest) (*service.ProposeResponse, error) {
	if n.node == nil {
		return nil, status.Error(codes.FailedPrecondition, "not a raft member")
	}
	if err := checkProposal(ctx, req.GetLogs()); err != nil {
		return nil, err
	}
	// the leader as of the last Ready: Status() is a round trip into the raft goroutine and copies the progress
	if n.leader.Load() != uint64(n.id) {
		return &service.ProposeResponse{Acked: false}, fmt.Errorf("not the leader")
	}

	for _, log := range req.GetLogs() {
		if id, ok := nodeIdOf(log.GetNodeId()); ok && n.isRemoved(id) {
			return nil, status.Errorf(codes.PermissionDenied, "node %d was removed from the cluster", id)
		}
	}
	// the batch of the follower becomes one raft entry
	if err := n.proposeLogs(req.GetLogs()); err != nil {
		return nil, err
	}
	return &service.ProposeResponse{Acked: true}, nil
}

// GetTransitions returns the compiled local transitions of this node.
func (n *Node[T, K]) GetTransitions(context.Context, *service.GetTransitionsRequest) (*service.GetTransitionsResponse, error) {
	return &service.GetTransitionsResponse{Transitions: n.localTransitionProtos()}, nil
}

// AddTransitions merges transitions of another node.
// A transition with the same name but a different signature is rejected with FailedPrecondition.
func (n *Node[T, K]) AddTransitions(ctx context.Context, req *service.AddTransitionsRequest) (*service.AddTransitionsResponse, error) {
	for _, t := range req.GetTransitions() {
		if err := ownName(ctx, t.GetNodeId()); err != nil {
			return nil, err
		}
	}
	if err := n.addRemoteTransitions(req.GetTransitions()); err != nil {
		n.logger.Error(fmt.Sprintf("rejected transitions: %v", err))
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	names := make([]string, 0, len(req.GetTransitions()))
	owner := ""
	for _, t := range req.GetTransitions() {
		names = append(names, t.GetName())
		owner = t.GetNodeId()
	}
	n.logger.Info(fmt.Sprintf("added transitions %v of node %s", names, owner))
	return &service.AddTransitionsResponse{}, nil
}

// ForwardEvent processes events another node forwarded: this node runs its own func transitions
// matching them, except the excluded ones.
// With wait the sender lends its lock: the events run right away on the sent entity and the entity is returned.
// Otherwise the work is handed over: it is queued per container id and runs once this node got the lock. The unary
// rpc only answers the accept, the ForwardEvents stream also tells the sender once the work ran.
func (n *Node[T, K]) ForwardEvent(ctx context.Context, req *service.ForwardEventRequest) (*service.ForwardEventResponse, error) {
	resp, _, err := n.forwardEvent(ctx, req, nil)
	return resp, err
}

// forwardEvent is ForwardEvent. A hand-over is accepted once it is queued (see acceptHandOver): executed reports that a
// hand-over with its id ran already, handOverF gets the outcome of an accepted one later: nil once its work ran, else
// the error it was failed back with.
func (n *Node[T, K]) forwardEvent(ctx context.Context, req *service.ForwardEventRequest, handOverF func(error)) (*service.ForwardEventResponse, bool, error) {
	n.metrics.forwardsReceived.Add(1)
	// the sender, and the last lender of a waited request, is the caller
	if err := ownName(ctx, req.GetSenderNodeId()); err != nil {
		return nil, false, err
	}
	if chain := req.GetLendChain(); len(chain) > 0 {
		if err := ownName(ctx, chain[len(chain)-1]); err != nil {
			return nil, false, err
		}
	}
	// the lock table would refuse it anyway (applyAcquire)
	if err := checkWaitPath(req.GetWaitPath()); err != nil {
		return nil, false, status.Error(codes.InvalidArgument, err.Error())
	}
	otherEntity := req.GetSenderEntity() != "" && req.GetSenderEntity() != n.entityName()

	var entity proto.Message
	if req.GetEntity() != nil && !otherEntity {
		m, err := req.GetEntity().UnmarshalNew()
		if err != nil {
			return nil, false, status.Errorf(codes.InvalidArgument, "error decoding entity: %v", err)
		}
		if _, ok := m.(T); !ok {
			return nil, false, status.Errorf(codes.InvalidArgument, "entity of type %T does not match the entity type of the node", m)
		}
		entity = m
	}

	payloads := make([]any, 0, len(req.GetPayloads()))
	for _, p := range req.GetPayloads() {
		m, err := p.UnmarshalNew()
		if err != nil {
			return nil, false, status.Errorf(codes.InvalidArgument, "error decoding payload: %v", err)
		}
		payloads = append(payloads, m)
	}

	if req.GetTargetId() != "" {
		resp, err := n.forwardTarget(ctx, req, payloads)
		return resp, false, err
	}
	if otherEntity {
		return n.forwardOtherEntity(ctx, req, payloads, handOverF)
	}

	if !req.GetWait() {
		executed, err := n.acceptHandOver(req.GetHandoverId(), []handedOver{{id: req.GetContainerId(), item: forwardedExecution{
			eventName: req.GetEventName(),
			r: &remoteExecution{
				payloads: payloads,
				exclude:  req.GetExclude(),
				from:     req.GetSenderNodeId(),
				traces:   payloadTraces(req.GetTraces()),
			},
		}}}, handOverF)
		if err != nil {
			return nil, false, err
		}
		return &service.ForwardEventResponse{}, executed, nil
	}

	// the sender lends the lock of the container for the execution
	if err := n.verifyLending(ctx, req.GetContainerId(), req.GetLendChain()); err != nil {
		return nil, false, status.Error(codes.FailedPrecondition, err.Error())
	}

	// the container gets a copy, transitions may change the entity in place and the response compares
	// the result with the sent entity
	var working proto.Message
	if entity != nil {
		working = proto.Clone(entity)
	}
	r := &remoteExecution{
		entity:   working,
		payloads: payloads,
		exclude:  req.GetExclude(),
		from:     req.GetSenderNodeId(),
		chain:    req.GetLendChain(),
		waitPath: req.GetWaitPath(),
		traces:   payloadTraces(req.GetTraces()),
		waited:   true,
	}

	doneC := make(chan struct{}, 1)
	err := n.sendToContainer(ctx, req.GetContainerId(), true, req.GetEventName(), nil,
		withRemoteExecution(r), WithDoneChannel(doneC), withCloseAfter())
	if err != nil {
		return nil, false, err
	}

	if err := waitDone(ctx, doneC); err != nil {
		return nil, false, err
	}

	if r.err != nil {
		return nil, false, status.Errorf(codes.Aborted, "transitions of event %s failed: %v", req.GetEventName(), r.err)
	}

	resp := &service.ForwardEventResponse{}
	if err := addReply(resp, r); err != nil {
		return nil, false, err
	}
	if r.result == nil {
		return resp, false, nil
	}
	if changed, ok := n.changedState(entity, r.result); ok {
		resp.ChangedState = changed.toProto()
		return resp, false, nil
	}
	a, err := anypb.New(r.result)
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "error encoding entity: %v", err)
	}
	resp.Entity = a
	return resp, false, nil
}

// forwardOtherEntity processes events from a container of another entity: the container id func of this node
// chooses the container of every payload. The containers run on their own locks, also when the sender waits,
// so nothing is lent, and no entity is returned, it couldn't be merged into the sender's entity.
func (n *Node[T, K]) forwardOtherEntity(ctx context.Context, req *service.ForwardEventRequest, payloads []any, handOverF func(error)) (*service.ForwardEventResponse, bool, error) {
	if n.containerIdF == nil {
		return nil, false, status.Errorf(codes.FailedPrecondition, "events of entity %s need a container id func (WithContainerIdFunc)", req.GetSenderEntity())
	}

	// payloads by container, in the order of their first payload, with their traces
	ids := make([]string, 0, 1)
	groups := make(map[string][]any)
	indexes := make(map[string][]int)
	traces := make(map[string][]*service.TraceRef)
	reqTraces := payloadTraces(req.GetTraces())
	for i, p := range payloads {
		m, ok := p.(proto.Message)
		if !ok {
			return nil, false, status.Errorf(codes.InvalidArgument, "payload of type %T is no protobuf message", p)
		}
		id := n.containerIdF(req.GetEventName(), m)
		if id == "" {
			return nil, false, status.Errorf(codes.InvalidArgument, "no container for event %s with payload %T", req.GetEventName(), p)
		}
		if _, ok := groups[id]; !ok {
			ids = append(ids, id)
		}
		groups[id] = append(groups[id], p)
		indexes[id] = append(indexes[id], i)
		if i < len(reqTraces) {
			traces[id] = append(traces[id], reqTraces[i])
		}
	}

	executions := make(map[string]*remoteExecution, len(ids))
	for _, id := range ids {
		executions[id] = &remoteExecution{
			payloads:    groups[id],
			exclude:     req.GetExclude(),
			from:        req.GetSenderNodeId(),
			otherEntity: true,
			traces:      traces[id],
			waited:      req.GetWait(),
		}
	}

	if !req.GetWait() {
		// one hand-over, an execution per container: executed once all of them ran
		handedOvers := make([]handedOver, 0, len(ids))
		for _, id := range ids {
			handedOvers = append(handedOvers, handedOver{id: id, item: forwardedExecution{eventName: req.GetEventName(), r: executions[id]}})
		}
		executed, err := n.acceptHandOver(req.GetHandoverId(), handedOvers, handOverF)
		if err != nil {
			return nil, false, err
		}
		return &service.ForwardEventResponse{}, executed, nil
	}

	var errs []error
	resp := &service.ForwardEventResponse{}
	for _, id := range ids {
		r := executions[id]
		// the container is open on another node, or kept there: its payloads run there
		if result, ok, err := n.redirectToHolder(ctx, id, payloadsOf(req, indexes[id])); ok {
			switch {
			case status.Code(err) == codes.Aborted:
				errs = append(errs, fmt.Errorf("container %s: %w", id, err))
			case err != nil:
				return nil, false, err
			}
			for _, reply := range result.replies {
				r.reply = reply
				if err := addReply(resp, r); err != nil {
					return nil, false, err
				}
			}
			continue
		}
		if err := n.callOtherEntity(ctx, id, req, r); err != nil {
			return nil, false, err
		}
		if r.err != nil {
			errs = append(errs, fmt.Errorf("container %s: %w", id, r.err))
		}
		if err := addReply(resp, r); err != nil {
			return nil, false, err
		}
	}
	if len(errs) > 0 {
		return nil, false, status.Errorf(codes.Aborted, "transitions of event %s failed: %v", req.GetEventName(), errors.Join(errs...))
	}
	return resp, false, nil
}

// payloadsOf returns the request with the payloads at the indexes only, and their traces.
func payloadsOf(req *service.ForwardEventRequest, indexes []int) *service.ForwardEventRequest {
	sub := req.CloneVT()
	sub.Payloads, sub.Traces = nil, nil
	for _, i := range indexes {
		sub.Payloads = append(sub.Payloads, req.GetPayloads()[i])
		if i < len(req.GetTraces()) {
			sub.Traces = append(sub.Traces, req.GetTraces()[i])
		}
	}
	return sub
}

// addReply adds the reply of a waited execution to the response; replies to other nodes are protobuf messages.
func addReply(resp *service.ForwardEventResponse, r *remoteExecution) error {
	if r.reply == nil {
		return nil
	}
	m, ok := r.reply.(proto.Message)
	if !ok {
		return status.Errorf(codes.Aborted, "reply of type %T is no protobuf message", r.reply)
	}
	a, err := anypb.New(m)
	if err != nil {
		return status.Errorf(codes.Internal, "error encoding reply: %v", err)
	}
	resp.Replies = append(resp.Replies, a)
	return nil
}

// callOtherEntity runs a waited execution of events of another entity in the container of the id and waits for it.
// With a wait path the lock table knows who waits for the container, see callContainer.
func (n *Node[T, K]) callOtherEntity(ctx context.Context, id string, req *service.ForwardEventRequest, r *remoteExecution) error {
	if len(req.GetWaitPath()) == 0 || n.skipRaft {
		doneC := make(chan struct{}, 1)
		err := n.sendToContainer(ctx, id, false, req.GetEventName(), nil,
			withRemoteExecution(r), WithDoneChannel(doneC), withCloseAfter())
		if err != nil {
			return err
		}
		return waitDone(ctx, doneC)
	}

	r.waitPath = req.GetWaitPath()
	err := n.callContainer(ctx, id, req.GetWaitPath(), req.GetEventName(), withRemoteExecution(r))
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrWouldDeadlock):
		return status.Errorf(codes.Aborted, "event %s for container %s: %v", req.GetEventName(), id, err)
	case status.Code(err) != codes.Unknown:
		return err
	case ctx.Err() != nil:
		return status.FromContextError(ctx.Err()).Err()
	default:
		return status.Errorf(codes.Unavailable, "error calling container %s: %v", id, err)
	}
}

// payloadTraces returns the traces of forwarded payloads; a payload without trace gets nil.
func payloadTraces(refs []*service.TraceRef) []*service.TraceRef {
	if len(refs) == 0 {
		return nil
	}
	traces := make([]*service.TraceRef, len(refs))
	for i, ref := range refs {
		if ref.GetTraceId() != "" {
			traces[i] = ref
		}
	}
	return traces
}

// CheckLending answers a node this node lent a lock on to: whether a container of this node runs on the lent lock
// of the container id right now, with the chain it received.
func (n *Node[T, K]) CheckLending(_ context.Context, req *service.CheckLendingRequest) (*service.CheckLendingResponse, error) {
	if len(req.GetLendChain()) == 0 {
		return &service.CheckLendingResponse{}, nil
	}
	n.containerMap.mu.RLock()
	c, ok := n.containerMap.m[req.GetContainerId()]
	n.containerMap.mu.RUnlock()

	return &service.CheckLendingResponse{Lending: ok && slices.Equal(c.lendChain(), req.GetLendChain())}, nil
}

// changedState returns the bits of the properties which differ between the sent entity and the result,
// if applying them to the sent entity gives the result. Otherwise bits alone can't describe the changes.
// It changes sent.
func (n *Node[T, K]) changedState(sent, result proto.Message) (Bits, bool) {
	if isNilMessage(sent) {
		return nil, false
	}
	bm := n.getBitmapByTypeF(result, n.layout.Load())
	if bm == nil {
		return nil, false
	}

	sentState := EvaluateEntityProperties(sent, bm)
	changed := changedBits(bm, sentState, EvaluateEntityProperties(result, bm))

	// sent isn't needed afterwards: the state is applied to it in place, no copy
	ApplyStateToEntity(sent, mergeState(bm, sentState, changed), bm)
	if !proto.Equal(sent, result) {
		return nil, false
	}
	return changed, true
}
