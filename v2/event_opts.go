package asm

import (
	"google.golang.org/protobuf/proto"
	"rumue.io/asm/service"
)

type (
	EventOption interface {
		apply(*eventOptions)
	}
	// funcEventOption wraps a function that modifies eventOptions into an
	// implementation of the EventOption interface.
	funcEventOption struct {
		f func(*eventOptions)
	}
	eventOptions struct {
		doneC      chan<- struct{}
		waitRemote bool
		closeAfter bool
		remote     *remoteExecution
		// target is the id of another container of the entity the emitted event runs in
		target string
		// replyF gets the reply of the target container's transitions
		replyF func(any)
	}
	// remoteExecution is set on the events a node receives from another node.
	// The node runs its own func transitions matching the payloads, except the excluded ones.
	remoteExecution struct {
		// entity of the sender, set to the builder before executing
		entity proto.Message
		// payloads of the forwarded events, processed in order
		payloads []any
		// exclude names the transitions the sender or another node runs
		exclude []string
		// from is the node which sent the events
		from string
		// chain names the nodes lending the lock for a waited execution, the sender last; nil for handed over work
		chain []string
		// otherEntity: the events come from a container of another entity, they run on this container's own lock
		otherEntity bool
		// waitPath holds the lock tokens of the containers waiting for the execution, nil for handed over work
		waitPath []string
		// traces holds the trace of every payload, nil without tracing
		traces []*service.TraceRef
		// result is the entity after executing, set before the done channel is signaled
		result proto.Message
		// errs collects the errors of the executed transitions, err joins them before the done channel is signaled
		errs []error
		err  error
		// waited: the sender waits for the execution and takes the reply
		waited bool
		// reply is set by a transition of the receiving container with Container.Reply
		reply any
	}
)

func (fdo *funcEventOption) apply(do *eventOptions) {
	fdo.f(do)
}

func newFuncEventOption(f func(*eventOptions)) *funcEventOption {
	return &funcEventOption{
		f: f,
	}
}

// WithDoneChannel signals the channel once the event went through all matching transitions.
// The send blocks, so use a buffered channel.
func WithDoneChannel(doneC chan<- struct{}) EventOption {
	return newFuncEventOption(func(options *eventOptions) {
		options.doneC = doneC
	})
}

// WithWaitRemote waits for remote transitions matching the event: the container lends its lock to the owner,
// blocks until the owner has finished and applies the entity the owner returns to the builder.
// Without it the work is handed over to the owner, which executes it in its own container
// once this container released the lock.
func WithWaitRemote() EventOption {
	return newFuncEventOption(func(options *eventOptions) {
		options.waitRemote = true
	})
}

// WithTarget runs an emitted event in the container of another id of the same entity, on that container's own lock,
// like a call of another entity: its own func transitions and the state transitions matching the event run there.
// With WithWaitRemote the emitting transition blocks until they ran and Emit returns their errors; a call which would
// deadlock is refused with ErrWouldDeadlock. Without it the work is handed over to the target container.
// Only for Container.Emit of a container of a node.
func WithTarget(id string) EventOption {
	return newFuncEventOption(func(options *eventOptions) {
		options.target = id
	})
}

// WithReply gets the replies the receivers' transitions set with Container.Reply for an event emitted with
// WithWaitRemote: the target container's (WithTarget), or one per container that replied for waited forwards, also
// of another entity. f runs on the emitting transition before Emit returns; it isn't called if nobody replied.
func WithReply(f func(reply any)) EventOption {
	return newFuncEventOption(func(options *eventOptions) {
		options.replyF = f
	})
}

// withCloseAfter sends the close signal to the container once the event is done.
// The container sends it itself, so the sender never touches a container which closed meanwhile.
func withCloseAfter() EventOption {
	return newFuncEventOption(func(options *eventOptions) {
		options.closeAfter = true
	})
}

// withRemoteExecution processes events another node forwarded.
func withRemoteExecution(r *remoteExecution) EventOption {
	return newFuncEventOption(func(options *eventOptions) {
		options.remote = r
	})
}
