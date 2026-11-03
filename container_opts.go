package asm

import (
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type (
	ContainerOption interface {
		apply(*containerOptions)
	}
	funcContainerOption struct {
		f func(*containerOptions)
	}
	containerOptions struct {
		maxInFlight                  int
		saveF, initF, closeF, errorF any
		dependencies                 Dependencies
		logger                       *zap.Logger
		// reentrant is set by WithContainerReentrant or WithContainerNotReentrant, nil keeps the default
		reentrant *reentrance
	}
	// reentrance tells whether a container runs other events while a transition waits, and which (all if empty)
	reentrance struct {
		on     bool
		events []string
	}
)

func (no *funcContainerOption) apply(o *containerOptions) {
	no.f(o)
}

func newFuncContainerOption(f func(*containerOptions)) *funcContainerOption {
	return &funcContainerOption{
		f: f,
	}
}

func WithMaxInFlight(maxInFlight int) ContainerOption {
	return newFuncContainerOption(func(options *containerOptions) {
		options.maxInFlight = maxInFlight
	})
}

func WithContainerSaveFunc[T proto.Message, K Builder[T]](f lifecycleFunc[T, K]) ContainerOption {
	return newFuncContainerOption(func(options *containerOptions) {
		options.saveF = f
	})
}

func WithContainerInitFunc[T proto.Message, K Builder[T]](f lifecycleFunc[T, K]) ContainerOption {
	return newFuncContainerOption(func(options *containerOptions) {
		options.initF = f
	})
}

func WithContainerCloseFunc[T proto.Message, K Builder[T]](f lifecycleFunc[T, K]) ContainerOption {
	return newFuncContainerOption(func(options *containerOptions) {
		options.closeF = f
	})
}

func WithContainerErrorFunc[T proto.Message, K Builder[T]](f errorFunc[T, K]) ContainerOption {
	return newFuncContainerOption(func(options *containerOptions) {
		options.errorF = f
	})
}

func WithContainerDependencies(d Dependencies) ContainerOption {
	return newFuncContainerOption(func(options *containerOptions) {
		options.dependencies = d
	})
}

// WithContainerReentrant makes the container reentrant, overriding the node's default: while a transition waits in a
// waited Emit, the container runs other events, only the named ones if given (see WithReentrant).
func WithContainerReentrant(events ...string) ContainerOption {
	return newFuncContainerOption(func(options *containerOptions) {
		options.reentrant = &reentrance{on: true, events: events}
	})
}

// WithContainerNotReentrant turns reentrance off for the container, overriding the node's WithReentrant.
func WithContainerNotReentrant() ContainerOption {
	return newFuncContainerOption(func(options *containerOptions) {
		options.reentrant = &reentrance{}
	})
}

func WithContainerLogger(logger *zap.Logger) ContainerOption {
	return newFuncContainerOption(func(options *containerOptions) {
		options.logger = logger
	})
}
