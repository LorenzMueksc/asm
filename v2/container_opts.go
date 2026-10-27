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

func WithContainerLogger(logger *zap.Logger) ContainerOption {
	return newFuncContainerOption(func(options *containerOptions) {
		options.logger = logger
	})
}
