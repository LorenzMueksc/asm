package asm

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// debugf logs a debug message. It is only formatted if the logger logs debug messages: the hot paths log a lot.
func debugf(l *zap.Logger, format string, args ...any) {
	if l.Core().Enabled(zapcore.DebugLevel) {
		l.Debug(fmt.Sprintf(format, args...))
	}
}

// containerLogger names the container in the log messages of the logger. The field is only encoded once the
// container logs (WithLazy); a logger which logs nothing is used as it is.
func containerLogger(l *zap.Logger, id string) *zap.Logger {
	if !l.Core().Enabled(zapcore.FatalLevel) {
		return l
	}
	return l.WithLazy(zap.String("container-id", id))
}

// raftLogger hands the log messages of raft to the node's logger (raft.Logger), named "raft".
// The sugared logger formats a message only if its level is enabled.
type raftLogger struct {
	s *zap.SugaredLogger
}

func newRaftLogger(l *zap.Logger) *raftLogger {
	return &raftLogger{s: l.Named("raft").Sugar()}
}

func (r *raftLogger) Debug(v ...any)                 { r.s.Debug(v...) }
func (r *raftLogger) Debugf(format string, v ...any) { r.s.Debugf(format, v...) }
func (r *raftLogger) Info(v ...any)                  { r.s.Info(v...) }

// Infof logs the messages about a stable entry acked again at debug: with AsyncStorageWrites raft acks the entries
// in progress with every append, so they come with most appends ("...; ignoring", log_unstable.go).
func (r *raftLogger) Infof(format string, v ...any) {
	if strings.HasSuffix(format, "; ignoring") {
		r.s.Debugf(format, v...)
		return
	}
	r.s.Infof(format, v...)
}

func (r *raftLogger) Warning(v ...any)                 { r.s.Warn(v...) }
func (r *raftLogger) Warningf(format string, v ...any) { r.s.Warnf(format, v...) }
func (r *raftLogger) Error(v ...any)                   { r.s.Error(v...) }
func (r *raftLogger) Errorf(format string, v ...any)   { r.s.Errorf(format, v...) }
func (r *raftLogger) Fatal(v ...any)                   { r.s.Fatal(v...) }
func (r *raftLogger) Fatalf(format string, v ...any)   { r.s.Fatalf(format, v...) }
func (r *raftLogger) Panic(v ...any)                   { r.s.Panic(v...) }
func (r *raftLogger) Panicf(format string, v ...any)   { r.s.Panicf(format, v...) }
