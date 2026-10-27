package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"rumue.io/asm/service"
)

// With many players the nodes don't log every event (-log stats): they count what happened and print
// the counts of the last interval instead, with a summary of the node's metrics.

const statsInterval = 5 * time.Second

// eventLogs logs every event, set from -log
var eventLogs = true

var stats = &counters{m: make(map[string]int64)}

type counters struct {
	mu sync.Mutex
	m  map[string]int64
}

func (c *counters) add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key]++
}

// take returns the counts and starts counting from zero.
func (c *counters) take() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.m
	c.m = make(map[string]int64)
	return m
}

// say counts what happened under key and logs the message if every event is logged.
func say(logger *zap.Logger, key, msg string) {
	stats.add(key)
	if eventLogs {
		logger.Info(msg)
	}
}

// inspector is implemented by the nodes of both entities
type inspector interface {
	Inspect(context.Context, *service.InspectRequest) (*service.InspectResponse, error)
}

// printStats logs the counts of every interval, after extra (eg. the active sessions of a gateway), if any,
// and the metrics of the node.
func printStats(logger *zap.Logger, n inspector, extra func() string) {
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()
	m := &metricsSummary{n: n}
	m.summary()
	for range ticker.C {
		counts := stats.take()
		parts := make([]string, 0, len(counts)+1)
		if extra != nil {
			parts = append(parts, extra())
		}
		for _, key := range slices.Sorted(maps.Keys(counts)) {
			parts = append(parts, fmt.Sprintf("%s %d", key, counts[key]))
		}
		if len(parts) > 0 {
			logger.Info(fmt.Sprintf("last %s: %s", statsInterval, strings.Join(parts, " · ")))
		}
		logger.Info("metrics: " + m.summary())
	}
}

// metricsSummary describes the metrics of a node over the time since the last summary.
type metricsSummary struct {
	n    inspector
	prev map[string]*service.Metric
	at   time.Time
}

func (s *metricsSummary) summary() string {
	resp, err := s.n.Inspect(context.Background(), &service.InspectRequest{Limit: 1})
	if err != nil {
		return err.Error()
	}
	cur := make(map[string]*service.Metric, len(resp.GetMetrics()))
	for _, m := range resp.GetMetrics() {
		cur[m.GetName()] = m
	}
	prev, seconds := s.prev, time.Since(s.at).Seconds()
	s.prev, s.at = cur, time.Now()
	if prev == nil {
		return "collecting"
	}

	rate := func(name string) string {
		c, p := cur[name], prev[name]
		if c == nil || p == nil {
			return "-"
		}
		return number((c.GetValue() - p.GetValue()) / seconds)
	}
	value := func(name string) float64 { return cur[name].GetValue() }
	p99 := func(name string) string {
		c, p := cur[name], prev[name]
		if c == nil || p == nil {
			return "-"
		}
		var total uint64
		delta := make([]uint64, len(c.GetCounts()))
		for i, count := range c.GetCounts() {
			if i < len(p.GetCounts()) {
				count -= p.GetCounts()[i]
			}
			delta[i] = count
			total += count
		}
		if total == 0 {
			return "-"
		}
		// the upper bound of the bucket holding the 99th percentile
		rank, seen := uint64(float64(total)*0.99+0.5), uint64(0)
		for i, count := range delta {
			seen += count
			if seen >= rank {
				if i >= len(c.GetBounds()) {
					return ">30s"
				}
				return (time.Duration(c.GetBounds()[i] * float64(time.Second))).String()
			}
		}
		return "-"
	}

	return fmt.Sprintf("events %s/s (p99 %s) · forwards %s/s (waited p99 %s) · raft applied %s logs/s in %s entries/s, proposed %s/s, apply %.0f%% · lock wait p99 %s · containers %.0f · goroutines %.0f · heap %.0f MiB · gc cpu %.0f%%",
		rate("asm_events_total"), p99("asm_event_seconds"),
		rate("asm_forwards_total"), p99("asm_forward_wait_seconds"),
		rate("asm_raft_logs_applied_total"), rate("asm_raft_entries_applied_total"), rate("asm_raft_proposals_total"),
		100*(value("asm_raft_apply_seconds_total")-prev["asm_raft_apply_seconds_total"].GetValue())/seconds,
		p99("asm_lock_wait_seconds"),
		value("asm_containers_open"), value("go_goroutines"), value("go_heap_objects_bytes")/(1<<20),
		100*(value("go_gc_cpu_seconds_total")-prev["go_gc_cpu_seconds_total"].GetValue())/seconds)
}

func number(v float64) string {
	if v >= 10_000 {
		return fmt.Sprintf("%.1fk", v/1000)
	}
	return fmt.Sprintf("%.0f", v)
}
