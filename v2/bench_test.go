package asm

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Benchmarks of the main operations, on a single node and on 3 and 5 node clusters on localhost.
//
//	go test ./v2/ -run '^$' -bench . -benchtime 3s
//
// Besides ns/op they report ops/s and the p50/p99 latency of one operation in microseconds.
// On Windows the clock only advances about every 0.5ms, so single latencies below that measure as 0
// and the percentiles come in 0.5ms steps; ns/op is exact.
// The clusters are started once per size and reused by all benchmarks of a run.

type benchNode = Node[*wrapperspb.StringValue, *stringValueBuilder]

const benchTransition = "bench-work"

// benchCluster is a cluster of real raft nodes. sender is a follower which sends events,
// owner is another node owning the func transition benchTransition.
type benchCluster struct {
	nodes  []*benchNode
	sender *benchNode
	owner  *benchNode
}

var (
	benchClustersMu sync.Mutex
	benchClusters   = make(map[string]*benchCluster)
	benchIds        atomic.Int64
	// benchExecutions counts the executions of benchTransition on the owners
	benchExecutions atomic.Int64
)

func benchAddr(b *testing.B) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func benchNewNode(b *testing.B, id int, joinAddr string, addr string, opts ...NodeSetupOption) *benchNode {
	opts = append(opts,
		WithBitmap(clusterBitmap("pending", "done")),
		WithLogger(zap.NewNop()),
		// the benchmarks measure raft and the transport, not the container limit
		WithMaxContainerCount(100000),
	)
	if joinAddr != "" {
		opts = append(opts, WithJoinAddr(joinAddr))
	}
	return NewNode(id, clusterBitmapByType, func() *stringValueBuilder { return &stringValueBuilder{} }, addr, opts...)
}

func benchWaitReady(b *testing.B, n *benchNode) {
	select {
	case <-n.Ready():
	case err := <-n.Err():
		b.Fatalf("node %s failed: %v", n.Id(), err)
	case <-time.After(30 * time.Second):
		b.Fatalf("node %s did not become ready", n.Id())
	}
}

// getBenchCluster starts a cluster of the given size, or returns the one started before. With wal the members
// persist the raft log in a temporary directory, which stays (the cluster runs until the process ends).
func getBenchCluster(b *testing.B, size int, wal ...bool) *benchCluster {
	benchClustersMu.Lock()
	defer benchClustersMu.Unlock()

	persist := len(wal) > 0 && wal[0]
	key := fmt.Sprintf("%d/%v", size, persist)
	if bc, ok := benchClusters[key]; ok {
		return bc
	}
	nodeOpts := func(id int) []NodeSetupOption {
		if !persist {
			return nil
		}
		dir, err := os.MkdirTemp("", "asm-bench-")
		if err != nil {
			b.Fatal(err)
		}
		return []NodeSetupOption{WithDataDir(filepath.Join(dir, fmt.Sprint(id)))}
	}

	bc := &benchCluster{}
	leaderAddr := benchAddr(b)
	leader := benchNewNode(b, 1, "", leaderAddr, nodeOpts(1)...)
	benchWaitReady(b, leader)
	bc.nodes = append(bc.nodes, leader)

	for id := 2; id <= size; id++ {
		n := benchNewNode(b, id, leaderAddr, benchAddr(b), nodeOpts(id)...)
		if id == size {
			n.AddFuncTransition(benchTransition, "WORK", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
				benchExecutions.Add(1)
				c.Builder().Set(wrapperspb.String("done"))
				return false, nil
			}, WithFuncKey(benchTransition))
		}
		benchWaitReady(b, n)
		bc.nodes = append(bc.nodes, n)
	}
	bc.sender = bc.nodes[1]
	bc.owner = bc.nodes[size-1]

	deadline := time.Now().Add(10 * time.Second)
	for !slices.ContainsFunc(bc.sender.registry.published.Load().list(), func(t *Transition) bool { return t.Name == benchTransition }) {
		if time.Now().After(deadline) {
			b.Fatal("the sender did not get the transition of the owner")
		}
		time.Sleep(10 * time.Millisecond)
	}

	benchClusters[key] = bc
	return bc
}

// latencies collects the duration of every operation and reports throughput and percentiles.
type latencies struct {
	mu        sync.Mutex
	durations []time.Duration
	start     time.Time
}

func newLatencies() *latencies {
	return &latencies{start: time.Now()}
}

func (l *latencies) add(d time.Duration) {
	l.mu.Lock()
	l.durations = append(l.durations, d)
	l.mu.Unlock()
}

func (l *latencies) report(b *testing.B) {
	elapsed := time.Since(l.start)

	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.durations) == 0 {
		return
	}
	slices.Sort(l.durations)
	percentile := func(p float64) float64 {
		i := min(int(float64(len(l.durations))*p), len(l.durations)-1)
		return float64(l.durations[i].Nanoseconds()) / 1000
	}

	b.ReportMetric(float64(len(l.durations))/elapsed.Seconds(), "ops/s")
	b.ReportMetric(percentile(0.50), "p50-us")
	b.ReportMetric(percentile(0.99), "p99-us")
	b.ReportMetric(float64(l.durations[len(l.durations)-1].Nanoseconds())/1000, "max-us")
}

func benchId(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, benchIds.Add(1))
}

// startClose starts and closes a container: a lock acquire and release through the raft log.
func startClose(b *testing.B, n *benchNode, id string) {
	c, _, err := n.StartContainer(context.Background(), id)
	if err != nil {
		b.Fatal(err)
	}
	c.Close()
}

// sendWork sends an event matching the remote transition and waits until the owner executed it.
func sendWork(b *testing.B, c *Container[*wrapperspb.StringValue, *stringValueBuilder]) {
	doneC := make(chan struct{}, 1)
	if err := c.Event("WORK", wrapperspb.Int64(1), WithWaitRemote(), WithDoneChannel(doneC)); err != nil {
		b.Fatal(err)
	}
	<-doneC
}

// BenchmarkLocalEvent measures processing an event with a func transition in a container, without raft.
func BenchmarkLocalEvent(b *testing.B) {
	n := NewNode(1, clusterBitmapByType, func() *stringValueBuilder { return &stringValueBuilder{} }, "",
		WithSkipRaft(), WithBitmap(clusterBitmap("pending", "done")), WithLogger(zap.NewNop()))
	n.AddFuncTransition("work", "WORK", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
		return false, nil
	})
	defer n.Shutdown()

	c, _, err := n.StartContainer(context.Background(), "local")
	if err != nil {
		b.Fatal(err)
	}
	c.Builder().Set(wrapperspb.String("pending"))

	l := newLatencies()
	b.ResetTimer()
	for range b.N {
		start := time.Now()
		doneC := make(chan struct{}, 1)
		if err := c.Event("WORK", wrapperspb.Int64(1), WithDoneChannel(doneC)); err != nil {
			b.Fatal(err)
		}
		<-doneC
		l.add(time.Since(start))
	}
	b.StopTimer()
	l.report(b)
	c.Close()
}

// BenchmarkStartClose measures starting and closing a container on a follower:
// one lock acquire and one release through the raft log.
func BenchmarkStartClose(b *testing.B) {
	for _, size := range []int{3, 5} {
		for _, wal := range []bool{false, true} {
			b.Run(benchName(size, wal), func(b *testing.B) {
				bc := getBenchCluster(b, size, wal)
				l := newLatencies()
				b.ResetTimer()
				for range b.N {
					start := time.Now()
					startClose(b, bc.sender, benchId("start-close"))
					l.add(time.Since(start))
				}
				b.StopTimer()
				l.report(b)
			})
		}
	}
}

// benchName names a sub-benchmark by cluster size and whether the members persist the raft log.
func benchName(size int, wal bool) string {
	if wal {
		return fmt.Sprintf("nodes=%d/wal", size)
	}
	return fmt.Sprintf("nodes=%d", size)
}

// BenchmarkStartCloseParallel measures how many containers per second the cluster can start and close
// when many goroutines start different containers at the same time.
func BenchmarkStartCloseParallel(b *testing.B) {
	for _, size := range []int{3, 5} {
		for _, wal := range []bool{false, true} {
			b.Run(benchName(size, wal), func(b *testing.B) {
				bc := getBenchCluster(b, size, wal)
				l := newLatencies()
				b.SetParallelism(16)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						start := time.Now()
						startClose(b, bc.sender, benchId("start-close-parallel"))
						l.add(time.Since(start))
					}
				})
				b.StopTimer()
				l.report(b)
			})
		}
	}
}

// BenchmarkRemoteExecution measures one forwarded event: the sender evaluates the conditions,
// sends the event to the owner and applies the returned entity. The container is open the whole time.
func BenchmarkRemoteExecution(b *testing.B) {
	for _, size := range []int{3, 5} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			bc := getBenchCluster(b, size)
			c, _, err := bc.sender.StartContainer(context.Background(), benchId("remote"))
			if err != nil {
				b.Fatal(err)
			}
			c.Builder().Set(wrapperspb.String("pending"))

			l := newLatencies()
			executions := benchExecutions.Load()
			b.ResetTimer()
			for range b.N {
				start := time.Now()
				sendWork(b, c)
				l.add(time.Since(start))
			}
			b.StopTimer()
			l.report(b)
			b.ReportMetric(float64(benchExecutions.Load()-executions)/float64(b.N), "executions/op")
			c.Close()
		})
	}
}

// BenchmarkRemoteExecutionParallel measures forwarded events of many containers at the same time.
func BenchmarkRemoteExecutionParallel(b *testing.B) {
	for _, size := range []int{3, 5} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			bc := getBenchCluster(b, size)
			l := newLatencies()
			executions := benchExecutions.Load()
			b.SetParallelism(16)
			b.Cleanup(func() {
				b.ReportMetric(float64(benchExecutions.Load()-executions)/float64(b.N), "executions/op")
			})
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				c, _, err := bc.sender.StartContainer(context.Background(), benchId("remote-parallel"))
				if err != nil {
					b.Error(err)
					return
				}
				defer c.Close()
				c.Builder().Set(wrapperspb.String("pending"))

				for pb.Next() {
					start := time.Now()
					sendWork(b, c)
					l.add(time.Since(start))
				}
			})
			b.StopTimer()
			l.report(b)
		})
	}
}

// BenchmarkSession measures a player session like in the demo, without think times:
// start the container, forward three events to the owner, close it. Sessions run in parallel.
func BenchmarkSession(b *testing.B) {
	for _, size := range []int{3, 5} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			bc := getBenchCluster(b, size)
			l := newLatencies()
			b.SetParallelism(16)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					start := time.Now()
					c, _, err := bc.sender.StartContainer(context.Background(), benchId("session"))
					if err != nil {
						b.Error(err)
						return
					}
					for range 3 {
						c.Builder().Set(wrapperspb.String("pending"))
						sendWork(b, c)
					}
					c.Close()
					l.add(time.Since(start))
				}
			})
			b.StopTimer()
			l.report(b)
		})
	}
}

// BenchmarkHandOverParallel measures hand-overs (forwards without wait) end to end: a container starts, hands an
// event over to the owner and closes; the owner acquires the lock, runs the transition and replies executed.
// The time stops once the owner ran all of them.
func BenchmarkHandOverParallel(b *testing.B) {
	for _, size := range []int{3, 5} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			bc := getBenchCluster(b, size)
			executions := benchExecutions.Load()
			b.SetParallelism(16)
			b.ResetTimer()
			start := time.Now()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c, _, err := bc.sender.StartContainer(context.Background(), benchId("handover"))
					if err != nil {
						b.Error(err)
						return
					}
					c.Builder().Set(wrapperspb.String("pending"))
					if err := c.Event("WORK", wrapperspb.Int64(1)); err != nil {
						b.Error(err)
					}
					c.Close()
				}
			})
			deadline := time.Now().Add(time.Minute)
			for benchExecutions.Load()-executions < int64(b.N) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			b.StopTimer()
			b.ReportMetric(float64(benchExecutions.Load()-executions)/float64(b.N), "executions/op")
			b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "ops/s")
		})
	}
}

// BenchmarkBitmapState measures evaluating an entity into its state and merging it, by bitmap size.
// Every property gets its last value, the worst case of a linear value lookup.
func BenchmarkBitmapState(b *testing.B) {
	for _, size := range []struct{ props, values int }{{8, 4}, {32, 8}, {64, 16}, {128, 32}, {256, 64}} {
		fields := make([]reflect.StructField, size.props)
		properties := make([]*Property, size.props)
		for i := range size.props {
			fields[i] = reflect.StructField{Name: fmt.Sprintf("P%d", i), Type: reflect.TypeFor[string](), Tag: reflect.StructTag(fmt.Sprintf(`json:"p%d"`, i))}
			values := make([]any, size.values)
			for v := range size.values {
				values[v] = fmt.Sprintf("v%d", v)
			}
			properties[i] = &Property{Name: fmt.Sprintf("p%d", i), Values: values}
		}
		pos := 0
		bm := BuildBitmap(&Property{Name: "E", Properties: properties}, &pos)
		entity := reflect.New(reflect.StructOf(fields))
		for i := range size.props {
			entity.Elem().Field(i).SetString(fmt.Sprintf("v%d", size.values-1))
		}
		e := entity.Interface()

		b.Run(fmt.Sprintf("%d_bits", size.props*size.values), func(b *testing.B) {
			b.ReportAllocs()
			var state Bits
			for b.Loop() {
				state = mergeState(bm, state, EvaluateEntityProperties(e, bm))
			}
		})
	}
}
