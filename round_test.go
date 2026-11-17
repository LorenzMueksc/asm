package asm

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

type roundNode = Node[*TestStateEntity, *mockBuilder]
type roundContainer = Container[*TestStateEntity, *mockBuilder]

// newRoundNode starts a standalone node for round specs.
func newRoundNode(opts ...NodeSetupOption) *roundNode {
	bitmap, _ := createSimpleTestBitmap()
	n := NewNode(1, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "",
		append([]NodeSetupOption{WithSkipRaft(), WithBitmap(bitmap), WithLogger(zap.NewNop())}, opts...)...)
	return n
}

// startRoundContainers starts containers with the ids, all held by the caller.
func startRoundContainers(n *roundNode, ids ...string) []*roundContainer {
	GinkgoHelper()
	cs := make([]*roundContainer, 0, len(ids))
	for _, id := range ids {
		c, _, err := n.StartContainer(context.Background(), id)
		Expect(err).NotTo(HaveOccurred())
		cs = append(cs, c)
	}
	return cs
}

// sendAndWait sends an event to the container and waits until it is done.
func sendAndWait(c *roundContainer, event string, payload any) {
	GinkgoHelper()
	doneC := make(chan struct{}, 1)
	Expect(c.Event(event, payload, WithDoneChannel(doneC))).To(Succeed())
	Eventually(doneC).Should(Receive())
}

// roundLog records what the containers of round specs did, safe for concurrent use.
type roundLog struct {
	mu      sync.Mutex
	entries []string
}

func (l *roundLog) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, fmt.Sprintf(format, args...))
}

func (l *roundLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.entries)
}

// perturb changes the scheduling of the goroutine running a transition at random.
func perturb() {
	switch rand.IntN(8) {
	case 0:
		runtime.Gosched()
	case 1:
		time.Sleep(time.Duration(rand.IntN(100)) * time.Microsecond)
	case 2:
		// spin a little
		x := 0
		for i := range rand.IntN(20000) {
			x += i
		}
		_ = x
	}
}

var _ = Describe("Rounds", func() {
	It("should deliver the same events in the same order to every container in every run", func() {
		const containers, rounds, runs = 200, 20, 5

		// ids sort like their numbers
		id := func(i int) string { return fmt.Sprintf("c%03d", i) }
		// the targets of container i in round r: the first one is a permutation of the containers, so every container
		// receives events in every round and sends in the next
		targetsOf := func(i, r int) []int {
			return []int{(i + 1 + r) % containers, (i*7 + r) % containers, (i*13 + 5 + 3*r) % containers, (i + r*r) % containers}
		}

		// contention: goroutines burning the CPUs while the rounds run
		stopC := make(chan struct{})
		var burners sync.WaitGroup
		for range runtime.NumCPU() {
			burners.Add(1)
			go func() {
				defer burners.Done()
				x := 0
				for {
					select {
					case <-stopC:
						return
					default:
						for i := range 10000 {
							x += i
						}
						runtime.Gosched()
					}
				}
			}()
		}
		defer func() {
			close(stopC)
			burners.Wait()
		}()

		runOnce := func(run int) map[string][]string {
			// every container's inbox: "round from seq", each written only by the transitions of its container
			inboxes := make(map[string]*[]string, containers)
			index := make(map[string]int, containers)
			for i := range containers {
				inboxes[id(i)] = &[]string{}
				index[id(i)] = i
			}
			// the round in which a container sent last, also written only by its own transitions
			sentIn := make(map[string]*string, containers)
			for i := range containers {
				sentIn[id(i)] = new(string)
			}

			opts := []NodeSetupOption{WithMaxContainerCount(containers)}
			if run%2 == 1 {
				// a goroutine per event instead of keeping it a while
				opts = append(opts, WithContainerKeepAlive(0))
			}
			n := newRoundNode(opts...)
			defer n.Shutdown()

			send := func(ctx context.Context, c *roundContainer, round int) error {
				for seq, t := range targetsOf(index[c.Id()], round) {
					perturb()
					if err := c.Emit(ctx, "MSG", &TestEvent{Value: c.Id(), Amount: int64(seq)}, WithTarget(id(t)), WithDeferred()); err != nil {
						return err
					}
				}
				return nil
			}
			n.AddFuncTransition("start", "START", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				return false, send(ctx, c, 0)
			})
			n.AddFuncTransition("receive", "MSG", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				perturb()
				round := RoundOf(ctx)
				e := a.(*TestEvent)
				inbox := inboxes[c.Id()]
				*inbox = append(*inbox, fmt.Sprintf("%s %s %d", round, e.Value, e.Amount))
				// the first event of a round makes the container send
				if *sentIn[c.Id()] == round {
					return false, nil
				}
				*sentIn[c.Id()] = round
				var r int
				_, _ = fmt.Sscanf(round, "r%d", &r)
				return false, send(ctx, c, r)
			})

			cs := make([]*roundContainer, containers)
			for _, i := range rand.Perm(containers) {
				c, _, err := n.StartContainer(context.Background(), id(i))
				Expect(err).NotTo(HaveOccurred())
				cs[i] = c
			}
			// the first events from many goroutines, in any order
			var started sync.WaitGroup
			for _, i := range rand.Perm(containers) {
				started.Add(1)
				go func() {
					defer started.Done()
					defer GinkgoRecover()
					sendAndWait(cs[i], "START", &TestEvent{})
				}()
			}
			started.Wait()

			for r := 1; r <= rounds; r++ {
				Expect(n.Round(context.Background(), fmt.Sprintf("r%d", r))).To(Succeed())
			}

			result := make(map[string][]string, containers)
			for name, inbox := range inboxes {
				result[name] = *inbox
			}
			return result
		}

		var first map[string][]string
		for run := range runs {
			got := runOnce(run)
			for name, inbox := range got {
				// every container received events in every round, sorted by (sender, sequence)
				for r := 1; r <= rounds; r++ {
					prefix := fmt.Sprintf("r%d ", r)
					var inRound []string
					for _, e := range inbox {
						if strings.HasPrefix(e, prefix) {
							inRound = append(inRound, e)
						}
					}
					Expect(inRound).NotTo(BeEmpty(), "container %s in round %d", name, r)
					Expect(slices.IsSorted(inRound)).To(BeTrue(), "container %s in round %d: %v", name, r, inRound)
				}
			}
			if first == nil {
				first = got
				continue
			}
			Expect(got).To(Equal(first), "run %d", run)
		}
	})

	Context("on a node", func() {
		var (
			n   *roundNode
			log *roundLog
		)

		BeforeEach(func() {
			log = &roundLog{}
			n = newRoundNode()
			// MSG relays the event along the path in its value, one hop per round
			n.AddFuncTransition("relay", "MSG", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				log.add("%s got %s in %q (node in %q)", c.Id(), a.(*TestEvent).Value, RoundOf(ctx), n.RoundName())
				next, rest, _ := strings.Cut(a.(*TestEvent).Value, ",")
				if next == "" {
					return false, nil
				}
				return false, c.Emit(ctx, "MSG", &TestEvent{Value: rest}, WithTarget(next), WithDeferred())
			})
			n.AddFuncTransition("start", "START", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				log.add("%s started in %q", c.Id(), RoundOf(ctx))
				next, rest, _ := strings.Cut(a.(*TestEvent).Value, ",")
				return false, c.Emit(ctx, "MSG", &TestEvent{Value: rest}, WithTarget(next), WithDeferred())
			})
		})

		AfterEach(func() {
			n.Shutdown()
		})

		It("should deliver the events emitted in a round in the next one", func() {
			cs := startRoundContainers(n, "a", "b", "c")
			sendAndWait(cs[0], "START", &TestEvent{Value: "b,c,a"})
			Expect(log.get()).To(Equal([]string{`a started in ""`}))

			Expect(n.Round(context.Background(), "one")).To(Succeed())
			Expect(log.get()[1:]).To(Equal([]string{`b got c,a in "one" (node in "one")`}))
			Expect(n.Round(context.Background(), "two")).To(Succeed())
			Expect(log.get()[2:]).To(Equal([]string{`c got a in "two" (node in "two")`}))
			Expect(n.Round(context.Background(), "three")).To(Succeed())
			Expect(log.get()[3:]).To(Equal([]string{`a got  in "three" (node in "three")`}))
			Expect(n.RoundName()).To(BeEmpty())

			By("running nothing without deferred events")
			Expect(n.Round(context.Background(), "four")).To(Succeed())
			Expect(log.get()).To(HaveLen(4))
		})

		It("should deliver a deferred event to the sender itself in the next round", func() {
			cs := startRoundContainers(n, "a")
			sendAndWait(cs[0], "START", &TestEvent{Value: "a,a"})
			Expect(n.Round(context.Background(), "one")).To(Succeed())
			Expect(n.Round(context.Background(), "two")).To(Succeed())
			Expect(log.get()).To(Equal([]string{`a started in ""`, `a got a in "one" (node in "one")`, `a got  in "two" (node in "two")`}))
		})

		It("should return once every container ran its inbox", func() {
			var running, ran atomic.Int32
			n.AddFuncTransition("slow", "SLOW", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				running.Add(1)
				time.Sleep(20 * time.Millisecond)
				ran.Add(1)
				return false, nil
			})
			n.AddFuncTransition("fan", "FAN", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				for _, to := range []string{"a", "b", "c", "d"} {
					for range 3 {
						if err := c.Emit(ctx, "SLOW", nil, WithTarget(to), WithDeferred()); err != nil {
							return false, err
						}
					}
				}
				return false, nil
			})
			cs := startRoundContainers(n, "a", "b", "c", "d")
			sendAndWait(cs[0], "FAN", nil)

			start := time.Now()
			Expect(n.Round(context.Background(), "slow")).To(Succeed())
			Expect(ran.Load()).To(Equal(int32(12)))
			// the containers ran concurrently, each its 3 events in a row
			Expect(time.Since(start)).To(BeNumerically("<", 12*20*time.Millisecond))
		})

		It("should return an unknown target as error, deliver nothing and keep the outboxes", func() {
			cs := startRoundContainers(n, "a", "b")
			sendAndWait(cs[0], "START", &TestEvent{Value: "nobody"})
			sendAndWait(cs[1], "START", &TestEvent{Value: "a"})

			err := n.Round(context.Background(), "one")
			Expect(err).To(MatchError(ErrUnknownTarget))
			Expect(err).To(MatchError(ContainSubstring("container nobody")))
			Expect(log.get()).To(HaveLen(2))
			Expect(n.RoundName()).To(BeEmpty())

			By("delivering everything once the target is open")
			startRoundContainers(n, "nobody")
			Expect(n.Round(context.Background(), "two")).To(Succeed())
			Expect(log.get()[2:]).To(ConsistOf(`a got  in "two" (node in "two")`, `nobody got  in "two" (node in "two")`))

			By("failing for a container closed meanwhile, and dropping the outboxes")
			sendAndWait(cs[0], "START", &TestEvent{Value: "b"})
			cs[1].Close()
			Expect(n.Round(context.Background(), "three")).To(MatchError(ErrUnknownTarget))
			Expect(n.Round(context.Background(), "three")).To(MatchError(ErrUnknownTarget))
			Expect(n.DropOutboxes(context.Background())).To(Equal(1))
			Expect(n.Round(context.Background(), "four")).To(Succeed())
			Expect(log.get()).To(HaveLen(5))
		})

		It("should fail a round whose hand-over without wait or batch timer runs on after it", func() {
			n.AddFuncTransition("hand", "HAND", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				// handed over without waiting: it runs in b on its own
				return false, c.Emit(ctx, "SLEEP", nil, WithTarget("b"))
			})
			n.AddFuncTransition("sleep", "SLEEP", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				time.Sleep(200 * time.Millisecond)
				return false, nil
			})
			n.AddFuncTransition("batch", "BATCH", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				return false, nil
			}, WithBatching(10, 200*time.Millisecond))
			n.AddFuncTransition("send", "SEND", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				return false, c.Emit(ctx, a.(*TestEvent).Value, nil, WithTarget("a"), WithDeferred())
			})
			cs := startRoundContainers(n, "a", "b")

			sendAndWait(cs[1], "SEND", &TestEvent{Value: "HAND"})
			Expect(n.Round(context.Background(), "one")).To(MatchError(ErrRoundOverlap))
			Eventually(n.rounds.background.Load).Should(BeZero())
			Expect(n.Round(context.Background(), "two")).To(Succeed())

			By("a batch timer")
			sendAndWait(cs[1], "SEND", &TestEvent{Value: "BATCH"})
			Expect(n.Round(context.Background(), "three")).To(MatchError(ContainSubstring("1 hand-overs without wait or batch timers")))
			Eventually(n.rounds.background.Load).Should(BeZero())
			Expect(n.Round(context.Background(), "four")).To(Succeed())
		})

		It("should return the error of the container with the lowest id and pass all to the error func", func() {
			var errorFuncGot atomic.Int32
			n.Shutdown()
			n = newRoundNode(WithErrorFunc(func(ctx context.Context, c *roundContainer, err error) error {
				errorFuncGot.Add(1)
				return nil
			}))
			n.AddFuncTransition("fail", "FAIL", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				// the containers with higher ids fail first
				time.Sleep(time.Duration(5*(int('z')-int(c.Id()[0]))) * time.Millisecond)
				return false, errors.New(c.Id() + " failed")
			})
			n.AddFuncTransition("send", "SEND", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				for _, to := range []string{"d", "b", "c"} {
					if err := c.Emit(ctx, "FAIL", nil, WithTarget(to), WithDeferred()); err != nil {
						return false, err
					}
				}
				return false, nil
			})
			cs := startRoundContainers(n, "a", "b", "c", "d")
			sendAndWait(cs[0], "SEND", nil)

			Expect(n.Round(context.Background(), "one")).To(MatchError(`round one: container b: b failed`))
			Eventually(errorFuncGot.Load).Should(Equal(int32(3)))

			By("not returning errors of events outside of rounds")
			sendAndWait(cs[0], "FAIL", nil)
			Expect(n.Round(context.Background(), "two")).To(Succeed())
		})

		It("should run one round at a time and let ctx bound the wait", func() {
			release := make(chan struct{})
			// a failed expectation must not leave the transition blocked, Shutdown would wait for it
			releaseOnce := sync.OnceFunc(func() { close(release) })
			defer releaseOnce()
			n.AddFuncTransition("block", "BLOCK", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				<-release
				return false, nil
			})
			n.AddFuncTransition("send", "SEND", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				return false, c.Emit(ctx, "BLOCK", nil, WithTarget("b"), WithDeferred())
			})
			cs := startRoundContainers(n, "a", "b")
			sendAndWait(cs[0], "SEND", nil)

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			Expect(n.Round(ctx, "blocked")).To(MatchError(context.DeadlineExceeded))
			Expect(n.RoundName()).To(Equal("blocked"))

			By("waiting for the round going on")
			ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel2()
			Expect(n.Round(ctx2, "next")).To(MatchError(context.DeadlineExceeded))

			releaseOnce()
			Eventually(n.RoundName).Should(BeEmpty())
			Expect(n.Round(context.Background(), "next")).To(Succeed())
		})
	})
})

// BenchmarkRound measures rounds of 10k containers: empty ones, ones delivering one event to every container, ones
// running a round batch of every container with one event each (the batch path), and ones running the round batch of
// every container without events (WithRunAll).
//
//	go test . -run '^$' -bench BenchmarkRound -benchmem
func BenchmarkRound(b *testing.B) {
	const containers = 10000
	n := newRoundNode(WithMaxContainerCount(containers))
	defer n.Shutdown()
	// TICK sends TICK to the container its value names, delivered in the next round
	n.AddFuncTransition("tick", "TICK", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
		return false, c.Emit(ctx, "TICK", a, WithTarget(a.(*TestEvent).Value), WithDeferred())
	})
	// STEP is a round batch which sends STEP to its container for the next round if it got one: KICK starts it
	n.AddFuncTransition("step", "STEP", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
		if len(a.([]Message)) == 0 {
			return false, nil
		}
		return false, c.Emit(ctx, "STEP", nil, WithTarget(c.Id()), WithDeferred())
	}, WithBatchOnRound())
	n.AddFuncTransition("kick", "KICK", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
		return false, c.Emit(ctx, "STEP", nil, WithTarget(c.Id()), WithDeferred())
	})
	cs := make([]*roundContainer, containers)
	for i := range containers {
		c, _, err := n.StartContainer(context.Background(), fmt.Sprintf("c%05d", i))
		if err != nil {
			b.Fatal(err)
		}
		cs[i] = c
	}

	b.Run("empty", func(b *testing.B) {
		for b.Loop() {
			if err := n.Round(context.Background(), "empty"); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("event-per-container", func(b *testing.B) {
		// every container sends to the next one in every round
		for i, c := range cs {
			doneC := make(chan struct{}, 1)
			if err := c.Event("TICK", &TestEvent{Value: fmt.Sprintf("c%05d", (i+1)%containers)}, WithDoneChannel(doneC)); err != nil {
				b.Fatal(err)
			}
			<-doneC
		}
		b.ResetTimer()
		for b.Loop() {
			if err := n.Round(context.Background(), "full"); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/containers, "ns/event")
	})

	b.Run("batch-per-container", func(b *testing.B) {
		if _, err := n.DropOutboxes(context.Background()); err != nil {
			b.Fatal(err)
		}
		for _, c := range cs {
			doneC := make(chan struct{}, 1)
			if err := c.Event("KICK", nil, WithDoneChannel(doneC)); err != nil {
				b.Fatal(err)
			}
			<-doneC
		}
		b.ResetTimer()
		for b.Loop() {
			if err := n.Round(context.Background(), "batch"); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/containers, "ns/container")
	})

	b.Run("run-all-empty", func(b *testing.B) {
		if _, err := n.DropOutboxes(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for b.Loop() {
			if err := n.Round(context.Background(), "all", WithRunAll()); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/containers, "ns/container")
	})
}
