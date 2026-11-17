package asm

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Rounds running all containers", func() {
	var (
		n   *roundNode
		log *roundLog
	)

	BeforeEach(func() {
		log = &roundLog{}
		n = newRoundNode()
	})

	AfterEach(func() {
		n.Shutdown()
	})

	// step adds STEP round batches: step logs its messages, done only runs for completed containers
	addSteps := func() {
		n.AddFuncTransition("step", "STEP", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			msgs := a.([]Message)
			Expect(msgs).NotTo(BeNil())
			log.add("%s stepped in %q: %d", c.Id(), RoundOf(ctx), len(msgs))
			return false, nil
		}, WithBatchOnRound())
		n.AddFuncTransition("done", "STEP", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			log.add("%s done: %d", c.Id(), len(a.([]Message)))
			return false, nil
		}, WithBatchOnRound(), WithConditions(n.All(&TestStateEntity{Status: "completed"})))
		n.AddFuncTransition("send", "SEND", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, c.Emit(ctx, "STEP", &TestEvent{}, WithTarget(a.(*TestEvent).Value), WithDeferred())
		})
		n.AddFuncTransition("spawn", "SPAWN", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, c.Spawn(ctx, a.(*TestEvent).Value, nil)
		})
	}

	It("should run the round batches of every open container, those without events with an empty batch", func() {
		addSteps()
		cs := startRoundContainers(n, "a", "b", "c")
		cs[2].Builder().Set(&TestStateEntity{Status: "completed"})

		By("running only the targets without it")
		sendAndWait(cs[0], "SEND", &TestEvent{Value: "b"})
		Expect(n.Round(context.Background(), "one")).To(Succeed())
		Expect(log.get()).To(Equal([]string{`b stepped in "one": 1`}))

		By("running every container with it, once each")
		sendAndWait(cs[0], "SEND", &TestEvent{Value: "b"})
		sendAndWait(cs[0], "SPAWN", &TestEvent{Value: "d"})
		Expect(n.Round(context.Background(), "two", WithRunAll())).To(Succeed())
		Expect(log.get()[1:]).To(ConsistOf(`a stepped in "two": 0`, `b stepped in "two": 1`, `c done: 0`,
			`c stepped in "two": 0`, `d stepped in "two": 0`))
		// by priority, then name, in each container
		Expect(slices.Index(log.get(), `c done: 0`)).To(BeNumerically("<", slices.Index(log.get(), `c stepped in "two": 0`)))

		By("leaving closed containers out")
		cs[1].Close()
		Expect(n.Round(context.Background(), "three", WithRunAll())).To(Succeed())
		Expect(log.get()[6:]).To(ConsistOf(`a stepped in "three": 0`, `c done: 0`, `c stepped in "three": 0`,
			`d stepped in "three": 0`))
	})

	It("should run a container spawned in a round from the next round on", func() {
		addSteps()
		n.AddFuncTransition("grow", "GROW", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, c.Spawn(ctx, a.(*TestEvent).Value, nil)
		})
		n.AddFuncTransition("defer", "DEFER", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, c.Emit(ctx, "GROW", a, WithTarget(c.Id()), WithDeferred())
		})
		cs := startRoundContainers(n, "a")
		sendAndWait(cs[0], "DEFER", &TestEvent{Value: "e"})
		Expect(n.Round(context.Background(), "one", WithRunAll())).To(Succeed())
		Expect(log.get()).To(Equal([]string{`a stepped in "one": 0`}))
		Expect(n.hasContainer("e")).To(BeTrue())

		Expect(n.Round(context.Background(), "two", WithRunAll())).To(Succeed())
		Expect(log.get()[1:]).To(ConsistOf(`a stepped in "two": 0`, `e stepped in "two": 0`))
	})

	It("should run containers waiting for each other's rounds, more of them than the dispatch has workers", func() {
		const containers = 300
		n.Shutdown()
		n = newRoundNode(WithMaxContainerCount(containers))
		var arrived atomic.Int32
		n.AddFuncTransition("meet", "MEET", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			// every container waits until all of them run: the dispatch's workers alone would wait forever
			arrived.Add(1)
			deadline := time.Now().Add(10 * time.Second)
			for arrived.Load() < containers {
				if time.Now().After(deadline) {
					return false, fmt.Errorf("%s: only %d containers ran", c.Id(), arrived.Load())
				}
				time.Sleep(100 * time.Microsecond)
			}
			return false, nil
		}, WithBatchOnRound())
		for i := range containers {
			startRoundContainers(n, fmt.Sprintf("m%03d", i))
		}
		Expect(n.Round(context.Background(), "meet", WithRunAll())).To(Succeed())
		Expect(arrived.Load()).To(BeNumerically("==", containers))
	})

	It("should change nothing on a node without round batches", func() {
		var ran atomic.Int32
		n.AddFuncTransition("tick", "TICK", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			ran.Add(1)
			return false, nil
		})
		startRoundContainers(n, "a", "b")
		Expect(n.Round(context.Background(), "one", WithRunAll())).To(Succeed())
		Expect(ran.Load()).To(BeZero())
	})

	It("should step every container the same way in every run", func() {
		const containers, rounds, runs = 50, 8, 3
		id := func(i int) string { return fmt.Sprintf("c%02d", i) }
		var first []string
		for run := range runs {
			n.Shutdown()
			n = newRoundNode()
			log = &roundLog{}
			// every container sends its round number to two others in every round, without any event to start
			n.AddFuncTransition("step", "STEP", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				perturb()
				var from []string
				for _, m := range a.([]Message) {
					from = append(from, fmt.Sprintf("%s/%d", m.From, m.Seq))
				}
				log.add("%s %s: %s", RoundOf(ctx), c.Id(), strings.Join(from, ","))
				var i, r int
				fmt.Sscanf(c.Id(), "c%d", &i)
				fmt.Sscanf(RoundOf(ctx), "r%d", &r)
				for _, to := range []int{(i + r + 1) % containers, (i*3 + r) % containers} {
					if err := c.Emit(ctx, "STEP", nil, WithTarget(id(to)), WithDeferred()); err != nil {
						return false, err
					}
				}
				return false, nil
			}, WithBatchOnRound())
			for i := range containers {
				startRoundContainers(n, id(i))
			}
			for r := range rounds {
				Expect(n.Round(context.Background(), fmt.Sprintf("r%d", r), WithRunAll())).To(Succeed())
			}
			got := log.get()
			Expect(got).To(HaveLen(containers * rounds))
			slices.Sort(got)
			if run == 0 {
				first = got
				continue
			}
			Expect(got).To(Equal(first), "run %d", run)
		}
	})
})

var _ = Describe("Strict round overlap", func() {
	var n *roundNode

	BeforeEach(func() {
		n = newRoundNode()
		n.AddFuncTransition("quick", "QUICK", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, nil
		})
		// HAND hands QUICK over to b and waits until it ran: nothing runs on after the round
		n.AddFuncTransition("hand", "HAND", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			if err := c.Emit(ctx, "QUICK", nil, WithTarget("b")); err != nil {
				return false, err
			}
			deadline := time.Now().Add(5 * time.Second)
			for n.rounds.background.Load() != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			return false, nil
		})
		n.AddFuncTransition("send", "SEND", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, c.Emit(ctx, "HAND", nil, WithTarget("a"), WithDeferred())
		})
	})

	AfterEach(func() {
		n.Shutdown()
	})

	It("should fail a round during which a hand-over without wait started, also one done before it ended", func() {
		cs := startRoundContainers(n, "a", "b")
		sendAndWait(cs[1], "SEND", nil)
		err := n.Round(context.Background(), "one")
		Expect(err).To(MatchError(ErrRoundOverlap))
		Expect(err).To(MatchError(ContainSubstring("1 hand-overs without wait or batch timers started during the round, 0 running after it")))
		Expect(n.Round(context.Background(), "two")).To(Succeed())
	})

	It("should not count a hand-over started and done between the rounds", func() {
		cs := startRoundContainers(n, "a", "b")
		sendAndWait(cs[0], "HAND", nil)
		Eventually(n.rounds.background.Load).Should(BeZero())
		Expect(n.Round(context.Background(), "one")).To(Succeed())
	})
})

var _ = Describe("Rounds in immutable-entity mode", func() {
	It("should copy no entity on the round path, unlike copy mode, and count the copies in asm_entity_copies_total", func() {
		var copies atomic.Int64
		count := func() { copies.Add(1) }
		entityCopyHook.Store(&count)
		DeferCleanup(func() { entityCopyHook.Store(nil) })

		for _, immutable := range []bool{true, false} {
			var opts []NodeSetupOption
			if immutable {
				opts = append(opts, WithImmutableEntities())
			}
			n := newRoundNode(opts...)
			// a read-only transition makes every writer publish the entity for readers
			n.AddFuncTransition("peek", "PEEK", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				return false, nil
			}, WithReadOnly())
			n.AddFuncTransition("tick", "TICK", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				c.Builder().Set(&TestStateEntity{Count: 1})
				return false, c.Emit(ctx, "TICK", nil, WithTarget(c.Id()), WithDeferred())
			})
			n.AddFuncTransition("step", "TICK", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				return false, nil
			}, WithBatchOnRound())
			cs := startRoundContainers(n, "a", "b")
			for _, c := range cs {
				c.Builder().Set(&TestStateEntity{})
				sendAndWait(c, "TICK", nil)
			}
			copies.Store(0)
			before := metricValue(n.collectMetrics(), "asm_entity_copies_total")
			for r := range 3 {
				Expect(n.Round(context.Background(), fmt.Sprint(r), WithRunAll())).To(Succeed())
			}
			counted := metricValue(n.collectMetrics(), "asm_entity_copies_total")
			if immutable {
				Expect(copies.Load()).To(BeZero())
				Expect(before).To(BeZero())
				Expect(counted).To(BeZero())
			} else {
				Expect(copies.Load()).To(BeNumerically(">=", 6))
				Expect(counted - before).To(BeNumerically("==", copies.Load()))
			}
			n.Shutdown()
		}
	})
})
