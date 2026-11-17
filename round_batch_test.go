package asm

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Round batches", func() {
	var (
		n   *roundNode
		log *roundLog
	)
	// describe writes the messages of a batch as "from/seq/value"
	describe := func(msgs []Message) string {
		parts := make([]string, 0, len(msgs))
		for _, m := range msgs {
			parts = append(parts, fmt.Sprintf("%s/%d/%s", m.From, m.Seq, m.Payload.(*TestEvent).Value))
		}
		return strings.Join(parts, " ")
	}

	BeforeEach(func() {
		log = &roundLog{}
		n = newRoundNode()
		// ORDER runs per event and in the round's batch of the market
		n.AddFuncTransition("note", "ORDER", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			log.add("%s noted %s", c.Id(), a.(*TestEvent).Value)
			return false, nil
		})
		n.AddFuncTransition("clear", "ORDER", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			log.add("%s cleared in %q: %s", c.Id(), RoundOf(ctx), describe(a.([]Message)))
			return false, nil
		}, WithBatchOnRound())
		// ORDERS sends the orders in the value to the market, deferred: "m:x,m:y"
		n.AddFuncTransition("orders", "ORDERS", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			for _, order := range strings.Split(a.(*TestEvent).Value, ",") {
				to, value, _ := strings.Cut(order, ":")
				if err := c.Emit(ctx, "ORDER", &TestEvent{Value: c.Id() + value}, WithTarget(to), WithDeferred()); err != nil {
					return false, err
				}
			}
			return false, nil
		})
	})

	AfterEach(func() {
		n.Shutdown()
	})

	It("should run the batch once per round with the round's events sorted by sender and sequence", func() {
		cs := startRoundContainers(n, "m", "c", "a", "b")
		// the senders queue their orders in any order
		var wg sync.WaitGroup
		for _, i := range rand.Perm(3) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()
				sendAndWait(cs[1+i], "ORDERS", &TestEvent{Value: "m:1,m:2"})
			}()
		}
		wg.Wait()

		Expect(n.Round(context.Background(), "one")).To(Succeed())
		Expect(log.get()).To(Equal([]string{
			"m noted a1", "m noted a2", "m noted b1", "m noted b2", "m noted c1", "m noted c2",
			`m cleared in "one": a/0/a1 a/1/a2 b/0/b1 b/1/b2 c/0/c1 c/1/c2`,
		}))

		By("not running it without events")
		Expect(n.Round(context.Background(), "two")).To(Succeed())
		Expect(log.get()).To(HaveLen(7))
	})

	It("should run a batch of each transition and every container", func() {
		// a second round batch of the event, run first; WithBatching after WithBatchOnRound changes nothing
		n.AddFuncTransition("count", "ORDER", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			log.add("%s counted %d", c.Id(), len(a.([]Message)))
			return false, nil
		}, WithBatchOnRound(), WithPriority(1), WithBatching(3, 0))
		cs := startRoundContainers(n, "a", "m", "x")
		sendAndWait(cs[0], "ORDERS", &TestEvent{Value: "m:1,x:2,m:3"})

		Expect(n.Round(context.Background(), "one")).To(Succeed())
		Expect(log.get()).To(ConsistOf(
			"m noted a1", "m noted a3", "m counted 2", `m cleared in "one": a/0/a1 a/2/a3`,
			"x noted a2", "x counted 1", `x cleared in "one": a/1/a2`,
		))
		// the batches after the events, by priority
		var mOnly []string
		for _, e := range log.get() {
			if strings.HasPrefix(e, "m ") {
				mOnly = append(mOnly, e)
			}
		}
		Expect(mOnly).To(Equal([]string{"m noted a1", "m noted a3", "m counted 2", `m cleared in "one": a/0/a1 a/2/a3`}))
	})

	It("should run the transition at once with the event alone on a container without rounds", func() {
		bitmap, _ := createSimpleTestBitmap()
		got := make(chan []Message, 2)
		t := &Transition{Name: "clear", EventName: "ORDER", Batching: Batching{Count: 1},
			Actions: []any{transitionFunc[*TestStateEntity, *mockBuilder](func(ctx context.Context, c *roundContainer, a any) (bool, error) {
				got <- slices.Clone(a.([]Message))
				return false, nil
			})}}
		WithBatchOnRound().apply(t)
		c := NewContainer("m", bitmap, []*Transition{t}, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, newMockBuilder)
		c.start(context.Background(), nil)
		defer c.Close()

		sendAndWait(c, "ORDER", &TestEvent{Value: "x"})
		sendAndWait(c, "ORDER", &TestEvent{Value: "y"})
		Expect(got).To(Receive(Equal([]Message{{Payload: &TestEvent{Value: "x"}}})))
		Expect(got).To(Receive(Equal([]Message{{Payload: &TestEvent{Value: "y"}}})))
	})

	It("should never put a live event into a round batch, only run its other transitions", func() {
		n.Shutdown()
		n = newRoundNode()
		// the market calls x while it clears, x calls back with an order: the reentrant market runs it meanwhile
		n.AddFuncTransition("clear", "ORDER", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			log.add("%s cleared in %q: %s", c.Id(), RoundOf(ctx), describe(a.([]Message)))
			if RoundOf(ctx) == "one" {
				return false, c.Emit(ctx, "PING", &TestEvent{}, WithTarget("x"), WithWaitRemote())
			}
			return false, nil
		}, WithBatchOnRound())
		n.AddFuncTransition("note", "ORDER", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			m, delivered := MessageOf(ctx)
			log.add("%s noted %s (delivered %v from %q)", c.Id(), a.(*TestEvent).Value, delivered, m.From)
			return false, nil
		})
		n.AddFuncTransition("ping", "PING", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, c.Emit(ctx, "ORDER", &TestEvent{Value: "late"}, WithTarget("m"), WithWaitRemote())
		})
		n.AddFuncTransition("orders", "ORDERS", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, c.Emit(ctx, "ORDER", &TestEvent{Value: c.Id() + a.(*TestEvent).Value}, WithTarget("m"), WithDeferred())
		})
		m, _, err := n.StartContainer(context.Background(), "m", WithContainerReentrant())
		Expect(err).NotTo(HaveOccurred())
		cs := startRoundContainers(n, "a", "x")
		sendAndWait(cs[0], "ORDERS", &TestEvent{Value: "1"})

		Expect(n.Round(context.Background(), "one")).To(Succeed())
		Expect(log.get()).To(Equal([]string{
			`m noted a1 (delivered true from "a")`,
			`m cleared in "one": a/0/a1`,
			`m noted late (delivered false from "")`,
		}))

		By("not running the batch for the late order in the next round")
		sendAndWait(cs[0], "ORDERS", &TestEvent{Value: "2"})
		Expect(n.Round(context.Background(), "two")).To(Succeed())
		Expect(log.get()[3:]).To(Equal([]string{`m noted a2 (delivered true from "a")`, `m cleared in "two": a/0/a2`}))

		By("not running a batch for an event which arrived between rounds")
		sendAndWait(m, "ORDER", &TestEvent{Value: "live"})
		Expect(n.Round(context.Background(), "three")).To(Succeed())
		Expect(log.get()[5:]).To(Equal([]string{`m noted live (delivered false from "")`}))
	})

	It("should give a transition of a delivered event its message, not the events it emits", func() {
		got := make(chan string, 4)
		n.AddFuncTransition("first", "FIRST", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			m, ok := MessageOf(ctx)
			got <- fmt.Sprintf("first %v %s/%d/%s", ok, m.From, m.Seq, m.Payload.(*TestEvent).Value)
			// an event the transition emits is live
			return false, c.Emit(ctx, "SECOND", &TestEvent{Value: "inner"})
		})
		n.AddFuncTransition("second", "SECOND", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			_, ok := MessageOf(ctx)
			got <- fmt.Sprintf("second %v", ok)
			return false, nil
		})
		n.AddFuncTransition("send", "SEND", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			if err := c.Emit(ctx, "ORDER", &TestEvent{Value: "o"}, WithTarget("b"), WithDeferred()); err != nil {
				return false, err
			}
			return false, c.Emit(ctx, "FIRST", &TestEvent{Value: "x"}, WithTarget("b"), WithDeferred())
		})
		cs := startRoundContainers(n, "a", "b")
		sendAndWait(cs[0], "SEND", nil)
		_, ok := MessageOf(context.Background())
		Expect(ok).To(BeFalse())

		Expect(n.Round(context.Background(), "one")).To(Succeed())
		Expect(got).To(Receive(Equal("first true a/1/x")))
		Expect(got).To(Receive(Equal("second false")))
	})

	It("should refuse a round batch transition which is read-only", func() {
		t := &Transition{Name: "clear", EventName: "ORDER"}
		Expect(checkRoundBatch(t, true)).To(Succeed())
		WithBatchOnRound().apply(t)
		Expect(checkRoundBatch(t, false)).To(Succeed())
		Expect(checkRoundBatch(t, true)).To(MatchError(ContainSubstring("can't be read-only")))

		// a round batch never runs as a reader
		t.ReadOnly = true
		t.Actions = []any{transitionFunc[*TestStateEntity, *mockBuilder](func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, nil
		})}
		Expect(t.readOnlyFunc()).To(BeFalse())
		t.roundBatch = false
		Expect(t.readOnlyFunc()).To(BeTrue())
	})

	It("should fail the node compiling a round batch transition marked read-only", func() {
		failing := newRoundNode()
		defer failing.Shutdown()
		failing.AddFuncTransition("clear", "ORDER", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, nil
		}, WithBatchOnRound(), WithReadOnly())
		Eventually(failing.Err(), 5*time.Second).Should(Receive(MatchError(ContainSubstring("can't be read-only"))))
	})
})

var _ = Describe("Rounds with reader transitions", func() {
	var (
		n    *roundNode
		errC chan error
	)

	BeforeEach(func() {
		n = newRoundNode()
		errC = make(chan error, 3)
		// PEEK is a read event: it runs as a reader, which can't take part in rounds
		n.AddFuncTransition("peek", "PEEK", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			errC <- c.Emit(ctx, "ORDER", &TestEvent{Value: "x"}, WithTarget(c.Id()), WithDeferred())
			errC <- c.Spawn(ctx, c.Id()+"-kid", nil)
			errC <- c.CloseAfterRound(ctx)
			return false, nil
		}, WithReadOnly())
		n.AddFuncTransition("spawn", "SPAWN", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, c.Spawn(ctx, a.(*TestEvent).Value, &TestStateEntity{Count: 7})
		})
		n.AddFuncTransition("order", "ORDER", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, nil
		})
	})

	AfterEach(func() {
		n.Shutdown()
	})

	expectReadOnly := func() {
		GinkgoHelper()
		for range 3 {
			var err error
			Eventually(errC, 5*time.Second).Should(Receive(&err))
			Expect(errors.Is(err, ErrReadOnly)).To(BeTrue(), "%v", err)
		}
	}

	It("should refuse a reader's deferred events, spawns and closes with ErrReadOnly", func() {
		c := startRoundContainers(n, "a")[0]
		Expect(c.Event("PEEK", &TestEvent{})).To(Succeed())
		expectReadOnly()
		Expect(n.Round(context.Background(), "one")).To(Succeed())
		Expect(n.hasContainer("a-kid")).To(BeFalse())
		Expect(n.hasContainer("a")).To(BeTrue())
	})

	It("should run read events of a spawned container as readers", func() {
		c := startRoundContainers(n, "a")[0]
		sendAndWait(c, "SPAWN", &TestEvent{Value: "kid"})
		Expect(n.Round(context.Background(), "one")).To(Succeed())
		n.containerMap.mu.RLock()
		kid, ok := n.containerMap.m["kid"]
		n.containerMap.mu.RUnlock()
		Expect(ok).To(BeTrue())
		Expect(kid.Builder().Build().Count).To(BeNumerically("==", 7))

		// as a writer the reader's deferred event, spawn and close would succeed
		Expect(kid.Event("PEEK", &TestEvent{})).To(Succeed())
		expectReadOnly()
	})
})
