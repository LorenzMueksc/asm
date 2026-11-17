package asm

import (
	"context"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"rumue.io/asm/service"
)

var _ = Describe("Spawning and closing containers in rounds", func() {
	var (
		n   *roundNode
		log *roundLog
	)

	// setup registers the transitions of the specs:
	//   - SEND defers the events in the value, "to:EVENT:value;...";
	//   - SPLIT spawns a child "<id>/<child>" per child in the value ("b,a") and defers a HELLO to each;
	//   - HELLO records what a container got and the status of its entity;
	//   - SPAWN spawns the container the value names, without entity;
	//   - BYE defers a HELLO to y and closes the container after the round.
	setup := func(n *roundNode) {
		n.AddFuncTransition("send", "SEND", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			for _, msg := range strings.Split(a.(*TestEvent).Value, ";") {
				parts := strings.SplitN(msg, ":", 3)
				if err := c.Emit(ctx, parts[1], &TestEvent{Value: parts[2]}, WithTarget(parts[0]), WithDeferred()); err != nil {
					return false, err
				}
			}
			return false, nil
		})
		n.AddFuncTransition("split", "SPLIT", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			for _, child := range strings.Split(a.(*TestEvent).Value, ",") {
				id := c.Id() + "/" + child
				if err := c.Spawn(ctx, id, &TestStateEntity{Status: "child of " + c.Id()}); err != nil {
					return false, err
				}
				if err := c.Emit(ctx, "HELLO", &TestEvent{Value: c.Id()}, WithTarget(id), WithDeferred()); err != nil {
					return false, err
				}
			}
			return false, nil
		})
		n.AddFuncTransition("spawn", "SPAWN", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, c.Spawn(ctx, a.(*TestEvent).Value, nil)
		})
		n.AddFuncTransition("hello", "HELLO", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			status := ""
			if e := c.Builder().Build(); e != nil {
				status = e.Status
			}
			log.add("%s got hello from %s in %q as %q", c.Id(), a.(*TestEvent).Value, RoundOf(ctx), status)
			return false, nil
		})
		n.AddFuncTransition("bye", "BYE", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			log.add("%s closes in %q", c.Id(), RoundOf(ctx))
			if err := c.Emit(ctx, "HELLO", &TestEvent{Value: c.Id()}, WithTarget("y"), WithDeferred()); err != nil {
				return false, err
			}
			return false, c.CloseAfterRound(ctx)
		})
	}

	// started returns the ids of the containers in the order they started, from the event trace
	started := func(n *roundNode) []string {
		n.trace.mu.Lock()
		defer n.trace.mu.Unlock()
		var ids []string
		for _, e := range n.trace.entries {
			if e.GetKind() == service.TraceEvent_CONTAINER_STARTED {
				ids = append(ids, e.GetContainerId())
			}
		}
		return ids
	}

	BeforeEach(func() {
		log = &roundLog{}
		n = newRoundNode(WithEventTrace(1000))
		setup(n)
	})

	AfterEach(func() {
		n.Shutdown()
	})

	It("should start children spawned in a round at its end in id order and deliver what was deferred to them next", func() {
		runOnce := func() ([]string, []string) {
			log = &roundLog{}
			n := newRoundNode(WithEventTrace(1000))
			defer n.Shutdown()
			setup(n)
			cs := startRoundContainers(n, "s", "q", "p")
			// round one: the parents get SPLIT, the children start at its end
			sendAndWait(cs[0], "SEND", &TestEvent{Value: "q:SPLIT:b,a;p:SPLIT:b,a"})

			Expect(n.Round(context.Background(), "one")).To(Succeed())
			Expect(log.get()).To(BeEmpty())
			Expect(started(n)).To(Equal([]string{"s", "q", "p", "p/a", "p/b", "q/a", "q/b"}))
			resp, err := n.Inspect(context.Background(), &service.InspectRequest{})
			Expect(err).NotTo(HaveOccurred())
			var ids []string
			for _, info := range resp.GetContainers() {
				ids = append(ids, info.GetId())
			}
			Expect(ids).To(Equal([]string{"p", "p/a", "p/b", "q", "q/a", "q/b", "s"}))

			// round two: the children get their HELLO
			Expect(n.Round(context.Background(), "two")).To(Succeed())
			got := log.get()
			slices.Sort(got)
			return got, started(n)
		}

		got, order := runOnce()
		Expect(got).To(Equal([]string{
			`p/a got hello from p in "two" as "child of p"`,
			`p/b got hello from p in "two" as "child of p"`,
			`q/a got hello from q in "two" as "child of q"`,
			`q/b got hello from q in "two" as "child of q"`,
		}))
		Expect(order).To(Equal([]string{"s", "q", "p", "p/a", "p/b", "q/a", "q/b"}))

		By("starting the same containers in the same order again")
		for range 3 {
			got2, order2 := runOnce()
			Expect(got2).To(Equal(got))
			Expect(order2).To(Equal(order))
		}
	})

	It("should spawn children of several parents running concurrently in id order", func() {
		parents := []string{"p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"}
		cs := startRoundContainers(n, parents...)
		var wg sync.WaitGroup
		for _, i := range rand.Perm(len(cs)) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()
				sendAndWait(cs[i], "SPLIT", &TestEvent{Value: "c,b,a"})
			}()
		}
		wg.Wait()

		Expect(n.Round(context.Background(), "one")).To(Succeed())
		order := started(n)[len(parents):]
		Expect(order).To(HaveLen(3 * len(parents)))
		Expect(slices.IsSorted(order)).To(BeTrue(), "%v", order)
		Expect(log.get()).To(HaveLen(3 * len(parents)))
	})

	It("should start a container spawned in a round after the closes of the round, also of an id closing in it", func() {
		cs := startRoundContainers(n, "a", "x", "y")
		sendAndWait(cs[0], "SEND", &TestEvent{Value: "x:BYE:;a:SPAWN:x"})

		Expect(n.Round(context.Background(), "one")).To(Succeed())
		Expect(log.get()).To(Equal([]string{`x closes in "one"`}))
		Expect(cs[1].isAccepting()).To(BeFalse())
		Expect(n.hasContainer("x")).To(BeTrue())
		var xs []service.TraceEvent_Kind
		n.trace.mu.Lock()
		for _, e := range n.trace.entries {
			if e.GetContainerId() == "x" && e.GetKind() != service.TraceEvent_EVENT {
				xs = append(xs, e.GetKind())
			}
		}
		n.trace.mu.Unlock()
		Expect(xs).To(Equal([]service.TraceEvent_Kind{service.TraceEvent_CONTAINER_STARTED,
			service.TraceEvent_CONTAINER_CLOSED, service.TraceEvent_CONTAINER_STARTED}))

		By("delivering the events of the round to the new container in the next one")
		sendAndWait(cs[0], "SEND", &TestEvent{Value: "x:HELLO:a"})
		Expect(n.Round(context.Background(), "two")).To(Succeed())
		Expect(log.get()[1:]).To(ConsistOf(`y got hello from x in "two" as ""`, `x got hello from a in "two" as ""`))
	})

	It("should fail a round whose spawns find their id open after the closes, start none and keep them queued", func() {
		cs := startRoundContainers(n, "p", "p/a")
		sendAndWait(cs[0], "SEND", &TestEvent{Value: "p:SPLIT:b,a"})
		err := n.Round(context.Background(), "one")
		Expect(err).To(MatchError(ErrContainerExists))
		Expect(err).To(MatchError(ContainSubstring("round one: spawning container p/a (from p)")))
		Expect(n.hasContainer("p/b")).To(BeFalse())
		Expect(cs[0].round.spawns).To(HaveLen(2))
		Expect(cs[0].round.outbox).To(HaveLen(2))

		By("failing the start of the next round while the id is open, starting and delivering once it closed")
		Expect(n.Round(context.Background(), "two")).To(MatchError(ErrContainerExists))
		cs[1].Close()
		Expect(n.Round(context.Background(), "three")).To(Succeed())
		Expect(log.get()).To(ConsistOf(
			`p/a got hello from p in "three" as "child of p"`,
			`p/b got hello from p in "three" as "child of p"`,
		))

		By("refusing two spawns of the same id in a round, dropped with DropOutboxes")
		sendAndWait(cs[0], "SEND", &TestEvent{Value: "p:SPLIT:c,c"})
		Expect(n.Round(context.Background(), "four")).To(MatchError(ContainSubstring("round four: spawning container p/c")))
		Expect(n.hasContainer("p/c")).To(BeFalse())
		dropped, err := n.DropOutboxes(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(dropped).To(Equal(4))
		Expect(n.Round(context.Background(), "five")).To(Succeed())
		Expect(n.hasContainer("p/c")).To(BeFalse())
	})

	It("should start a container spawned in a round without a slot when none is free, without blocking", func() {
		n.Shutdown()
		n = newRoundNode(WithMaxContainerCount(1))
		setup(n)
		cs := startRoundContainers(n, "a")
		sendAndWait(cs[0], "SEND", &TestEvent{Value: "a:SPAWN:b"})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		Expect(n.Round(ctx, "one")).To(Succeed())
		Expect(n.hasContainer("b")).To(BeTrue())
	})

	It("should return an existing id as error and start and deliver nothing, keeping what was queued", func() {
		cs := startRoundContainers(n, "p", "p/a")
		sendAndWait(cs[0], "SPLIT", &TestEvent{Value: "a,b"})
		err := n.Round(context.Background(), "one")
		Expect(err).To(MatchError(ErrContainerExists))
		Expect(err).To(MatchError(ContainSubstring("spawning container p/a (from p)")))
		Expect(n.hasContainer("p/b")).To(BeFalse())
		Expect(log.get()).To(BeEmpty())
		Expect(cs[0].round.spawns).To(HaveLen(2))
		Expect(cs[0].round.outbox).To(HaveLen(2))

		By("failing again while the id is open, then starting and delivering everything once it closed")
		Expect(n.Round(context.Background(), "two")).To(MatchError(ErrContainerExists))
		cs[1].Close()
		Expect(n.Round(context.Background(), "three")).To(Succeed())
		Expect(n.hasContainer("p/a")).To(BeTrue())
		Expect(log.get()).To(ConsistOf(
			`p/a got hello from p in "three" as "child of p"`,
			`p/b got hello from p in "three" as "child of p"`,
		))

		By("refusing two spawns of the same id, dropped with DropOutboxes")
		sendAndWait(cs[0], "SPLIT", &TestEvent{Value: "c,c"})
		Expect(n.Round(context.Background(), "four")).To(MatchError(ErrContainerExists))
		Expect(n.hasContainer("p/c")).To(BeFalse())
		dropped, err := n.DropOutboxes(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(dropped).To(Equal(4))
		Expect(n.Round(context.Background(), "five")).To(Succeed())
		Expect(n.hasContainer("p/c")).To(BeFalse())
	})

	It("should close a container at the end of the round and deliver its deferred events in the next one", func() {
		cs := startRoundContainers(n, "a", "x", "y")
		sendAndWait(cs[0], "SEND", &TestEvent{Value: "x:BYE:"})

		Expect(n.Round(context.Background(), "one")).To(Succeed())
		Expect(log.get()).To(Equal([]string{`x closes in "one"`}))
		Expect(n.hasContainer("x")).To(BeFalse())
		Expect(cs[1].isAccepting()).To(BeFalse())

		Expect(n.Round(context.Background(), "two")).To(Succeed())
		Expect(log.get()[1:]).To(Equal([]string{`y got hello from x in "two" as ""`}))

		By("failing events deferred to the closed container")
		sendAndWait(cs[0], "SEND", &TestEvent{Value: "x:HELLO:a"})
		Expect(n.Round(context.Background(), "three")).To(MatchError(ErrUnknownTarget))

		By("spawning the closed id again, which gets the kept event")
		sendAndWait(cs[0], "SPAWN", &TestEvent{Value: "x"})
		Expect(n.Round(context.Background(), "four")).To(Succeed())
		Expect(n.hasContainer("x")).To(BeTrue())
		Expect(log.get()[2:]).To(Equal([]string{`x got hello from a in "four" as ""`}))
	})

	It("should refuse spawns and closes outside of a transition, and an empty id", func() {
		cs := startRoundContainers(n, "a")
		Expect(cs[0].Spawn(context.Background(), "b", nil)).To(MatchError(ErrNotInTransition))
		Expect(cs[0].CloseAfterRound(context.Background())).To(MatchError(ErrNotInTransition))

		errC := make(chan error, 1)
		n.AddFuncTransition("empty", "EMPTY", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			errC <- c.Spawn(ctx, "", nil)
			return false, nil
		})
		sendAndWait(cs[0], "EMPTY", nil)
		Expect(errC).To(Receive(MatchError(ContainSubstring("empty id"))))
	})

	It("should keep a spawned container open after calls and close one asked twice once", func() {
		cs := startRoundContainers(n, "a", "b")
		sendAndWait(cs[0], "SPAWN", &TestEvent{Value: "kid"})
		Expect(n.Round(context.Background(), "one")).To(Succeed())
		Expect(n.hasContainer("kid")).To(BeTrue())

		// a live call of the spawned container doesn't close it, unlike a container the call opened
		n.AddFuncTransition("call", "CALL", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			return false, c.Emit(ctx, "HELLO", &TestEvent{Value: c.Id()}, WithTarget("kid"), WithWaitRemote())
		})
		sendAndWait(cs[1], "CALL", nil)
		Consistently(func() bool { return n.hasContainer("kid") }, 100*time.Millisecond).Should(BeTrue())

		n.AddFuncTransition("twice", "TWICE", func(ctx context.Context, c *roundContainer, a any) (bool, error) {
			if err := c.CloseAfterRound(ctx); err != nil {
				return false, err
			}
			return false, c.CloseAfterRound(ctx)
		})
		sendAndWait(cs[1], "SEND", &TestEvent{Value: "kid:TWICE:"})
		Expect(n.Round(context.Background(), "two")).To(Succeed())
		Expect(n.hasContainer("kid")).To(BeFalse())
		Expect(n.Round(context.Background(), "three")).To(Succeed())
	})
})
