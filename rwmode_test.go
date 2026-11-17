package asm

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"rumue.io/asm/service"
)

var _ = Describe("Reader and writer transitions", func() {
	type read struct {
		value int64
		// writing: the slow or blocking writer ran while the reader read
		writing bool
	}
	type testContainer = Container[*wrapperspb.Int64Value, *int64Builder]
	type transitionF = func(ctx context.Context, c *testContainer, a any) (bool, error)
	var (
		n        *counterNode
		readC    chan read
		replyC   chan any
		errC     chan error
		blockedC chan struct{}
		releaseC chan struct{}
		release  func()
		writing  atomic.Bool
		// barrier, if set, is called by every QUOTE reader before it reads
		barrier func()
		active  atomic.Int32
		overlap atomic.Bool
	)

	// newNode adds the transitions, all changing the entity in place:
	// SLOW (writer) sets 10 and works 200 ms; BLOCK (writer) sets 10 and waits for release;
	// INC (writer) increments the value, slowly, recording overlapping writers in overlap;
	// READ and QUOTE (readers) send what they see on readC, QUOTE also replies it to a caller;
	// ASK (writer) calls QUOTE of the container named by the payload
	newNode := func(opts ...NodeSetupOption) {
		pos := 0
		bm := &Bitmap{Name: "root", Children: map[string]*Bitmap{
			"Int64Value": BuildBitmap(&Property{Name: "Int64Value", Properties: []*Property{{Name: "value", Values: []any{int64(0), int64(1)}}}}, &pos),
		}}
		n = NewNode(1, counterBitmapByType, func() *int64Builder { return &int64Builder{} }, "",
			append([]NodeSetupOption{WithSkipRaft(), WithBitmap(bm), WithLogger(zap.NewNop()), WithMaxContainerCount(200)}, opts...)...)
		n.AddFuncTransition("slow", "SLOW", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			writing.Store(true)
			defer writing.Store(false)
			c.Builder().Build().Value = 10
			time.Sleep(200 * time.Millisecond)
			return false, nil
		})
		n.AddFuncTransition("block", "BLOCK", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			writing.Store(true)
			defer writing.Store(false)
			c.Builder().Build().Value = 10
			blockedC <- struct{}{}
			<-releaseC
			return false, nil
		})
		n.AddFuncTransition("inc", "INC", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			if active.Add(1) > 1 {
				overlap.Store(true)
			}
			defer active.Add(-1)
			e := c.Builder().Build()
			v := e.Value
			time.Sleep(20 * time.Millisecond)
			e.Value = v + 1
			return false, nil
		})
		readF := func(reply bool) transitionF {
			return func(ctx context.Context, c *testContainer, a any) (bool, error) {
				if barrier != nil {
					barrier()
				}
				r := read{value: c.Builder().Build().GetValue(), writing: writing.Load()}
				readC <- r
				if reply {
					// ErrNoCaller for QUOTE events sent to the container
					_ = c.Reply(ctx, r.value)
				}
				return false, nil
			}
		}
		n.AddFuncTransition("read", "READ", readF(false), WithReadOnly())
		n.AddFuncTransition("quote", "QUOTE", readF(true), WithReadOnly())
		n.AddFuncTransition("ask", "ASK", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			errC <- c.Emit(ctx, "QUOTE", wrapperspb.String(""), WithTarget(a.(*wrapperspb.StringValue).GetValue()), WithWaitRemote(),
				WithOnBusy(OnBusyFail), WithReply(func(r any) { replyC <- r }))
			return false, nil
		})
	}
	open := func(id string, opts ...ContainerOption) *testContainer {
		GinkgoHelper()
		c, _, err := n.StartContainer(context.Background(), id, opts...)
		Expect(err).NotTo(HaveOccurred())
		return c
	}
	start := func(id string, opts ...ContainerOption) *testContainer {
		GinkgoHelper()
		c := open(id, opts...)
		c.Builder().Set(wrapperspb.Int64(1))
		return c
	}
	send := func(c *testContainer, event string, payload string) chan struct{} {
		GinkgoHelper()
		doneC := make(chan struct{}, 1)
		Expect(c.Event(event, wrapperspb.String(payload), WithDoneChannel(doneC))).To(Succeed())
		return doneC
	}
	value := func(c *testContainer) int64 {
		return c.Builder().Build().GetValue()
	}

	BeforeEach(func() {
		readC, replyC, errC = make(chan read, 200), make(chan any, 200), make(chan error, 200)
		blockedC, releaseC = make(chan struct{}, 1), make(chan struct{})
		release = sync.OnceFunc(func() { close(releaseC) })
		barrier = nil
		active.Store(0)
		overlap.Store(false)
	})

	AfterEach(func() {
		release()
		n.Shutdown()
	})

	It("should run a reader during a slow writer on the entity from before the writer, without waiting", func() {
		newNode()
		a := start("a")
		writeDone := send(a, "SLOW", "")

		time.Sleep(50 * time.Millisecond)
		sent := time.Now()
		readDone := send(a, "READ", "")
		Eventually(readDone, 120*time.Millisecond).Should(Receive())
		Expect(time.Since(sent)).To(BeNumerically("<", 120*time.Millisecond))
		Expect(writeDone).NotTo(Receive())
		var r read
		Expect(readC).To(Receive(&r))
		Expect(r).To(Equal(read{value: 1, writing: true}))

		By("readers after the writer see its changes")
		Eventually(writeDone).Should(Receive())
		Eventually(send(a, "READ", "")).Should(Receive())
		Expect(readC).To(Receive(&r))
		Expect(r).To(Equal(read{value: 10}))
		Expect(value(a)).To(BeNumerically("==", 10))
	})

	It("should run writers one at a time, each on the entity the one before left", func() {
		newNode()
		a := start("a")
		callers := []*testContainer{start("b"), start("c")}
		n.AddFuncTransition("call-inc", "CALL_INC", func(ctx context.Context, c *testContainer, p any) (bool, error) {
			errC <- c.Emit(ctx, "INC", wrapperspb.String(""), WithTarget("a"), WithWaitRemote())
			return false, nil
		})

		var wg sync.WaitGroup
		dones := make(chan chan struct{}, 4)
		for i := range 4 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				if i < 2 {
					dones <- send(a, "INC", "")
				} else {
					dones <- send(callers[i-2], "CALL_INC", "")
				}
			}()
		}
		wg.Wait()
		close(dones)
		for doneC := range dones {
			Eventually(doneC, 5*time.Second).Should(Receive())
		}
		Eventually(errC).Should(Receive(BeNil()))
		Eventually(errC).Should(Receive(BeNil()))
		Expect(overlap.Load()).To(BeFalse())
		Expect(value(a)).To(BeNumerically("==", 5))
	})

	It("should run 100 readers concurrently with each other and with a writer", func() {
		newNode()
		a := start("a")
		writeDone := send(a, "BLOCK", "")
		Eventually(blockedC).Should(Receive())

		// a reader goes on once all of them run: readers which ran one at a time would never get there
		var started atomic.Int32
		allC := make(chan struct{})
		barrier = func() {
			if started.Add(1) == 100 {
				close(allC)
			}
			select {
			case <-allC:
			case <-time.After(10 * time.Second):
			}
		}
		var callers []*testContainer
		for i := range 10 {
			callers = append(callers, start(string(rune('b'+i))))
		}
		var dones []chan struct{}
		for i := range 100 {
			if i < len(callers) {
				dones = append(dones, send(callers[i], "ASK", "a"))
			} else {
				dones = append(dones, send(a, "QUOTE", ""))
			}
		}
		for _, doneC := range dones {
			Eventually(doneC, 5*time.Second).Should(Receive())
		}
		Expect(started.Load()).To(BeNumerically("==", 100))
		for range 100 {
			var r read
			Expect(readC).To(Receive(&r))
			Expect(r).To(Equal(read{value: 1, writing: true}))
		}
		for range callers {
			Expect(errC).To(Receive(BeNil()))
			Expect(replyC).To(Receive(Equal(int64(1))))
		}
		Expect(writeDone).NotTo(Receive())

		release()
		Eventually(writeDone).Should(Receive())
		barrier = nil
		Eventually(send(a, "QUOTE", "")).Should(Receive())
		var r read
		Expect(readC).To(Receive(&r))
		Expect(r.value).To(BeNumerically("==", 10))
	})

	It("should answer a call of a read event during a writer with the entity from before it, and not be busy", func() {
		newNode()
		a, b := start("a"), start("b")
		writeDone := send(a, "BLOCK", "")
		Eventually(blockedC).Should(Receive())

		Eventually(send(b, "ASK", "a")).Should(Receive())
		Expect(errC).To(Receive(BeNil()))
		Expect(replyC).To(Receive(Equal(int64(1))))
		Expect(n.containerBusy("a", "QUOTE")).To(BeFalse())
		Expect(n.containerBusy("a", "INC")).To(BeTrue())

		release()
		Eventually(writeDone).Should(Receive())
		Eventually(send(b, "ASK", "a")).Should(Receive())
		Expect(errC).To(Receive(BeNil()))
		Expect(replyC).To(Receive(Equal(int64(10))))
	})

	It("should let a reader emit read events and to other containers, but not writers of its own container", func() {
		newNode()
		var errs struct {
			sync.Mutex
			own, read, target error
		}
		n.AddFuncTransition("emitter", "EMIT", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			own := c.Emit(ctx, "INC", wrapperspb.String(""))
			read := c.Emit(ctx, "READ", wrapperspb.String(""))
			target := c.Emit(ctx, "INC", wrapperspb.String(""), WithTarget("b"), WithWaitRemote())
			errs.Lock()
			errs.own, errs.read, errs.target = own, read, target
			errs.Unlock()
			return false, nil
		}, WithReadOnly())
		a, b := start("a"), start("b")
		writeDone := send(a, "BLOCK", "")
		Eventually(blockedC).Should(Receive())

		Eventually(send(a, "EMIT", ""), 5*time.Second).Should(Receive())
		errs.Lock()
		Expect(errors.Is(errs.own, ErrReadOnly)).To(BeTrue(), "%v", errs.own)
		Expect(errs.read).NotTo(HaveOccurred())
		Expect(errs.target).NotTo(HaveOccurred())
		errs.Unlock()
		var r read
		Expect(readC).To(Receive(&r))
		Expect(r.value).To(BeNumerically("==", 1))
		Expect(value(b)).To(BeNumerically("==", 2))

		release()
		Eventually(writeDone).Should(Receive())
		Expect(value(a)).To(BeNumerically("==", 10))
	})

	It("should run an event with a writer among its transitions as a writer", func() {
		newNode()
		n.AddFuncTransition("mixed-read", "MIXED", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			return false, nil
		}, WithReadOnly())
		n.AddFuncTransition("mixed-write", "MIXED", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			return false, nil
		})
		a := start("a")
		writeDone := send(a, "BLOCK", "")
		Eventually(blockedC).Should(Receive())

		mixedDone := send(a, "MIXED", "")
		Consistently(mixedDone, 200*time.Millisecond).ShouldNot(Receive())
		release()
		Eventually(writeDone).Should(Receive())
		Eventually(mixedDone).Should(Receive())
	})

	It("should drop what a reader sets on its builder", func() {
		newNode()
		n.AddFuncTransition("setter", "SET", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			c.Builder().Set(wrapperspb.Int64(99))
			return false, nil
		}, WithReadOnly())
		a := start("a")
		Eventually(send(a, "SET", "")).Should(Receive())
		Eventually(send(a, "READ", "")).Should(Receive())
		var r read
		Expect(readC).To(Receive(&r))
		Expect(r.value).To(BeNumerically("==", 1))
		Expect(value(a)).To(BeNumerically("==", 1))
	})

	It("should give readers the entity the init func loaded, also while the first writer runs", func() {
		newNode(WithInitFunc(func(ctx context.Context, c *testContainer) error {
			c.Builder().Set(wrapperspb.Int64(7))
			return nil
		}))

		By("a reader as the first event")
		a := open("a")
		Eventually(send(a, "READ", "")).Should(Receive())
		var r read
		Expect(readC).To(Receive(&r))
		Expect(r.value).To(BeNumerically("==", 7))

		By("a reader during the first writer")
		b := open("b")
		writeDone := send(b, "BLOCK", "")
		Eventually(blockedC).Should(Receive())
		Eventually(send(b, "READ", "")).Should(Receive())
		Expect(readC).To(Receive(&r))
		Expect(r).To(Equal(read{value: 7, writing: true}))
		release()
		Eventually(writeDone).Should(Receive())
	})

	It("should run readers while a writer waits in a waited Emit, reentrant or not", func() {
		newNode(WithReentrant())
		n.AddFuncTransition("act", "ACT", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			c.Builder().Build().Value = 5
			return false, c.Emit(ctx, "BLOCK", wrapperspb.String(""), WithTarget("b"), WithWaitRemote())
		})
		for _, opts := range [][]ContainerOption{nil, {WithContainerNotReentrant()}, {WithContainerReentrant("PING")}} {
			blockedC, releaseC = make(chan struct{}, 1), make(chan struct{})
			release = sync.OnceFunc(func() { close(releaseC) })
			a, _ := start("a", opts...), start("b")
			actDone := send(a, "ACT", "")
			Eventually(blockedC).Should(Receive())

			Eventually(send(a, "READ", "")).Should(Receive())
			var r read
			Expect(readC).To(Receive(&r))
			Expect(r.value).To(BeNumerically("==", 1))

			release()
			Eventually(actDone).Should(Receive())
			Expect(value(a)).To(BeNumerically("==", 5))
			a.Close()
			n.DeleteContainer("b")
		}
		Expect(n.metrics.eventsInterleaved.Load()).To(BeZero())
	})

	It("should classify events by their transitions", func() {
		f := transitionFunc[*wrapperspb.Int64Value, *int64Builder](func(ctx context.Context, c *testContainer, a any) (bool, error) { return false, nil })
		ro := func(name, event string) *Transition {
			return &Transition{Name: name, EventName: event, Actions: []any{f}, Batching: Batching{Count: 1}, ReadOnly: true}
		}
		batched := ro("batched", "BATCHED")
		batched.Batching.Count = 2
		remote := &Transition{Name: "remote", EventName: "ROUTED", Remote: true}
		state := &Transition{Name: "state", EventName: "STATE", Actions: []any{Bits{1}}, ReadOnly: true}
		writer := &Transition{Name: "writer", EventName: "BOTH", Actions: []any{f}, Batching: Batching{Count: 1}}
		idx := newPublishedTransitions([]*Transition{
			ro("read", "READ"), ro("routed", "ROUTED"), remote, batched, state, ro("both", "BOTH"), writer,
		}).readIndex()

		Expect(idx.readers).To(BeTrue())
		Expect(idx.direct).To(Equal(map[string]bool{"READ": true}))
		Expect(idx.executions).To(Equal(map[string]bool{"READ": true, "ROUTED": true}))
		Expect(newPublishedTransitions([]*Transition{writer}).readIndex().readers).To(BeFalse())
		Expect((*publishedTransitions)(nil).readIndex()).To(BeNil())
	})

	It("should copy entities for readers", func() {
		v := wrapperspb.Int64(3)
		cp := copyEntity(v, nil)
		Expect(cp).NotTo(BeIdenticalTo(v))
		Expect(proto.Equal(cp, v)).To(BeTrue())

		vt := &service.Transition{Name: "t", EventName: "E"}
		vtCp := copyEntity(vt, nil)
		Expect(vtCp).NotTo(BeIdenticalTo(vt))
		Expect(vtCp.EqualVT(vt)).To(BeTrue())

		fake := &TestStateEntity{Status: "pending", Count: 1}
		fakeCp := copyEntity(fake, nil)
		Expect(fakeCp).NotTo(BeIdenticalTo(fake))
		Expect(*fakeCp).To(Equal(*fake))

		Expect(copyEntity[*TestStateEntity](nil, nil)).To(BeNil())
	})

	It("should run n+1 readers n at a time with WithMaxReaders, the last one waiting for a slot", func() {
		newNode()
		var running, maxRunning atomic.Int32
		startedC, holdC := make(chan struct{}, 10), make(chan struct{})
		n.AddFuncTransition("hold", "HOLD", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			r := running.Add(1)
			for m := maxRunning.Load(); r > m && !maxRunning.CompareAndSwap(m, r); m = maxRunning.Load() {
			}
			startedC <- struct{}{}
			<-holdC
			running.Add(-1)
			return false, nil
		}, WithReadOnly())
		a := start("a", WithMaxReaders(2))

		// Event neither blocks nor fails beyond the cap
		dones := []chan struct{}{send(a, "HOLD", ""), send(a, "HOLD", ""), send(a, "HOLD", "")}
		Eventually(startedC).Should(Receive())
		Eventually(startedC).Should(Receive())
		Consistently(startedC, 200*time.Millisecond).ShouldNot(Receive())
		Expect(n.containerBusy("a", "HOLD")).To(BeFalse())
		Expect(n.containerBusy("a", "INC")).To(BeFalse())

		By("a reader ending: the waiting one runs")
		holdC <- struct{}{}
		Eventually(startedC).Should(Receive())
		holdC <- struct{}{}
		holdC <- struct{}{}
		for _, doneC := range dones {
			Eventually(doneC).Should(Receive())
		}
		Expect(maxRunning.Load()).To(BeNumerically("==", 2))

		By("writers aren't capped")
		Eventually(send(a, "INC", "")).Should(Receive())
		Expect(value(a)).To(BeNumerically("==", 2))
	})

	It("should cap the readers of every container of the node with WithMaxReadersPerContainer, its own cap overriding it", func() {
		newNode(WithMaxReadersPerContainer(1))
		startedC, holdC := make(chan string, 10), make(chan struct{})
		n.AddFuncTransition("hold", "HOLD", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			startedC <- c.Id()
			<-holdC
			return false, nil
		}, WithReadOnly())
		n.AddFuncTransition("ask-hold", "ASK_HOLD", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			return false, c.Emit(ctx, "HOLD", wrapperspb.String(""), WithTarget(a.(*wrapperspb.StringValue).GetValue()), WithWaitRemote())
		})
		a, b := start("a"), start("b", WithMaxReaders(2))

		dones := []chan struct{}{send(a, "HOLD", ""), send(a, "HOLD", ""), send(b, "HOLD", ""), send(b, "HOLD", "")}
		got := map[string]int{}
		for range 3 {
			var id string
			Eventually(startedC).Should(Receive(&id))
			got[id]++
		}
		Consistently(startedC, 200*time.Millisecond).ShouldNot(Receive())
		Expect(got).To(Equal(map[string]int{"a": 1, "b": 2}))
		for range 4 {
			holdC <- struct{}{}
		}
		for _, doneC := range dones {
			Eventually(doneC).Should(Receive())
		}

		By("capping a container the node opened for a call")
		askDone := send(a, "ASK_HOLD", "c")
		Eventually(startedC).Should(Receive(Equal("c")))
		n.containerMap.mu.RLock()
		c := n.containerMap.m["c"]
		n.containerMap.mu.RUnlock()
		Expect(c).NotTo(BeNil())
		Expect(cap(c.rw.slots)).To(Equal(1))
		holdC <- struct{}{}
		Eventually(askDone).Should(Receive())
	})

	It("should give a capped reader's slot to a read call back while the reader waits in a call", func() {
		newNode()
		// LOOKUP (reader of a) calls RELAY of b, which calls QUOTE (reader) back in a and passes its reply on
		n.AddFuncTransition("lookup", "LOOKUP", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			errC <- c.Emit(ctx, "RELAY", wrapperspb.String(c.Id()), WithTarget("b"), WithWaitRemote(), WithReply(func(r any) { replyC <- r }))
			return false, nil
		}, WithReadOnly())
		n.AddFuncTransition("relay", "RELAY", func(ctx context.Context, c *testContainer, a any) (bool, error) {
			var quote any
			if err := c.Emit(ctx, "QUOTE", wrapperspb.String(""), WithTarget(a.(*wrapperspb.StringValue).GetValue()),
				WithWaitRemote(), WithReply(func(r any) { quote = r })); err != nil {
				return false, err
			}
			return false, c.Reply(ctx, quote)
		})
		a, _ := start("a", WithMaxReaders(1)), start("b")

		Eventually(send(a, "LOOKUP", ""), 5*time.Second).Should(Receive())
		Expect(errC).To(Receive(BeNil()))
		Expect(replyC).To(Receive(Equal(int64(1))))
		Expect(readC).To(Receive())
	})

	Context("in immutable-entity mode", func() {
		var (
			ptrC   chan *wrapperspb.Int64Value
			copies atomic.Int64
		)

		// addImmutable adds transitions which replace the entity instead of changing it:
		// SWAP (writer) sets value+10 and waits for release, saving after it; BUMP (writer) sets value+1;
		// PTR (reader) sends the entity it sees on ptrC; CALL_BUMP (writer) calls BUMP of the container named by the
		// payload; POKE (writer, a caller bug in this mode) changes the entity in place
		addImmutable := func() {
			n.AddFuncTransition("swap", "SWAP", func(ctx context.Context, c *testContainer, a any) (bool, error) {
				writing.Store(true)
				defer writing.Store(false)
				c.Builder().Set(wrapperspb.Int64(c.Builder().Build().GetValue() + 10))
				blockedC <- struct{}{}
				<-releaseC
				return false, nil
			}, WithSaveAfter())
			n.AddFuncTransition("bump", "BUMP", func(ctx context.Context, c *testContainer, a any) (bool, error) {
				c.Builder().Set(wrapperspb.Int64(c.Builder().Build().GetValue() + 1))
				return false, nil
			})
			n.AddFuncTransition("ptr", "PTR", func(ctx context.Context, c *testContainer, a any) (bool, error) {
				ptrC <- c.Builder().Build()
				return false, nil
			}, WithReadOnly())
			n.AddFuncTransition("call-bump", "CALL_BUMP", func(ctx context.Context, c *testContainer, a any) (bool, error) {
				errC <- c.Emit(ctx, "BUMP", wrapperspb.String(""), WithTarget(a.(*wrapperspb.StringValue).GetValue()), WithWaitRemote())
				return false, nil
			})
			n.AddFuncTransition("poke", "POKE", func(ctx context.Context, c *testContainer, a any) (bool, error) {
				c.Builder().Build().Value = 42
				return false, nil
			})
		}
		peek := func(c *testContainer) *wrapperspb.Int64Value {
			GinkgoHelper()
			Eventually(send(c, "PTR", "")).Should(Receive())
			var e *wrapperspb.Int64Value
			Expect(ptrC).To(Receive(&e))
			return e
		}

		BeforeEach(func() {
			ptrC = make(chan *wrapperspb.Int64Value, 200)
			copies.Store(0)
			count := func() { copies.Add(1) }
			entityCopyHook.Store(&count)
			DeferCleanup(func() { entityCopyHook.Store(nil) })
		})

		It("should give readers the entity itself: the one from before a running writer, then the one it set", func() {
			newNode(WithImmutableEntities())
			addImmutable()
			a := start("a")
			e0 := a.Builder().Build()

			writeDone := send(a, "SWAP", "")
			Eventually(blockedC).Should(Receive())
			Expect(peek(a)).To(BeIdenticalTo(e0))

			release()
			Eventually(writeDone).Should(Receive())
			e1 := peek(a)
			Expect(e1).To(BeIdenticalTo(a.Builder().Build()))
			Expect(e1.GetValue()).To(BeNumerically("==", 11))
			Expect(e0.GetValue()).To(BeNumerically("==", 1))
			Expect(copies.Load()).To(BeZero())
		})

		It("should copy the entity neither for writers nor for calls nor for the trace, unlike the copy mode", func() {
			run := func(opts ...NodeSetupOption) int64 {
				GinkgoHelper()
				newNode(append(opts, WithEventTrace(100))...)
				addImmutable()
				copies.Store(0)
				a, b := start("a"), start("b")
				for range 3 {
					Eventually(send(a, "BUMP", "")).Should(Receive())
					Expect(peek(a).GetValue()).To(Equal(value(a)))
					Eventually(send(b, "CALL_BUMP", "a")).Should(Receive())
					Expect(errC).To(Receive(BeNil()))
				}
				Expect(value(a)).To(BeNumerically("==", 7))
				n.Shutdown()
				return copies.Load()
			}
			Expect(run()).To(BeNumerically(">", 0))
			Expect(run(WithImmutableEntities())).To(BeZero())
		})

		It("should apply a state transition to a copy which replaces the entity", func() {
			for _, immutable := range []bool{false, true} {
				var opts []NodeSetupOption
				if immutable {
					opts = append(opts, WithImmutableEntities())
				}
				newNode(opts...)
				n.AddStateTransition("reset", "RESET", wrapperspb.Int64(0))
				a := start("a")
				e0 := a.Builder().Build()
				Eventually(send(a, "RESET", "")).Should(Receive())
				Expect(value(a)).To(BeNumerically("==", 0))
				if immutable {
					Expect(a.Builder().Build()).NotTo(BeIdenticalTo(e0))
					Expect(e0.GetValue()).To(BeNumerically("==", 1))
				} else {
					// in place, as before
					Expect(a.Builder().Build()).To(BeIdenticalTo(e0))
				}
				n.Shutdown()
			}
		})

		It("should save the entity readers get next, without a copy", func() {
			savedC := make(chan *wrapperspb.Int64Value, 10)
			newNode(WithImmutableEntities(), WithSaveFunc(func(ctx context.Context, c *testContainer) error {
				savedC <- c.Builder().Build()
				return nil
			}))
			addImmutable()
			a := start("a")
			writeDone := send(a, "SWAP", "")
			Eventually(blockedC).Should(Receive())
			release()
			Eventually(writeDone).Should(Receive())

			var saved *wrapperspb.Int64Value
			Eventually(savedC).Should(Receive(&saved))
			Expect(saved.GetValue()).To(BeNumerically("==", 11))
			Expect(peek(a)).To(BeIdenticalTo(saved))
			Expect(copies.Load()).To(BeZero())
		})

		It("should report a writer changing the entity in place to the error func with the check on", func() {
			var errs struct {
				sync.Mutex
				list []error
			}
			reported := func() []error {
				errs.Lock()
				defer errs.Unlock()
				return slices.Clone(errs.list)
			}
			newNode(WithImmutableEntities(), WithImmutableEntityCheck(), WithErrorFunc(func(ctx context.Context, c *testContainer, err error) error {
				errs.Lock()
				defer errs.Unlock()
				errs.list = append(errs.list, err)
				return nil
			}))
			addImmutable()
			a := start("a")
			e0 := a.Builder().Build()

			Eventually(send(a, "BUMP", "")).Should(Receive())
			Consistently(reported, 100*time.Millisecond).Should(BeEmpty())
			Expect(e0.GetValue()).To(BeNumerically("==", 1))

			Eventually(send(a, "POKE", "")).Should(Receive())
			Eventually(reported).Should(ConsistOf(MatchError(ErrEntityMutated)))
		})
	})
})

var _ = Describe("Reader and writer transitions in a cluster", Label("cluster"), func() {
	type stringContainer = Container[*wrapperspb.StringValue, *stringValueBuilder]
	var (
		tc     *testCluster
		errC   chan error
		replyC chan string
	)

	// TRADE (writer of a) sets "trading" and calls ACCEPT of b, waiting; ACCEPT (writer of b) calls the event the payload
	// names back in a, waiting, and replies what it got: QUOTE (reader) replies the entity it sees, POKE (writer) too
	setup := func(n *clusterNode) {
		n.AddFuncTransition("trade", "TRADE", func(ctx context.Context, c *stringContainer, a any) (bool, error) {
			c.Builder().Set(wrapperspb.String("trading"))
			var reply any
			err := c.Emit(ctx, "ACCEPT", a, WithTarget("b"), WithWaitRemote(), WithReply(func(r any) { reply = r }))
			errC <- err
			if err == nil {
				replyC <- reply.(*wrapperspb.StringValue).GetValue()
			}
			return false, nil
		})
		n.AddFuncTransition("accept", "ACCEPT", func(ctx context.Context, c *stringContainer, a any) (bool, error) {
			var quote any
			err := c.Emit(ctx, a.(*wrapperspb.StringValue).GetValue(), wrapperspb.String(""), WithTarget("a"), WithWaitRemote(),
				WithReply(func(r any) { quote = r }))
			if err != nil {
				return false, err
			}
			return false, c.Reply(ctx, quote)
		})
		quote := func(ctx context.Context, c *stringContainer, a any) (bool, error) {
			return false, c.Reply(ctx, wrapperspb.String(c.Builder().Build().GetValue()))
		}
		n.AddFuncTransition("quote", "QUOTE", quote, WithReadOnly())
		n.AddFuncTransition("poke", "POKE", quote)
	}
	// trade starts a's TRADE, b calling back callBack, and returns when it sent it
	trade := func(callBack string) time.Time {
		GinkgoHelper()
		n := tc.start(1, "", clusterBitmap("pending", "done"), setup)
		expectReady(n)
		a, _, err := n.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())
		a.Builder().Set(wrapperspb.String("pending"))
		Expect(a.Event("TRADE", wrapperspb.String(callBack))).To(Succeed())
		return time.Now()
	}

	BeforeEach(func() {
		tc = newTestCluster()
		errC, replyC = make(chan error, 2), make(chan string, 2)
	})

	AfterEach(func() {
		tc.stopAll()
	})

	It("should run a read call back into a container whose writer waits for the caller", func() {
		start := trade("QUOTE")
		Eventually(errC, 10*time.Second).Should(Receive(BeNil()))
		// the reader saw the entity from before the waiting writer
		Expect(replyC).To(Receive(Equal("pending")))
		Expect(time.Since(start)).To(BeNumerically("<", 5*time.Second))
	})

	It("should still refuse a writer call back closing the cycle", func() {
		start := trade("POKE")
		Eventually(errC, 10*time.Second).Should(Receive(MatchError(ErrWouldDeadlock)))
		Expect(time.Since(start)).To(BeNumerically("<", 5*time.Second))
	})
})

// durationBuilder holds a two-field entity without a lock of its own: the container must order every access to it.
type durationBuilder struct {
	v *durationpb.Duration
}

func (b *durationBuilder) Build() *durationpb.Duration { return b.v }
func (b *durationBuilder) Set(v *durationpb.Duration)  { b.v = v }
func (b *durationBuilder) IsSet() bool                 { return b.v != nil }

var _ = Describe("Immutable-entity mode", func() {
	type durationContainer = Container[*durationpb.Duration, *durationBuilder]

	It("should let 100 concurrent readers see only whole entities while writers replace it", func() {
		const writers, readers, senders = 50, 100, 10
		var errs, torn, started atomic.Int32
		n := NewNode(1, func(any, *Bitmap) *Bitmap { return nil }, func() *durationBuilder { return &durationBuilder{} }, "",
			WithSkipRaft(), WithLogger(zap.NewNop()), WithImmutableEntities(), WithImmutableEntityCheck(),
			WithErrorFunc(func(ctx context.Context, c *durationContainer, err error) error {
				errs.Add(1)
				return nil
			}))
		DeferCleanup(n.Shutdown)

		// a writer builds a new entity field by field and publishes it with Set
		n.AddFuncTransition("write", "WRITE", func(ctx context.Context, c *durationContainer, a any) (bool, error) {
			k := c.Builder().Build().GetSeconds() + 1
			e := &durationpb.Duration{Seconds: k}
			time.Sleep(100 * time.Microsecond)
			e.Nanos = int32(k)
			c.Builder().Set(e)
			return false, nil
		})
		// a reader reads one field, waits until all readers run (they overlap each other and the writers), then reads
		// the other one
		allC := make(chan struct{})
		seen := make(chan int64, readers)
		n.AddFuncTransition("read", "READ", func(ctx context.Context, c *durationContainer, a any) (bool, error) {
			e := c.Builder().Build()
			s := e.GetSeconds()
			if started.Add(1) == readers {
				close(allC)
			}
			select {
			case <-allC:
			case <-time.After(10 * time.Second):
			}
			if int64(e.GetNanos()) != s {
				torn.Add(1)
			}
			seen <- s
			return false, nil
		}, WithReadOnly())

		c, _, err := n.StartContainer(context.Background(), "a")
		Expect(err).NotTo(HaveOccurred())
		c.Builder().Set(&durationpb.Duration{})

		var wg sync.WaitGroup
		dones := make(chan chan struct{}, writers+readers)
		for range senders {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				// every third event is a writer: 5 writers and 10 readers per sender
				for i := range (writers + readers) / senders {
					event := "READ"
					if i%3 == 0 {
						event = "WRITE"
					}
					doneC := make(chan struct{}, 1)
					Expect(c.Event(event, wrapperspb.String(""), WithDoneChannel(doneC))).To(Succeed())
					dones <- doneC
				}
			}()
		}
		wg.Wait()
		close(dones)
		for doneC := range dones {
			Eventually(doneC, 15*time.Second).Should(Receive())
		}

		Expect(started.Load()).To(BeNumerically("==", readers))
		Expect(torn.Load()).To(BeZero())
		Expect(errs.Load()).To(BeZero())
		Expect(c.Builder().Build().GetSeconds()).To(BeNumerically("==", writers))
		Expect(seen).To(HaveLen(readers))
		for range readers {
			Expect(<-seen).To(BeNumerically("<=", writers))
		}
	})
})
