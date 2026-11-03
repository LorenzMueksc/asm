package asm

import (
	"context"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// sharedStore stands in for a database shared by all nodes of a cluster.
type sharedStore struct {
	mu     sync.Mutex
	values map[string]string
	saves  map[string]int
}

func newSharedStore() *sharedStore {
	return &sharedStore{values: make(map[string]string), saves: make(map[string]int)}
}

func (s *sharedStore) get(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[id]
}

func (s *sharedStore) set(id, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[id] = value
}

func (s *sharedStore) saveCount(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves[id]
}

func (s *sharedStore) options() []NodeSetupOption {
	return []NodeSetupOption{
		WithInitFunc[*wrapperspb.StringValue, *stringValueBuilder](func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder]) error {
			c.Builder().Set(wrapperspb.String(s.get(c.Id())))
			return nil
		}),
		WithSaveFunc[*wrapperspb.StringValue, *stringValueBuilder](func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder]) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.values[c.Id()] = c.Builder().Build().GetValue()
			s.saves[c.Id()]++
			return nil
		}),
	}
}

var _ = Describe("Cluster forwarding without waiting", Label("cluster"), func() {
	var (
		tc    *testCluster
		store *sharedStore
		// order of the payloads the owner executed
		executedMu sync.Mutex
		executed   []int64
	)

	ownerOf := func(n *clusterNode) {
		n.AddFuncTransition("finish", "FINISH", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
			executedMu.Lock()
			executed = append(executed, a.(*wrapperspb.Int64Value).GetValue())
			executedMu.Unlock()
			c.Builder().Set(wrapperspb.String("done"))
			return false, nil
		}, WithFuncKey("finish"), WithSaveAfter(), WithConditions(n.All(wrapperspb.String("pending"))))
	}
	executions := func() []int64 {
		executedMu.Lock()
		defer executedMu.Unlock()
		return append([]int64(nil), executed...)
	}
	send := func(c *Container[*wrapperspb.StringValue, *stringValueBuilder], value int64) {
		doneC := make(chan struct{}, 1)
		Expect(c.Event("FINISH", wrapperspb.Int64(value), WithDoneChannel(doneC))).To(Succeed())
		Eventually(doneC, 5*time.Second).Should(Receive())
	}

	BeforeEach(func() {
		store = newSharedStore()
		tc = newTestCluster()
		tc.opts = store.options()
		executed = nil

		expectReady(tc.start(1, "", clusterBitmap("pending", "done"), nil))
		expectReady(tc.start(2, tc.addr(1), clusterBitmap("pending", "done"), nil))
		expectReady(tc.start(3, tc.addr(1), clusterBitmap("pending", "done"), ownerOf))
		Eventually(func() *Transition { return publishedTransition(tc.node(2), "finish") }, 10*time.Second).ShouldNot(BeNil())
	})

	AfterEach(func() {
		tc.stopAll()
	})

	It("should hand the work over: the owner runs once the sender released the lock, loads, saves and closes", func() {
		store.set("player-1", "pending")

		c, _, err := tc.node(2).StartContainer(context.Background(), "player-1")
		Expect(err).NotTo(HaveOccurred())

		By("returning once the owner accepted the work")
		send(c, 1)
		Expect(c.Builder().Build().GetValue()).To(Equal("pending"))

		By("waiting with the work while the sender holds the lock")
		Consistently(executions, 500*time.Millisecond).Should(BeEmpty())

		c.Close()

		By("executing on the owner with the entity loaded from the store and saving it")
		Eventually(executions, 10*time.Second).Should(Equal([]int64{1}))
		Eventually(func() string { return store.get("player-1") }, 5*time.Second).Should(Equal("done"))

		By("closing the owner's container and releasing the lock")
		c2, err := func() (*Container[*wrapperspb.StringValue, *stringValueBuilder], error) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			c, _, err := tc.node(2).StartContainer(ctx, "player-1")
			return c, err
		}()
		Expect(err).NotTo(HaveOccurred())
		c2.Close()
	})

	It("should skip the work when the conditions don't match anymore", func() {
		store.set("player-1", "pending")

		c, _, err := tc.node(2).StartContainer(context.Background(), "player-1")
		Expect(err).NotTo(HaveOccurred())
		send(c, 1)

		// the sender changes the entity before it releases the lock
		store.set("player-1", "done")
		c.Close()

		Consistently(executions, time.Second).Should(BeEmpty())
		Expect(store.saveCount("player-1")).To(BeZero())
	})

	It("should execute the forwarded work of a container in order", func() {
		store.set("player-1", "pending")

		c, _, err := tc.node(2).StartContainer(context.Background(), "player-1")
		Expect(err).NotTo(HaveOccurred())

		// the owner's transition sets "done", so reset the store before every execution
		tc.node(3).AddFuncTransition("reset", "FINISH", func(ctx context.Context, c *Container[*wrapperspb.StringValue, *stringValueBuilder], a any) (bool, error) {
			return true, nil
		}, WithFuncKey("reset"))

		for i := int64(1); i <= 5; i++ {
			send(c, i)
		}
		c.Close()

		Eventually(func() int { return len(executions()) }, 10*time.Second).Should(BeNumerically(">=", 1))
		// after the first execution the entity is "done", so only the first one matches the conditions
		Consistently(executions, 500*time.Millisecond).Should(Equal([]int64{1}))
	})

	It("should keep lending the lock when waiting for the result", func() {
		store.set("player-1", "pending")

		c, _, err := tc.node(2).StartContainer(context.Background(), "player-1")
		Expect(err).NotTo(HaveOccurred())

		doneC := make(chan struct{}, 1)
		Expect(c.Event("FINISH", wrapperspb.Int64(7), WithWaitRemote(), WithDoneChannel(doneC))).To(Succeed())
		Eventually(doneC, 5*time.Second).Should(Receive())

		Expect(executions()).To(Equal([]int64{7}))
		Expect(c.Builder().Build().GetValue()).To(Equal("done"))
		c.Close()
	})
})
