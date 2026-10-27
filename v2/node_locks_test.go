package asm

import (
	"fmt"
	"math/rand/v2"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Lock table queue", func() {
	var (
		lt lockTable
		t0 = time.Now()
	)

	holder := func(id string) string {
		l, _, _ := lt.get(id)
		return l.holder + "/" + l.token
	}
	queue := func(id string) []lockWaiter {
		l, _, _ := lt.get(id)
		return l.queue
	}
	acquire := func(node, token string, at time.Time) {
		lt.applyAcquire(node, at, "p", token, nil, "")
	}
	w := func(node, token string) lockWaiter {
		return lockWaiter{node: node, token: token}
	}

	BeforeEach(func() {
		lt = newLockTable()
		for _, node := range []string{"a", "b", "c"} {
			lt.applyHeartbeat(node, t0, 10*time.Second)
		}
	})

	It("should hand the lock to the waiters in the order they asked", func() {
		acquire("a", "a1", t0)
		acquire("b", "b1", t0)
		acquire("c", "c1", t0)
		Expect(holder("p")).To(Equal("a/a1"))
		Expect(queue("p")).To(Equal([]lockWaiter{w("b", "b1"), w("c", "c1")}))

		lt.applyRelease("a", t0, "p", "a1")
		Expect(holder("p")).To(Equal("b/b1"))
		lt.applyRelease("b", t0, "p", "b1")
		Expect(holder("p")).To(Equal("c/c1"))
		lt.applyRelease("c", t0, "p", "c1")
		_, held, _ := lt.get("p")
		Expect(held).To(BeFalse())
	})

	It("should give the holder the lock again only if nobody waits, otherwise queue it", func() {
		acquire("a", "a1", t0)
		acquire("a", "a2", t0)
		Expect(holder("p")).To(Equal("a/a2"))

		acquire("b", "b1", t0)
		acquire("a", "a3", t0)
		Expect(holder("p")).To(Equal("a/a2"))
		Expect(queue("p")).To(Equal([]lockWaiter{w("b", "b1"), w("a", "a3")}))

		By("ignoring the release of an old token")
		lt.applyRelease("a", t0, "p", "a1")
		Expect(holder("p")).To(Equal("a/a2"))

		lt.applyRelease("a", t0, "p", "a2")
		Expect(holder("p")).To(Equal("b/b1"))
		lt.applyRelease("b", t0, "p", "b1")
		Expect(holder("p")).To(Equal("a/a3"))
	})

	It("should remove a cancelled waiter and pass on a lock handed to a cancelled attempt", func() {
		acquire("a", "a1", t0)
		acquire("b", "b1", t0)
		acquire("c", "c1", t0)

		lt.applyCancel("c", t0, "p", "c1")
		Expect(queue("p")).To(Equal([]lockWaiter{w("b", "b1")}))

		acquire("c", "c2", t0)
		lt.applyRelease("a", t0, "p", "a1")
		Expect(holder("p")).To(Equal("b/b1"))

		lt.applyCancel("b", t0, "p", "b1")
		Expect(holder("p")).To(Equal("c/c2"))
		Expect(queue("p")).To(BeEmpty())
	})

	It("should skip waiters whose node expired", func() {
		lt.applyHeartbeat("b", t0, time.Second)
		acquire("a", "a1", t0)
		acquire("b", "b1", t0)
		acquire("c", "c1", t0)

		lt.applyRelease("a", t0.Add(2*time.Second), "p", "a1")
		Expect(holder("p")).To(Equal("c/c1"))
		Expect(queue("p")).To(BeEmpty())
	})

	It("should hand an expired lock to the first waiter, not to the node asking", func() {
		lt.applyHeartbeat("a", t0, time.Second)
		acquire("a", "a1", t0)
		acquire("b", "b1", t0)

		acquire("c", "c1", t0.Add(2*time.Second))
		Expect(holder("p")).To(Equal("b/b1"))
		Expect(queue("p")).To(Equal([]lockWaiter{w("c", "c1")}))
	})

	It("should drop a removed node from the queues and pass its locks on", func() {
		acquire("a", "a1", t0)
		acquire("b", "b1", t0)
		acquire("c", "c1", t0)

		changes := lt.removeNode("a")
		Expect(changes).To(HaveKey("p"))
		Expect(holder("p")).To(Equal("b/b1"))

		lt.removeNode("c")
		Expect(queue("p")).To(BeEmpty())
	})
})

var _ = Describe("Lock table deadlock check", func() {
	var (
		lt lockTable
		t0 = time.Now()
	)

	// call proposes the acquire of a waited call with the lock tokens of the containers waiting for it
	call := func(node, id, token string, path ...string) (joined, refused bool) {
		_, after, refused := lt.applyAcquire(node, t0, id, token, path, "")
		return after.holder == node && after.token != token && after.called(token), refused
	}
	join := func(node, id, token, joinToken string, path ...string) (joined, refused bool) {
		_, after, refused := lt.applyAcquire(node, t0, id, token, path, joinToken)
		return after.holder == node && after.token != token && after.called(token), refused
	}
	holder := func(id string) string {
		l, _, _ := lt.get(id)
		return l.holder + "/" + l.token
	}

	BeforeEach(func() {
		lt = newLockTable()
		for _, node := range []string{"a", "b", "c"} {
			lt.applyHeartbeat(node, t0, 10*time.Second)
		}
		// player p held by node a, match m held by node b
		lt.applyAcquire("a", t0, "p", "tp", nil, "")
		lt.applyAcquire("b", t0, "m", "tm", nil, "")
	})

	It("should refuse a call back to a container waiting for it", func() {
		By("p calling q, which gets the lock")
		_, refused := call("c", "q", "tq", "tp")
		Expect(refused).To(BeFalse())
		Expect(holder("q")).To(Equal("c/tq"))

		By("q calling p")
		_, refused = call("a", "p", "x1", "tp", "tq")
		Expect(refused).To(BeTrue())
		Expect(holder("p")).To(Equal("a/tp"))
		l, _, _ := lt.get("p")
		Expect(l.queue).To(BeEmpty())
	})

	It("should refuse the call closing a cycle across containers, in either order", func() {
		By("p waiting for m")
		_, refused := call("b", "m", "x1", "tp")
		Expect(refused).To(BeFalse())

		By("m calling p")
		_, refused = call("a", "p", "x2", "tm")
		Expect(refused).To(BeTrue())

		By("the other order: q waiting for p first")
		lt.applyAcquire("c", t0, "q", "tq", nil, "")
		_, refused = call("a", "p", "x3", "tq")
		Expect(refused).To(BeFalse())
		_, refused = call("c", "q", "x4", "tp")
		Expect(refused).To(BeTrue())
	})

	It("should find cycles through several locks", func() {
		lt.applyAcquire("c", t0, "q", "tq", nil, "")
		// p waits for m, m waits for q
		_, refused := call("b", "m", "x1", "tp")
		Expect(refused).To(BeFalse())
		_, refused = call("c", "q", "x2", "tm")
		Expect(refused).To(BeFalse())

		By("q calling p")
		_, refused = call("a", "p", "x3", "tq")
		Expect(refused).To(BeTrue())

		By("an unrelated container calling p queues")
		_, refused = call("a", "p", "x4", "tz")
		Expect(refused).To(BeFalse())
	})

	It("should let calls of the same caller queue behind each other", func() {
		By("p calling q, done, while q's container still holds the lock")
		_, refused := call("c", "q", "tq", "tp")
		Expect(refused).To(BeFalse())
		lt.applyDone("c", t0, "q", "tq")

		By("p calling q again")
		_, refused = call("c", "q", "x1", "tp")
		Expect(refused).To(BeFalse())
		l, _, _ := lt.get("q")
		Expect(l.queue).To(HaveLen(1))

		lt.applyRelease("c", t0, "q", "tq")
		Expect(holder("q")).To(Equal("c/x1"))
		l, _, _ = lt.get("q")
		Expect(l.called("x1")).To(BeTrue())
	})

	It("should join the open container a call asked for", func() {
		joined, refused := join("b", "m", "x1", "tm", "tp")
		Expect(joined).To(BeTrue())
		Expect(refused).To(BeFalse())

		By("asking for a container which isn't the holder anymore")
		joined, _ = join("b", "m", "x2", "old", "tq")
		Expect(joined).To(BeFalse())
		l, _, _ := lt.get("m")
		Expect(l.queued("b", "x2")).To(BeTrue())
	})

	It("should know joined calls wait for the container until they are done", func() {
		joined, _ := join("b", "m", "x1", "tm", "tp")
		Expect(joined).To(BeTrue())

		By("m calling p while p's call waits for m")
		_, refused := call("a", "p", "x2", "tm")
		Expect(refused).To(BeTrue())

		By("the joined call being done")
		lt.applyDone("b", t0, "m", "x1")
		_, refused = call("a", "p", "x3", "tm")
		Expect(refused).To(BeFalse())
	})

	It("should drop the calls of a container when its lock is released", func() {
		joined, _ := join("b", "m", "x1", "tm", "tp")
		Expect(joined).To(BeTrue())
		lt.applyRelease("b", t0, "m", "tm")
		_, held, _ := lt.get("m")
		Expect(held).To(BeFalse())
		Expect(lt.waitedFor).To(BeEmpty())
	})

	It("should not give the lock again to a node with calls running", func() {
		joined, _ := join("b", "m", "x1", "tm", "tp")
		Expect(joined).To(BeTrue())
		lt.applyAcquire("b", t0, "m", "tm2", nil, "")
		Expect(holder("m")).To(Equal("b/tm"))
		l, _, _ := lt.get("m")
		Expect(l.queued("b", "tm2")).To(BeTrue())
	})

	It("should drop a call which gave up", func() {
		joined, _ := join("b", "m", "x1", "tm", "tp")
		Expect(joined).To(BeTrue())
		lt.applyCancel("b", t0, "m", "x1")
		l, _, _ := lt.get("m")
		Expect(l.calls).To(BeEmpty())
	})
})

var _ = Describe("Lock table wait path index", func() {
	It("should stay the same as one built from the locks, whatever happens to them", func() {
		lt := newLockTable()
		t0 := time.Now()
		nodes := []string{"a", "b", "c"}
		for _, node := range nodes {
			lt.applyHeartbeat(node, t0, time.Hour)
		}
		rebuilt := func() map[string]map[string]int {
			index := make(map[string]map[string]int)
			for id, l := range lt.locks {
				l.waitPaths(func(path []string) {
					for _, t := range path {
						if index[t] == nil {
							index[t] = make(map[string]int)
						}
						index[t][id]++
					}
				})
			}
			return index
		}

		r := rand.New(rand.NewPCG(1, 2))
		ids := []string{"p", "q", "m"}
		var tokens []string
		for i := range 2000 {
			node, id := nodes[r.IntN(len(nodes))], ids[r.IntN(len(ids))]
			token := fmt.Sprintf("%s%d", node, i)
			var path []string
			if r.IntN(2) == 0 && len(tokens) > 0 {
				path = []string{tokens[r.IntN(len(tokens))]}
			}
			old := ""
			if len(tokens) > 0 {
				old = tokens[r.IntN(len(tokens))]
			}
			switch r.IntN(6) {
			case 0, 1:
				join := ""
				if l, ok := lt.locks[id]; ok && r.IntN(2) == 0 {
					join = l.token
				}
				lt.applyAcquire(node, t0, id, token, path, join)
				tokens = append(tokens, token)
			case 2:
				lt.applyRelease(node, t0, id, old)
			case 3:
				lt.applyDone(node, t0, id, old)
			case 4:
				lt.applyCancel(node, t0, id, old)
			case 5:
				if r.IntN(20) == 0 {
					lt.removeNode(node)
					lt.applyHeartbeat(node, t0, time.Hour)
				}
			}
			Expect(lt.waitedFor).To(Equal(rebuilt()), "after operation %d", i)
		}
	})
})
