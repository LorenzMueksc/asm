package asm

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"rumue.io/asm/service"
)

// protoOf returns the transition in its wire format as sent by the given node.
func protoOf(t *Transition, nodeId string) []*service.Transition {
	return []*service.Transition{t.toProto(nodeId)}
}

var _ = Describe("Transitions", func() {
	var (
		node     *Node[*TestStateEntity, *mockBuilder]
		stateMap map[string]int
	)

	actionA := func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
		return false, nil
	}
	actionB := func(ctx context.Context, c *Container[*TestStateEntity, *mockBuilder], a any) (bool, error) {
		return true, nil
	}

	BeforeEach(func() {
		var entityBitmap *Bitmap
		entityBitmap, stateMap = createSimpleTestBitmap()
		node = NewNode(
			1,
			getTestBitmapByType,
			newMockBuilder,
			"",
			WithSkipRaft(),
			WithBitmap(&Bitmap{Name: "root", Children: map[string]*Bitmap{entityBitmap.Name: entityBitmap}}),
			WithLogger(zap.NewNop()),
		)
		Eventually(node.Ready()).Should(BeClosed())
	})

	published := func() []*Transition {
		return node.registry.published.Load().list()
	}
	byName := func(name string) *Transition {
		for _, t := range published() {
			if t.Name == name {
				return t
			}
		}
		return nil
	}

	Context("signature", func() {
		It("should not depend on the order of conditions", func() {
			node.AddFuncTransition("a", "E", actionA, WithConditions(
				node.All(&TestStateEntity{Status: "pending"}),
				node.None(&TestStateEntity{Active: true}),
			))
			node.AddFuncTransition("b", "E", actionA, WithConditions(
				node.None(&TestStateEntity{Active: true}),
				node.All(&TestStateEntity{Status: "pending"}),
			))

			Expect(byName("a").Signature).To(Equal(byName("b").Signature))
		})

		It("should differ for different funcs, conditions and event names", func() {
			node.AddFuncTransition("base", "E", actionA)
			node.AddFuncTransition("func", "E", actionB)
			node.AddFuncTransition("event", "F", actionA)
			node.AddFuncTransition("cond", "E", actionA, WithConditions(node.All(&TestStateEntity{Status: "done"})))

			base := byName("base").Signature
			Expect(byName("func").Signature).NotTo(Equal(base))
			Expect(byName("event").Signature).NotTo(Equal(base))
			Expect(byName("cond").Signature).NotTo(Equal(base))
		})

		It("should not depend on priority, batching or save on change", func() {
			node.AddFuncTransition("a", "E", actionA, WithFuncKey("key"))
			node.AddFuncTransition("b", "E", actionA, WithFuncKey("key"), WithPriority(5), WithBatching(3, time.Second), WithSaveAfter())

			Expect(byName("a").Signature).To(Equal(byName("b").Signature))
		})

		It("should not depend on the committed bitmap, only on the example entities", func() {
			node.AddFuncTransition("a", "E", actionA, WithFuncKey("key"), WithConditions(node.All(&TestStateEntity{Status: "unknown"})))
			t := byName("a")
			// "unknown" isn't a bitmap value, so it has no bit, which doesn't change the signature
			Expect(t.Signature).To(Equal(t.computeSignature()))
			withBit := copyTransition(t)
			withBit.Conditions[0].States = map[string][]Bits{"TestStateEntity": {NewBits(42)}}
			Expect(withBit.computeSignature()).To(Equal(t.Signature))
			Expect(withBit.compiledKey()).NotTo(Equal(t.compiledKey()))
		})

		It("should use the func key instead of the func name", func() {
			node.AddFuncTransition("a", "E", actionA, WithFuncKey("key"))
			node.AddFuncTransition("b", "E", actionB, WithFuncKey("key"))

			Expect(byName("a").Signature).To(Equal(byName("b").Signature))
		})
	})

	Context("compiling", func() {
		It("should compile state transitions to the target bits", func() {
			node.AddStateTransition("done", "E", &TestStateEntity{Status: "completed", Count: 10})

			state, ok := byName("done").stateAction()
			Expect(ok).To(BeTrue())
			Expect(state).To(Equal(mask(stateMap["status_completed"] | stateMap["count_10"] | stateMap["active_false"])))
		})

		It("should fail the node when a local transition is registered twice with different signatures", func() {
			node.AddFuncTransition("a", "E", actionA)
			node.AddFuncTransition("a", "E", actionB)

			Eventually(node.Err()).Should(Receive(MatchError(ContainSubstring("registered twice"))))
		})
	})

	Context("wire format", func() {
		It("should convert func transitions to remote transitions", func() {
			node.AddFuncTransition("a", "E", actionA, WithBatching(3, time.Second), WithPriority(2),
				WithConditions(node.All(&TestStateEntity{Status: "pending"})))

			t := transitionFromProto(byName("a").toProto("7"))
			Expect(t.Remote).To(BeTrue())
			Expect(t.NodeIds).To(Equal([]string{"7"}))
			Expect(t.Actions).To(BeEmpty())
			Expect(t.Batching).To(Equal(Batching{Count: 3, Period: time.Second}))
			Expect(t.Priority).To(Equal(2))
			Expect(t.Signature).To(Equal(byName("a").Signature))
			Expect(t.Conditions[0].States).To(Equal(byName("a").Conditions[0].States))
		})

		It("should keep negative priorities", func() {
			node.AddFuncTransition("a", "E", actionA, WithPriority(-3))
			node.AddStateTransition("done", "E", &TestStateEntity{Status: "completed"}, WithPriority(-7))

			Expect(byName("a").toProto("7").GetPriority()).To(Equal(int32(-3)))
			Expect(transitionFromProto(byName("a").toProto("7")).Priority).To(Equal(-3))
			Expect(transitionFromProto(byName("done").toProto("7")).Priority).To(Equal(-7))
		})

		It("should convert state transitions to local state transitions", func() {
			node.AddStateTransition("done", "E", &TestStateEntity{Status: "completed"})

			t := transitionFromProto(byName("done").toProto("7"))
			Expect(t.Remote).To(BeFalse())
			Expect(t.Actions).To(Equal(byName("done").Actions))
			Expect(t.Signature).To(Equal(byName("done").Signature))
		})
	})

	Context("merging transitions of other nodes", func() {
		var remote *Transition

		BeforeEach(func() {
			node.AddFuncTransition("local", "E", actionA, WithFuncKey("local"))
			remote = copyTransition(byName("local"))
			remote.Name = "remote"
		})

		It("should add transitions of other nodes as remote transitions", func() {
			Expect(node.addRemoteTransitions(protoOf(remote, "2"))).To(Succeed())

			t := byName("remote")
			Expect(t.Remote).To(BeTrue())
			Expect(t.NodeIds).To(Equal([]string{"2"}))
		})

		It("should add owners with the same signature for load balancing", func() {
			Expect(node.addRemoteTransitions(protoOf(remote, "2"))).To(Succeed())
			Expect(node.addRemoteTransitions(protoOf(remote, "3"))).To(Succeed())

			Expect(byName("remote").NodeIds).To(ConsistOf("2", "3"))
		})

		It("should add other nodes as owners of a local transition with the same signature", func() {
			Expect(node.addRemoteTransitions(protoOf(byName("local"), "2"))).To(Succeed())

			t := byName("local")
			Expect(t.Remote).To(BeFalse())
			Expect(t.NodeIds).To(Equal([]string{"1", "2"}))
		})

		It("should reject a transition with the same name and a different signature", func() {
			Expect(node.addRemoteTransitions(protoOf(remote, "2"))).To(Succeed())

			other := copyTransition(remote)
			other.EventName = "other"
			other.Signature = other.computeSignature()
			Expect(node.addRemoteTransitions(protoOf(other, "3"))).To(MatchError(ContainSubstring("different signature")))

			conflictingLocal := copyTransition(byName("local"))
			conflictingLocal.Signature = "other"
			Expect(node.addRemoteTransitions(protoOf(conflictingLocal, "3"))).To(MatchError(ContainSubstring("different signature")))
		})

		It("should replace the transition of a node sending it again", func() {
			Expect(node.addRemoteTransitions(protoOf(remote, "2"))).To(Succeed())

			updated := copyTransition(remote)
			updated.EventName = "other"
			updated.Signature = updated.computeSignature()
			Expect(node.addRemoteTransitions(protoOf(updated, "2"))).To(Succeed())

			Expect(byName("remote").EventName).To(Equal("other"))
		})

		It("should remove owners which left the cluster", func() {
			Expect(node.addRemoteTransitions(protoOf(remote, "2"))).To(Succeed())
			Expect(node.addRemoteTransitions(protoOf(remote, "3"))).To(Succeed())

			node.removeTransitionOwner("2")
			Expect(byName("remote").NodeIds).To(Equal([]string{"3"}))

			node.removeTransitionOwner("3")
			Expect(byName("remote")).To(BeNil())
		})
	})
})
