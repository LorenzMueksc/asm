package asm

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bits", func() {
	It("should only add words when needed", func() {
		Expect(NewBits()).To(BeNil())
		Expect(NewBits(0, 63)).To(HaveLen(1))
		Expect(NewBits(64)).To(HaveLen(2))
		Expect(NewBits(130)).To(HaveLen(3))
	})

	It("should set and report bits beyond the first word", func() {
		b := NewBits(1, 64, 200)
		Expect(b.Has(1)).To(BeTrue())
		Expect(b.Has(64)).To(BeTrue())
		Expect(b.Has(200)).To(BeTrue())
		Expect(b.Has(2)).To(BeFalse())
		Expect(b.Has(1000)).To(BeFalse())
		Expect(b.Indices()).To(Equal([]uint64{1, 64, 200}))
	})

	It("should not modify the receiver", func() {
		b := NewBits(1)
		_ = b.With(2)
		_ = b.Or(NewBits(3))
		Expect(b).To(Equal(NewBits(1)))
	})

	It("should combine bitsets of different lengths", func() {
		a := NewBits(1, 70)
		b := NewBits(1, 2)
		Expect(a.Or(b)).To(Equal(NewBits(1, 2, 70)))
		Expect(a.And(b)).To(Equal(NewBits(1)))
		Expect(a.AndNot(b)).To(Equal(NewBits(70)))
		Expect(b.AndNot(a)).To(Equal(NewBits(2)))
	})

	It("should trim trailing zero words", func() {
		a := NewBits(1, 70)
		Expect(a.AndNot(NewBits(70))).To(HaveLen(1))
		Expect(a.AndNot(a)).To(BeNil())
	})

	It("should check containment across words", func() {
		state := NewBits(1, 65)
		Expect(state.ContainsAll(NewBits(65))).To(BeTrue())
		Expect(state.ContainsAll(NewBits(1, 65))).To(BeTrue())
		Expect(state.ContainsAll(NewBits(130))).To(BeFalse())
		Expect(state.ContainsAll(nil)).To(BeTrue())
		Expect(state.ContainsNone(NewBits(2, 130))).To(BeTrue())
		Expect(state.ContainsNone(NewBits(65))).To(BeFalse())
	})

	It("should compare bitsets ignoring trailing zero words", func() {
		Expect(Bits{1, 0}.Equal(Bits{1})).To(BeTrue())
		Expect(Bits{}.Equal(nil)).To(BeTrue())
		Expect(NewBits(1).Equal(NewBits(2))).To(BeFalse())
	})

	It("should round trip through the wire format", func() {
		b := NewBits(3, 99)
		Expect(bitsFromProto(b.toProto())).To(Equal(b))
	})
})
