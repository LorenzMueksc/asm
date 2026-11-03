package asm

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bitmap layout", func() {
	entityBitmap := func(name string, properties ...*Property) *Bitmap {
		pos := 0
		return BuildBitmap(&Property{Name: name, Properties: properties}, &pos)
	}
	indexOf := func(bm *Bitmap, path []string, value any) (uint64, bool) {
		for _, p := range path {
			bm = bm.Children[p]
			if bm == nil {
				return 0, false
			}
		}
		if v := getExactValue(bm.Values, value); v != nil {
			return v.Index, true
		}
		return 0, false
	}

	It("should adopt the first definition with its indices", func() {
		entry := entityBitmap("Order",
			&Property{Name: "status", Values: []any{"pending", "done"}},
			&Property{Name: "express", Values: []any{true, false}},
		)

		layout, err := applyBitmapEntry(nil, entry)
		Expect(err).NotTo(HaveOccurred())
		Expect(layout.Children["Order"]).To(Equal(entry))
		Expect(layout.Children["Order"]).NotTo(BeIdenticalTo(entry))
	})

	It("should keep the committed indices for the same values in a different order", func() {
		first := entityBitmap("Order", &Property{Name: "status", Values: []any{"pending", "done"}})
		second := entityBitmap("Order", &Property{Name: "status", Values: []any{"done", "pending"}})

		layout, err := applyBitmapEntry(nil, first)
		Expect(err).NotTo(HaveOccurred())
		merged, err := applyBitmapEntry(layout, second)
		Expect(err).NotTo(HaveOccurred())

		Expect(merged).To(Equal(layout))
	})

	It("should append missing values with the next free index and never move indices", func() {
		first := entityBitmap("Order",
			&Property{Name: "status", Values: []any{"pending", "done"}},
			&Property{Name: "express", Values: []any{true}},
		)
		second := entityBitmap("Order",
			&Property{Name: "status", Values: []any{"cancelled", "pending"}},
			&Property{Name: "priority", Values: []any{int64(1)}},
		)

		layout, err := applyBitmapEntry(nil, first)
		Expect(err).NotTo(HaveOccurred())
		merged, err := applyBitmapEntry(layout, second)
		Expect(err).NotTo(HaveOccurred())

		order := merged.Children["Order"]
		for _, check := range []struct {
			path  string
			value any
			index uint64
		}{
			{"status", "pending", 0},
			{"status", "done", 1},
			{"express", true, 2},
			// children are walked in name order, so priority is merged before status
			{"priority", int64(1), 3},
			{"status", "cancelled", 4},
		} {
			index, ok := indexOf(order, []string{check.path}, check.value)
			Expect(ok).To(BeTrue(), "%v", check.value)
			Expect(index).To(Equal(check.index), "%v", check.value)
		}

		// the original layout is not modified
		_, ok := indexOf(layout.Children["Order"], []string{"status"}, "cancelled")
		Expect(ok).To(BeFalse())
	})

	It("should assign the same indices independent of the order of children", func() {
		first := entityBitmap("Order", &Property{Name: "status", Values: []any{"pending"}})
		a := entityBitmap("Order",
			&Property{Name: "b", Values: []any{"x"}},
			&Property{Name: "a", Values: []any{"y"}},
		)
		b := entityBitmap("Order",
			&Property{Name: "a", Values: []any{"y"}},
			&Property{Name: "b", Values: []any{"x"}},
		)

		layout, _ := applyBitmapEntry(nil, first)
		mergedA, err := applyBitmapEntry(layout, a)
		Expect(err).NotTo(HaveOccurred())
		mergedB, err := applyBitmapEntry(layout, b)
		Expect(err).NotTo(HaveOccurred())

		Expect(mergedA).To(Equal(mergedB))
	})

	It("should merge nested properties", func() {
		first := entityBitmap("Order", &Property{Name: "address", Properties: []*Property{{Name: "country", Values: []any{"AT"}}}})
		second := entityBitmap("Order", &Property{Name: "address", Properties: []*Property{{Name: "country", Values: []any{"DE", "AT"}}}})

		layout, _ := applyBitmapEntry(nil, first)
		merged, err := applyBitmapEntry(layout, second)
		Expect(err).NotTo(HaveOccurred())

		index, ok := indexOf(merged.Children["Order"], []string{"address", "country"}, "DE")
		Expect(ok).To(BeTrue())
		Expect(index).To(Equal(uint64(1)))
	})

	It("should reject a value of a different type in the same property", func() {
		first := entityBitmap("Order", &Property{Name: "priority", Values: []any{int64(1)}})
		second := entityBitmap("Order", &Property{Name: "priority", Values: []any{"high"}})

		layout, _ := applyBitmapEntry(nil, first)
		_, err := applyBitmapEntry(layout, second)
		Expect(err).To(MatchError(ContainSubstring("conflicts with values of type")))
	})

	It("should reject a property which is nested on one node and has values on another", func() {
		first := entityBitmap("Order", &Property{Name: "address", Values: []any{"AT"}})
		second := entityBitmap("Order", &Property{Name: "address", Properties: []*Property{{Name: "country", Values: []any{"AT"}}}})

		layout, _ := applyBitmapEntry(nil, first)
		_, err := applyBitmapEntry(layout, second)
		Expect(err).To(MatchError(ContainSubstring("nested on one node")))
	})

	It("should reject a first definition using an index twice", func() {
		entry := &Bitmap{Name: "Order", Children: map[string]*Bitmap{
			"a": {Name: "a", Values: []*BitValue{{Value: "x", Index: 0}}},
			"b": {Name: "b", Values: []*BitValue{{Value: "y", Index: 0}}},
		}}

		_, err := applyBitmapEntry(nil, entry)
		Expect(err).To(MatchError(ContainSubstring("index 0 used by")))
	})

	It("should round trip through the wire format with normalized value types", func() {
		bm := entityBitmap("Order",
			&Property{Name: "status", Values: []any{"pending"}},
			&Property{Name: "priority", Values: []any{1, int64(2)}},
			&Property{Name: "express", Values: []any{true}},
			&Property{Name: "weight", Values: []any{1.5}},
		)

		pb, err := bitmapToProto(bm)
		Expect(err).NotTo(HaveOccurred())
		decoded, err := bitmapFromProto(pb)
		Expect(err).NotTo(HaveOccurred())

		Expect(decoded.Children["priority"].Values[0].Value).To(Equal(int64(1)))
		Expect(decoded.Children["priority"].Values[1].Value).To(Equal(int64(2)))
		Expect(decoded.Children["status"].Values[0]).To(Equal(&BitValue{Value: "pending", Index: 0}))
		Expect(decoded.Children["express"].Values[0].Value).To(Equal(true))
		Expect(decoded.Children["weight"].Values[0].Value).To(Equal(1.5))
	})

	It("should reject unsupported value types", func() {
		bm := entityBitmap("Order", &Property{Name: "tags", Values: []any{[]string{"a"}}})

		_, err := bitmapToProto(bm)
		Expect(err).To(MatchError(ContainSubstring("unsupported bitmap value type")))
	})
})
