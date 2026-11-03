// bitmaps_test.go
package asm

import (
	"os"
	"path/filepath"
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type SimpleEntity struct {
	Status string `json:"status,omitempty"`
	Count  int    `json:"count,omitempty"`
	Active bool   `json:"active,omitempty"`
}

type NestedEntity struct {
	ID         string        `json:"id,omitempty"`
	Simple     *SimpleEntity `json:"simple,omitempty"`
	unexported int           // Should be ignored
	NoJsonTag  string
}

type EnumType int

const (
	EnumVal1 EnumType = 10
	EnumVal2 EnumType = 20
)

type EnumEntity struct {
	EnumTypeField EnumType `json:"enum_type,omitempty"`
	IntField      int      `json:"int_field,omitempty"` // Use int in bitmap
}

var _ = Describe("Bitmaps Functionality", func() {

	var (
		testDir    string
		tomlPath   string
		sampleBM   *Bitmap
		nestedBM   *Bitmap
		enumBM     *Bitmap
		bitCounter int
	)

	// --- Setup ---
	BeforeEach(func() {
		var err error
		testDir, err = os.MkdirTemp("", "bitmap-test")
		Expect(err).NotTo(HaveOccurred())

		tomlPath = filepath.Join(testDir, "test_bitmap.toml")

		// Create a sample Bitmap structure manually for most tests
		// This avoids file I/O dependency for core logic tests
		bitCounter = 0 // Reset bit counter for each test where BuildBitmap is used

		// Sample Simple Bitmap
		sampleBMRoot := &Property{
			Name: "simpleEntity",
			Properties: []*Property{
				{Name: "status", Values: []any{"pending", "processing", "completed"}},
				{Name: "count", Values: []any{0, 1, 10}},
				{Name: "active", Values: []any{true, false}},
			},
		}
		sampleBM = BuildBitmap(sampleBMRoot, &bitCounter)

		// Sample Nested Bitmap
		bitCounter = 0
		nestedPropsRoot := &Property{
			Name: "nestedEntity",
			Properties: []*Property{
				{Name: "id", Values: []any{"id1", "id2"}},
				{Name: "simple", Properties: []*Property{
					{Name: "status", Values: []any{"pending", "processing"}},
					{Name: "count", Values: []any{5, 15}},
					{Name: "active", Values: []any{true}},
				}},
			},
		}
		nestedBM = BuildBitmap(nestedPropsRoot, &bitCounter)
		// Manually set names for nested children
		if simpleChild, ok := nestedBM.Children["simple"]; ok {
			for name := range simpleChild.Children {
				simpleChild.Children[name].Name = name
			}
		}

		// Sample Enum Bitmap
		bitCounter = 0
		enumPropsRoot := &Property{
			Name: "enumEntity",
			Properties: []*Property{
				{Name: "enum_type", Values: []any{int(EnumVal1), int(EnumVal2)}}, // Use underlying type in bitmap definition
				{Name: "int_field", Values: []any{10, 20}},
			},
		}
		enumBM = BuildBitmap(enumPropsRoot, &bitCounter)
	})

	// --- Teardown ---
	AfterEach(func() {
		err := os.RemoveAll(testDir)
		Expect(err).NotTo(HaveOccurred())
	})

	// --- Tests for LoadBitMapFromDisk ---
	Describe("Loading Bitmap from Disk", func() {
		Context("when the TOML file is valid", func() {
			BeforeEach(func() {
				tomlContent := `
version = "1.0"
[[properties]]
name = "status"
values = ["pending", "processing", "completed"]
[[properties]]
name = "nested"
	[[properties.properties]]
	name = "active"
	values = [true, false]
	[[properties.properties]]
	name = "level"
	values = [1, 2]
`
				err := os.WriteFile(tomlPath, []byte(tomlContent), 0644)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should load the bitmap structure correctly", func() {
				bm, err := LoadBitMapFromDisk(tomlPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(bm).NotTo(BeNil())
				Expect(bm.Name).To(Equal("root"))
				Expect(bm.Children).To(HaveLen(2))

				Expect(bm.Children).To(HaveKey("status"))
				statusChild := bm.Children["status"]
				Expect(statusChild.Name).To(Equal("status"))
				Expect(statusChild.Values).To(HaveLen(3))
				Expect(statusChild.Values[0].Value).To(Equal("pending"))
				Expect(statusChild.Values[0].Index).To(Equal(uint64(0))) // Bit depends on processing order, assume "nested" processed first
				Expect(statusChild.Values[1].Value).To(Equal("processing"))
				Expect(statusChild.Values[1].Index).To(Equal(uint64(1)))
				Expect(statusChild.Values[2].Value).To(Equal("completed"))
				Expect(statusChild.Values[2].Index).To(Equal(uint64(2)))

				Expect(bm.Children).To(HaveKey("nested"))
				nestedChild := bm.Children["nested"]
				Expect(nestedChild.Name).To(Equal("nested"))
				Expect(nestedChild.Values).To(BeEmpty()) // No direct values
				Expect(nestedChild.Children).To(HaveLen(2))

				Expect(nestedChild.Children).To(HaveKey("active"))
				activeGrandchild := nestedChild.Children["active"]
				Expect(activeGrandchild.Name).To(Equal("active"))
				Expect(activeGrandchild.Values).To(HaveLen(2))
				Expect(activeGrandchild.Values[0].Value).To(Equal(true))
				Expect(activeGrandchild.Values[0].Index).To(Equal(uint64(0))) // First bit assigned
				Expect(activeGrandchild.Values[1].Value).To(Equal(false))
				Expect(activeGrandchild.Values[1].Index).To(Equal(uint64(1))) // Second bit assigned

				Expect(nestedChild.Children).To(HaveKey("level"))
				levelGrandchild := nestedChild.Children["level"]
				Expect(levelGrandchild.Name).To(Equal("level"))
				Expect(levelGrandchild.Values).To(HaveLen(2))
				Expect(levelGrandchild.Values[0].Value).To(Equal(int64(1)))  // TOML parses ints as int64
				Expect(levelGrandchild.Values[0].Index).To(Equal(uint64(2))) // Third bit assigned
				Expect(levelGrandchild.Values[1].Value).To(Equal(int64(2)))
				Expect(levelGrandchild.Values[1].Index).To(Equal(uint64(3))) // Fourth bit assigned
			})
		})

		Context("when the file does not exist", func() {
			It("should return an error", func() {
				bm, err := LoadBitMapFromDisk("nonexistent/path/bitmap.toml")
				Expect(err).To(HaveOccurred())
				Expect(bm).To(BeNil())
			})
		})

		Context("when the TOML file is invalid", func() {
			BeforeEach(func() {
				tomlContent := `invalid toml content`
				err := os.WriteFile(tomlPath, []byte(tomlContent), 0644)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should return an error", func() {
				bm, err := LoadBitMapFromDisk(tomlPath)
				Expect(err).To(HaveOccurred())
				Expect(bm).To(BeNil())
			})
		})
	})

	// --- Tests for BuildBitmap ---
	Describe("Building Bitmap", func() {
		It("should build a flat bitmap correctly", func() {
			bitPos := 0
			rootProp := &Property{
				Properties: []*Property{
					{Name: "propA", Values: []any{"a1", "a2"}},
					{Name: "propB", Values: []any{100, 200}},
				},
			}
			bm := BuildBitmap(rootProp, &bitPos)

			Expect(bm.Children).To(HaveLen(2))
			Expect(bm.Children).To(HaveKey("propA"))
			Expect(bm.Children).To(HaveKey("propB"))

			propAChild := bm.Children["propA"]
			Expect(propAChild.Name).To(Equal("propA"))
			Expect(propAChild.Values).To(HaveLen(2))
			Expect(propAChild.Values[0]).To(Equal(&BitValue{Value: "a1", Index: 0}))
			Expect(propAChild.Values[1]).To(Equal(&BitValue{Value: "a2", Index: 1}))
			Expect(propAChild.Children).To(BeEmpty())

			propBChild := bm.Children["propB"]
			Expect(propBChild.Name).To(Equal("propB"))
			Expect(propBChild.Values).To(HaveLen(2))
			Expect(propBChild.Values[0]).To(Equal(&BitValue{Value: 100, Index: 2}))
			Expect(propBChild.Values[1]).To(Equal(&BitValue{Value: 200, Index: 3}))
			Expect(propBChild.Children).To(BeEmpty())

			Expect(bitPos).To(Equal(4)) // Bit position should be incremented
		})

		It("should build a nested bitmap correctly", func() {
			bitPos := 0
			rootProp := &Property{
				Properties: []*Property{
					{Name: "level1", Properties: []*Property{
						{Name: "level2A", Values: []any{true, false}},
						{Name: "level2B", Properties: []*Property{
							{Name: "level3", Values: []any{"deep"}},
						}},
					}},
					{Name: "anotherRoot", Values: []any{1}},
				},
			}
			bm := BuildBitmap(rootProp, &bitPos)

			Expect(bm.Children).To(HaveLen(2))
			Expect(bm.Children).To(HaveKey("level1"))
			Expect(bm.Children).To(HaveKey("anotherRoot"))

			level1 := bm.Children["level1"]
			Expect(level1.Name).To(Equal("level1"))
			Expect(level1.Values).To(BeEmpty())
			Expect(level1.Children).To(HaveLen(2))
			Expect(level1.Children).To(HaveKey("level2A"))
			Expect(level1.Children).To(HaveKey("level2B"))

			level2A := level1.Children["level2A"]
			Expect(level2A.Name).To(Equal("level2A"))
			Expect(level2A.Values).To(HaveLen(2))
			Expect(level2A.Values[0]).To(Equal(&BitValue{Value: true, Index: 0}))
			Expect(level2A.Values[1]).To(Equal(&BitValue{Value: false, Index: 1}))
			Expect(level2A.Children).To(BeEmpty())

			level2B := level1.Children["level2B"]
			Expect(level2B.Name).To(Equal("level2B"))
			Expect(level2B.Values).To(BeEmpty())
			Expect(level2B.Children).To(HaveLen(1))
			Expect(level2B.Children).To(HaveKey("level3"))

			level3 := level2B.Children["level3"]
			Expect(level3.Name).To(Equal("level3"))
			Expect(level3.Values).To(HaveLen(1))
			Expect(level3.Values[0]).To(Equal(&BitValue{Value: "deep", Index: 2}))
			Expect(level3.Children).To(BeEmpty())

			anotherRoot := bm.Children["anotherRoot"]
			Expect(anotherRoot.Name).To(Equal("anotherRoot"))
			Expect(anotherRoot.Values).To(HaveLen(1))
			Expect(anotherRoot.Values[0]).To(Equal(&BitValue{Value: 1, Index: 3}))
			Expect(anotherRoot.Children).To(BeEmpty())

			Expect(bitPos).To(Equal(4))
		})

		It("should handle properties with only children or only values", func() {
			bitPos := 0
			rootProp := &Property{
				Properties: []*Property{
					{Name: "onlyChild", Properties: []*Property{{Name: "childVal", Values: []any{"c"}}}},
					{Name: "onlyVal", Values: []any{"v"}},
				},
			}
			bm := BuildBitmap(rootProp, &bitPos)

			Expect(bm.Children).To(HaveLen(2))
			Expect(bm.Children["onlyChild"].Values).To(BeEmpty())
			Expect(bm.Children["onlyChild"].Children).To(HaveLen(1))
			Expect(bm.Children["onlyChild"].Children["childVal"].Values).To(HaveLen(1))
			Expect(bm.Children["onlyChild"].Children["childVal"].Values[0]).To(Equal(&BitValue{Value: "c", Index: 0}))

			Expect(bm.Children["onlyVal"].Values).To(HaveLen(1))
			Expect(bm.Children["onlyVal"].Values[0]).To(Equal(&BitValue{Value: "v", Index: 1}))
			Expect(bm.Children["onlyVal"].Children).To(BeEmpty())
			Expect(bitPos).To(Equal(2))
		})

		It("should handle empty properties slice", func() {
			bitPos := 0
			bm := BuildBitmap(&Property{}, &bitPos)
			Expect(bm.Children).To(BeEmpty())
			Expect(bm.Values).To(BeEmpty())
			Expect(bitPos).To(Equal(0))
		})
	})

	// --- Tests for EvaluateEntityProperties ---
	Describe("Evaluating Entity Properties", func() {
		Context("with a simple entity and bitmap", func() {
			It("should return 0 if the entity is nil", func() {
				state := EvaluateEntityProperties(nil, sampleBM)
				Expect(state).To(Equal(mask(0)))
			})

			It("should evaluate matching properties correctly", func() {
				entity := &SimpleEntity{
					Status: "processing", // Bit 1 << 1 = 2 (assuming status bits are 0,1,2)
					Count:  10,           // Bit 1 << 5 = 32 (assuming count bits are 3,4,5)
					Active: false,        // Bit 1 << 7 = 128 (assuming active bits are 6,7)
				}
				// Recalculate expected bits based on sampleBM build order
				// status: "pending" (0), "processing" (1), "completed" (2) -> Bits 1<<0, 1<<1, 1<<2
				// count: 0 (3), 1 (4), 10 (5) -> Bits 1<<3, 1<<4, 1<<5
				// active: true (6), false (7) -> Bits 1<<6, 1<<7
				expectedState := (1 << 1) | (1 << 5) | (1 << 7) // 2 | 32 | 128 = 162
				state := EvaluateEntityProperties(entity, sampleBM)
				Expect(state).To(Equal(mask(expectedState)))
			})

			It("should ignore properties not in the bitmap", func() {
				entity := &SimpleEntity{
					Status: "unknown", // Not in bitmap values
					Count:  1,         // Bit 1 << 4 = 16
					Active: false,     // Bit 1 << 7 = 128
				}
				expectedState := 1<<4 + 1<<7 // 144
				state := EvaluateEntityProperties(entity, sampleBM)
				Expect(state).To(Equal(mask(expectedState)))
			})

			It("should ignore fields without json tags or non-matching tags", func() {
				entity := &NestedEntity{ // Use NestedEntity struct but SimpleEntity bitmap
					ID:         "id1", // No 'id' in sampleBM
					NoJsonTag:  "ignored",
					unexported: 1,
				}
				state := EvaluateEntityProperties(entity, sampleBM)
				Expect(state).To(Equal(mask(0)))
			})
		})

		Context("with a nested entity and bitmap", func() {
			It("should evaluate nested properties correctly", func() {
				entity := &NestedEntity{
					ID: "id2", // Bit 1 << 1 = 2 (assuming id bits are 0, 1)
					Simple: &SimpleEntity{
						Status: "pending", // Bit 1 << 2 = 4 (assuming simple.status bits are 2, 3)
						Count:  5,         // Bit 1 << 4 = 16 (assuming simple.count bits are 4, 5)
						Active: true,      // Bit 1 << 6 = 64 (assuming simple.active bit is 6)
					},
				}
				// Recalculate expected bits based on nestedBM build order
				// id: "id1"(0), "id2"(1) -> Bits 1<<0, 1<<1
				// simple.status: "pending"(2), "processing"(3) -> Bits 1<<2, 1<<3
				// simple.count: 5(4), 15(5) -> Bits 1<<4, 1<<5
				// simple.active: true(6) -> Bit 1<<6
				expectedState := (1 << 1) | (1 << 2) | (1 << 4) | (1 << 6) // 2 | 4 | 16 | 64 = 86
				state := EvaluateEntityProperties(entity, nestedBM)
				Expect(state).To(Equal(mask(expectedState)))
			})

			It("should handle nil nested structs", func() {
				entity := &NestedEntity{
					ID:     "id1", // Bit 1 << 0 = 1
					Simple: nil,
				}
				expectedState := 1 << 0 // 1
				state := EvaluateEntityProperties(entity, nestedBM)
				Expect(state).To(Equal(mask(expectedState)))
			})

			It("should handle zero value nested structs", func() {
				entity := &NestedEntity{
					ID:     "id1",           // Bit 1 << 0 = 1
					Simple: &SimpleEntity{}, // Fields have zero values, won't match bitmap unless 0/""/false are defined
				}
				expectedState := 1 << 0 // 1 (only ID matches)
				state := EvaluateEntityProperties(entity, nestedBM)
				Expect(state).To(Equal(mask(expectedState)))
			})
		})

		Context("with enum types", func() {
			It("should evaluate enum properties correctly using underlying type", func() {
				entity := &EnumEntity{
					EnumTypeField: EnumVal2, // Bit 1 << 1 = 2 (enum bits 0, 1)
					IntField:      10,       // Bit 1 << 2 = 4 (int bits 2, 3)
				}
				// Recalculate based on enumBM build order
				// enum_type: int(EnumVal1)(0), int(EnumVal2)(1) -> Bits 1<<0, 1<<1
				// int_field: 10(2), 20(3) -> Bits 1<<2, 1<<3
				expectedState := (1 << 1) | (1 << 2) // 2 | 4 = 6
				state := EvaluateEntityProperties(entity, enumBM)
				Expect(state).To(Equal(mask(expectedState)))
			})
		})
	})

	// --- Tests for getValue ---
	Describe("getValue helper", func() {
		var bitValues []*BitValue

		BeforeEach(func() {
			bitValues = []*BitValue{
				{Value: "apple", Index: 0},
				{Value: int64(100), Index: 1}, // Use int64 as TOML does
				{Value: true, Index: 2},
				{Value: int(EnumVal1), Index: 3}, // Store underlying type
			}
		})

		It("should return the correct BitValue when value matches (string)", func() {
			bv := getValue(bitValues, "apple")
			Expect(bv).NotTo(BeNil())
			Expect(bv).To(Equal(bitValues[0]))
		})

		It("should return the correct BitValue when value matches (int64)", func() {
			bv := getValue(bitValues, int64(100))
			Expect(bv).NotTo(BeNil())
			Expect(bv).To(Equal(bitValues[1]))
		})

		It("should return the correct BitValue when value matches (bool)", func() {
			bv := getValue(bitValues, true)
			Expect(bv).NotTo(BeNil())
			Expect(bv).To(Equal(bitValues[2]))
		})

		It("should return the correct BitValue for enum type matching underlying int", func() {
			bv := getValue(bitValues, EnumVal1) // Input EnumType, bitmap has int
			Expect(bv).NotTo(BeNil())
			Expect(bv).To(Equal(bitValues[3]))
		})

		It("should return nil when value does not match", func() {
			bv := getValue(bitValues, "banana")
			Expect(bv).To(BeNil())
		})
	})

	// --- Tests for ApplyStateToEntity ---
	Describe("Applying State to Entity", func() {
		var entity *NestedEntity
		var state Bits

		BeforeEach(func() {
			entity = &NestedEntity{} // Start with zero value entity
			// nestedBM bits: id(0,1), simple.status(2,3), simple.count(4,5), simple.active(6)
		})

		Context("applying state to top-level fields", func() {
			It("should set the correct string value", func() {
				state = mask(1 << 0) // Corresponds to "id1" in nestedBM
				ApplyStateToEntity(entity, state, nestedBM)
				Expect(entity.ID).To(Equal("id1"))
				Expect(entity.Simple).To(BeNil()) // Not affected
			})
		})

		Context("applying state to nested fields", func() {
			It("should set the correct nested string value and initialize struct", func() {
				state = mask(1 << 3) // Corresponds to "processing" in nestedBM.Simple
				ApplyStateToEntity(entity, state, nestedBM)
				Expect(entity.ID).To(BeEmpty()) // Not affected
				Expect(entity.Simple).NotTo(BeNil())
				Expect(entity.Simple.Status).To(Equal("processing"))
				Expect(entity.Simple.Count).To(Equal(0)) // Not affected
			})

			It("should set multiple nested fields", func() {
				state = mask((1 << 3) | (1 << 4) | (1 << 6)) // processing, count=5, active=true
				ApplyStateToEntity(entity, state, nestedBM)
				Expect(entity.Simple).NotTo(BeNil())
				Expect(entity.Simple.Status).To(Equal("processing"))
				Expect(entity.Simple.Count).To(Equal(5))
				Expect(entity.Simple.Active).To(BeTrue())
			})

			It("should modify existing nested structs in place", func() {
				existing := &SimpleEntity{Status: "initial", Count: 99}
				entity.Simple = existing
				state = mask(1 << 2) // pending
				ApplyStateToEntity(entity, state, nestedBM)
				Expect(entity.Simple).To(BeIdenticalTo(existing))
				Expect(entity.Simple.Status).To(Equal("pending"))
				Expect(entity.Simple.Count).To(Equal(99)) // not set by the state, so it is kept
			})
		})

		Context("applying state with multiple bits for the same property", func() {
			It("should apply the value corresponding to the highest bit", func() {
				// Bits for sampleBM.status: pending(1<<0), processing(1<<1), completed(1<<2)
				state = mask((1 << 0) | (1 << 2)) // pending and completed bits set
				simpleEntity := &SimpleEntity{}
				ApplyStateToEntity(simpleEntity, state, sampleBM)
				Expect(simpleEntity.Status).To(Equal("completed")) // completed has highest bit (1<<2)
			})
		})

		Context("applying state with bits not defined in the bitmap", func() {
			It("should not set any fields", func() {
				state = mask(1 << 10) // Undefined bit
				ApplyStateToEntity(entity, state, nestedBM)
				Expect(entity.ID).To(BeEmpty())
				Expect(entity.Simple).To(BeNil())
			})
		})

		Context("when the field cannot be set (e.g., unexported)", func() {
			// This is tricky to test directly without specific unexported fields with json tags,
			// which is unusual. We rely on the `CanSet()` check.
			// We can test that it doesn't panic for fields without json tags.
			It("should not panic for fields without json tags or unexported", func() {
				state = mask(1 << 0) // id1
				entityWithUnexported := &struct {
					ID         string `json:"id,omitempty"`
					unexported string `json:"unexported,omitempty"` // Won't be found by getFieldByJsonName anyway
					NoJsonTag  string
				}{}
				// Use nestedBM which has "id"
				bmForTest := &Bitmap{Name: "root", Children: map[string]*Bitmap{"id": nestedBM.Children["id"]}}

				Expect(func() { ApplyStateToEntity(entityWithUnexported, state, bmForTest) }).ShouldNot(Panic())
				Expect(entityWithUnexported.ID).To(Equal("id1")) // Should still set the valid field
			})
		})

		Context("with enum types", func() {
			It("should set the enum field correctly", func() {
				enumEntity := &EnumEntity{}
				// enumBM: enum_type(0,1), int_field(2,3) -> Val1(1<<0), Val2(1<<1), 10(1<<2), 20(1<<3)
				state := mask(1 << 1) // Corresponds to EnumVal2
				ApplyStateToEntity(enumEntity, state, enumBM)
				Expect(enumEntity.EnumTypeField).To(Equal(EnumVal2))
			})
		})
	})

	// --- Tests for getFieldByJsonName ---
	Describe("getFieldByJsonName helper", func() {
		type JsonTest struct {
			FieldOne string `json:"field_one,omitempty"`
			FieldTwo int    `json:"field_two,omitempty"`
		}

		It("should find the correct field by json tag", func() {
			t := reflect.TypeOf(&JsonTest{})
			f, err := getFieldByJsonName(t, "field_one")
			Expect(err).NotTo(HaveOccurred())
			Expect(f.Name).To(Equal("FieldOne"))

			f, err = getFieldByJsonName(t, "field_two")
			Expect(err).NotTo(HaveOccurred())
			Expect(f.Name).To(Equal("FieldTwo"))
		})

		It("should return an error if json tag is not found", func() {
			t := reflect.TypeOf(&JsonTest{})
			_, err := getFieldByJsonName(t, "non_existent_field")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("could not find json Name"))
		})

		It("should return an error if called on non-struct type", func() {
			// NOTE: The current implementation uses t.Elem(), so it expects a pointer to a struct.
			// Passing a non-pointer or non-struct might panic or behave unexpectedly.
			// Let's test with a pointer to a non-struct
			var i int
			t := reflect.TypeOf(&i)
			Expect(func() { _, _ = getFieldByJsonName(t, "any") }).Should(Panic()) // Elem() panics on non-ptr/interface
			// Test with struct value (not pointer) - should also panic
			t = reflect.TypeOf(JsonTest{})
			Expect(func() { _, _ = getFieldByJsonName(t, "field_one") }).Should(Panic()) // Elem() panics on non-ptr/interface
		})
	})

	// --- Tests for parseValueToType ---
	Describe("parseValueToType helper", func() {
		It("should convert to int types correctly", func() {
			val := int64(123) // Simulate value from bitmap (like TOML int)
			targetType := reflect.TypeOf(int(0))
			result := parseValueToType(val, targetType)
			Expect(result.IsValid()).To(BeTrue())
			Expect(result.Type()).To(Equal(targetType))
			Expect(result.Interface()).To(BeEquivalentTo(123)) // Use BeEquivalentTo for numeric types

			targetType32 := reflect.TypeOf(int32(0))
			result32 := parseValueToType(val, targetType32)
			Expect(result32.IsValid()).To(BeTrue())
			Expect(result32.Type()).To(Equal(targetType32))
			Expect(result32.Interface()).To(BeEquivalentTo(int32(123)))
		})

		It("should handle bool type", func() {
			val := true
			targetType := reflect.TypeOf(false)
			result := parseValueToType(val, targetType)
			Expect(result.IsValid()).To(BeTrue())
			Expect(result.Type()).To(Equal(targetType))
			Expect(result.Interface()).To(Equal(true))
		})

		It("should handle other types (like string) directly", func() {
			val := "hello"
			targetType := reflect.TypeOf("")
			result := parseValueToType(val, targetType)
			Expect(result.IsValid()).To(BeTrue())
			Expect(result.Type()).To(Equal(targetType))
			Expect(result.Interface()).To(Equal("hello"))
		})

		It("should handle custom types if assignable (like enum based on int)", func() {
			val := int(EnumVal1)                   // value from bitmap is underlying int
			targetType := reflect.TypeOf(EnumVal1) // target field is EnumType
			result := parseValueToType(val, targetType)

			// The current parseValueToType doesn't explicitly handle custom types.
			// It falls through to `reflect.ValueOf(value)`.
			// Conversion to the specific custom type happens later during `field.Set(v)`.
			// So, we check if the raw value is returned correctly.
			Expect(result.IsValid()).To(BeTrue())
			Expect(result.Type()).To(Equal(targetType)) // Still int type here
			Expect(result.Interface()).To(Equal(EnumVal1))
		})
	})

	// --- Tests for getKeyByBit ---
	Describe("getKeyByBit helper", func() {
		var bitValues []*BitValue
		BeforeEach(func() {
			bitValues = []*BitValue{
				{Value: "low", Index: 0},  // 1
				{Value: "mid", Index: 2},  // 4
				{Value: "high", Index: 5}, // 32
			}
		})

		It("should return the value for a single matching bit", func() {
			key := getKeyByBit(mask(1<<2), bitValues) // 4
			Expect(key).To(Equal("mid"))
		})

		It("should return the value for the highest matching bit if multiple match", func() {
			key := getKeyByBit(mask((1<<0)|(1<<5)|(1<<2)), bitValues) // 1 | 32 | 4 = 37
			Expect(key).To(Equal("high"))                             // 32 is highest bit
		})

		It("should return the value even if other non-matching bits are set", func() {
			key := getKeyByBit(mask((1<<0)|(1<<3)|(1<<5)), bitValues) // 1 | 8 | 32 = 41
			Expect(key).To(Equal("high"))                             // 32 is highest matching bit
		})

		It("should return nil if no bits match", func() {
			key := getKeyByBit(mask(1<<1), bitValues) // 2
			Expect(key).To(BeNil())
		})

		It("should return nil if input bit is 0", func() {
			key := getKeyByBit(mask(0), bitValues)
			Expect(key).To(BeNil())
		})

		It("should return nil for empty or nil valueMap", func() {
			Expect(getKeyByBit(mask(1<<2), nil)).To(BeNil())
			Expect(getKeyByBit(mask(1<<2), []*BitValue{})).To(BeNil())
		})
	})

	// --- Tests for allBits / noneBits ---
	Describe("Bit Checking Helpers", func() {
		Describe("allBits", func() {
			It("should return true if all required bits are set", func() {
				Expect(allBits(mask(0b1101), mask(0b1001))).To(BeTrue())
				Expect(allBits(mask(0b1111), mask(0b0110))).To(BeTrue())
				Expect(allBits(mask(0b1010), mask(0b1010))).To(BeTrue())
				Expect(allBits(mask(0b1111), mask(0))).To(BeTrue()) // All bits of 0 are set
			})
			It("should return false if any required bit is not set", func() {
				Expect(allBits(mask(0b1101), mask(0b1111))).To(BeFalse())
				Expect(allBits(mask(0b1010), mask(0b0101))).To(BeFalse())
				Expect(allBits(mask(0), mask(0b0001))).To(BeFalse())
			})
		})

		Describe("noneBits", func() {
			It("should return true if none of the specified bits are set", func() {
				Expect(noneBits(mask(0b1101), mask(0b0010))).To(BeTrue())
				Expect(noneBits(mask(0b0000), mask(0b1111))).To(BeTrue())
				Expect(noneBits(mask(0b1010), mask(0b0101))).To(BeTrue())
				Expect(noneBits(mask(0b1111), mask(0))).To(BeTrue()) // None of the bits in 0 are set
			})
			It("should return false if any of the specified bits are set", func() {
				Expect(noneBits(mask(0b1101), mask(0b0100))).To(BeFalse())
				Expect(noneBits(mask(0b1010), mask(0b1000))).To(BeFalse())
				Expect(noneBits(mask(0b1111), mask(0b0001))).To(BeFalse())
			})
		})
	})

	// --- Tests for mergeState ---
	Describe("mergeState", func() {
		var bm *Bitmap

		BeforeEach(func() {
			bm = &Bitmap{
				Name: "entity",
				Children: map[string]*Bitmap{
					"status": {Values: []*BitValue{{Value: "pending", Index: 0}, {Value: "processing", Index: 1}, {Value: "completed", Index: 2}}},
					"active": {Values: []*BitValue{{Value: true, Index: 3}, {Value: false, Index: 4}}},
				},
			}
		})

		It("should replace the bits of a property set in the new state", func() {
			currentState := 1<<0 | 1<<3 // pending, active
			Expect(mergeState(bm, mask(currentState), mask(1<<1))).To(Equal(mask(1<<1 | 1<<3)))
		})

		It("should allow moving to a lower bit of the same property", func() {
			currentState := 1<<2 | 1<<4 // completed, inactive
			Expect(mergeState(bm, mask(currentState), mask(1<<0))).To(Equal(mask(1<<0 | 1<<4)))
		})

		It("should keep the current state when the new state is 0", func() {
			currentState := 1<<0 | 1<<3
			Expect(mergeState(bm, mask(currentState), mask(0))).To(Equal(mask(currentState)))
		})

		It("should replace multiple properties at once", func() {
			currentState := 1<<0 | 1<<3
			Expect(mergeState(bm, mask(currentState), mask(1<<2|1<<4))).To(Equal(mask(1<<2 | 1<<4)))
		})

		It("should handle nested properties", func() {
			nested := &Bitmap{
				Name: "root",
				Children: map[string]*Bitmap{
					"inner": {Children: map[string]*Bitmap{
						"status": {Values: []*BitValue{{Value: "a", Index: 0}, {Value: "b", Index: 1}}},
					}},
				},
			}
			Expect(mergeState(nested, mask(1<<0), mask(1<<1))).To(Equal(mask(1 << 1)))
		})

		It("should combine both states when the bitmap is nil", func() {
			Expect(mergeState(nil, mask(0b01), mask(0b10))).To(Equal(mask(0b11)))
		})
	})

})

var _ = Describe("Bitmap lookup tables", func() {
	// status has a value appended later with an index in the second word, like a value another node added
	newBitmap := func() *Bitmap {
		return &Bitmap{Name: "TestStateEntity", Children: map[string]*Bitmap{
			"status": {Name: "status", Values: []*BitValue{{Value: "pending", Index: 0}, {Value: "done", Index: 1}, {Value: "returned", Index: 70}}},
			"count":  {Name: "count", Values: []*BitValue{{Value: int64(0), Index: 2}, {Value: int64(1), Index: 3}}},
		}}
	}

	It("should merge a property whose values span several words without changing the inputs", func() {
		bm := newBitmap()
		current, update := NewBits(1, 3), NewBits(70)

		merged := mergeState(bm, current, update)

		Expect(merged.Indices()).To(Equal([]uint64{3, 70}))
		Expect(current.Indices()).To(Equal([]uint64{1, 3}))
		Expect(update.Indices()).To(Equal([]uint64{70}))
		Expect(changedBits(bm, current, merged).Indices()).To(Equal([]uint64{70}))
	})

	It("should apply a value of the second word and evaluate it back", func() {
		bm := newBitmap()
		entity := &TestStateEntity{Status: "pending", Count: 1}

		ApplyStateToEntity(entity, NewBits(70), bm)

		Expect(entity.Status).To(Equal("returned"))
		Expect(entity.Count).To(Equal(int64(1)))
		Expect(EvaluateEntityProperties(entity, bm).Indices()).To(Equal([]uint64{3, 70}))
	})

	It("should use the tables of the bitmap passed, a new layout gets its own", func() {
		layout, err := applyBitmapEntry(nil, &Bitmap{Name: "TestStateEntity", Children: map[string]*Bitmap{
			"status": {Name: "status", Values: []*BitValue{{Value: "pending", Index: 0}}},
		}})
		Expect(err).NotTo(HaveOccurred())
		entity := &TestStateEntity{Status: "done"}
		Expect(EvaluateEntityProperties(entity, layout.Children["TestStateEntity"])).To(BeNil())

		grown, err := applyBitmapEntry(layout, &Bitmap{Name: "TestStateEntity", Children: map[string]*Bitmap{
			"status": {Name: "status", Values: []*BitValue{{Value: "done", Index: 0}}},
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(EvaluateEntityProperties(entity, grown.Children["TestStateEntity"]).Indices()).To(Equal([]uint64{1}))
		Expect(EvaluateEntityProperties(entity, layout.Children["TestStateEntity"])).To(BeNil())
	})
})

var _ = Describe("Condition evaluation", func() {
	It("should check the conditions after a matching AtLeastOne", func() {
		conditions := []*Condition{
			{Operator: AtLeastOne, States: map[string][]Bits{"E": {NewBits(0), NewBits(1)}}},
			{Operator: All, States: map[string][]Bits{"E": {NewBits(5)}}},
		}
		Expect(evaluateConditions(conditions, map[string]Bits{"E": NewBits(1)})).To(BeFalse())
		Expect(evaluateConditions(conditions, map[string]Bits{"E": NewBits(1, 5)})).To(BeTrue())
		Expect(evaluateConditions(conditions, map[string]Bits{"E": NewBits(5)})).To(BeFalse())
	})
})
