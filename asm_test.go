package asm

import (
	"reflect"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestAsm(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ASM V2 Suite")
}

type mockBuilder struct {
	entity *TestStateEntity
	isSet  bool
	mu     sync.Mutex // Add mutex for potential concurrent access if needed by tests
}

func newMockBuilder() *mockBuilder {
	return &mockBuilder{entity: nil}
}

func (m *mockBuilder) Build() *TestStateEntity {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entity
}

func (m *mockBuilder) Set(e *TestStateEntity) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entity = e
	m.isSet = true
}

func (m *mockBuilder) IsSet() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isSet
}

// Ensure mockBuilder implements Builder[*structpb.Struct]
var _ Builder[*TestStateEntity] = (*mockBuilder)(nil)

// --- Mock Dependencies ---
type mockDeps struct {
	Value string
}

// --- Simple Entity for Testing State/Evaluation ---
type TestStateEntity struct {
	Status string `json:"status,omitempty"`
	Count  int64  `json:"count,omitempty"` // Use int64 like TOML
	Active bool   `json:"active,omitempty"`
}

type TestRemoteStateEntity struct {
	Status string `json:"status,omitempty"`
	Count  int64  `json:"count,omitempty"` // Use int64 like TOML
	Active bool   `json:"active,omitempty"`
}

// --- Simple Event for Testing ---
type TestEvent struct {
	Value  string `json:"value,omitempty"`
	Amount int64  `json:"amount,omitempty"`
}

// Implement proto.Message for TestEvent and TestStateEntity if needed for options/remote
// (Using Any manually or specific proto definitions is usually required for real remote calls)
// For local tests focusing on state evaluation via JSON tags, this is often sufficient.
func (t *TestEvent) Reset()                           {}
func (t *TestEvent) String() string                   { return "" }
func (*TestEvent) ProtoMessage()                      {}
func (*TestEvent) ProtoReflect() protoreflect.Message { return nil }

func (t *TestStateEntity) Reset()                           {}
func (t *TestStateEntity) String() string                   { return "" }
func (*TestStateEntity) ProtoMessage()                      {}
func (*TestStateEntity) ProtoReflect() protoreflect.Message { return nil }

func (t *TestRemoteStateEntity) Reset()                           {}
func (t *TestRemoteStateEntity) String() string                   { return "" }
func (*TestRemoteStateEntity) ProtoMessage()                      {}
func (*TestRemoteStateEntity) ProtoReflect() protoreflect.Message { return nil }

// Helper to create a simple Bitmap for testing
func createSimpleTestBitmap() (*Bitmap, map[string]int) {
	bitPos := 0
	props := &Property{
		Name: "TestStateEntity",
		Properties: []*Property{
			{Name: "status", Values: []any{"pending", "processing", "completed"}}, // bits 0, 1, 2 -> 1, 2, 4
			{Name: "count", Values: []any{int64(0), int64(1), int64(10)}},         // bits 3, 4, 5 -> 8, 16, 32
			{Name: "active", Values: []any{true, false}},                          // bits 6, 7 -> 64, 128
		},
	}
	bm := BuildBitmap(props, &bitPos)
	stateMap := map[string]int{
		"status_pending":    1 << 0,
		"status_processing": 1 << 1,
		"status_completed":  1 << 2,
		"count_0":           1 << 3,
		"count_1":           1 << 4,
		"count_10":          1 << 5,
		"active_true":       1 << 6,
		"active_false":      1 << 7,
	}
	return bm, stateMap
}

// Helper to create a simple Bitmap for testing
func createSimpleRemoteTestBitmap() *Bitmap {
	bitPos := 0
	props := &Property{
		Name: "TestRemoteStateEntity",
		Properties: []*Property{
			{Name: "status", Values: []any{"pending", "processing", "completed"}}, // bits 0, 1, 2 -> 1, 2, 4
			{Name: "count", Values: []any{int64(0), int64(1), int64(10)}},         // bits 3, 4, 5 -> 8, 16, 32
			{Name: "active", Values: []any{true, false}},                          // bits 6, 7 -> 64, 128
		},
	}
	bm := BuildBitmap(props, &bitPos)
	return bm
}

// Helper to create a simple event Bitmap
func createSimpleEventBitmap() (*Bitmap, map[string]int) {
	bitPos := 0
	props := &Property{
		Name: "TestEvent",
		Properties: []*Property{
			{Name: "value", Values: []any{"eventA", "eventB"}},      // bits 0, 1 -> 1, 2
			{Name: "amount", Values: []any{int64(100), int64(200)}}, // bits 2, 3 -> 4, 8
		},
	}
	bm := BuildBitmap(props, &bitPos)

	eventMap := map[string]int{
		"value_eventA": 1 << 0,
		"value_eventB": 1 << 1,
		"amount_100":   1 << 2,
		"amount_200":   1 << 3,
	}
	return bm, eventMap
}

// Simple getBitmapByTypeFunc for tests
// This version uses reflect to get the type name, matching the Bitmap Names created above.
func getTestBitmapByType(entity any, rootBitmap *Bitmap) *Bitmap {
	if entity == nil || rootBitmap == nil {
		return nil
	}
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	typeName := t.Name()

	// Check children first (common case if loaded from TOML/config)
	if child, ok := rootBitmap.Children[typeName]; ok {
		return child
	}
	// Check if the root Bitmap itself matches
	if rootBitmap.Name == typeName {
		return rootBitmap
	}

	switch entity.(type) {
	case *TestStateEntity:
		return rootBitmap.Children["TestStateEntity"]
	case *TestRemoteStateEntity:
		return rootBitmap.Children["TestRemoteStateEntity"]
	}

	// fmt.Printf("WARN: getTestBitmapByType: Bitmap not found for type %s\n", typeName)
	return nil // Not found
}

// mask converts an int bit mask to Bits, for tests written with masks.
func mask(m int) Bits {
	var b Bits
	for i := 0; i < 63; i++ {
		if m&(1<<i) != 0 {
			b = b.With(uint64(i))
		}
	}
	return b
}
