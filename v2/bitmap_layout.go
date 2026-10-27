package asm

import (
	"fmt"
	"reflect"
	"slices"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"rumue.io/asm/service"
)

// The committed bitmap layout is shared through the raft log and is append-only:
// the first committed definition of a root entity bitmap is adopted with its indices,
// later definitions only append values which are missing, using the next free index.
// Indices never move, so a state means the same entity values on every node.

// normalizeValue converts a bitmap value to the type it has after a round trip through the wire format.
func normalizeValue(value any) (any, error) {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.String:
		return v.String(), nil
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint(), nil
	case reflect.Float32, reflect.Float64:
		return v.Float(), nil
	default:
		return nil, fmt.Errorf("unsupported bitmap value type %T", value)
	}
}

func valueToAny(value any) (*anypb.Any, error) {
	normalized, err := normalizeValue(value)
	if err != nil {
		return nil, err
	}

	var m proto.Message
	switch v := normalized.(type) {
	case string:
		m = wrapperspb.String(v)
	case bool:
		m = wrapperspb.Bool(v)
	case int64:
		m = wrapperspb.Int64(v)
	case uint64:
		m = wrapperspb.UInt64(v)
	case float64:
		m = wrapperspb.Double(v)
	}
	return anypb.New(m)
}

func valueFromAny(a *anypb.Any) (any, error) {
	m, err := a.UnmarshalNew()
	if err != nil {
		return nil, err
	}

	switch v := m.(type) {
	case *wrapperspb.StringValue:
		return v.GetValue(), nil
	case *wrapperspb.BoolValue:
		return v.GetValue(), nil
	case *wrapperspb.Int64Value:
		return v.GetValue(), nil
	case *wrapperspb.UInt64Value:
		return v.GetValue(), nil
	case *wrapperspb.DoubleValue:
		return v.GetValue(), nil
	default:
		return nil, fmt.Errorf("unsupported bitmap value type %T", m)
	}
}

func bitmapToProto(bm *Bitmap) (*service.Bitmap, error) {
	pb := &service.Bitmap{
		Name:     bm.Name,
		Children: make(map[string]*service.Bitmap, len(bm.Children)),
	}
	for _, val := range bm.Values {
		a, err := valueToAny(val.Value)
		if err != nil {
			return nil, fmt.Errorf("bitmap %s: %w", bm.Name, err)
		}
		pb.Values = append(pb.Values, &service.Bitmap_BitValue{Value: a, Index: val.Index})
	}
	for name, child := range bm.Children {
		pc, err := bitmapToProto(child)
		if err != nil {
			return nil, err
		}
		pb.Children[name] = pc
	}
	return pb, nil
}

func bitmapFromProto(pb *service.Bitmap) (*Bitmap, error) {
	bm := &Bitmap{
		Name:     pb.GetName(),
		Values:   make([]*BitValue, 0, len(pb.GetValues())),
		Children: make(map[string]*Bitmap, len(pb.GetChildren())),
	}
	for _, val := range pb.GetValues() {
		v, err := valueFromAny(val.GetValue())
		if err != nil {
			return nil, fmt.Errorf("bitmap %s: %w", pb.GetName(), err)
		}
		bm.Values = append(bm.Values, &BitValue{Value: v, Index: val.GetIndex()})
	}
	for name, child := range pb.GetChildren() {
		c, err := bitmapFromProto(child)
		if err != nil {
			return nil, err
		}
		bm.Children[name] = c
	}
	return bm, nil
}

func cloneBitmap(bm *Bitmap) *Bitmap {
	if bm == nil {
		return nil
	}
	c := &Bitmap{
		Name:     bm.Name,
		Children: make(map[string]*Bitmap, len(bm.Children)),
	}
	if bm.Values != nil {
		c.Values = make([]*BitValue, len(bm.Values))
	}
	for i, val := range bm.Values {
		c.Values[i] = &BitValue{Value: val.Value, Index: val.Index}
	}
	for name, child := range bm.Children {
		c.Children[name] = cloneBitmap(child)
	}
	return c
}

func sortedChildNames(bm *Bitmap) []string {
	names := make([]string, 0, len(bm.Children))
	for name := range bm.Children {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// nextFreeIndex returns the index after the highest index used in the bitmap.
func nextFreeIndex(bm *Bitmap) uint64 {
	var next uint64
	for _, val := range bm.Values {
		next = max(next, val.Index+1)
	}
	for _, child := range bm.Children {
		next = max(next, nextFreeIndex(child))
	}
	return next
}

// validateIndices checks that no index is used twice in the bitmap.
func validateIndices(bm *Bitmap, used map[uint64]string, path string) error {
	for _, val := range bm.Values {
		if other, ok := used[val.Index]; ok {
			return fmt.Errorf("bitmap %s: index %d used by %s and %s", path, val.Index, other, path)
		}
		used[val.Index] = path
	}
	for _, name := range sortedChildNames(bm) {
		if err := validateIndices(bm.Children[name], used, path+"."+name); err != nil {
			return err
		}
	}
	return nil
}

// applyBitmapEntry merges a committed root entity bitmap into the layout and returns the new layout.
// The given layout is not modified. A conflict returns an error and leaves the layout unchanged.
func applyBitmapEntry(layout *Bitmap, entry *Bitmap) (*Bitmap, error) {
	result := cloneBitmap(layout)
	if result == nil {
		result = &Bitmap{Name: "root", Children: make(map[string]*Bitmap)}
	}

	existing, ok := result.Children[entry.Name]
	if !ok {
		// the first definition of an entity is adopted with its indices
		if err := validateIndices(entry, make(map[uint64]string), entry.Name); err != nil {
			return nil, err
		}
		result.Children[entry.Name] = cloneBitmap(entry)
		return result, nil
	}

	next := nextFreeIndex(existing)
	if err := mergeBitmap(existing, entry, &next, entry.Name); err != nil {
		return nil, err
	}
	return result, nil
}

// mergeBitmap appends the values of src missing in dst with the next free index.
// Children are walked in name order, so every node assigns the same indices.
func mergeBitmap(dst, src *Bitmap, next *uint64, path string) error {
	dstLeaf := len(dst.Values) > 0 && len(dst.Children) == 0
	srcLeaf := len(src.Values) > 0 && len(src.Children) == 0
	if (dstLeaf && len(src.Children) > 0) || (srcLeaf && len(dst.Children) > 0) {
		return fmt.Errorf("bitmap %s: property is nested on one node and has values on another", path)
	}

	for _, val := range src.Values {
		if getExactValue(dst.Values, val.Value) != nil {
			continue
		}
		if len(dst.Values) > 0 && reflect.TypeOf(dst.Values[0].Value) != reflect.TypeOf(val.Value) {
			return fmt.Errorf("bitmap %s: value %v of type %T conflicts with values of type %T",
				path, val.Value, val.Value, dst.Values[0].Value)
		}
		dst.Values = append(dst.Values, &BitValue{Value: val.Value, Index: *next})
		*next++
	}

	for _, name := range sortedChildNames(src) {
		child, ok := dst.Children[name]
		if !ok {
			child = &Bitmap{Name: name, Values: make([]*BitValue, 0), Children: make(map[string]*Bitmap)}
			dst.Children[name] = child
		}
		if err := mergeBitmap(child, src.Children[name], next, path+"."+name); err != nil {
			return err
		}
	}
	return nil
}

// getExactValue returns the bit value with the same type and value.
func getExactValue(values []*BitValue, value any) *BitValue {
	for _, v := range values {
		if reflect.TypeOf(v.Value) == reflect.TypeOf(value) && v.Value == value {
			return v
		}
	}
	return nil
}
