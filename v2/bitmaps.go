package asm

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/BurntSushi/toml"
)

type (
	// Property represents a single property in the bitmap toml file
	Property struct {
		Name       string      `toml:"Name"`
		Values     []any       `toml:"values"`
		Properties []*Property `toml:"properties"`
	}
	Bitmap struct {
		Name     string
		Values   []*BitValue
		Children map[string]*Bitmap
	}
	BitValue struct {
		Value any
		// Index of the bit in the bitset of the root entity bitmap
		Index uint64
	}
)

func LoadBitMapFromDisk(path string) (*Bitmap, error) {
	var bitmapConfig struct {
		Version    string      `yaml:"version"`
		Properties []*Property `yaml:"properties"`
	}
	_, err := toml.DecodeFile(path, &bitmapConfig)
	if err != nil {
		return nil, fmt.Errorf("error loading bitmap file: %w", err)
	}

	rootBitmap := &Bitmap{
		Name:     "root",
		Values:   make([]*BitValue, 0),
		Children: make(map[string]*Bitmap),
	}

	for _, property := range bitmapConfig.Properties {
		pos := 0
		child := BuildBitmap(property, &pos)

		rootBitmap.Children[property.Name] = child
	}
	return rootBitmap, nil
}

// BuildBitmap takes in an array of Properties and a starting Bit index
// while iterating through the Properties, based on the current Property,
// it creates a nested object or a Bit-Value map
func BuildBitmap(property *Property, bitPosition *int) *Bitmap {
	b := &Bitmap{
		Name:     property.Name,
		Values:   make([]*BitValue, 0),
		Children: make(map[string]*Bitmap),
	}
	buildBitmapProperty(b, property, bitPosition)

	return b
}

// buildBitmapProperty builds a Bit-Value map for a given Property
// it gets called recursively to build all nested Properties as well
func buildBitmapProperty(b *Bitmap, p *Property, bitPosition *int) {
	if len(p.Properties) > 0 {
		for _, childProperty := range p.Properties {
			if _, ok := b.Children[childProperty.Name]; !ok {
				b.Children[childProperty.Name] = &Bitmap{
					Name:     childProperty.Name,
					Values:   make([]*BitValue, 0),
					Children: make(map[string]*Bitmap),
				}
			}
			childBitMap := b.Children[childProperty.Name]
			buildBitmapProperty(childBitMap, childProperty, bitPosition)
		}
	}

	if len(p.Values) > 0 {
		for _, value := range p.Values {
			if b.Values == nil {
				b.Values = []*BitValue{}
			}
			b.Values = append(b.Values, &BitValue{
				Value: value,
				Index: uint64(*bitPosition),
			})
			*bitPosition++
		}
	}
}

// EvaluateEntityProperties returns the bits of the property values of a pointer to a struct.
// Fields are matched to properties by their json name, nested structs are evaluated with their nested bitmap.
// A field value matches a bitmap value if it converts to the type of the bitmap value and equals it then
// (eg. an enum field matches an int64 value). The lookup tables of the bitmap are built once (bitmap_tables.go).
func EvaluateEntityProperties(entity any, bitMap *Bitmap) Bits {
	if entity == nil || bitMap == nil {
		return nil
	}
	v := reflect.ValueOf(entity)
	if v.IsZero() || v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Struct {
		return nil
	}

	t := tablesOf(bitMap)
	state := make(Bits, 0, t.words)
	t.root.evaluate(&state, v.Elem())
	if len(state) == 0 {
		return nil
	}
	return state
}

func getValue(values []*BitValue, value any) *BitValue {
	for _, v := range values {
		// due to using complex types such as "enums" which have an underlying primitive type
		// the Value needs to be converted to the primitive type to be comparable if one of the values is primitive.
		if reflect.ValueOf(value).CanConvert(reflect.TypeOf(v.Value)) &&
			reflect.ValueOf(value).Convert(reflect.TypeOf(v.Value)).Interface() == v.Value {
			return v
		}
	}
	return nil
}

// ApplyStateToEntity sets the fields of a pointer to a struct to the values the state holds for their properties.
// A property with more than one bit set gets the value with the highest index.
// Nested structs are modified in place, so fields which are not in the bitmap are kept;
// a new struct is only created when the field is nil.
func ApplyStateToEntity(entity any, state Bits, bitMap *Bitmap) {
	if entity == nil || bitMap == nil {
		return
	}
	v := reflect.ValueOf(entity)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return
	}

	tablesOf(bitMap).root.apply(state, v)
}

func getFieldByJsonName(t reflect.Type, propertyName string) (reflect.StructField, error) {
	for i := range t.Elem().NumField() {
		f := t.Elem().Field(i)
		// splitting json tag to remove options like "omitempty"
		if strings.Split(f.Tag.Get("json"), ",")[0] == propertyName {
			return f, nil
		}
	}
	return reflect.StructField{}, fmt.Errorf("error: could not find json Name in fields: %s", propertyName)
}

func parseValueToType(value any, fieldType reflect.Type) reflect.Value {
	return reflect.ValueOf(value).Convert(fieldType)
}

// getKeyByBit returns the key from the highest Bit so when getting the key
// from a Bit state which includes more than one Value it will return the biggest
func getKeyByBit(state Bits, valueMap []*BitValue) any {
	var key any
	found := false
	var highestIndex uint64

	for _, val := range valueMap {
		if state.Has(val.Index) && (!found || val.Index > highestIndex) {
			found = true
			highestIndex = val.Index
			key = val.Value
		}
	}
	return key
}

// EvaluateConditions iterates through Conditions and checks if bits are present/not present based on the Operator.
func evaluateConditions(conditions []*Condition, containerStates map[string]Bits) bool {
	for _, condition := range conditions {
		switch condition.Operator {
		case AtLeastOne:
			// a match satisfies this condition, the following ones are still checked
			matched := false
			for bmName, states := range condition.States {
				for _, state := range states {
					if allBits(containerStates[bmName], state) {
						matched = true
						break
					}
				}
				if matched {
					break
				}
			}
			if !matched {
				return false
			}
		case All:
			for bmName, states := range condition.States {
				for _, state := range states {
					if !allBits(containerStates[bmName], state) {
						return false
					}
				}
			}
		case None:
			for bmName, states := range condition.States {
				for _, state := range states {
					if !noneBits(containerStates[bmName], state) {
						return false
					}
				}
			}
		case OneOf:
			matchCount := 0
			for bmName, states := range condition.States {
				for _, state := range states {
					if allBits(containerStates[bmName], state) {
						matchCount++
					}
				}
			}
			if matchCount != 1 {
				return false
			}

		default:
			return false
		}
	}
	return true
}

// Function to check if all bits are set in the state
func allBits(currentState, state Bits) bool {
	return currentState.ContainsAll(state)
}

// Function to check if noneBits of the bits are set in the state
func noneBits(currentState, state Bits) bool {
	return currentState.ContainsNone(state)
}

// changedBits returns the bits newState sets for every leaf property of the Bitmap whose bits differ from oldState.
// A property newState has no bits for, but oldState has, is not part of the result.
func changedBits(bm *Bitmap, oldState, newState Bits) Bits {
	var changed Bits
	for _, mask := range tablesOf(bm).props {
		if !slices.ContainsFunc(mask, func(m maskWord) bool { return oldState.wordAt(m.word)&m.bits != newState.wordAt(m.word)&m.bits }) {
			continue
		}
		for _, m := range mask {
			if bits := newState.wordAt(m.word) & m.bits; bits != 0 {
				changed.orWord(m.word, bits)
			}
		}
	}
	return changed.trim()
}

// mergeState replaces, for every leaf property of the Bitmap, the bits in currentState
// with the bits newState sets for that property. Properties newState does not set keep their bits,
// so a property can never hold two values at once (eg. status=pending and status=processing).
func mergeState(bm *Bitmap, currentState, newState Bits) Bits {
	if bm == nil {
		return currentState.Or(newState)
	}

	// the result is a copy made on the first replaced property, bitsets are never changed in place
	var result Bits
	for _, mask := range tablesOf(bm).props {
		if !intersects(newState, mask) {
			continue
		}
		if result == nil {
			result = make(Bits, max(len(currentState), len(newState)))
			copy(result, currentState)
		}
		for _, m := range mask {
			if m.word < len(result) {
				result[m.word] = result[m.word]&^m.bits | newState.wordAt(m.word)&m.bits
			}
		}
	}
	if result == nil {
		return currentState
	}
	return result.trim()
}
