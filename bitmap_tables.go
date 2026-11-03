package asm

import (
	"fmt"
	"math/bits"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"weak"
)

// Lookup tables of a bitmap, built once on first use: the bits of every property, a map from value to bit index
// per property and, per struct type, which field holds which property. Evaluating, merging and applying states
// then don't walk the bitmap with reflection on every call.
//
// A bitmap must not change once it is used. The tables are cached by a weak pointer and don't reference
// the bitmap, so a bitmap which isn't used anymore is collected together with its tables.

type (
	bitmapTables struct {
		root *nodeTables
		// props holds the bits of every property with values in the tree
		props [][]maskWord
		// words is the number of words the highest index of the tree needs
		words int
	}
	nodeTables struct {
		children map[string]*nodeTables
		nested   bool
		// values finds the bit index of a value of the property, nil without values
		values *valueLookup
		// mask holds the bits of the values, by ascending word
		mask []maskWord
		// valueAt holds the value of every bit index of the property
		valueAt map[uint64]any
		// evaluators and setters by struct type
		evaluators sync.Map // reflect.Type -> []fieldEvaluator
		setters    sync.Map // reflect.Type -> []fieldSetter
	}
	maskWord struct {
		word int
		bits uint64
	}
	// fieldEvaluator evaluates a struct field holding a property
	fieldEvaluator struct {
		field int
		node  *nodeTables
	}
	// fieldSetter applies a property to a struct field
	fieldSetter struct {
		field int
		node  *nodeTables
		// settable is false for unexported fields
		settable bool
		// converted holds the values of the property converted to the type of the field, by bit index.
		// Values which can't be converted are missing.
		converted map[uint64]reflect.Value
	}
	// valueLookup finds the bit index of a value. Values are grouped by their type,
	// a field value matches a value if it converts to the value's type and equals it then.
	valueLookup struct {
		groups []valueGroup
	}
	valueGroup struct {
		typ    reflect.Type
		strs   map[string]valuePos
		ints   map[int64]valuePos
		uints  map[uint64]valuePos
		floats map[float64]valuePos
		bools  map[bool]valuePos
		others map[any]valuePos
	}
	// valuePos is the bit index of a value and its position in the values of the property,
	// the first matching value wins
	valuePos struct {
		index uint64
		pos   int
	}
)

var bitmapTablesCache sync.Map // weak.Pointer[Bitmap] -> *bitmapTables

// tablesOf returns the lookup tables of the bitmap, building them on first use.
func tablesOf(bm *Bitmap) *bitmapTables {
	key := weak.Make(bm)
	if t, ok := bitmapTablesCache.Load(key); ok {
		return t.(*bitmapTables)
	}

	t := &bitmapTables{}
	t.root = t.buildNode(bm)
	if actual, loaded := bitmapTablesCache.LoadOrStore(key, t); loaded {
		return actual.(*bitmapTables)
	}
	runtime.AddCleanup(bm, func(key weak.Pointer[Bitmap]) { bitmapTablesCache.Delete(key) }, key)
	return t
}

func (t *bitmapTables) buildNode(bm *Bitmap) *nodeTables {
	n := &nodeTables{children: make(map[string]*nodeTables, len(bm.Children)), nested: len(bm.Children) > 0}

	if len(bm.Values) > 0 {
		n.values = newValueLookup(bm.Values)
		n.valueAt = make(map[uint64]any, len(bm.Values))
		var mask Bits
		for _, v := range bm.Values {
			mask.set(v.Index)
			if _, ok := n.valueAt[v.Index]; !ok {
				n.valueAt[v.Index] = v.Value
			}
		}
		for w, b := range mask {
			if b != 0 {
				n.mask = append(n.mask, maskWord{word: w, bits: b})
			}
		}
		t.props = append(t.props, n.mask)
		t.words = max(t.words, len(mask))
	}

	for name, child := range bm.Children {
		n.children[name] = t.buildNode(child)
	}
	return n
}

func newValueLookup(values []*BitValue) *valueLookup {
	l := &valueLookup{}
	for pos, v := range values {
		typ := reflect.TypeOf(v.Value)
		if typ == nil {
			continue
		}
		i := slices.IndexFunc(l.groups, func(g valueGroup) bool { return g.typ == typ })
		if i < 0 {
			l.groups = append(l.groups, valueGroup{typ: typ})
			i = len(l.groups) - 1
		}
		l.groups[i].add(reflect.ValueOf(v.Value), valuePos{index: v.Index, pos: pos})
	}
	return l
}

// add keeps the first position of a value.
func (g *valueGroup) add(v reflect.Value, p valuePos) {
	switch v.Kind() {
	case reflect.String:
		g.strs = addFirst(g.strs, v.String(), p)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		g.ints = addFirst(g.ints, v.Int(), p)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		g.uints = addFirst(g.uints, v.Uint(), p)
	case reflect.Float32, reflect.Float64:
		g.floats = addFirst(g.floats, v.Float(), p)
	case reflect.Bool:
		g.bools = addFirst(g.bools, v.Bool(), p)
	default:
		if v.Type().Comparable() {
			g.others = addFirst(g.others, v.Interface(), p)
		}
	}
}

func addFirst[K comparable](m map[K]valuePos, k K, p valuePos) map[K]valuePos {
	if m == nil {
		m = make(map[K]valuePos)
	}
	if _, ok := m[k]; !ok {
		m[k] = p
	}
	return m
}

// find returns the bit index of the first value the field value matches.
func (l *valueLookup) find(fv reflect.Value) (uint64, bool) {
	if fv.Kind() == reflect.Interface {
		fv = fv.Elem()
	}
	if !fv.IsValid() {
		return 0, false
	}

	best := valuePos{pos: -1}
	for i := range l.groups {
		if p, ok := l.groups[i].find(fv); ok && (best.pos < 0 || p.pos < best.pos) {
			best = p
		}
	}
	return best.index, best.pos >= 0
}

var (
	stringType  = reflect.TypeFor[string]()
	int64Type   = reflect.TypeFor[int64]()
	uint64Type  = reflect.TypeFor[uint64]()
	float64Type = reflect.TypeFor[float64]()
	boolType    = reflect.TypeFor[bool]()
)

func (g *valueGroup) find(fv reflect.Value) (valuePos, bool) {
	// fast paths: values of the basic types and fields of the same kind convert without loss
	kind := fv.Kind()
	switch {
	case g.typ == stringType && kind == reflect.String:
		p, ok := g.strs[fv.String()]
		return p, ok
	case g.typ == int64Type && fv.CanInt():
		p, ok := g.ints[fv.Int()]
		return p, ok
	case g.typ == uint64Type && fv.CanUint():
		p, ok := g.uints[fv.Uint()]
		return p, ok
	case g.typ == float64Type && fv.CanFloat():
		p, ok := g.floats[fv.Float()]
		return p, ok
	case g.typ == boolType && kind == reflect.Bool:
		p, ok := g.bools[fv.Bool()]
		return p, ok
	}

	// otherwise convert like before: the field value has to convert to the type of the values
	if !fv.CanConvert(g.typ) {
		return valuePos{}, false
	}
	c := fv.Convert(g.typ)
	var p valuePos
	var ok bool
	switch c.Kind() {
	case reflect.String:
		p, ok = g.strs[c.String()]
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		p, ok = g.ints[c.Int()]
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		p, ok = g.uints[c.Uint()]
	case reflect.Float32, reflect.Float64:
		p, ok = g.floats[c.Float()]
	case reflect.Bool:
		p, ok = g.bools[c.Bool()]
	default:
		if c.Type().Comparable() && c.CanInterface() {
			p, ok = g.others[c.Interface()]
		}
	}
	return p, ok
}

// jsonName returns the json name of a struct field, the tag before the first comma.
func jsonName(f reflect.StructField) string {
	name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
	return name
}

// evaluatorsFor returns the exported fields of the struct type holding a property of the node, in field order.
func (n *nodeTables) evaluatorsFor(t reflect.Type) []fieldEvaluator {
	if e, ok := n.evaluators.Load(t); ok {
		return e.([]fieldEvaluator)
	}

	evaluators := make([]fieldEvaluator, 0)
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		if child, ok := n.children[jsonName(f)]; ok {
			evaluators = append(evaluators, fieldEvaluator{field: i, node: child})
		}
	}
	n.evaluators.Store(t, evaluators)
	return evaluators
}

// evaluate sets the bits of the property values of the struct.
func (n *nodeTables) evaluate(state *Bits, v reflect.Value) {
	for _, e := range n.evaluatorsFor(v.Type()) {
		fv := v.Field(e.field)
		if e.node.values != nil {
			if index, ok := e.node.values.find(fv); ok {
				state.set(index)
				continue
			}
		}
		if e.node.nested && !fv.IsZero() {
			if fv.Kind() == reflect.Interface {
				fv = fv.Elem()
			}
			if fv.Kind() == reflect.Pointer && !fv.IsNil() && fv.Elem().Kind() == reflect.Struct {
				e.node.evaluate(state, fv.Elem())
			}
		}
	}
}

// settersFor returns the fields of the struct type holding a property of the node:
// the first field with the json name of the property.
func (n *nodeTables) settersFor(t reflect.Type) []fieldSetter {
	if s, ok := n.setters.Load(t); ok {
		return s.([]fieldSetter)
	}

	fields := make(map[string]int)
	for i := range t.NumField() {
		if name := jsonName(t.Field(i)); name != "" {
			if _, ok := fields[name]; !ok {
				fields[name] = i
			}
		}
	}

	setters := make([]fieldSetter, 0, len(n.children))
	for name, child := range n.children {
		i, ok := fields[name]
		if !ok {
			continue
		}
		f := t.Field(i)
		s := fieldSetter{field: i, node: child, settable: f.IsExported()}
		if child.valueAt != nil {
			s.converted = make(map[uint64]reflect.Value, len(child.valueAt))
			for index, value := range child.valueAt {
				if rv := reflect.ValueOf(value); rv.IsValid() && rv.CanConvert(f.Type) {
					s.converted[index] = rv.Convert(f.Type)
				}
			}
		}
		setters = append(setters, s)
	}
	n.setters.Store(t, setters)
	return setters
}

// apply sets the fields of the struct ptr points to from the state and reports whether it set one.
// Nested structs are modified in place, a new one is only created when the field is nil and gets a value.
func (n *nodeTables) apply(state Bits, ptr reflect.Value) bool {
	st := ptr.Type().Elem()
	if st.Kind() != reflect.Struct {
		return false
	}

	changed := false
	for _, s := range n.settersFor(st) {
		if !s.settable {
			fmt.Printf("field could not be set %s", st.Field(s.field).Name)
			continue
		}
		field := ptr.Elem().Field(s.field)

		if s.node.nested {
			if field.Kind() != reflect.Pointer {
				continue
			}
			v, created := field, false
			if field.IsNil() {
				v, created = reflect.New(field.Type().Elem()), true
			}
			if s.node.apply(state, v) {
				changed = true
				if created {
					field.Set(v)
				}
			}
			continue
		}

		index, ok := s.node.highest(state)
		if !ok {
			continue
		}
		if v, ok := s.converted[index]; ok {
			field.Set(v)
			changed = true
		}
	}
	return changed
}

// highest returns the highest bit index of the property set in the state.
func (n *nodeTables) highest(state Bits) (uint64, bool) {
	for i := len(n.mask) - 1; i >= 0; i-- {
		m := n.mask[i]
		if m.word >= len(state) {
			continue
		}
		if w := state[m.word] & m.bits; w != 0 {
			return uint64(m.word)*64 + uint64(63-bits.LeadingZeros64(w)), true
		}
	}
	return 0, false
}

// set sets the bit at index i in place, growing the bitset if needed.
func (b *Bits) set(i uint64) {
	w := int(i / 64)
	if w >= len(*b) {
		if w < cap(*b) {
			old := len(*b)
			*b = (*b)[:w+1]
			clear((*b)[old:])
		} else {
			grown := make(Bits, w+1, max(w+1, 2*cap(*b)))
			copy(grown, *b)
			*b = grown
		}
	}
	(*b)[w] |= 1 << (i % 64)
}

// orWord sets the bits in word w in place, growing the bitset if needed.
func (b *Bits) orWord(w int, bits uint64) {
	if w >= len(*b) {
		grown := make(Bits, w+1)
		copy(grown, *b)
		*b = grown
	}
	(*b)[w] |= bits
}

// wordAt returns the word i of the bitset, 0 beyond its end.
func (b Bits) wordAt(i int) uint64 {
	if i < len(b) {
		return b[i]
	}
	return 0
}

// intersects reports whether the bitset has a bit of the mask set.
func intersects(b Bits, mask []maskWord) bool {
	for _, m := range mask {
		if b.wordAt(m.word)&m.bits != 0 {
			return true
		}
	}
	return false
}
