package asm

import (
	"math/bits"

	"rumue.io/asm/service"
)

// Bits is a packed bitset. Bit i is stored in Bits[i/64] at position i%64.
// A new word is only added when a bit beyond the last word is set, so the bitset stays as small as possible.
// All operations return new bitsets without trailing zero words, the zero bitset is nil.
type Bits []uint64

// NewBits returns a bitset with the bits at the given indices set.
func NewBits(indices ...uint64) Bits {
	var b Bits
	for _, i := range indices {
		b = b.With(i)
	}
	return b
}

// Has reports whether the bit at index i is set.
func (b Bits) Has(i uint64) bool {
	word := i / 64
	if word >= uint64(len(b)) {
		return false
	}
	return b[word]&(1<<(i%64)) != 0
}

// With returns a copy of the bitset with the bit at index i set.
func (b Bits) With(i uint64) Bits {
	word := int(i / 64)
	size := max(len(b), word+1)
	result := make(Bits, size)
	copy(result, b)
	result[word] |= 1 << (i % 64)
	return result
}

// Or returns the union of both bitsets.
func (b Bits) Or(o Bits) Bits {
	result := make(Bits, max(len(b), len(o)))
	copy(result, b)
	for i, w := range o {
		result[i] |= w
	}
	return result.trim()
}

// And returns the intersection of both bitsets.
func (b Bits) And(o Bits) Bits {
	result := make(Bits, min(len(b), len(o)))
	for i := range result {
		result[i] = b[i] & o[i]
	}
	return result.trim()
}

// AndNot returns the bits of b which are not set in o.
func (b Bits) AndNot(o Bits) Bits {
	result := make(Bits, len(b))
	copy(result, b)
	for i := range result {
		if i < len(o) {
			result[i] &^= o[i]
		}
	}
	return result.trim()
}

// ContainsAll reports whether all bits of o are set in b.
func (b Bits) ContainsAll(o Bits) bool {
	for i, w := range o {
		var bw uint64
		if i < len(b) {
			bw = b[i]
		}
		if bw&w != w {
			return false
		}
	}
	return true
}

// ContainsNone reports whether none of the bits of o are set in b.
func (b Bits) ContainsNone(o Bits) bool {
	for i := range min(len(b), len(o)) {
		if b[i]&o[i] != 0 {
			return false
		}
	}
	return true
}

// IsZero reports whether no bit is set.
func (b Bits) IsZero() bool {
	for _, w := range b {
		if w != 0 {
			return false
		}
	}
	return true
}

// Equal reports whether both bitsets have the same bits set.
func (b Bits) Equal(o Bits) bool {
	b, o = b.trim(), o.trim()
	if len(b) != len(o) {
		return false
	}
	for i := range b {
		if b[i] != o[i] {
			return false
		}
	}
	return true
}

// Indices returns the indices of all set bits in ascending order.
func (b Bits) Indices() []uint64 {
	indices := make([]uint64, 0)
	for i, w := range b {
		for w != 0 {
			pos := uint64(bits.TrailingZeros64(w))
			indices = append(indices, uint64(i)*64+pos)
			w &^= 1 << pos
		}
	}
	return indices
}

func (b Bits) trim() Bits {
	end := len(b)
	for end > 0 && b[end-1] == 0 {
		end--
	}
	if end == 0 {
		return nil
	}
	return b[:end]
}

func (b Bits) toProto() *service.Bits {
	return &service.Bits{Words: append([]uint64(nil), b.trim()...)}
}

func bitsFromProto(b *service.Bits) Bits {
	return Bits(append([]uint64(nil), b.GetWords()...)).trim()
}
