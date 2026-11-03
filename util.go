package asm

func mapSlice[T, V any](ts []T, fn func(T) V) []V {
	result := make([]V, len(ts))
	for i, t := range ts {
		result[i] = fn(t)
	}
	return result
}

func Map[T, V comparable, K, G any](ts map[T]K, fn func(T, K) (V, G)) map[V]G {
	result := make(map[V]G, len(ts))
	for k, v := range ts {
		newK, newV := fn(k, v)
		result[newK] = newV
	}
	return result
}
