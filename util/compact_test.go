package util

import (
	"testing"
)

type compactInt struct {
	elements []int
}

func (ch compactInt) Len() int {
	return len(ch.elements)
}

func (ch compactInt) Remove(i int) bool {
	return ch.elements[i] < 0
}

func (ch compactInt) Swap(i, j int) {
	t := ch.elements[i]
	ch.elements[i] = ch.elements[j]
	ch.elements[j] = t
}

func TestCompact(t *testing.T) {
	ints := compactInt{[]int{-1, 2, 4, -10, 3, -11, -12, -15}}
	compacted := Compact(ints)
	if compacted != 3 {
		t.Fatalf("unexpected value %d", compacted)
	}
	for i := 0; i < compacted; i++ {
		if ints.elements[i] < 0 {
			t.Fatalf("non compacted element %v at index %d", ints.elements[i], i)
		}
	}
	for i := compacted; i < len(ints.elements); i++ {
		if ints.elements[i] >= 0 {
			t.Fatalf("non compacted element %v at index %d", ints.elements[i], i)
		}
	}
}

func TestCompactOnlyTrash(t *testing.T) {
	ints := compactInt{[]int{-1, -2, -4, -10, -3, -11, -12, -15}}
	compacted := Compact(ints)
	if compacted != 0 {
		t.Fatalf("unexpected value %d", compacted)
	}
	for i := 0; i < compacted; i++ {
		if ints.elements[i] < 0 {
			t.Fatalf("non compacted element %v at index %d", ints.elements[i], i)
		}
	}
	for i := compacted; i < len(ints.elements); i++ {
		if ints.elements[i] >= 0 {
			t.Fatalf("non compacted element %v at index %d", ints.elements[i], i)
		}
	}
}

func TestCompactNoTrash(t *testing.T) {
	ints := compactInt{[]int{1, 2, 4, 10, 3, 11, 12, 15}}
	compacted := Compact(ints)
	if compacted != 8 {
		t.Fatalf("unexpected value %d", compacted)
	}
	for i := 0; i < compacted; i++ {
		if ints.elements[i] < 0 {
			t.Fatalf("non compacted element %v at index %d", ints.elements[i], i)
		}
	}
	for i := compacted; i < len(ints.elements); i++ {
		if ints.elements[i] >= 0 {
			t.Fatalf("non compacted element %v at index %d", ints.elements[i], i)
		}
	}
}
