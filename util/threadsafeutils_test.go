package util

import (
	"testing"
)

func TestList(t *testing.T) {
	l := ThreadSafeLinkedList{}
	s1 := "1"
	s2 := "2"
	s3 := "3"
	l.Push(s1)
	l.Push(s2, s3)
	vs := l.GetAll()
	if len(vs) != 3 {
		t.Fatalf("unexpected value %d", len(vs))
	}
	if l.Pop() != s3 {
		t.Fatalf("unexpected value %d", len(vs))
	}
	if l.Pop() != s2 {
		t.Fatalf("unexpected value %d", len(vs))
	}
	if l.Pop() != s1 {
		t.Fatalf("unexpected value %d", len(vs))
	}
	if l.Pop() != nil {
		t.Fatalf("unexpected value %d", len(vs))
	}
	vs = l.GetAll()
	if len(vs) != 0 {
		t.Fatalf("unexpected value %d", len(vs))
	}
}

func TestListSnip(t *testing.T) {
	l := ThreadSafeLinkedList{}
	l.Push(1, 2, 3, 4, 10, 11, 12, 13, 14)
	l.Snip(func(v interface{}) bool { return v.(int) < 10 })
	if l.Len() != 5 {
		t.Fatalf("unexpected value %d", l.Len())
	}
}
