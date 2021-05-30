package rpc

import (
	"strings"
	"testing"
)

func TestParseAddress(t *testing.T) {
	a, e := ParseAddress("")
	if e != nil || !a.Equal(BlankAddress) {
		t.Errorf("failed parsing address: %v %v", a, e)
	}

	a, e = ParseAddress("a-_")
	if e != nil || a.String() != "a-_" {
		t.Errorf("failed parsing address: %v %v", a, e)
	}

	a, e = ParseAddress("a:b:c")
	if e != nil || a.String() != "a:b:c" {
		t.Errorf("failed parsing address: %v %v", a, e)
	}

	a, e = ParseAddress(":")
	if e == nil {
		t.Errorf("failed parsing address: %v %v", a, e)
	}

	a, e = ParseAddress(":asdasd:")
	if e == nil {
		t.Errorf("failed parsing address: %v %v", a, e)
	}

	a, e = ParseAddress("a:b::c")
	if e == nil {
		t.Errorf("failed parsing address: %v %v", a, e)
	}
}

func creates(size int) string {
	var sb strings.Builder
	sb.Grow(size)
	for i := 0; i < size; i++ {
		sb.WriteByte(byte('A' + (i % 26)))
	}
	return sb.String()
}

func BenchmarkStrings(b *testing.B) {
	s := creates(1024)
	for i := 0; i < b.N; i++ {
		bs := []byte(s)
		t := string(bs)
		_ = len(t)
	}
}

func BenchmarkStrings2(b *testing.B) {
	s := creates(1024)
	for i := 0; i < b.N; i++ {
		bs := stringToBytes(s)
		t := bytesToString(bs)
		_ = len(t)
	}
}

func BenchmarkStrings3(b *testing.B) {
	s := creates(1024)
	for i := 0; i < b.N; i++ {
		bs := stringToBytes2(s)
		t := bytesToString2(bs)
		_ = len(t)
	}
}

func TestIBList(t *testing.T) {
	ibl := NewIBList()
	if ibl.Len() != 0 {
		t.Fatalf("unexpected len %d", ibl.Len())
	}
	ibl = NewIBList("abcd")
	if ibl.Len() != 1 {
		t.Fatalf("unexpected len %d", ibl.Len())
	}
	bs := ibl.Get(0)
	if len(bs) != 4 {
		t.Fatal()
	}
	ibl = NewIBList("abcd", "e")
	if ibl.Len() != 2 {
		t.Fatalf("unexpected len %d", ibl.Len())
	}
	bs = ibl.Get(1)
	if len(bs) != 1 || bs[0] != 'e' {
		t.Fatal()
	}
	ibl = NewIBList("abcd", "e", "fghijk")
	if ibl.Len() != 3 {
		t.Fatalf("unexpected len %d", ibl.Len())
	}
	bs = ibl.Get(1)
	if len(bs) != 1 || bs[0] != 'e' {
		t.Fatal()
	}
	bs = ibl.Get(2)
	if len(bs) != 6 || bs[0] != 'f' {
		t.Fatal()
	}
	ibl = NewIBList("abcd", "e", "fghijk")
	if ibl.String() != "abcd:e:fghijk" {
		t.Fatalf("unexpected string %s", ibl.String())
	}
}

func TestAddrBuilder(t *testing.T) {
	ab := NewAddress("abcd", "e", "fghijk")
	if ab.Len() != 3 {
		t.Fatalf("unexpected value %d", ab.Len())
	}
	if ab.Get(0) != "abcd" || ab.Get(1) != "e" || ab.Get(2) != "fghijk" || ab.String() != "abcd:e:fghijk" {
		t.Fatalf("unexpected value %s", ab.String())
	}
	ab = ab.Appends("foo", "bar")
	if ab.Len() != 5 {
		t.Fatalf("unexpected value %d", ab.Len())
	}
	if ab.Get(1) != "e" || ab.Get(3) != "foo" || ab.Get(4) != "bar" || ab.String() != "abcd:e:fghijk:foo:bar" {
		t.Fatalf("unexpected value %s", ab.String())
	}

	s1 := ab.Slice(0, 2)
	if s1.Len() != 2 || s1.String() != "abcd:e" {
		t.Fatalf("unexpected value %s", s1.String())
	}
	s2 := ab.Slice(1, 3)
	if s2.Len() != 2 || s2.String() != "e:fghijk" {
		t.Fatalf("unexpected value %s", s2.String())
	}
	s3 := ab.Slice(5, 5)
	ab = s1.Append(s2, s3, ab)
	if ab.Len() != 9 {
		t.Fatalf("unexpected value %d", ab.Len())
	}
	s1 = ab.Slice(1, 8)
	if s1.Len() != 7 {
		t.Fatalf("unexpected value %d", s1.Len())
	}

	// TODO s1.String() is actually too long!

	if !s1.StartsWith(NewAddress("e", "e")) {
		t.Fatalf("unexpected result")
	}
}
