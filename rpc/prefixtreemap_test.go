package rpc

import (
	"fmt"
	"testing"
)

func TestBasicTreeOps(t *testing.T) {
	ptm := PrefixTreeMap{}
	addr1 := MustParseAddress("foo:bar:test")
	addr2 := MustParseAddress("foo:bar")
	addr3 := MustParseAddress("foo")
	addr4 := MustParseAddress("foo:doo")
	key5 := NewAddress("foo", bytesToString([]byte{2, 1, 0, 3, 4, 128, 255, 198, 254}))
	ptm.Put(addr1, "foo:bar:test")
	ptm.Put(addr2, "foo:bar")
	ptm.Put(addr4, "foo:doo")
	ptm.Put(key5, "bytes")

	if x := ptm.Get(BlankAddress); x != nil {
		t.Fatalf("unexpected value %v", x)
	}
	if ptm.Size() != 4 {
		t.Fatalf("unexpected value %d", ptm.Size())
	}
	if x := ptm.Get(addr3); x != nil {
		t.Fatalf("unexpected value %v", x)
	}
	if x := ptm.Get(addr1); x != "foo:bar:test" {
		t.Fatalf("unexpected value %v", x)
	}
	if x := ptm.Get(key5); x != "bytes" {
		t.Fatalf("unexpected value %v", x)
	}

	cs := ptm.GetChildren(BlankAddress)
	if len(cs) != 1 {
		t.Fatalf("unexpected value %v", cs)
	}
	if cs[0].Key.Compare(addr3) != 0 || cs[0].Value != nil {
		t.Fatalf("unexpected value %v", cs[0])
	}

	cs = ptm.GetChildren(addr3)
	if len(cs) != 3 {
		t.Fatalf("unexpected value %v", cs)
	}

	es := ptm.GetSubtree(BlankAddress)
	if len(es) != 4 {
		t.Fatalf("unexpected value %v", es)
	}

	es = ptm.GetSubtree(addr2)
	if len(es) != 2 {
		t.Fatalf("unexpected value %v", es)
	}

	rem := ptm.Remove(addr2)
	if rem != "foo:bar" {
		t.Fatalf("unexpected value %v", rem)
	}
	if ptm.Size() != 3 {
		t.Fatalf("unexpected value %d", ptm.Size())
	}
	ptm.Remove(addr4)
	ptm.Remove(key5)
	ptm.Remove(addr1)
	if ptm.Size() != 0 {
		t.Fatalf("unexpected value %d", ptm.Size())
	}
	es = ptm.GetChildren(BlankAddress)
	if len(es) != 0 {
		t.Fatalf("unexpected value %v", es)
	}
}

func (ptm PrefixTreeMap) printTree() {
	ptm.IterateSubtree(BlankAddress, func(e PrefixMapEntry) bool {
		fmt.Printf("%v %v\n", e.Key, e.Value)
		return true
	})
}

func TestIteratreSubTree(t *testing.T) {
	ptm := PrefixTreeMap{}
	addr1 := MustParseAddress("foo:bar:test")
	addr2 := MustParseAddress("foo:bar")
	//addr3 := MustParseAddress("foo")
	addr4 := MustParseAddress("foo:doo")
	key5 := NewAddress("foo", bytesToString([]byte{2, 1, 0, 3, 4, 128, 255, 198, 254}))
	ptm.Put(addr1, "foo:bar:test")
	ptm.Put(addr2, "foo:bar")
	ptm.Put(addr4, "foo:doo")
	ptm.Put(key5, "bytes")

	ptm.printTree()

	iter := make(chan PrefixMapEntry)
	go func() {
		ptm.IterateSubtree(BlankAddress, func(e PrefixMapEntry) bool { iter <- e; return true })
		close(iter)
	}()
	v := <-iter
	if v.Value != nil {
		t.Fatalf("unexpected value %v", v)
	}
	v = <-iter
	if v.Value != nil {
		t.Fatalf("unexpected value %v", v)
	}
	// the output order is undefined and changes actually quite a lot, using map to mimic a set
	vset := make(map[string]bool)
	v = <-iter
	vset[v.Value.(string)] = true
	v = <-iter
	vset[v.Value.(string)] = true
	v = <-iter
	vset[v.Value.(string)] = true
	v = <-iter
	vset[v.Value.(string)] = true
	v, ok := <-iter
	if ok {
		t.Fatalf("unexpected value %v", v)
	}

	_, ok = vset["foo:bar"]
	if !ok {
		t.Fatalf("unexpected value %v", v)
	}
	_, ok = vset["foo:bar:test"]
	if !ok {
		t.Fatalf("unexpected value %v", v)
	}
	_, ok = vset["foo:doo"]
	if !ok {
		t.Fatalf("unexpected value %v", v)
	}
	_, ok = vset["bytes"]
	if !ok {
		t.Fatalf("unexpected value %v", v)
	}
}
