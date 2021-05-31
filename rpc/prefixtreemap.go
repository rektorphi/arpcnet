package rpc

type PrefixTreeMap struct {
	prefixMapNode
	size int
}

type PrefixMapEntry struct {
	Key   *Address
	Value interface{}
}

func (ptm *PrefixTreeMap) Size() int {
	return ptm.size
}

func (ptm *PrefixTreeMap) Get(key *Address) interface{} {
	d, n := ptm.getNearest(0, key)
	if d == key.Len() {
		return n.value
	}
	return nil
}

// GetNearest returns the entry of the tree with the most matching prefix of the given key and the value associated with it, which can be nil.
// Returns a length 0 key if no entry with any matching prefix exist.
func (ptm PrefixTreeMap) GetNearest(key *Address) (depth int, value interface{}) {
	d, n := ptm.getNearest(0, key)
	return d, n.value
}

// GetChildren returns the direct children of a node, if one exists for the given key. Otherwise returns nil.
// Values can be nil if the child node has no value directly assigned to it.
func (ptm PrefixTreeMap) GetChildren(key *Address) (children []PrefixMapEntry) {
	d, n := ptm.getNearest(0, key)
	if d != key.Len() {
		return nil
	}
	children = make([]PrefixMapEntry, 0, len(n.children))
	for pk, c := range n.children {
		children = append(children, PrefixMapEntry{key.Appends(pk), c.value})
	}
	return children
}

// GetSubtree returns all keys with values of the subtree including the given key, or nil if the key is not part of the tree.
func (ptm PrefixTreeMap) GetSubtree(key *Address) (entries []PrefixMapEntry) {
	entries = make([]PrefixMapEntry, 0)
	ptm.IterateSubtree(key, func(e PrefixMapEntry) bool {
		if e.Value != nil {
			entries = append(entries, e)
		}
		return true
	})
	return entries
}

// RemoveSubtree removes the subtree including the given key and returns the removed entries, or nil if the key is not part of the tree.
func (ptm PrefixTreeMap) RemoveSubtree(key *Address) (entries []PrefixMapEntry) {
	entries = make([]PrefixMapEntry, 0)
	d, n := ptm.getNearest(0, key)
	if d != key.Len() {
		return
	}
	n.iterateSubtree(d, key, -1, func(e PrefixMapEntry) bool {
		if e.Value != nil {
			entries = append(entries, e)
		}
		return true
	})
	ptm.size -= len(entries)
	n.children = nil
	n.value = nil
	if n.parent != nil {
		n.parent.removeChild(d-1, key)
	}
	return entries
}

// IterateSubtree performs the action on all nodes of the entire subtree, including keys with nil values.
// Aborts when the action returns false.
func (ptm PrefixTreeMap) IterateSubtree(key *Address, action func(PrefixMapEntry) bool) {
	d, n := ptm.getNearest(0, key)
	if d != key.Len() {
		return
	}
	n.iterateSubtree(d, key, -1, action)
}

// Put maps the given key to the given value and returns the previously mapped value, which can be nil.
func (ptm *PrefixTreeMap) Put(key *Address, value interface{}) (previousValue interface{}) {
	kl := key.Len()
	if kl == 0 {
		panic("cannot store empty key")
	}
	if value == nil {
		return ptm.Remove(key)
	}
	d, p := ptm.getNearest(0, key)
	if d == key.Len() {
		previousValue = p.value
		p.value = value
		if previousValue == nil {
			ptm.size++
		}
		return previousValue
	}
	for d < kl-1 {
		n := &prefixMapNode{p, nil, nil}
		p.addChild(d, key, n)
		p = n
		d++
	}
	p.addChild(d, key, &prefixMapNode{p, value, nil})
	ptm.size++
	return nil
}

// Remove clears the mapped value of the given key. If the key is parent of another key, its value is set to nil.
// Returns the previous value, which can be nil.
func (ptm *PrefixTreeMap) Remove(key *Address) (previousValue interface{}) {
	kl := key.Len()
	if kl == 0 {
		panic("cannot remove empty key")
	}
	d, p := ptm.getNearest(0, key)
	if d != kl || p.value == nil {
		return nil
	}
	previousValue = p.value
	if previousValue != nil {
		ptm.size--
		p.value = nil
	}
	if p.children == nil {
		p.parent.removeChild(d-1, key)
	}
	return previousValue
}

// Clear removes all entries from the tree.
func (ptm *PrefixTreeMap) Clear() {
	ptm.children = nil
	ptm.size = 0
}

type prefixMapNode struct {
	parent   *prefixMapNode
	value    interface{}
	children map[string]*prefixMapNode
}

func (pmn *prefixMapNode) getNearest(d int, key *Address) (int, *prefixMapNode) {
	if key.Len() <= d {
		return d, pmn
	}
	k := key.Get(d)
	if pmn.children == nil {
		return d, pmn
	}
	c := pmn.children[k]
	if c != nil {
		return c.getNearest(d+1, key)
	}
	return d, pmn
}

func (pmn *prefixMapNode) iterateSubtree(d int, key *Address, maxDepth int, action func(PrefixMapEntry) bool) bool {
	thiskey := key.Slice(0, d)
	cont := action(PrefixMapEntry{thiskey, pmn.value})
	if !cont {
		return false
	}
	if maxDepth > 0 && d >= maxDepth {
		return false
	}
	for pk, c := range pmn.children {
		k := thiskey.Appends(pk)
		cont = c.iterateSubtree(d+1, k, maxDepth, action)
		if !cont {
			return false
		}
	}
	return true
}

// d is the depth of this node, the child will be one deeper
func (pmn *prefixMapNode) addChild(d int, key *Address, child *prefixMapNode) {
	if pmn.children == nil {
		pmn.children = make(map[string]*prefixMapNode, 4)
	}
	k := key.Get(d)
	pmn.children[k] = child
}

func (pmn *prefixMapNode) removeChild(d int, key *Address) {
	k := key.Get(d)
	delete(pmn.children, k)
	if len(pmn.children) == 0 {
		pmn.children = nil
	}
	if pmn.children == nil && pmn.value == nil && pmn.parent != nil {
		pmn.parent.removeChild(d-1, key)
	}
}
