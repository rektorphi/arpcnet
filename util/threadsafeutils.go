package util

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type cowList struct {
	list []interface{}
	m    sync.Mutex
}

func (l *cowList) Add(v interface{}) {
	l.m.Lock()
	list := l.list
	list = append(list, v)
	l.list = list
	l.m.Unlock()
}

func (l *cowList) Remove(v interface{}) {
	l.m.Lock()
	l.m.Unlock()
}

func (l *cowList) GetAll() func() interface{} {
	list := l.list
	i := 0
	return func() interface{} {
		if i >= len(list) {
			return nil
		}
		r := list[i]
		i++
		return r
	}
}

// A light-weight single linked list.
// Elements are added at the front, multiple additions and iterations can run concurrently and are lockfree.
// Removal operations are thread-safe and use a lock but run concurrently to additions and iterations.
// All operations must traverse the list from the front.
type ThreadSafeLinkedList struct {
	first *listElement
	size  uint32
	rm    sync.Mutex
}

type listElement struct {
	next  *listElement
	value interface{}
}

func (l *ThreadSafeLinkedList) Len() int {
	return (int)(atomic.LoadUint32(&l.size))
}

// Push inserts given values to the front of the list.
// Values are inserted in fifo order, i.e. the last value is at the front of the list, the same as if the values would be added in a loop.
// The values are ensured to be inserted as one sequence, uninterrupted by other concurrrent changes.
func (l *ThreadSafeLinkedList) Push(vs ...interface{}) {
	if len(vs) == 0 {
		return
	}
	// going backwards through vs to keep the fifo behavior the same as multiple Push calls
	f := &listElement{next: nil, value: vs[len(vs)-1]}
	e := f
	for i := len(vs) - 2; i >= 0; i-- {
		e.next = &listElement{next: nil, value: vs[i]}
		e = e.next
	}
	// now we have a chain of the values in reverse order, f points to the first, e the last element
	for {
		// Load pointer to current first element
		un := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.first)))
		// Set next of the element to be inserted to the current first
		e.next = (*listElement)(un)
		// So far it is all harmless since element cannot be accessed by anybody yet
		// Now we try to atomically change the next pointer of anchor to the element to be inserted.
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.first)), un, unsafe.Pointer(f)) {
			atomic.AddUint32(&l.size, uint32(len(vs)))
			break
		}
		// Else achor.next was changed by someone else already in a race condition and try again, including reloading anchor.next.
	}
}

// Peek returns the element at the front of the list. This is the most recently added element.
func (l *ThreadSafeLinkedList) Peek() interface{} {
	front := l.first
	if front == nil {
		return nil
	}
	return front.value
}

// Pop returns and removes the element at the front of the list. This is the most recently added element.
func (l *ThreadSafeLinkedList) Pop() interface{} {
	// We can deal with the concurrent adds/iterations but not concurrent deletes.
	l.rm.Lock()
	p := l.first
	for p != nil {
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.first)), unsafe.Pointer(p), unsafe.Pointer(p.next)) {
			atomic.AddUint32(&l.size, ^uint32(0))
			l.rm.Unlock()
			return p.value
		}
		// if the swap failed, an add just happened and we need to find our new previous pointer again
		p = (*listElement)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(l.first))))
	}
	l.rm.Unlock()
	return nil
}

// Snip removes all elements from the list after and including the element for which the provided function returns true.
// The function might be called multiple times for an element.
func (l *ThreadSafeLinkedList) Snip(when func(interface{}) bool) int {
	var prev **listElement = &l.first
	p := (*listElement)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(prev))))
	// look through the list for v
	for p != nil {
		if when(p.value) {
			goto found
		}
		prev = &p.next
		p = (*listElement)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(prev))))
	}
	return 0
found:
	// We can deal with the concurrent adds/iterations but not concurrent deletes.
	l.rm.Lock()
	for p != nil {
		if when(p.value) {
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(prev)), unsafe.Pointer(p), nil) {
				l.rm.Unlock()
				goto countsnip
			}
			// if the swap failed, an add just happened and we need to find our new previous pointer again
		}
		prev = &p.next
		p = (*listElement)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(prev))))
	}
	l.rm.Unlock()
countsnip:
	// now have to count the snipped off elements
	c := 1
	for p.next != nil {
		p = p.next
		c++
	}
	atomic.AddUint32(&l.size, ^uint32(c-1))
	return c
}

func (l *ThreadSafeLinkedList) RemoveFirst(pred func(interface{}) bool) bool {
	var prev **listElement = &l.first
	p := (*listElement)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(prev))))
	// look through the list for v
	for p != nil {
		if pred(p.value) {
			goto found
		}
		prev = &p.next
		p = (*listElement)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(prev))))
	}
	return false
found:
	// We can deal with the concurrent adds/iterations but not concurrent deletes.
	l.rm.Lock()
	for p != nil {
		if pred(p.value) {
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(prev)), unsafe.Pointer(p), unsafe.Pointer(p.next)) {
				atomic.AddUint32(&l.size, ^uint32(0))
				l.rm.Unlock()
				return true
			}
			// if the swap failed, an add just happened and we need to find our new previous pointer again
		}
		prev = &p.next
		p = (*listElement)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(prev))))
	}
	return false
}

func (l *ThreadSafeLinkedList) Iterate() func() interface{} {
	p := (*listElement)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.first))))
	return func() interface{} {
		if p == nil {
			return nil
		}
		res := p.value
		p = (*listElement)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&p.next))))
		return res
	}
}

func (l *ThreadSafeLinkedList) GetAll() []interface{} {
	res := make([]interface{}, 0)
	iter := l.Iterate()
	for i := iter(); i != nil; i = iter() {
		res = append(res, i)
	}
	return res
}
