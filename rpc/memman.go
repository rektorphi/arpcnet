package rpc

import (
	"context"
	"log"
	"sync"
)

// A MemoryTicket allows an amount of memory to be used.
type MemoryTicket interface {
	// Size returns the amount of memory that can be used with this ticket.
	Size() int
	// Reduce can be used to lower the ticket size if it turns out that less memory is needed.
	// Implementations can choose to ignore this or shrink less than requested.
	ReduceTo(newSize int)
	// Release to set the ticket size to 0 and free all memory.
	Release()
}

// A MemoryManager helps track and limit memory consumption of certain processes.
// It guards an amount of memory. To use some of this memory, a ticket must be aquired and released later to free the memory for other users.
type MemoryManager interface {
	// The total memory managed by this instance.
	Max() int
	// A snapshot of the memory amount currently acquired, only for debugging purposes and may be slow.
	Used() int
	// Reserve some of the memory from the manager. This blocks until enough memory is available.
	// The returned ticket must be released eventually!
	Acquire(int) MemoryTicket
	// Reserve some of the memory from the manager only if enough memory is available, never blocks.
	// If a ticket is returned, it must be released eventually!
	AcquireLow(int) MemoryTicket

	AcquireCtx(context.Context, int) (MemoryTicket, error)
	// Internal function to release memory to the manager from tickets.
	release(int)
}

type noManImpl struct {
}

func (m *noManImpl) Acquire(request int) MemoryTicket {
	return &ticketImpl{request, m}
}

func (m *noManImpl) AcquireCtx(ctx context.Context, request int) (MemoryTicket, error) {
	return &ticketImpl{request, m}, nil
}

func (m *noManImpl) AcquireLow(request int) MemoryTicket {
	return &ticketImpl{request, m}
}

func (m *noManImpl) Max() int {
	return 0
}

func (m *noManImpl) Used() int {
	return 0
}

func (m *noManImpl) release(int) {
}

func NewNoMemoryManager() MemoryManager {
	return &noManImpl{}
}

type lockManImpl struct {
	max  int
	free int
	l    sync.Mutex
	c    sync.Cond
}

// NewCondMemoryManager creates a memory manager that uses a central mutex for all memory interactions.
func NewCondMemoryManager(max int) MemoryManager {
	r := lockManImpl{
		max:  max,
		free: max,
		l:    sync.Mutex{},
	}
	r.c = *sync.NewCond(&r.l)
	return &r
}

func (m *lockManImpl) Acquire(request int) MemoryTicket {
	if request <= 0 {
		return &ticketImpl{0, m}
	} else if request > m.max {
		log.Fatalf("memory request %d exceeds max %d", request, m.max)
	}
	m.l.Lock()
	for m.free < request {
		m.c.Wait()
	}
	m.free -= request
	m.l.Unlock()
	return &ticketImpl{request, m}
}

func (m *lockManImpl) AcquireLow(request int) MemoryTicket {
	if request <= 0 {
		return &ticketImpl{0, m}
	} else if request > m.max {
		log.Fatalf("memory request %d exceeds max %d", request, m.max)
	}
	m.l.Lock()
	if m.free <= request || m.free < m.max>>2 {
		return nil
	}
	m.free -= request
	m.l.Unlock()
	return &ticketImpl{request, m}
}

func (m *lockManImpl) AcquireCtx(ctx context.Context, request int) (MemoryTicket, error) {
	if request <= 0 {
		return &ticketImpl{0, m}, nil
	}
	go func(context.Context) {
		<-ctx.Done()
		m.c.Broadcast()
	}(ctx)
	m.l.Lock()
	for m.free < request {
		m.c.Wait()
		err := ctx.Err()
		if err != nil {
			m.l.Unlock()
			return nil, err
		}
	}
	m.free -= request
	m.l.Unlock()
	return &ticketImpl{request, m}, nil
}

func (m *lockManImpl) Max() int {
	return m.max
}

func (m *lockManImpl) Used() int {
	m.l.Lock()
	defer m.l.Unlock()
	return m.max - m.free
}

func (m *lockManImpl) release(amount int) {
	m.l.Lock()
	m.free += amount
	m.c.Broadcast()
	m.l.Unlock()
}

type ticketImpl struct {
	size  int
	owner MemoryManager
}

func (t ticketImpl) Size() int {
	return t.size
}

func (t *ticketImpl) ReduceTo(newSize int) {
	if newSize <= 0 {
		t.Release()
		return
	}
	toRelease := t.size - newSize
	if toRelease <= 0 {
		return
	}
	t.owner.release(toRelease)
	t.size = newSize
}

func (t *ticketImpl) Release() {
	if t.size <= 0 {
		return
	}
	t.owner.release(t.size)
	t.size = 0
}

/*
func MergeTickets(t1 MemoryTicket, t2 MemoryTicket) MemoryTicket {

}

type multiTicket struct {
	size    int
	tickets []MemoryTicket
}

func (t multiTicket) Size() int {
	return t.size
}

func (t *multiTicket) ReduceTo(newSize int) {
	if newSize <= 0 {
		t.Release()
		return
	}
	toRelease := t.size - newSize
	if toRelease <= 0 {
		return
	}
	t.owner.release(toRelease)
	t.size = newSize
}

func (t *multiTicket) Release() {
	if t.size <= 0 {
		return
	}
	t.owner.release(t.size)
	t.size = 0
}
*/
