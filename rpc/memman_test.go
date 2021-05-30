package rpc

import (
	context "context"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestMemManWorkflow(t *testing.T) {
	total := 32 * 1000
	mm := NewCondMemoryManager(total)
	if mm.Max() != total {
		t.Errorf("max %d not expected", mm.Max())
	}
	if mm.Used() != 0 {
		t.Errorf("used %d not expected", mm.Used())
	}
	tk := mm.Acquire(10)
	if tk.Size() != 10 {
		t.Errorf("size %d not expected", tk.Size())
	}
	if mm.Max() != total {
		t.Errorf("max %d not expected", mm.Max())
	}
	if mm.Used() != 10 {
		t.Errorf("used %d not expected", mm.Used())
	}
	tk.ReduceTo(5)
	if tk.Size() != 5 {
		t.Errorf("size %d not expected", tk.Size())
	}
	if mm.Used() != 5 {
		t.Errorf("used %d not expected", mm.Used())
	}
	tk.Release()
	if tk.Size() != 0 {
		t.Errorf("size %d not expected", tk.Size())
	}
	if mm.Used() != 0 {
		t.Errorf("used %d not expected", mm.Used())
	}
}

func TestBlocking(t *testing.T) {
	mm := NewCondMemoryManager(10)
	bt := mm.Acquire(10)

	ch := make(chan bool)
	go func(chan bool) {
		mm.Acquire(5)
		ch <- true
		close(ch)
	}(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	select {
	case <-ch:
		t.Errorf("acquire did not block")
	case <-ctx.Done():
	}
	cancel()

	bt.ReduceTo(6)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	select {
	case <-ch:
		t.Errorf("acquire did not block")
	case <-ctx.Done():
	}
	cancel()

	bt.ReduceTo(5)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	select {
	case <-ch:
	case <-ctx.Done():
		t.Errorf("acquire blocked")
	}
	cancel()
}

func TestMemManStress(t *testing.T) {
	total := 10001
	n := 100
	mm := NewCondMemoryManager(total)

	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int, wg *sync.WaitGroup) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				req := rand.Intn(total)
				tk, err := mm.AcquireCtx(ctx, req)
				if err != nil {
					return
				}
				tk.ReduceTo(req / 2)
				tk.Release()
			}
		}(i, &wg)
	}

	wg.Wait()

	if mm.Used() != 0 {
		t.Errorf("used is %d instead of 0", mm.Used())
	}
}
