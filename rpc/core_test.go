package rpc

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	v1 "github.com/rektorphi/arpcnet/generated/rektorphi/arpcnet/v1"
	"google.golang.org/grpc/codes"
)

func TestRouteAndDispatch(t *testing.T) {
	ctx, canctx := context.WithTimeout(context.Background(), 5*time.Second)
	defer canctx()

	core := NewCore(NewAddress("test"), -1)
	scf := ServerCallFactory{core.MemMan(), ctx, UnaryServerCall1}
	r := NewSingleRPCHandler(12, "test", scf.Init)
	core.Router().DestinationUpdate(MustParseAddress("test:dest"), r, Metric{10})

	ccall, chandler := NewClientCall(10, "", ctx, core.MemMan())
	fullId := RandomFullID(MustParseAddress("test:source"), MustParseAddress("test:dest"))

	err := core.RouteAndDispatch(ctx, NewUpStartFrame(fullId, []string{}, make(map[string][]byte)), chandler)
	if err != nil {
		t.Fatalf("Call route failed with error: %v\n", err)
	}

	err = ccall.UnaryClientCall1()
	if err != nil {
		t.Fatalf("Call finished with error: %v\n", err)
	}

	fmt.Printf("memm %d\n", core.MemMan().Used())
}

func TestStartRPC(t *testing.T) {
	ctx, canctx := context.WithTimeout(context.Background(), 5*time.Second)
	defer canctx()

	core := NewCore(NewAddress("test"), -1)
	scf := ServerCallFactory{core.MemMan(), ctx, UnaryServerCall1}
	r := NewSingleRPCHandler(12, "test", scf.Init)
	core.Router().DestinationUpdate(MustParseAddress("test.dest"), r, Metric{10})

	ccall, chandler := NewClientCall(10, "", ctx, core.MemMan())

	err := core.StartRPC(ctx, MustParseAddress("test.dest"), []string{}, make(map[string][]byte), chandler)
	if err != nil {
		t.Fatalf("Call route failed with error: %v\n", err)
	}

	err = ccall.UnaryClientCall1()
	if err != nil {
		t.Fatalf("Call finished with error: %v\n", err)
	}

	fmt.Printf("memm %d\n", core.MemMan().Used())
}

func TestRouteFailed(t *testing.T) {
	ctx, canctx := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer canctx()

	core := NewCore(NewAddress("test"), -1)
	scf := ServerCallFactory{core.MemMan(), ctx, UnaryServerCall1}
	r := NewSingleRPCHandler(12, "test", scf.Init)
	core.Router().DestinationUpdate(MustParseAddress("test.other"), r, Metric{10})

	ccall, chandler := NewClientCall(10, "", ctx, core.MemMan())
	fullId := RandomFullID(MustParseAddress("test.source"), MustParseAddress("test.dest"))

	err := core.RouteAndDispatch(ctx, NewUpStartFrame(fullId, []string{}, make(map[string][]byte)), chandler)
	if err == nil {
		t.Fatalf("Call route should have failed with error\n")
	}

	err = ccall.UnaryClientCall1()
	if err == nil {
		t.Fatalf("Call should have failed with error\n")
	}

	fmt.Printf("memm %d\n", core.MemMan().Used())
}

func Benchmark1CoreUnaryCall(b *testing.B) {
	LogLevel = LOff
	core := NewCore(NewAddress("test"), -1)
	ctx := context.Background()
	sch := NewServerCallHandler(12, "", ctx, core.MemMan(), UnaryServerCall1)
	dest := NewAddress("test", "dest")
	core.Router().DestinationUpdate(dest, sch, Metric{10})

	for n := 0; n < b.N; n++ {
		ccall, chandler := NewClientCall(10, "", ctx, core.MemMan())
		core.StartRPC(ctx, dest, []string{}, make(map[string][]byte), chandler)
		err := ccall.UnaryClientCall1()
		if err != nil {
			b.Fatalf("Call finished with error: %v\n", err)
		}
	}
}

func Benchmark2CoresUnaryCall(b *testing.B) {
	LogLevel = LOff
	ctx := context.Background()
	// Two cores
	core1 := NewCore(NewAddress("test"), -1)
	core2 := NewCore(NewAddress("test"), -1)
	// Core 2 has an address online
	r := NewServerCallHandler(12, "", ctx, core2.MemMan(), UnaryServerCall1)
	dest := NewAddress("test", "dest")
	core2.Router().DestinationUpdate(dest, r, Metric{10})
	// lets link core1 with 2
	l12, l21 := newCoreLink(core1, core2)
	core1.Router().Add(l12)
	core2.Router().Add(l21)

	for n := 0; n < b.N; n++ {
		ccall, chandler := NewClientCall(10, "", ctx, core1.MemMan())
		core1.StartRPC(ctx, dest, []string{}, make(map[string][]byte), chandler)
		err := ccall.UnaryClientCall1()
		if err != nil {
			b.Fatalf("Call finished with error: %v\n", err)
		}
	}
}

func TestQueryAnnounce(t *testing.T) {
	//log.SetFormatter(&simpleFormatter{time.Now()})
	ctx, ctxCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ctxCancel()
	// Two cores
	core1 := NewCore(NewAddress("test"), -1)
	core2 := NewCore(NewAddress("test"), -1)
	// Core 2 has an address online
	scf := ServerCallFactory{core2.MemMan(), ctx, UnaryServerCall1}
	r := NewSingleRPCHandler(12, "test", scf.Init)
	name := NewAddress("test", "dest")
	core2.Router().DestinationUpdate(name, r, Metric{10})

	// Confirm that core1 does not have the address
	tcx, tcan := context.WithTimeout(ctx, 50*time.Millisecond)
	_, _, err := core1.QueryRoute(tcx, name, true)
	if err == nil {
		t.Fatalf("error expected but none returned")
	}
	tcan()
	// Core2 has it
	tcx, tcan = context.WithTimeout(ctx, 100*time.Millisecond)
	_, h, err := core2.QueryRoute(tcx, name, true)
	if h == nil || err != nil {
		t.Fatalf("unexpected error %s", err.Error())
	}
	tcan()
	// lets link core1 with 2
	l12, l21 := newCoreLink(core1, core2)
	core1.Router().Add(l12)
	core2.Router().Add(l21)

	// now the query should work
	tcx, tcan = context.WithTimeout(ctx, 100*time.Millisecond)
	_, h, err = core1.QueryRoute(tcx, name, true)
	if h == nil || err != nil {
		t.Fatalf("unexpected error %s", err.Error())
	}
	tcan()

	openqs := core1.Quanda().OpenQueryNames()
	if len(openqs) != 0 {
		t.Fatalf("unexpected open queries %v", openqs)
	}
	openqs = core2.Quanda().OpenQueryNames()
	if len(openqs) != 0 {
		t.Fatalf("unexpected open queries %v", openqs)
	}
}

func TestQueryTimeout(t *testing.T) {
	ctx, ctxCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ctxCancel()
	// Two cores
	core1 := NewCore(NewAddress("test"), -1)
	core2 := NewCore(NewAddress("test"), -1)
	// Core 2 has an address online
	scf := ServerCallFactory{core2.MemMan(), ctx, UnaryServerCall1}
	r := NewSingleRPCHandler(12, "test", scf.Init)
	name := NewAddress("test", "dest")
	core2.Router().DestinationUpdate(name, r, Metric{10})
	// lets link core1 with 2
	l12, l21 := newCoreLink(core1, core2)
	core1.Router().Add(l12)
	core2.Router().Add(l21)

	openqs := core1.Quanda().OpenQueryNames()
	if len(openqs) != 0 {
		t.Fatalf("unexpected open queries %v", openqs)
	}
	openqs = core2.Quanda().OpenQueryNames()
	if len(openqs) != 0 {
		t.Fatalf("unexpected open queries %v", openqs)
	}

	// lets see if an unsolvable query is cleaned up correctly
	tctx, tcan := context.WithTimeout(ctx, 50*time.Millisecond)
	defer tcan()
	go core1.QueryRoute(tctx, NewAddress("not", "exist"), true)
	time.Sleep(10 * time.Millisecond)
	// now the query should live at both cores
	openqs = core1.Quanda().OpenQueryNames()
	if len(openqs) != 1 {
		t.Fatalf("unexpected open queries %v", openqs)
	}
	openqs = core2.Quanda().OpenQueryNames()
	if len(openqs) != 1 {
		t.Fatalf("unexpected open queries %v", openqs)
	}

	time.Sleep(60 * time.Millisecond)
	// now the query should have expired at both cores
	openqs = core1.Quanda().OpenQueryNames()
	if len(openqs) != 0 {
		t.Fatalf("unexpected open queries %v", openqs)
	}
	openqs = core2.Quanda().OpenQueryNames()
	if len(openqs) != 0 {
		t.Fatalf("unexpected open queries %v", openqs)
	}
}

// A route that was found by query must be removed if the target does not have the name online anymore.
func TestRemoveQueriedRoute(t *testing.T) {
	//log.SetFormatter(&simpleFormatter{time.Now()})

	ctx, ctxCancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer ctxCancel()
	// Two cores
	core1 := NewCoreExt(NewAddress("test"), -1, "1")
	core2 := NewCoreExt(NewAddress("test"), -1, "2")
	// Core 2 has an address online
	r := NewServerCallHandler(12, "", ctx, core2.MemMan(), UnaryServerCall1)
	name := NewAddress("test", "dest")
	core2.Router().DestinationUpdate(name, r, Metric{10})

	// lets link core1 with 2
	l12, l21 := newCoreLink(core1, core2)
	core1.Router().Add(l12)
	core2.Router().Add(l21)

	// call should work
	ccall, chandler := NewClientCall(10, "", ctx, core1.MemMan())
	core1.StartRPC(ctx, name, []string{}, make(map[string][]byte), chandler)
	err := ccall.UnaryClientCall1()
	if err != nil {
		t.Fatalf("Call finished with error: %v\n", err)
	}
	_, id, _ := core1.Router().GetNearest(name)
	if id == nil {
		t.Fatalf("Handler should not be nil: %v\n", id)
	}

	// take the route offline
	core2.Router().DestinationOffline(name, r)

	time.Sleep(100 * time.Millisecond)

	_, id, _ = core1.Router().GetNearest(name)
	if id != nil {
		t.Fatalf("Handler should be removed from router: %v\n", id)
	}

	tctx, tcan := context.WithTimeout(ctx, 100*time.Millisecond)
	ccall, chandler = NewClientCall(10, "", tctx, core1.MemMan())
	err = core1.StartRPC(tctx, name, []string{}, make(map[string][]byte), chandler)
	if err == nil {
		t.Fatalf("Call should have failed with timeout: %v\n", err)
	}
	tcan()
	err = ccall.UnaryClientCall1()
	if err == nil {
		t.Fatalf("Call should finish with error: %v\n", err)
	}

	// take the route online again
	core2.Router().DestinationUpdate(name, r, Metric{10})

	// call should work again
	ccall, chandler = NewClientCall(10, "", ctx, core1.MemMan())
	core1.StartRPC(ctx, name, []string{}, make(map[string][]byte), chandler)
	err = ccall.UnaryClientCall1()
	if err != nil {
		t.Fatalf("Call finished with error: %v\n", err)
	}
	_, id, _ = core1.Router().GetNearest(name)
	if id == nil {
		t.Fatalf("Handler should not be nil: %v\n", id)
	}
}

// A queried route that was not used in an RPC for some time must be removed.
func TestQueryAnnounceTimeout(t *testing.T) {
	LogLevel = LInfo
	log.Default().SetFlags(log.Ltime | log.Lmicroseconds)
	ctx, ctxCancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer ctxCancel()
	// Two cores
	core1 := NewCoreExt(NewAddress("test"), -1, "1")
	core2 := NewCoreExt(NewAddress("test"), -1, "2")
	// Core 2 has an address online
	r := NewServerCallHandler(12, "", ctx, core2.MemMan(), UnaryServerCall1)
	name := NewAddress("test", "dest")
	core2.Router().DestinationUpdate(name, r, Metric{10})

	// lets link core1 with 2
	l12, l21 := newCoreLink(core1, core2)
	core1.Router().Add(l12)
	core2.Router().Add(l21)

	// First call triggers query and should start announce timeout
	t0 := time.Now()
	fmt.Printf("0 %s\n", t0.Format(timeLayout))
	ccall, chandler := NewClientCall(10, "", ctx, core1.MemMan())
	core1.StartRPC(ctx, name, []string{}, make(map[string][]byte), chandler)
	err := ccall.UnaryClientCall1()
	if err != nil {
		t.Fatalf("Call finished with error: %v\n", err)
	}
	_, id, _ := core1.Router().GetNearest(name)
	if id == nil {
		t.Fatalf("Handler should not be nil: %v\n", id)
	}

	// the announce should now have a deadline for t0+2*baseAnnounceTimeout
	// before timeout has passed route must still be there
	tt := t0.Add(baseAnnounceTimeout + 50*time.Millisecond)
	fmt.Printf("1 %s\n", tt.Format(timeLayout))
	core1.Quanda().sweepExpiredAnnounces(tt)
	core2.Quanda().sweepExpiredAnnounces(tt)
	_, id, _ = core1.Router().GetNearest(name)
	if id == nil {
		t.Fatalf("Handler should not be nil: %v\n", id)
	}

	tt = t0.Add(baseAnnounceTimeout + 100*time.Millisecond)
	fmt.Printf("2 %s\n", tt.Format(timeLayout))
	// Now lets fake a call in the extended timeout time, this should extend the deadline to t0+3*base
	core2.Quanda().onDestUsed(name, l21, tt)
	time.Sleep(50 * time.Millisecond)
	// And the routes should still be there within 3 base timeouts
	tt = time.Now().Add(3*baseAnnounceTimeout - 100*time.Millisecond)
	fmt.Printf("3 %s\n", tt.Format(timeLayout))
	core1.Quanda().sweepExpiredAnnounces(tt)
	core2.Quanda().sweepExpiredAnnounces(tt)
	_, id, _ = core1.Router().GetNearest(name)
	if id == nil {
		t.Fatalf("Handler should not be nil: %v\n", id)
	}

	tt = time.Now().Add(3*baseAnnounceTimeout + 100*time.Millisecond)
	fmt.Printf("4 %s\n", tt.Format(timeLayout))
	// But everything should be removed after 3 base timeouts
	core1.Quanda().sweepExpiredAnnounces(tt)
	core2.Quanda().sweepExpiredAnnounces(tt)
	_, id, _ = core1.Router().GetNearest(name)
	if id != nil {
		t.Fatalf("Handler should be removed from router: %v\n", id)
	}
}

func newCoreLink(core1 *Core, core2 *Core) (lh1to2 *LinkHandler, lh2to1 *LinkHandler) {
	lh1to2 = NewLinkHandler(0, "link1->2", nil, nil)
	lh2to1 = NewLinkHandler(0, "link2->1", nil, nil)
	lh1to2.send = func(lf *v1.LinkFrame) error {
		go func() {
			switch m := lf.Type.(type) {
			case *v1.LinkFrame_Announce:
				core2.Quanda().HandleAnnounce(NewAnnounceFrameFromProto(m.Announce), lh2to1)
			case *v1.LinkFrame_Query:
				core2.Quanda().HandleQuery(NewQueryFrameFromProto(m.Query), lh2to1)
			}
		}()
		return nil
	}
	lh1to2.initRPC = func(r *RPC, rcv FrameReceiver, snd FrameSender) {
		go func() {
			for {
				rpcf, err := rcv(context.Background())
				if err != nil {
					break
				}
				err = core2.RouteAndDispatch(context.Background(), rpcf, lh2to1)
				if err != nil {
					snd(context.Background(), NewDownFinishFrame(r.fullID.id, int(codes.Unavailable), err.Error(), []string{}))
				}
			}
		}()
	}
	lh2to1.send = func(lf *v1.LinkFrame) error {
		go func() {
			switch m := lf.Type.(type) {
			case *v1.LinkFrame_Announce:
				core1.Quanda().HandleAnnounce(NewAnnounceFrameFromProto(m.Announce), lh1to2)
			case *v1.LinkFrame_Query:
				core1.Quanda().HandleQuery(NewQueryFrameFromProto(m.Query), lh1to2)
			}
		}()
		return nil
	}
	lh2to1.initRPC = func(r *RPC, rcv FrameReceiver, fs FrameSender) {
		go func() {
			for {
				rpcf, err := rcv(context.Background())
				if err != nil {
					break
				}
				err = core1.RouteAndDispatch(context.Background(), rpcf, lh1to2)
				if err != nil {
					fs(context.Background(), NewDownFinishFrame(r.fullID.id, int(codes.Unavailable), err.Error(), []string{}))
				}
			}
		}()
	}
	return
}
