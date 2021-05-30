package rpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rektorphi/arpcnet/util"
	"google.golang.org/genproto/googleapis/rpc/code"
)

func TestRPCManagerMemAndRpcCleanup(t *testing.T) {
	rpcm := NewManager()

	memm := NewCondMemoryManager(1000)

	ctx, canfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer canfunc()

	var echan = make(chan error)
	testcall := func(i uint64) {
		ccall, clientHandler := NewClientCall(i*2, "", ctx, memm)
		rpc := New(RandomFullID(MustParseAddress("test.source"), MustParseAddress("test.dest")))
		rpc.SetDownstreamHandler(clientHandler)
		shandler := NewServerCallHandler(i*2+1, "", ctx, memm, UnaryServerCall1)
		rpc.SetUpstreamHandler(shandler)
		rpcm.Add(rpc)
		echan <- ccall.UnaryClientCall1()
	}
	for i := 0; i < 100; i++ {
		go testcall(uint64(i))
	}

	for i := 0; i < 100; i++ {
		res := <-echan
		if res != nil {
			t.Logf("call failed with %v", res)
			t.Fail()
		}
	}

	if memm.Used() != 0 {
		t.Logf("Mem tickets not at 0: %v", memm.Used())
		t.Fail()
	}

	rpcs := rpcm.GetRPCs()
	if len(rpcs) != 0 {
		t.Fatalf("Open RPCs: %d", len(rpcs))
	}
}

func TestRPCManagerEvents(t *testing.T) {
	rpcm := NewManager()
	memm := NewCondMemoryManager(1000)
	ctx, canfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer canfunc()

	echan := make(chan Frame, 100)
	rpcm.AddListener(func(rpc *RPC, frame Frame) { echan <- frame })

	var dchan = make(chan error)
	testcall := func(i uint64) {
		fullId := RandomFullID(MustParseAddress("test.source"), MustParseAddress("test.dest"))
		ccall, clientHandler := NewClientCall(i*2, "", ctx, memm)
		startm := NewUpStartFrame(fullId, []string{}, make(map[string][]byte))
		rpc := New(fullId)
		rpc.SetDownstreamHandler(clientHandler)
		shandler := NewServerCallHandler(i*2+1, "", ctx, memm, UnaryServerCall1)
		rpc.SetUpstreamHandler(shandler)
		rpcm.Add(rpc)
		rpc.sendUpstream(ctx, startm)
		dchan <- ccall.UnaryClientCall1()
	}
	for i := 0; i < 1; i++ {
		go testcall(uint64(i))
	}
	for i := 0; i < 1; i++ {
		res := <-dchan
		if res != nil {
			t.Logf("call failed with %v", res)
			t.Fail()
		}
	}

	e := <-echan
	if _, ok := e.(UpStartFrame); !ok {
		t.Fatalf("unexpected event %v", e)
	}
	e = <-echan
	if _, ok := e.(UpDataFrame); !ok {
		t.Fatalf("unexpected event %v", e)
	}
	e = <-echan
	if _, ok := e.(UpCloseFrame); !ok {
		t.Fatalf("unexpected event %v", e)
	}
	e = <-echan
	if e, ok := e.(DownResponseFrame); !ok {
		t.Fatalf("unexpected event %v", e)
	}
	e = <-echan
	if e, ok := e.(DownDataFrame); !ok {
		t.Fatalf("unexpected event %v", e)
	}
	e = <-echan
	if e, ok := e.(DownFinishFrame); !ok {
		t.Fatalf("unexpected event %v", e)
	}

	if memm.Used() != 0 {
		t.Logf("Mem tickets not at 0: %v", memm.Used())
		t.Fail()
	}
	rpcs := rpcm.GetRPCs()
	if len(rpcs) != 0 {
		t.Fatalf("Open RPCs: %d", len(rpcs))
	}
}

func TestRPCManagerLinked(t *testing.T) {
	rpcm1 := NewManager()
	rpcm2 := NewManager()
	memm := NewCondMemoryManager(1000)
	ctx, canfunc := context.WithTimeout(context.Background(), 2*time.Second)
	defer canfunc()

	ccall, clientHandler := NewClientCall(1, "", ctx, memm)

	rpc1 := New(RandomFullID(MustParseAddress("test.source"), MustParseAddress("test.dest")))
	rpc1.SetDownstreamHandler(clientHandler)
	bh1 := BridgeHandler{ctx, 7, rpcm2, true}
	rpc1.SetUpstreamHandler(&bh1)
	rpcm1.Add(rpc1)

	rpc2 := New(rpc1.FullID())
	bh2 := BridgeHandler{ctx, 7, rpcm1, false}
	rpc2.SetDownstreamHandler(&bh2)
	shandler := NewServerCallHandler(2, "", ctx, memm, UnaryServerCall1)
	rpc2.SetUpstreamHandler(shandler)
	rpcm2.Add(rpc2)

	err := ccall.UnaryClientCall1()

	rpcs := rpcm1.GetRPCs()
	if len(rpcs) != 0 {
		t.Fatalf("Open RPCs: %d", len(rpcs))
	}
	rpcs = rpcm2.GetRPCs()
	if len(rpcs) != 0 {
		t.Fatalf("Open RPCs: %d", len(rpcs))
	}
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestRouteHandlerGoingOffline(t *testing.T) {
	// 2 RPCManagers are connected via a bridge: ClientCall - RPCM1 - Bridge - RPCM2 - ServerCall
	rpcm1 := NewManager()
	rpcm2 := NewManager()
	memm := NewCondMemoryManager(1000)
	ctx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	ctx2, cancelCtx2 := context.WithCancel(ctx)
	defer cancelCtx()

	ccall, clientHandler := NewClientCall(1, "", ctx, memm)

	rpc1 := New(RandomFullID(MustParseAddress("test.source"), MustParseAddress("test.dest")))
	rpc1.SetDownstreamHandler(clientHandler)
	bh1 := BridgeHandler{ctx2, 7, rpcm2, true}
	rpc1.SetUpstreamHandler(&bh1)
	rpcm1.Add(rpc1)

	rpc2 := New(rpc1.FullID())
	bh2 := BridgeHandler{ctx2, 7, rpcm1, false}
	rpc2.SetDownstreamHandler(&bh2)
	shandler := NewServerCallHandler(2, "", ctx, memm, UnaryServerCall1)
	rpc2.SetUpstreamHandler(shandler)
	rpcm2.Add(rpc2)

	// Now the bridge is taken offline
	cancelCtx2()
	// Lets run the call
	err := ccall.UnaryClientCall1()

	time.Sleep(100 * time.Millisecond)
	// The call must fail gracefully and get cleaned up
	rpcs := rpcm1.GetRPCs()
	if len(rpcs) != 0 {
		t.Fatalf("Open RPCs: %d", len(rpcs))
	}
	rpcs = rpcm2.GetRPCs()
	if len(rpcs) != 0 {
		t.Fatalf("Open RPCs: %d", len(rpcs))
	}
	if _, ok := err.(FinishError); !ok {
		t.Fatalf("unexpected error %v", err)
	}
}

type BridgeHandler struct {
	ctx  context.Context
	id   uint64
	dest *Manager
	up   bool
}

func (bh *BridgeHandler) ID() uint64 {
	return bh.id
}

func (bh *BridgeHandler) String() string {
	return util.B32.ToInfoStr(bh.id)
}

func (bh *BridgeHandler) Init(rpc *RPC, rcv FrameReceiver, send FrameSender) {
	go func() {
		for {
			frame, err := rcv(bh.ctx)
			if err != nil {
				fmt.Printf("bh %d fin: %v\n", bh.id, err)
				goto bail
			}
			err = bh.dest.Dispatch(bh.ctx, frame, bh.id)
			if err != nil {
				fmt.Printf("bh %d dispatch error: %v\n", bh.id, err)
				goto bail
			}
		}

	bail:
		if bh.up {
			send(context.Background(), NewDownFinishFrame(rpc.FullID().ID(), int(code.Code_ABORTED), "connection lost", []string{}))
		} else {
			send(context.Background(), NewUpCancelFrame(rpc.FullID().ID(), "connection lost"))
		}
	}()
}
