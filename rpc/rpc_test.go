package rpc

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func UnaryServerCall1(scall *ServerCall) {
	data, err := scall.Receive()
	if err != nil {
		fmt.Printf("Data from client expected but got error %v\n", err)
		scall.Finish()
	}
	scall.Send(data)
	_, err = scall.Receive()
	if _, ok := err.(HalfCloseError); !ok {
		fmt.Printf("HalfClose expected but got %v\n", err)
	}
	scall.Finish()
}

func (ccall *ClientCall) UnaryClientCall1() error {
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	ccall.Send(data)
	ccall.CloseSend()
	rdata, err := ccall.Receive()
	if err != nil {
		ccall.Cancel("cancel")
		fmt.Printf("Data from server expected but got error: %v\n", err)
		return err
	}
	if len(rdata) != len(data) {
		fmt.Printf("Data response has not the expected length\n")
	}
	_, err = ccall.Receive()
	if _, ok := err.(FinishError); !ok {
		fmt.Printf("Data finish expected but not received\n")
		ccall.Cancel("cancel")
		return err
	}
	return nil
}

func TestSimpleUnaryRPC(t *testing.T) {

	ctx, canfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer canfunc()

	memm := NewCondMemoryManager(10000000)

	// Create a downstream handler
	ccall, clientHandler := NewClientCall(1, "", ctx, memm)
	// normally, we would send the start message to create the rpc and be linked up
	fullId := RandomFullID(MustParseAddress("test.source"), MustParseAddress("test.dest"))
	// Since we link everything manually up, dont need start frame
	// NewUpStartFrame(fullId, []string{}, make(map[string][]byte))
	// Create a rpc
	rpc := New(fullId)
	rpc.SetDownstreamHandler(clientHandler)
	// Attach an upstream handler
	serverHandler := NewServerCallHandler(2, "", ctx, memm, UnaryServerCall1)
	rpc.SetUpstreamHandler(serverHandler)
	// Execute a unary call
	err := ccall.UnaryClientCall1()
	if err != nil {
		t.Fatalf("Call finished with error: %v\n", err)
	}

	if rpc.status != 0 {
		t.Fatalf("Call finished with unexpected state: %d\n", rpc.status)
	}

	fmt.Printf("memm %d\n", memm.Used())
}
