package rpc

import (
	"context"
	"math/rand"
	"sync"

	pb "github.com/rektorphi/arpcnet/generated/rektorphi/arpcnet/v1"
	"github.com/rektorphi/arpcnet/util"
)

type Identifiable interface {
	// Must be constant and sufficiently unique for security purposes.
	ID() uint64
	String() string
}

type InitFunc func(*RPC, FrameReceiver, FrameSender)
type FrameReceiver func(context.Context) (Frame, error)
type FrameSender func(context.Context, Frame) error

// Handler handles one direction of the communication for an RPC.
// An RPC has 2 Handler, one for upstream (to the server) and one downstream (to the client).
type Handler interface {
	Identifiable
	Init(*RPC, FrameReceiver, FrameSender)
}

// SingleRPCHandler is an implementation of Handler for a single RPC.
type SingleRPCHandler struct {
	id         uint64
	infoString string
	initRPC    InitFunc
	rpc        *RPC
}

func NewSingleRPCHandler(id uint64, infoString string, initRPC InitFunc) *SingleRPCHandler {
	if id == 0 {
		id = RandHandlerID()
	}
	if len(infoString) == 0 {
		infoString = "Handler" + util.B32.ToInfoStr(id)
	}
	return &SingleRPCHandler{id, infoString, initRPC, nil}
}

// NOT THREAD SAFE
func (r *SingleRPCHandler) Close() {
	if r.rpc != nil && !r.rpc.Finished() {
		r.rpc.Abort("handler closed")
	}
}

// ID implements Identifiable.ID
func (r *SingleRPCHandler) ID() uint64 {
	return r.id
}

// String implements Identifiable.String
func (r *SingleRPCHandler) String() string {
	return r.infoString
}

// Init implements Handler.Init.
// NOT THREAD SAFE
func (r *SingleRPCHandler) Init(rpc *RPC, rcv FrameReceiver, snd FrameSender) {
	if r.rpc != nil && !r.rpc.Finished() {
		r.rpc.Abort("reusing SingleRPCHandler")
	}
	r.rpc = rpc
	r.initRPC(rpc, rcv, snd)
}

// MultiRPCHandler is an implementation of Handler that tracks open RPCs and aborts them when the handler is closed.
type MultiRPCHandler struct {
	SingleRPCHandler
	rpcs []*RPC
}

func NewMultiRPCHandler(id uint64, infoString string, initRPC InitFunc) *MultiRPCHandler {
	return &MultiRPCHandler{*NewSingleRPCHandler(id, infoString, initRPC), make([]*RPC, 0, 256)}
}

// Close sends a signal to all active RPCs over this route to abort and push a finish frame.
// NOT THREAD SAFE
func (r *MultiRPCHandler) Close() {
	compactedSize := util.Compact(compactHelper{r.rpcs})
	r.rpcs = r.rpcs[:compactedSize]
	for _, rpc := range r.rpcs {
		rpc.Abort("handler closed")
	}
	r.rpcs = r.rpcs[:0]
}

// Init implements Handler.Init.
// NOT THREAD SAFE
func (r *MultiRPCHandler) Init(rpc *RPC, rcv FrameReceiver, snd FrameSender) {
	// Whenever we reach the capacity of the rpcs array, we clean up finished rpcs
	// and increase size only if remaining elements occupy more than half the slice.
	if len(r.rpcs) == cap(r.rpcs) {
		compactedSize := util.Compact(compactHelper{r.rpcs})
		if cap(r.rpcs) > 2*compactedSize {
			r.rpcs = r.rpcs[:compactedSize]
		} else {
			t := make([]*RPC, 0, cap(r.rpcs)*2)
			copy(t, r.rpcs[:compactedSize])
			r.rpcs = t
		}
	}
	r.rpcs = append(r.rpcs, rpc)
	r.initRPC(rpc, rcv, snd)
}

type compactHelper struct {
	elements []*RPC
}

func (ch compactHelper) Len() int {
	return len(ch.elements)
}

func (ch compactHelper) Remove(i int) bool {
	return ch.elements[i].Finished()
}

func (ch compactHelper) Swap(i, j int) {
	t := ch.elements[i]
	ch.elements[i] = ch.elements[j]
	ch.elements[j] = t
}

// implements Handler
type LinkHandler struct {
	MultiRPCHandler
	send  func(*pb.LinkFrame) error
	sendm sync.Mutex
}

func (lh *LinkHandler) Send(lf *pb.LinkFrame) error {
	lh.sendm.Lock()
	err := lh.send(lf)
	lh.sendm.Unlock()
	return err
}

func NewLinkHandler(id uint64, infoString string, initRPC InitFunc, send func(*pb.LinkFrame) error) *LinkHandler {
	return &LinkHandler{*NewMultiRPCHandler(id, infoString, initRPC), send, sync.Mutex{}}
}

var HANDLER_UNUSED_ID uint64 = 0

// RandHandlerID returns a random uint64 that excludes some reserved numbers.
func RandHandlerID() uint64 {
	for {
		k := rand.Uint64()
		if k > 99 {
			return k
		}
	}
}
