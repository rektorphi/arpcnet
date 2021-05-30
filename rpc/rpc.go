package rpc

import (
	"context"
	"sync/atomic"

	"github.com/rektorphi/arpcnet/util"
	"google.golang.org/genproto/googleapis/rpc/code"
)

// RPC is the local handler for an end-to-end bidirectional communication channel.
// 1. ClientCaller creates RPC
// 2. ClientCaller has downstream handler, sets it to RPC
// 3. Someone looks up upstream handler and sets it to RPC
// 4. Use RPC, send messages, start with Start
// 5. ClientCaller close the RPC to close the channels
type RPC struct {
	fullID *FullID
	// Messages to server.
	upstream chan Frame
	// Messages to client.
	downstream chan Frame

	upstreamHandler   Handler
	downstreamHandler Handler

	cbs        []FrameCallback
	status     uint32
	endMessage string
	log        util.Logger
}

type FrameCallback func(rpc *RPC, frame Frame)

const (
	active uint32 = 0x80
)

func New(ID *FullID) *RPC {
	return &RPC{
		fullID:     ID,
		upstream:   make(chan Frame, 4),
		downstream: make(chan Frame, 4),
		status:     active,
		endMessage: "",
		log:        util.NopLogger{},
	}
}

func (r *RPC) FullID() *FullID {
	return r.fullID
}

func (r *RPC) Log() util.Logger {
	return r.log
}

// Not thread safe, must be done before messaging is started.
func (rpc *RPC) addFrameCB(cb FrameCallback) {
	rpc.cbs = append(rpc.cbs, cb)
}

// Abort stops this RPC externally by sending cancel and finish frames in both directions of the RPC
func (rpc *RPC) Abort(msg string) {
	if atomic.CompareAndSwapUint32(&rpc.status, active, uint32(code.Code_ABORTED)) {
		rpc.endMessage = msg
		// to notify the callback of a finish message
		rpc.sendDownstream(context.Background(), NewDownFinishFrame(rpc.fullID.id, int(code.Code_ABORTED), msg, []string{}))
		// directly into the channel to bypass the callback
		rpc.upstream <- NewUpCancelFrame(rpc.fullID.id, msg)
		close(rpc.upstream)
		close(rpc.downstream)
	}
}

// Here we gracefully set the RPC to close due to message flow cancel/finish
func (rpc *RPC) internalClose(status int, msg string) {
	if atomic.CompareAndSwapUint32(&rpc.status, active, uint32(status)) {
		rpc.endMessage = msg
		close(rpc.upstream)
		close(rpc.downstream)
	}
}

// Finished returns true when the call has finished, either due to a server Finished-frame or a client Cancel-frame.
func (rpc *RPC) Finished() bool {
	return (atomic.LoadUint32(&rpc.status) & 0x80) == 0
}

func (rpc *RPC) Status() (uint32, string) {
	return rpc.status, rpc.endMessage
}

type ClosedError struct{}

func (r ClosedError) Error() string {
	return "RPC closed"
}

func (rpc *RPC) rcvUpstream(ctx context.Context) (Frame, error) {
	select {
	case m, ok := <-rpc.upstream:
		if !ok {
			return nil, ClosedError{}
		}
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (rpc *RPC) rcvDownstream(ctx context.Context) (Frame, error) {
	select {
	case m, ok := <-rpc.downstream:
		if !ok {
			return nil, ClosedError{}
		}
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (rpc *RPC) sendUpstream(ctx context.Context, f Frame) error {
	var reterr error = nil
	defer func() {
		if recover() != nil {
			reterr = ClosedError{}
		}
	}()
	select {
	case rpc.upstream <- f:
	case <-ctx.Done():
		return ctx.Err()
	}
	cf, ok := f.(UpCancelFrame)
	if ok {
		rpc.internalClose(1, cf.Message())
	}
	for _, cb := range rpc.cbs {
		cb(rpc, f)
	}
	return reterr
}

func (rpc *RPC) sendDownstream(ctx context.Context, f Frame) error {
	var reterr error = nil
	defer func() {
		if recover() != nil {
			reterr = ClosedError{}
		}
	}()
	select {
	case rpc.downstream <- f:
	case <-ctx.Done():
		return ctx.Err()
	}
	ff, ok := f.(DownFinishFrame)
	if ok {
		rpc.internalClose(ff.Status(), ff.Message())
	}
	for _, cb := range rpc.cbs {
		cb(rpc, f)
	}
	return reterr
}

// SetUpstreamHandler sets the handler on the server side of the connection, the receiver.
func (rpc *RPC) SetUpstreamHandler(handler Handler) {
	rpc.upstreamHandler = handler
	handler.Init(rpc, rpc.rcvUpstream, rpc.sendDownstream)
}

// SetDownstreamHandler sets the handler of the client side of the connection, the initiator.
func (rpc *RPC) SetDownstreamHandler(handler Handler) {
	rpc.downstreamHandler = handler
	handler.Init(rpc, rpc.rcvDownstream, rpc.sendUpstream)
}
