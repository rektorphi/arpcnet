package rpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
)

type RPCNotStartedError struct {
}

func (t RPCNotStartedError) Error() string {
	return "RPC not started"
}

type FinishError struct {
	fin DownFinishFrame
}

func (t FinishError) Error() string {
	return fmt.Sprintf("Call finished %d %s", t.fin.Status(), t.fin.Message())
}

func (t FinishError) StatusCode() int {
	return t.fin.Status()
}

func (t FinishError) Message() string {
	return t.fin.Message()
}

func (t FinishError) Metadata() []string {
	return t.fin.Metadata()
}

type HalfCloseError struct {
}

func (t HalfCloseError) Error() string {
	return "Client send closed"
}

type CancelError struct {
	Message string
}

func (t CancelError) Error() string {
	return fmt.Sprintf("Call canceled %s", t.Message)
}

type Call struct {
	ctx     context.Context
	memm    MemoryManager
	rpc     *RPC
	receive FrameReceiver
	send    FrameSender
}

func (call *Call) RPC() *RPC {
	return call.rpc
}

func (call *Call) Ctx() context.Context {
	return call.ctx
}

type ClientCall struct {
	Call
	respMeta []string
}

func (c *ClientCall) Init(rpc *RPC, rcv FrameReceiver, snd FrameSender) {
	c.rpc = rpc
	c.receive = rcv
	c.send = snd
}

func (c *ClientCall) Send(data []byte) error {
	if c.rpc == nil {
		return RPCNotStartedError{}
	}
	ticket := c.memm.Acquire(len(data))
	frame := NewUpDataFrame(c.rpc.fullID.id, &Chunk{data, 0, ticket})

	return c.send(c.ctx, frame)
}

func (c *ClientCall) Receive() ([]byte, error) {
	if c.rpc == nil {
		return nil, RPCNotStartedError{}
	}
	var buf []byte = nil
	for {
		frame, err := c.receive(c.ctx)
		if err != nil {
			return nil, err
		}
		switch n := frame.(type) {
		case DownDataFrame:
			res, err := assembleChunk(&buf, n.Chunk())
			if res != nil {
				return res, nil
			}
			if err != nil {
				return nil, err
			}
		case DownFinishFrame:
			return nil, FinishError{n}
		case DownResponseFrame:
			c.respMeta = n.Metadata()
		}
	}
}

func (c *ClientCall) CloseSend() {
	if c.rpc == nil {
		return
	}
	msg := NewUpCloseFrame(c.rpc.fullID.id)
	c.send(c.ctx, msg)
}

func (c *ClientCall) Cancel(message string) {
	if c.rpc == nil {
		return
	}
	msg := NewUpCancelFrame(c.rpc.fullID.id, message)
	c.send(c.ctx, msg)
}

type ServerCall struct {
	Call
	reqMeta []string
}

func (c *ServerCall) Send(data []byte) error {
	if c.rpc == nil {
		return RPCNotStartedError{}
	}
	ticket := c.memm.Acquire(len(data))
	frame := NewDownDataFrame(c.rpc.fullID.id, &Chunk{data, 0, ticket})
	return c.send(c.ctx, frame)
}

func (c *ServerCall) Receive() ([]byte, error) {
	if c.rpc == nil {
		return nil, RPCNotStartedError{}
	}
	var buf []byte = nil
	for {
		frame, err := c.receive(c.ctx)
		if err != nil {
			return nil, err
		}
		switch n := frame.(type) {
		case UpStartFrame:
			c.reqMeta = n.Metadata()
			c.send(c.ctx, NewDownResponseFrame(c.rpc.fullID.id, []string{}))
		case UpDataFrame:
			res, err := assembleChunk(&buf, n.Chunk())
			if res != nil {
				return res, nil
			}
			if err != nil {
				return nil, err
			}
		case UpCancelFrame:
			return nil, CancelError{n.Message()}
		case UpCloseFrame:
			return nil, HalfCloseError{}
		}
	}
}

func assembleChunk(bufp *[]byte, chunk *Chunk) ([]byte, error) {
	defer chunk.ticket.Release()
	if *bufp == nil {
		if chunk.Remaining == 0 {
			return chunk.Data, nil
		}
		// TODO lenght check and fail
		*bufp = make([]byte, 0, len(chunk.Data)+int(chunk.Remaining))
		*bufp = append(*bufp, chunk.Data...)
	} else {
		*bufp = append(*bufp, chunk.Data...)
		if chunk.Remaining == 0 {
			res := *bufp
			*bufp = nil
			return res, nil
		}
	}
	return nil, nil
}

func (c *ServerCall) Finish() {
	if c.rpc == nil {
		return
	}
	msg := NewDownFinishFrame(c.rpc.fullID.id, int(codes.OK), "", []string{})
	c.send(c.ctx, msg)
}

func (c *ServerCall) FinishExt(code codes.Code, message string, md []string) {
	if c.rpc == nil {
		return
	}
	msg := NewDownFinishFrame(c.rpc.fullID.id, int(code), message, md)
	c.send(c.ctx, msg)
}

type ServerCallFactory struct {
	memm              MemoryManager
	ctx               context.Context
	serverCallHandler func(serverCall *ServerCall)
}

func (scf *ServerCallFactory) Init(rpc *RPC, rcv FrameReceiver, snd FrameSender) {
	call := &ServerCall{Call{scf.ctx, scf.memm, rpc, rcv, snd}, nil}
	go scf.serverCallHandler(call)
}

func NewServerCallHandler(identity uint64, infoStr string, ctx context.Context, memm MemoryManager, serverCallHandler func(serverCall *ServerCall)) *MultiRPCHandler {
	serverFac := ServerCallFactory{memm, ctx, serverCallHandler}
	return NewMultiRPCHandler(identity, infoStr, serverFac.Init)
}

func NewClientCall(identity uint64, infoStr string, ctx context.Context, memm MemoryManager) (*ClientCall, Handler) {
	call := ClientCall{Call{ctx, memm, nil, nil, nil}, nil}
	return &call, NewSingleRPCHandler(identity, infoStr, call.Init)
}
