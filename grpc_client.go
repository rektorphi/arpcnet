package arpcnet

import (
	"context"
	"io"

	"github.com/rektorphi/arpcnet/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// GRPCClient is an RPC exit module of a Node that relays RPCs as gRPC calls to an actual gRPC server.
type GRPCClient struct {
	conn           *grpc.ClientConn
	route          *rpc.MultiRPCHandler
	mountPrefixLen int
}

// NewGRPCClient creates a new instance of a GRPCClient and associates it with the given core.
// The mountPrefixLen parameter is used to split the actual gRPC full method name from the AprcNet address
// and must be set to the length of address where this route handler is mounted at the core.
func NewGRPCClient(core *rpc.Core, mountPrefixLen int, target string, insecure bool) *GRPCClient {
	var conn *grpc.ClientConn
	var err error
	if insecure {
		conn, err = grpc.Dial(target, grpc.WithInsecure())
	} else {
		conn, err = grpc.Dial(target)
	}
	if err != nil {
		panic(err)
	}
	gc := &GRPCClient{conn, nil, mountPrefixLen}
	gc.route = rpc.NewServerCallHandler(0, "grpc://"+target, context.Background(), core.MemMan(), gc.handleServerCall)
	return gc
}

// Close terminates operation of this client.
func (gc *GRPCClient) Close() {
	gc.route.Close()
	gc.conn.Close()
}

// Handler returns the rpc.Handler interface of this client instance for hooking it into a Core.
func (gc *GRPCClient) Handler() rpc.Handler {
	return gc.route
}

var clientStreamDesc = &grpc.StreamDesc{
	ServerStreams: true,
	ClientStreams: true,
}

func (gc *GRPCClient) handleServerCall(serverCall *rpc.ServerCall) {
	clientCtx, cancelClient := context.WithCancel(serverCall.Ctx())
	fullMethodNameAddr := serverCall.RPC().FullID().Dest().Slice(gc.mountPrefixLen, -1)
	clientStream, err := grpc.NewClientStream(clientCtx, clientStreamDesc, gc.conn, ToFullMethodName(fullMethodNameAddr), grpc.CallCustomCodec(&rawCodec{}))

	if err != nil {
		go serverCall.FinishExt(codes.Internal, err.Error(), []string{})
		cancelClient()
		return
	}

	go func() {
		for {
			data, err := serverCall.Receive()
			if _, ok := err.(rpc.HalfCloseError); ok {
				clientStream.CloseSend()
				return
			} else if err != nil {
				cancelClient()
				return
			}
			m := rawMessage{data}
			err = clientStream.SendMsg(&m)
			if err == io.EOF {
				serverCall.Finish()
				return
			} else if err != nil {
				// TODO finish ext code
				serverCall.Finish()
				return
			}
		}
	}()
	go func() {
		defer cancelClient()
		for {
			m := rawMessage{}
			err := clientStream.RecvMsg(&m)
			if err == io.EOF {
				serverCall.Finish()
				return
			} else if err != nil {
				// TODO finish ext code
				serverCall.Finish()
				return
			}
			err = serverCall.Send(m.data)
			if err != nil {
				return
			}
		}
	}()
}
