package arpcnet

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/rektorphi/arpcnet/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

var addrGrpc rpc.Address = *rpc.NewAddress("grpc")

// GRPCAddrKey is the gRPC metadata key under which the destination ArpcNet address is stored.
const GRPCAddrKey = "arpcnet-addr"

// GRPCServer is a rpc.Core module for gRPCs entering the Arpc network.
// Arbitrary gRPC calls can be handled by this gRPC server and are routed via an rpc.Core.
type GRPCServer struct {
	identity uint64
	core     *rpc.Core
	port     int
	server   *grpc.Server
}

// NewGRPCServer creates a new GRPCServer instance and associates it to the given rpc.Core.
func NewGRPCServer(core *rpc.Core, port int) *GRPCServer {
	res := &GRPCServer{rpc.RandHandlerID(), core, port, nil}
	res.server = grpc.NewServer(grpc.CustomCodec(&rawCodec{}), grpc.UnknownServiceHandler(res.unknownServiceHandler))
	return res
}

// Serve blocks and must be called to operate the gRPC server.
func (gs *GRPCServer) Serve() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", gs.port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	gs.server.Serve(lis)
}

// Stop terminates operation of the gRPC server.
func (gs *GRPCServer) Stop() {
	gs.server.Stop()
}

type rawCodec struct {
}

type rawMessage struct {
	data []byte
}

func (c *rawCodec) Marshal(v interface{}) ([]byte, error) {
	out, ok := v.(*rawMessage)
	if !ok {
		return nil, fmt.Errorf("unexpected message type in marshal")
	}
	return out.data, nil

}

func (c *rawCodec) Unmarshal(data []byte, v interface{}) error {
	dst, ok := v.(*rawMessage)
	if !ok {
		return fmt.Errorf("unexpected message type in unmarshal")
	}
	dst.data = data
	return nil
}

func (c *rawCodec) String() string {
	return "rawcodec"
}

func (gs *GRPCServer) unknownServiceHandler(srv interface{}, serverStream grpc.ServerStream) error {
	md, ok := metadata.FromIncomingContext(serverStream.Context())
	if !ok {
		md = metadata.New(make(map[string]string))
	}
	remoteName := "grpc-unknown"
	peer, ok := peer.FromContext(serverStream.Context())
	if ok {
		remoteName = "grpc-" + peer.Addr.String()
	}
	method, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return fmt.Errorf("no method name in ServerStream")
	}
	methodParts, err := SplitFullMethodName(method)
	if err != nil {
		return err
	}
	//log.Printf("call to %s MD: %v\n", methodParts.String(), md)
	baseAddr := rpc.AddrLocal()
	addrVs := md.Get(GRPCAddrKey)
	if len(addrVs) > 0 {
		baseAddr, err = rpc.ParseAddress(addrVs[0])
		if err != nil {
			return err
		}
	}

	rpcCtx, ctxCancel := context.WithCancel(serverStream.Context())
	ccall, clientHandler := rpc.NewClientCall(gs.identity, remoteName, serverStream.Context(), gs.core.MemMan())
	err = gs.core.StartRPC(rpcCtx, baseAddr.Append(&addrGrpc).Appends(methodParts...), []string{}, make(map[string][]byte), clientHandler)
	if err != nil {
		ctxCancel()
		return err
	}
	arpcSendErrChan := make(chan error)
	arpcRcvErrChan := make(chan error)
	go func() {
		for {
			m := rawMessage{}
			err := serverStream.RecvMsg(&m)
			// grpc call is either finished io.EOF or other error, possibly cancelled
			if err == io.EOF {
				ccall.CloseSend()
				arpcSendErrChan <- nil
				return
			} else if err != nil {
				ccall.Cancel(err.Error())
				arpcSendErrChan <- err
				return
			}
			err = ccall.Send(m.data)
			if err != nil {
				ctxCancel()
				arpcSendErrChan <- err
				return
			}
		}
	}()
	go func() {
		for {
			data, err := ccall.Receive()
			// arpcnet call could be finished with FinishError or cancelled
			if err != nil {
				arpcRcvErrChan <- err
				return
			}
			m := rawMessage{data}
			err = serverStream.SendMsg(&m)
			if err == io.EOF {
				ccall.Cancel("unknown eof")
				return
			} else if err != nil {
				ccall.Cancel(err.Error())
				arpcRcvErrChan <- err
				return
			}
		}
	}()

	// with loop, cannot cancel the channels
	for i := 0; i < 2; i++ {
		select {
		case err := <-arpcSendErrChan:
			if err == nil {
				break
			}
			return err
		case err := <-arpcRcvErrChan:
			if finErr, ok := err.(rpc.FinishError); ok {
				return status.Error(codes.Code(finErr.StatusCode()), finErr.Message())
			}
			return err
		}
	}
	ctxCancel()
	return status.Errorf(codes.Internal, "should not get here")
}

// SplitFullMethodName splits the full name of a gRPC method, including proto package, service name and method name, into their individual name components.
func SplitFullMethodName(fullMethodName string) ([]string, error) {
	// First split off method name
	var tokens []string
	if fullMethodName[0] == '/' {
		tokens = strings.Split(fullMethodName[1:], "/")
	} else {
		tokens = strings.Split(fullMethodName, "/")
	}
	// then split package parts
	packageParts := strings.Split(tokens[0], ".")
	if len(tokens) >= 2 {
		return append(packageParts, tokens[1]), nil
	}
	return packageParts, nil
}

// ToFullMethodName converts an rpc.Address into a gRPC full method name.
// Note that this should be a suffix from a global ArpcNet address and thus the actual gRPC method part must be sliced off before using this function.
func ToFullMethodName(addr *rpc.Address) string {
	if addr.Len() == 0 {
		return ""
	}
	t := string(addr.Get(0))
	if addr.Len() == 1 {
		return t
	}
	for i := 1; i < addr.Len()-1; i++ {
		t = t + "." + string(addr.Get(i))
	}
	return t + "/" + string(addr.Get(addr.Len()-1))
}
