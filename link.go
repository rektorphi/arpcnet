package arpcnet

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/rektorphi/arpcnet/generated/rektorphi/arpcnet/v1"
	"github.com/rektorphi/arpcnet/rpc"
	"github.com/rektorphi/arpcnet/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// LinkTransport describes a bidirectional communication channel for LinkFrames to another Arpc node.
type LinkTransport interface {
	Send(*pb.LinkFrame) error
	Recv() (*pb.LinkFrame, error)
}

// Link is a communication channel between a local core and a remote core.
// RPC frames and routing queries are transported over this channel.
type Link struct {
	core      *rpc.Core
	transport LinkTransport
	sendm     sync.Mutex
	logString string
}

// NewLink creates a new instance of a Link and associates it with the given local core.
func NewLink(core *rpc.Core, linkt LinkTransport, logString string) (res *Link) {
	res = &Link{core, linkt, sync.Mutex{}, logString}
	return
}

func (lr *Link) String() string {
	return lr.logString
}

func (lr *Link) sendSafely(lf *pb.LinkFrame) (err error) {
	lr.sendm.Lock()
	err = lr.transport.Send(lf)
	lr.sendm.Unlock()
	return
}

// ReceiveAndDispatch uses and blocks the calling goroutine to receive and dispatch messages comming in over the link transport.
// The link is only established while this function is running. An error is returned when the link has failed.
func (lr *Link) ReceiveAndDispatch() (err error) {
tryAnotherId:
	route := rpc.NewLinkHandler(0, lr.logString, lr.initRPC, lr.sendSafely)
	if lr.core.Router().Add(route) != nil {
		goto tryAnotherId
	}
	for {
		var lf *pb.LinkFrame
		lf, err = lr.transport.Recv()
		if err != nil {
			break
		}
		switch m := lf.Type.(type) {
		case *pb.LinkFrame_RpcFrame:
			f := rpc.FrameFromProto(m.RpcFrame, lr.core.MemMan())
			err = lr.core.RouteAndDispatch(context.Background(), f, route)
			if err != nil {
				// First make sure to clear memory tickets since the message didnt make it through the core
				rpc.FrameToProtoClearTicket(f)
				// Then depending on the message, relay an abort message to the source so they can abort and clean up the rpc
				var fin rpc.Frame = nil
				if rpc.IsUpstream(f) {
					if f.Type() != rpc.UpCancel {
						fin = rpc.NewDownFinishFrame(f.ID(), int(codes.Internal), err.Error(), []string{})
					}
				} else {
					if f.Type() != rpc.DownFinish {
						fin = rpc.NewUpCancelFrame(f.ID(), err.Error())
					}
				}
				if fin != nil {
					lr.sendSafely(&pb.LinkFrame{Type: &pb.LinkFrame_RpcFrame{RpcFrame: fin.Proto()}})
				}
				//lr.core.Log.Printf("%s Error dispatching frame %s: %s\n", lr.String(), f.String(), err.Error())
			}
		case *pb.LinkFrame_Announce:
			lr.core.Quanda().HandleAnnounce(rpc.NewAnnounceFrameFromProto(m.Announce), route)
		case *pb.LinkFrame_Query:
			lr.core.Quanda().HandleQuery(rpc.NewQueryFrameFromProto(m.Query), route)
		}
	}
	route.Close()
	lr.core.Router().Remove(route)
	return err
}

func (lr *Link) initRPC(rrpc *rpc.RPC, rcv rpc.FrameReceiver, snd rpc.FrameSender) {
	go func() {
		for {
			rpcf, err := rcv(context.Background())
			if err != nil {
				break
			}
			lf := &pb.LinkFrame{Type: &pb.LinkFrame_RpcFrame{RpcFrame: rpc.FrameToProtoClearTicket(rpcf)}}
			err = lr.sendSafely(lf)
			if err != nil {
				break
			}
		}
	}()
}

// LinkClient is used to establish a link between this node and a remote node with a running LinkServer.
type LinkClient struct {
	id         uint64
	infoString string
	conn       *grpc.ClientConn
	core       *rpc.Core
	isClosed   int32
	log        util.Logger
}

func (lc *LinkClient) String() string {
	return lc.infoString
}

// NewLinkClient creates a new LinkClient instance and associates it with the given rpc.Core.
func NewLinkClient(conn *grpc.ClientConn, core *rpc.Core) (res *LinkClient) {
	istr := fmt.Sprintf("LinkClient %s", conn.Target())
	res = &LinkClient{rand.Uint64(), istr, conn, core, 0, core.Log().WithPrefix(istr + " ")}
	return
}

// Close closes this link and the underlying grpc.ClientConn.
func (lc *LinkClient) Close() {
	atomic.StoreInt32(&lc.isClosed, 1)
	lc.conn.Close()
}

// Run uses and blocks the calling goroutine to establish the link and to receive and dispatch incomming messages to the core.
// Until Close is called, it will continously block retry with a delay to establish the connection.
func (lc *LinkClient) Run() {
	client := pb.NewLinkServiceClient(lc.conn)
	lc.log.Printf("opening")
	for atomic.LoadInt32(&lc.isClosed) == 0 {
		var linkRoute *Link
		call, err := client.Link(context.Background())
		if err != nil {
			goto handleError
		}
		linkRoute = NewLink(lc.core, call, lc.infoString)
		err = linkRoute.ReceiveAndDispatch()
		if err != nil {
			goto handleError
		}
		continue
	handleError:
		if atomic.LoadInt32(&lc.isClosed) != 0 {
			break
		}
		status := status.Convert(err)
		if strings.Contains(status.Message(), "504") {
			time.Sleep(100 * time.Millisecond)
		} else {
			lc.log.Printf("failed: %s, retrying...\n", err.Error())
			time.Sleep(5000 * time.Millisecond)
		}
	}
	lc.log.Printf("closed")
}

// LinkServer is used to establish a links between this node and remote nodes with a LinkClients.
type LinkServer struct {
	pb.UnimplementedLinkServiceServer
	listen     net.Listener
	core       *rpc.Core
	infoString string
}

// NewLinkServer creates a new instance of a LinkServer.
func NewLinkServer(listen net.Listener, core *rpc.Core) *LinkServer {
	istr := fmt.Sprintf("LinkServer %s", listen.Addr().String())
	return &LinkServer{listen: listen, core: core, infoString: istr}
}

// Run blocks the calling goroutine to operate the server and handle incomming connections.
// The call ends when Stop is called.
func (ls *LinkServer) Run() {
	grpcServer := grpc.NewServer()
	pb.RegisterLinkServiceServer(grpcServer, ls)
	grpcServer.Serve(ls.listen)
}

// Stop closes the server and terminates all open links.
func (ls *LinkServer) Stop() {
	ls.listen.Close()
}

// Link implements v1.LinkServiceServer.Link.
func (ls *LinkServer) Link(call pb.LinkService_LinkServer) error {
	var linkString string
	p, ok := peer.FromContext(call.Context())
	if ok {
		linkString = fmt.Sprintf("%s from %s", ls.infoString, p.Addr.String())
	} else {
		linkString = fmt.Sprintf("%s from unknown", ls.infoString)
	}
	log := ls.core.Log()
	log.Printf("%s opened\n", linkString)
	lr := NewLink(ls.core, call, linkString)
	err := lr.ReceiveAndDispatch()
	log.Printf("%s failed: %s\n", linkString, err.Error())
	return err
}
