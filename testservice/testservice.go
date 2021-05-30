package testservice

import (
	"context"
	"fmt"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/rektorphi/arpcnet/generated/rektorphi/arpcnet/test"
	"github.com/rektorphi/arpcnet/lru"
)

type testServiceImpl struct {
	pb.UnimplementedTestServer
}

func StartTestServer(port int) func() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterTestServer(grpcServer, &testServiceImpl{})
	go grpcServer.Serve(lis)
	return func() { grpcServer.Stop() }
}

func NewTestClient(target string) (pb.TestClient, func()) {
	conn, err := grpc.Dial(target, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	return pb.NewTestClient(conn), func() { conn.Close() }
}

func (testServiceImpl) UnaryCall(ctx context.Context, request *pb.TestMessage) (*pb.TestMessage, error) {
	return &pb.TestMessage{Text: request.Text}, nil
}

/*
func (testServiceImpl) CallClientStream(clientStream pb.TestService_CallClientStreamServer) error {
	sb := strings.Builder{}
	for {
		req, err := clientStream.Recv()
		if err != nil {
			_ = clientStream.SendAndClose(&pb.TestMessage{Text: sb.String()})
			return nil
		}
		sb.WriteString(req.Text)
	}
}
*/

var re = regexp.MustCompile(`data ([0-9]+) ([0-9]+)`)

func (testServiceImpl) ServerStream(req *pb.TestMessage, serverStream pb.Test_ServerStreamServer) error {
	match := re.FindStringSubmatch(req.Text)
	if match == nil {
		_ = serverStream.Send(req)
		_ = serverStream.Send(req)
		_ = serverStream.Send(req)
		return status.New(codes.Internal, "test error msg").Err()
	}
	packetSize, _ := strconv.Atoi(match[1])
	numPackets, _ := strconv.Atoi(match[2])
	s := getOrCreateString(packetSize)
	for i := 0; i < numPackets; i++ {
		err := serverStream.Send(&pb.TestMessage{Text: s})
		if err != nil {
			return err
		}
	}
	return nil
}

func (testServiceImpl) BidiStream(bidiStream pb.Test_BidiStreamServer) error {
	for {
		req, err := bidiStream.Recv()
		if err != nil {
			return err
		}
		switch req.Text {
		case "end":
			return nil
		case "error":
			return status.New(codes.Internal, "test error msg").Err()
		default:
			_ = bidiStream.Send(req)
		}
	}
}

var scMux sync.Mutex = sync.Mutex{}
var stringCache lru.Cache = *lru.New(32)

func getOrCreateString(size int) string {
	scMux.Lock()
	s, ok := stringCache.Get(size)
	if ok == false {
		var sb strings.Builder
		sb.Grow(size)
		for i := 0; i < size; i++ {
			sb.WriteByte(byte('A' + (i % 26)))
		}
		s = sb.String()
		stringCache.Add(size, s)
	}
	scMux.Unlock()
	return s.(string)
}

func TestUnaryCall(ctx context.Context, nodeClient pb.TestClient, msgSize int) error {
	res, err := nodeClient.UnaryCall(ctx, &pb.TestMessage{Text: getOrCreateString(msgSize)})
	if err != nil {
		return err
	}
	if len(res.Text) != msgSize {
		return fmt.Errorf("unexpected unary response length, actual %d expected %d", len(res.Text), msgSize)
	}
	return nil
}

func TestStreamCall(ctx context.Context, nodeClient pb.TestClient, packetSize int, numPackets int) error {
	stream, err := nodeClient.ServerStream(ctx, &pb.TestMessage{Text: fmt.Sprintf("data %d %d", packetSize, numPackets)})
	if err != nil {
		return err
	}
	for i := 0; i < numPackets; i++ {
		m, err := stream.Recv()
		if err != nil {
			return err
		}
		if len(m.Text) != packetSize {
			return fmt.Errorf("invalid packet size actual %d, expected %d", len(m.Text), packetSize)
		}
	}
	return err
}
