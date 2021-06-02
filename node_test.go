package arpcnet

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"google.golang.org/grpc/metadata"

	"github.com/rektorphi/arpcnet/generated/rektorphi/arpcnet/test"
	"github.com/rektorphi/arpcnet/testservice"
)

type Setup struct {
	nodeClient     test.TestClient
	stopClient     func()
	stopTestServer func()
	node           *Node
}

func (s *Setup) Close() {
	s.stopClient()
	s.node.Stop()
	s.stopTestServer()
}

func NewSetup(coreMemory int) *Setup {
	setup := &Setup{}
	setup.stopTestServer = testservice.StartTestServer(40123)
	var err error
	setup.node, err = NewNode(&Config{
		GRPCPort:   44077,
		CoreMemory: bytefmt.ByteSize(uint64(coreMemory)),
		Group:      "test:group",
		GRPCMappings: []GRPCMapping{
			{Target: "localhost:40123", Mount: "", Methods: []string{"rektorphi.arpcnet.test"}},
		},
	})
	if err != nil {
		log.Fatalf("failed creating node %v", err)
	}
	go setup.node.Run()
	setup.nodeClient, setup.stopClient = testservice.NewTestClient(fmt.Sprintf("localhost:%d", 44077))
	return setup
}

func TestOneHopUnaryCall(t *testing.T) {
	setup := NewSetup(-1)
	defer setup.Close()

	ctx, cancelCtx := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	defer cancelCtx()
	ctx = metadata.AppendToOutgoingContext(ctx, GRPCAddrKey, "test:group")
	err := testservice.TestUnaryCall(ctx, setup.nodeClient, 1024)

	if err != nil {
		t.Fatalf("test call failed %v", err)
	}
	if setup.node.core.MemMan().Used() != 0 {
		t.Fatalf("managed memory not empty %d", setup.node.core.MemMan().Used())
	}
}

func TestOneHopMultiCallStress(t *testing.T) {
	setup := NewSetup(512)
	defer setup.Close()

	ctx, cancelCtx := context.WithTimeout(context.Background(), 10000*time.Millisecond)
	defer cancelCtx()
	ctx = metadata.AppendToOutgoingContext(ctx, GRPCAddrKey, "test:group")

	errchan := make(chan error, 1000)
	for i := 0; i < 5000; i++ {
		go func() {
			err := testservice.TestUnaryCall(ctx, setup.nodeClient, 128)
			errchan <- err
		}()
	}

	for i := 0; i < 5000; i++ {
		err := <-errchan
		if err != nil {
			t.Fatalf("test call failed %v", err)
		}
	}

	if setup.node.core.MemMan().Used() != 0 {
		t.Fatalf("managed memory not empty %d", setup.node.core.MemMan().Used())
	}
}

func TestOneHopStreamCall(t *testing.T) {
	setup := NewSetup(512)
	defer setup.Close()

	ctx, cancelCtx := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	defer cancelCtx()

	time.Sleep(50 * time.Millisecond)

	ctx = metadata.AppendToOutgoingContext(ctx, GRPCAddrKey, "test:group")
	err := testservice.TestStreamCall(ctx, setup.nodeClient, 128, 1000)

	if err != nil {
		t.Fatalf("test call failed %v", err)
	}
	if setup.node.core.MemMan().Used() != 0 {
		t.Fatalf("managed memory not empty %d", setup.node.core.MemMan().Used())
	}
}

func BenchmarkUnaryCalls(b *testing.B) {
	setup := NewSetup(-1)
	defer setup.Close()

	ctx, cancelCtx := context.WithTimeout(context.Background(), 20000*time.Millisecond)
	defer cancelCtx()
	ctx = metadata.AppendToOutgoingContext(ctx, GRPCAddrKey, "test:group")
	for i := 0; i < b.N; i++ {
		err := testservice.TestUnaryCall(ctx, setup.nodeClient, 1024)
		if err != nil {
			b.Fatalf("test call failed %v", err)
		}
	}
	if setup.node.core.MemMan().Used() != 0 {
		b.Fatalf("managed memory not empty %d", setup.node.core.MemMan().Used())
	}
}

func BenchmarkStreamingThrougput(b *testing.B) {
	setup := NewSetup(-1)
	defer setup.Close()

	ctx, cancelCtx := context.WithTimeout(context.Background(), 20000*time.Millisecond)
	defer cancelCtx()
	ctx = metadata.AppendToOutgoingContext(ctx, GRPCAddrKey, "test:group")
	err := testservice.TestStreamCall(ctx, setup.nodeClient, 1024, b.N)

	if err != nil {
		b.Fatalf("test call failed %v", err)
	}
}
