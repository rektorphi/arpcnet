package arpcnet

import (
	"net"
	"testing"
	"time"

	"github.com/rektorphi/arpcnet/rpc"
	"google.golang.org/grpc"
)

func TestLink(t *testing.T) {
	core := rpc.NewCore(rpc.NewAddress("test"), -1)

	listen, err := net.Listen("tcp", ":28972")
	if err != nil {
		t.Fatalf("listen failed %v", err)
	}
	defer listen.Close()
	ls := NewLinkServer(listen, core)
	ls.Start()

	conn, err := grpc.Dial("localhost:28972", grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial failed %v", err)
	}
	lc := NewLinkClient(conn, core)
	lc.Start()
	time.Sleep(50 * time.Millisecond)

	if len(core.Router().GetAll()) != 2 {
		t.Fatalf("unexpected routes %v", core.Router().GetAll())
	}

	lc.Close()
	time.Sleep(50 * time.Millisecond)

	if len(core.Router().GetAll()) != 0 {
		t.Fatalf("unexpected routes %v", core.Router().GetAll())
	}
}
