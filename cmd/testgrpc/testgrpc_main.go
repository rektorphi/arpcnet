package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/montanaflynn/stats"
	"google.golang.org/grpc/metadata"

	"github.com/rektorphi/arpcnet"
	pb "github.com/rektorphi/arpcnet/generated/rektorphi/arpcnet/test"
	"github.com/rektorphi/arpcnet/testservice"
)

func main() {
	var serverPort = flag.Int("sport", 13075, "Port for gRPC Test Service")
	var clientTarget = flag.String("conn", "localhost:13075", "Host:Port to connect gRPC Test Client to")
	var arpcnetDest = flag.String("addr", "default-group", "ArpcNet destination address")

	flag.Parse()
	log.Printf("Starting with server port %d and client connecting to %v with ArpcNet address %v\n", *serverPort, *clientTarget, *arpcnetDest)

	nodeClient, stopClient := testservice.NewTestClient(*clientTarget)
	defer stopClient()

	ctx := metadata.AppendToOutgoingContext(context.Background(), arpcnet.GRPCAddrKey, *arpcnetDest)

	var testArg = flag.Arg(0)
	testFunc, err := createTestFunc(testArg)
	if err != nil {
		log.Fatalf("Error creating test function with argument '%v': %v", testArg, err)
	} else {
		log.Printf("Created test function with argument: %v", testArg)
	}

	stopTestServer := testservice.StartTestServer(*serverPort)
	defer stopTestServer()

	testFunc(nodeClient, ctx)
}

type testType struct {
	name           string
	flags          *flag.FlagSet
	createTestFunc func() (func(pb.TestClient, context.Context), error)
}

var testTypes = map[string]*testType{
	testUnaryCalls.name:  testUnaryCalls,
	testStreamCalls.name: testStreamCalls,
	serveOnly.name:       serveOnly,
}

func createTestFunc(testArg string) (func(pb.TestClient, context.Context), error) {
	tokens := strings.SplitN(testArg, "=", 2)
	tt := testTypes[tokens[0]]
	if tt == nil {
		keys := make([]string, 0, len(testTypes))
		for k := range testTypes {
			keys = append(keys, k)
		}
		return nil, fmt.Errorf("no test type available for name '%v', available are: %v", tokens[0], strings.Join(keys, ", "))
	}
	var flagTokens = make([]string, 0)
	if len(tokens) == 2 {
		flagTokens = strings.Split(tokens[1], " ")
	}
	err := tt.flags.Parse(flagTokens)
	if err != nil {
		return nil, err
	}
	return tt.createTestFunc()
}

var testUnaryCalls *testType = func() *testType {
	tt := testType{name: "unary"}
	tt.flags = flag.NewFlagSet("unary", flag.ContinueOnError)
	var parallelism = tt.flags.Int("p", 20, "Number of calls in parallel")
	var callsPerSec = tt.flags.Int("cps", 1000, "Number of calls per second")
	var msgSize = tt.flags.Int("s", 1024, "Payload size (both request and response)")

	tt.createTestFunc = func() (func(nodeClient pb.TestClient, ctx context.Context), error) {
		return func(nodeClient pb.TestClient, ctx context.Context) {
			log.Printf("Making %d unary calls per second of size %d with parallelism of %d", *callsPerSec, *msgSize, *parallelism)
			clientCall := func() error {
				return testservice.TestUnaryCall(ctx, nodeClient, *msgSize)
			}
			runUnaryCalls(ctx, nodeClient, *parallelism, *callsPerSec, clientCall)
		}, nil
	}
	return &tt
}()

var testStreamCalls *testType = func() *testType {
	tt := testType{name: "stream"}
	tt.flags = flag.NewFlagSet("stream", flag.ContinueOnError)
	var parallelism = tt.flags.Int("p", 20, "Number of calls in parallel")
	var callsPerSec = tt.flags.Int("cps", 20, "Number of calls per second")
	var packetSize = tt.flags.Int("s", 1024, "Size per server stream message")
	var numPackets = tt.flags.Int("n", 50, "Number of server stream messages per call")

	tt.createTestFunc = func() (func(nodeClient pb.TestClient, ctx context.Context), error) {
		return func(nodeClient pb.TestClient, ctx context.Context) {
			log.Printf("Making %v stream calls per second, receiving %d packets of size %d with parallelism of %d", *callsPerSec, *numPackets, *packetSize, *parallelism)
			clientCall := func() error {
				return testservice.TestStreamCall(ctx, nodeClient, *packetSize, *numPackets)
			}
			runUnaryCalls(ctx, nodeClient, *parallelism, *callsPerSec, clientCall)
		}, nil
	}
	return &tt
}()

var serveOnly *testType = func() *testType {
	tt := testType{name: "server"}
	tt.flags = flag.NewFlagSet("server", flag.ContinueOnError)
	tt.createTestFunc = func() (func(nodeClient pb.TestClient, ctx context.Context), error) {
		return func(nodeClient pb.TestClient, ctx context.Context) {
			for {
				time.Sleep(5000 * time.Millisecond)
			}
		}, nil
	}
	return &tt
}()

func runUnaryCalls(ctx context.Context, nodeClient pb.TestClient, parallelism int, callsPerSec int, call func() error) {
	var cnt int32 = 0
	sampleSize := 25
	samples := make(stats.Float64Data, sampleSize)
	samplePtr := 0
	samples[samplePtr] = 0

	for {
		var workAvailable = int32(callsPerSec)
		var wg sync.WaitGroup
		deadline := time.Now().Add(1000 * time.Millisecond)
		start := time.Now()
		for j := 0; j < parallelism; j++ {
			wg.Add(1)
			go func() {
				for {
					x := atomic.AddInt32(&workAvailable, -1)
					if x < 0 {
						break
					}
					err := call()
					if err != nil {
						log.Printf("Call failed: %v", err)
						atomic.AddInt32(&workAvailable, 1)
						time.Sleep(500 * time.Millisecond)
					}
				}
				wg.Done()
			}()
		}
		wg.Wait()
		cnt += int32(callsPerSec)
		t := time.Since(start).Milliseconds()
		samples[samplePtr] = float64(t)
		samplePtr = (samplePtr + 1) % sampleSize
		m, err := samples.Mean()
		if err != nil {
			m = 0.0
		}
		std, err := samples.StandardDeviationSample()
		if err != nil {
			std = 0.0
		}
		log.Printf("total %d last %d m= %.2f (+/- %.2f)", cnt, t, m, std)
		time.Sleep(time.Until(deadline))
	}
}
