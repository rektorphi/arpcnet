package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	//_ "net/http/pprof"

	"github.com/rektorphi/arpcnet"
	"github.com/rektorphi/arpcnet/rpc"
)

type mountFlags []string

func (mf mountFlags) String() string {
	return strings.Join(mf, ",")
}

func (mf *mountFlags) Set(value string) error {
	*mf = append(*mf, value)
	return nil
}

func main() {
	//go http.ListenAndServe("localhost:8080", nil)
	rpc.LogLevel = rpc.LInfo
	rand.Seed(time.Now().UTC().UnixNano())

	var cfgFile = flag.String("cfg", "", "Configuration file")
	var verbose = flag.Bool("v", false, "Verbose logging")
	var group = flag.String("g", "default-group", "Arpcnet group of the node")
	var serverPort = flag.Int("gp", 8028, "Port for gRPC gateway server")
	var link = flag.String("l", "", "Address of a server node to link to")
	var linkPort = flag.Int("lp", 8029, "Port for incomming links from other nodes")
	var mounts mountFlags
	flag.Var(&mounts, "mg", "Map a gRPC server into the arpc network <host:port>=<arpc:name>")
	flag.Parse()

	if *verbose {
		rpc.LogLevel = rpc.LDetail
		log.SetFlags(log.Ltime | log.Lmicroseconds)
	}

	cmd := flag.Arg(0)
	if cmd == "example" {
		arpcnet.WriteConfig(os.Stdout, arpcnet.ExampleConfig())
		return
	}

	log.Printf("ArpcNet version %s\n", arpcnet.Version)
	var cfg *arpcnet.Config
	if len(*cfgFile) == 0 {
		cfg = &arpcnet.Config{GRPCPort: *serverPort, CoreMemory: "", Group: *group}

		if len(*link) != 0 {
			cfg.LinkClients = []arpcnet.LinkClientConfig{{Target: *link, Insecure: true}}
		}
		if *linkPort > 0 {
			cfg.LinkServers = []arpcnet.LinkServerConfig{{Port: *linkPort}}
		}
		for _, mount := range mounts {
			parts := strings.SplitN(mount, "=", 2)
			cfg.GRPCMappings = append(cfg.GRPCMappings, arpcnet.GRPCMapping{Target: parts[0], Mount: parts[1]})
		}
	} else {
		absCfgFile, err := filepath.Abs(*cfgFile)
		if err != nil {
			panic(err)
		}
		log.Printf("Loading config from %s", absCfgFile)
		cfg, err = arpcnet.LoadConfigfromFile(*cfgFile)
		if err != nil {
			panic(err)
		}
	}

	log.Printf("Opening gRPC gateway on port %d", cfg.GRPCPort)
	for _, ccfg := range cfg.LinkClients {
		log.Printf("Opening link to %s", ccfg.Target)
	}
	for _, scfg := range cfg.LinkServers {
		log.Printf("Opening link server on port %d", scfg.Port)
	}

	node, err := arpcnet.NewNode(cfg)
	if err != nil {
		panic(err)
	}
	log.Printf("Started node %s", node.ID().String())
	node.Run()
}
