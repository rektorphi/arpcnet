package arpcnet

import (
	"fmt"
	"io"
	"net"
	"os"

	"code.cloudfoundry.org/bytefmt"
	"github.com/rektorphi/arpcnet/rpc"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Group      string `yaml:"group"`
	GRPCPort   int    `yaml:"grpcPort"`
	CoreMemory string `yaml:"coreMemory,omitempty"`

	GRPCMappings []GRPCMapping `yaml:"grpcMappings"`

	LinkServers []LinkServerConfig `yaml:"linkServers"`

	LinkClients []LinkClientConfig `yaml:"linkClients"`
}

type GRPCMapping struct {
	Target string `yaml:"target"`
	Mount  string `yaml:"mount,omitempty"`
	// TODO doublecheck how this should work
	Methods []string `yaml:"methods,omitempty"`
}

type LinkClientConfig struct {
	Target   string `yaml:"target"`
	Insecure bool   `yaml:"insecure"`
}

type LinkServerConfig struct {
	Port int `yaml:"port"`
}

func ExampleConfig() (res *Config) {
	res = &Config{
		Group:      "my:group",
		GRPCPort:   13075,
		CoreMemory: bytefmt.ByteSize(64 * bytefmt.MEGABYTE),

		LinkServers: []LinkServerConfig{{Port: 14040}},
		LinkClients: []LinkClientConfig{{Target: "remotehost:14041"}},

		GRPCMappings: []GRPCMapping{
			{Target: "localhost:11001", Mount: "my:services1"},
			{Target: "localhost:11002", Methods: []string{"my/package/Service/MyMethod"}},
		},
	}
	return
}

func LoadConfigfromFile(name string) (*Config, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return LoadConfig(f)
}

func LoadConfig(r io.Reader) (*Config, error) {
	var cfg Config
	decoder := yaml.NewDecoder(r)
	err := decoder.Decode(&cfg)
	return &cfg, err
}

func WriteConfig(w io.Writer, cfg *Config) error {
	encoder := yaml.NewEncoder(w)
	return encoder.Encode(&cfg)
}

func (cfg *Config) Apply(n *Node) error {
	for i, serviceConfig := range cfg.GRPCMappings {
		mountAddr, err := rpc.ParseAddress(serviceConfig.Mount)
		if err != nil {
			return err
		}
		parsedMethods := make([][]string, 0, len(serviceConfig.Methods))
		for i, method := range serviceConfig.Methods {
			maddr, err := SplitFullMethodName(method)
			if err != nil {
				return fmt.Errorf("invalid configuration in ServiceConfig %d: Failed parsing method name %s: %s", i, method, err.Error())
			}
			parsedMethods = append(parsedMethods, maddr)
		}
		if mountAddr.Len() == 0 && len(parsedMethods) == 0 {
			return fmt.Errorf("invalid configuration in ServiceConfig %d: Mount and Methods cannot both be empty", i)
		}

		absMountAddr := n.core.Group().Append(mountAddr, &addrGrpc)
		gout := NewGRPCClient(n.core, absMountAddr.Len(), serviceConfig.Target, true)
		if len(parsedMethods) == 0 {
			n.core.Router().DestinationUpdate(absMountAddr, gout.Handler(), rpc.Metric{})
		} else {
			for _, maddr := range parsedMethods {
				n.core.Router().DestinationUpdate(absMountAddr.Appends(maddr...), gout.Handler(), rpc.Metric{})
			}
		}
	}
	for _, lscfg := range cfg.LinkServers {
		listen, err := net.Listen("tcp", fmt.Sprintf(":%d", lscfg.Port))
		if err != nil {
			return err
		}
		ls := NewLinkServer(listen, n.core)
		ls.Start()
		n.AddCloseable(func() { listen.Close() })
	}
	for _, lccfg := range cfg.LinkClients {
		var conn *grpc.ClientConn
		var err error
		if lccfg.Insecure {
			conn, err = grpc.Dial(lccfg.Target, grpc.WithInsecure())
		} else {
			conn, err = grpc.Dial(lccfg.Target)
		}
		if err != nil {
			return err
		}
		lc := NewLinkClient(conn, n.core)
		lc.Start()
		n.AddCloseable(func() { conn.Close() })
	}
	return nil
}