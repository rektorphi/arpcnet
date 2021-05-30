package arpcnet

import (
	"code.cloudfoundry.org/bytefmt"
	"github.com/rektorphi/arpcnet/rpc"
)

type Node struct {
	Group      *rpc.Address
	core       *rpc.Core
	gin        *GRPCServer
	closeables []func()
}

func NewNode(config *Config) (n *Node, err error) {
	group, err := rpc.ParseAddress(config.Group)
	if err != nil {
		return
	}
	coremem := -1
	if len(config.CoreMemory) > 0 {
		bs, err := bytefmt.ToBytes(config.CoreMemory)
		if err != nil {
			return nil, err
		}
		coremem = int(bs)
	}
	n = &Node{group, rpc.NewCore(group, coremem), nil, []func(){}}
	err = config.Apply(n)
	if err != nil {
		return
	}

	n.gin = NewGRPCServer(n.core, config.GRPCPort)
	return
}

func (n *Node) CoreID() *rpc.Address {
	return n.core.ID()
}

func (n *Node) Run() {
	n.gin.Serve()
}

func (n *Node) AddCloseable(c func()) {
	n.closeables = append(n.closeables, c)
}

func (n *Node) Stop() {
	n.gin.Stop()
	for _, c := range n.closeables {
		c()
	}
}
