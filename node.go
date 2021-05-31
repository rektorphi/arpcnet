package arpcnet

import (
	"code.cloudfoundry.org/bytefmt"
	"github.com/rektorphi/arpcnet/rpc"
)

// A Node is the runtime data structure for a node in the Arpc network.
type Node struct {
	group      *rpc.Address
	core       *rpc.Core
	gin        *GRPCServer
	closeables []func()
}

// NewNode creates a Node from the parameters in a Config struct.
// Returns an error if any configuration values are invalid or incompatible. Note that the Node is only fully operational when the Run()-function is called.
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
	err = config.apply(n)
	if err != nil {
		return
	}

	n.gin = NewGRPCServer(n.core, config.GRPCPort)
	return
}

// Group returns the identifier of the group this node belongs to.
func (n *Node) Group() *rpc.Address {
	return n.group
}

// ID returns the identifier of this node. It has the group as a prefix.
func (n *Node) ID() *rpc.Address {
	return n.core.ID()
}

// Run blocks the goroutine and takes the node fully operational.
func (n *Node) Run() {
	n.gin.Serve()
}

// AddCloseable adds a function call to this node that is called when the node is stopped.
// It can be used to link the lifetime of any module that depends on this node with its lifetime.
func (n *Node) AddCloseable(c func()) {
	n.closeables = append(n.closeables, c)
}

// Stop ends the operation of this node and terminates any associated modules.
func (n *Node) Stop() {
	n.gin.Stop()
	for _, c := range n.closeables {
		c()
	}
}
