package rpc

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rektorphi/arpcnet/util"
)

// Verbosity levels in logging.
const ( // iota is reset to 0
	LOff    = iota // Log nothing about core and rpc operation.
	LInfo   = iota // Log status of routes, links and names.
	LDetail = iota // Also log query-announce and rpc protocol events.
)

// LogLevel changes the amount of logged messages. It only applies when cores are being created.
var LogLevel int = LInfo

var addrLocal = *NewAddress("local")

// AddrLocal returns a static address prefix representing local addresses. This prefix will be substituted with the address of the local group.
func AddrLocal() *Address {
	return &addrLocal
}

// RoutingError is an error issued when an RPC cannot start because no route to its destination could be found.
type RoutingError struct {
	Dest  *Address
	Cause error
}

func (e RoutingError) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("no route available to destination '%s'", e.Dest.String())
	}
	return fmt.Sprintf("no route available to destination '%s', cause: %v", e.Dest.String(), e.Cause)
}

// Core is a central component of an Arpc network node.
// It is responsible for holding all active RPCs, all active routes, dispatching messages and resolving routes for new RPCs.
type Core struct {
	// TODO consider optimizing thread sync. there are also mutexes in manager and router, maybe 'm' is redundant, or the others are.
	// But the slow query requires a short mutex and the router but should not block the whole core.
	Manager
	group  *Address
	id     *Address
	m      sync.Mutex
	router Router
	memMan MemoryManager
	quanda *Quanda
	log    util.Logger
}

// Group is the address prexis this core operates in.
func (c *Core) Group() *Address {
	return c.group
}

// ID is a unique identifier for this core. It includes the group ID as a prefix.
func (c *Core) ID() *Address {
	return c.id
}

// Router returns the routing module of the core.
func (c *Core) Router() Router {
	return c.router
}

// MemMan returns the memory management module of the core.
func (c *Core) MemMan() MemoryManager {
	return c.memMan
}

// Quanda returns the query and announce module of the core.
func (c *Core) Quanda() *Quanda {
	return c.quanda
}

// Log returns the logger of the core. All log messages specific to a core should use this logger so logs from different cores can be distinguished in tests.
func (c *Core) Log() util.Logger {
	return c.log
}

// NewCoreExt creates a new core instance with given parameters. Provides more parameterization options than NewCore.
func NewCoreExt(group *Address, maxMemory int, coreID string) (core *Core) {
	var mm MemoryManager
	if maxMemory <= 0 {
		mm = NewNoMemoryManager()
	} else {
		mm = NewCondMemoryManager(maxMemory)
	}
	var logger util.Logger = util.NopLogger{}
	if LogLevel > LOff {
		l := log.Default()
		l.SetFlags(log.Default().Flags() | log.Lmsgprefix)
		logger = &util.StdLogger{Logger: l}
	}
	if len(coreID) > LOff {
		logger = logger.WithPrefix(fmt.Sprintf("[%s] ", coreID))
	} else {
		coreID = util.B32.RandomStr(8)
	}
	if group.Len() == 0 {
		group = NewAddress("default-group")
	}
	router := NewRouteMap()
	var quandalog util.Logger = util.NopLogger{}
	if LogLevel >= LDetail {
		quandalog = logger
	}
	core = &Core{
		group:   group,
		id:      group.Appends(coreID),
		Manager: *NewManager(),
		m:       sync.Mutex{},
		router:  router,
		memMan:  mm,
		quanda:  NewQuanda(router, quandalog),
		log:     logger,
	}
	core.Manager.AddListener(core.frameLogger)
	core.router.AddListener(core.destLogger)
	return
}

// NewCore creates a new core instance with a random core identifier suffix. This is recommended for general use.
func NewCore(group *Address, maxMemory int) *Core {
	return NewCoreExt(group, maxMemory, "")
}

// Stop terminates operation of this core.
func (c *Core) Stop() {
	c.quanda.Stop()
}

func (c *Core) frameLogger(rpc *RPC, frame Frame) {
	switch frame.Type() {
	case UpStart:
		rpc.log.Printf("Start")
	case UpData:
		f := frame.(UpDataFrame)
		rpc.log.Printf("Up %d (%d)", len(f.Chunk().Data), f.Chunk().Remaining)
	case DownData:
		f := frame.(DownDataFrame)
		rpc.log.Printf("Down %d (%d)", len(f.Chunk().Data), f.Chunk().Remaining)
	case UpCancel:
		fallthrough
	case DownFinish:
		rpc.log.Printf("Finish %d %s", rpc.status, rpc.endMessage)
	}
}

func (c *Core) destLogger(e *DestinationEvent) {
	if e.Metric.Hops >= 0 {
		c.log.Printf("Online %s via %s (%s)", e.Dest.String(), e.Route.String(), e.Metric.String())
		go c.quanda.handleDestOnline(e.Dest, e.Route.(Handler), e.Metric)
	} else {
		c.log.Printf("Offline %s via %s (%s)", e.Dest.String(), e.Route.String(), e.Metric.String())
		go c.quanda.handleDestOffline(e.Dest, e.Route.(Handler))
	}
}

func (c *Core) initRPCLogger(rpc *RPC) {
	if LogLevel >= LDetail {
		rpc.log = c.log.WithPrefix("RPC " + rpc.fullID.String() + " ")
	}
}

func (c *Core) resolveAddress(dest *Address) *Address {
	if dest.Len() == 0 {
		return BlankAddress
	}
	if dest.StartsWith(&addrLocal) {
		return c.group.Append(dest.Slice(addrLocal.len, -1))
	}
	return dest
}

// RouteAndDispatch dispatches frames to their destination. If it is a start frame, a route to its destination is looked up and a local routing entry for the RPC is tracked.
// Returns an error if no routing entry exists for the frame, if no route can be found to the destination of a start frame or if starting the RPC fails for other reasons.
func (c *Core) RouteAndDispatch(ctx context.Context, frame Frame, downstreamHandler Handler) error {
	// TODO this lock is temporary, needed because SetUpstreamHandler is not thread safe
	c.m.Lock()
	defer c.m.Unlock()
	if frame.Type() == UpStart {
		var err error
		start := frame.(UpStartFrame)
		resolvedDest := c.resolveAddress(start.Dest())
		_, handler, err := c.QueryRoute(ctx, resolvedDest, false)
		if err != nil {
			return RoutingError{start.Dest(), err}
		}

		rpc := New(NewFullID(start.ID(), start.Source(), resolvedDest, []byte{}))
		c.initRPCLogger(rpc)
		rpc.SetUpstreamHandler(handler)
		rpc.SetDownstreamHandler(downstreamHandler)

		err = c.Add(rpc)
		if err != nil {
			return err
		}
		lh, ok := downstreamHandler.(*LinkHandler)
		if ok {
			c.quanda.onDestUsed(resolvedDest, lh, time.Now())
		}
		return rpc.sendUpstream(ctx, frame)
	}
	return c.Dispatch(ctx, frame, downstreamHandler.ID())
}

// StartRPC starts a new RPC from this core. A route to its destination is looked up and a local routing entry for the RPC is tracked.
// Returns an error if no route can be found to the destination of a start frame or if starting the RPC fails for other reasons.
func (c *Core) StartRPC(ctx context.Context, dest *Address, metadata []string, props map[string][]byte, downstreamHandler Handler) error {
	resolvedDest := c.resolveAddress(dest)
	_, handler, err := c.QueryRoute(ctx, resolvedDest, true)
	if err != nil {
		return RoutingError{dest, err}
	}
	// TODO this lock is temporary, needed because SetUpstreamHandler is not thread safe
	c.m.Lock()
	defer c.m.Unlock()
	rpc := New(RandomFullID(c.id, resolvedDest))
	c.initRPCLogger(rpc)

	// TODO could check here if the ID already exists locally at least
	rpc.SetUpstreamHandler(handler)
	rpc.SetDownstreamHandler(downstreamHandler)
	err = c.Add(rpc)
	if err != nil {
		return err
	}
	lh, ok := downstreamHandler.(*LinkHandler)
	if ok {
		c.quanda.onDestUsed(resolvedDest, lh, time.Now())
	}
	return rpc.sendUpstream(ctx, NewUpStartFrame(rpc.fullID, metadata, props))
}

// QueryRoute looks up a route by quering the network. It returns immediately if a route to the destination is already known at the core.
// Otherwise a network query is run that blocks until it times out or a solution is found. The timeout can be shortened by setting one in the provided context.
// If a route is found, returns the address for the found route, which may be a parent of the queried destination, and the route handler.
// Otherwise returns an error.
func (c *Core) QueryRoute(ctx context.Context, dest *Address, remoteQuery bool) (*Address, Handler, error) {
	d, id, _ := c.router.GetNearest(dest)
	handler, ok := id.(Handler)
	if ok {
		return dest.Slice(0, d), handler, nil
	}
	if !remoteQuery {
		return nil, nil, RoutingError{dest, nil}
	}
	maxDeadline := time.Now().Add(maxQueryTimeout)
	deadline, ok := ctx.Deadline()
	if !ok || deadline.After(maxDeadline) {
		deadline = maxDeadline
	}
	rq := c.quanda.runQuery(dest, deadline, nil)
	return rq.await(ctx)
}
