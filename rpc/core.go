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

var LogLevel int = LInfo

var addrLocal = *NewAddress("local")

func AddrLocal() *Address {
	return &addrLocal
}

// TODO consider optimizing thread sync. there are also mutexes in manager and router, maybe 'm' is redundant, or the others are.
// But the slow query requires a short mutex and the router but should not block the whole core.
type Core struct {
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

// ID is a unique identifier for this core in the group. To obtain the full ID, append the id to the group.
func (c *Core) ID() *Address {
	return c.id
}

func (c *Core) Router() Router {
	return c.router
}

func (c *Core) MemMan() MemoryManager {
	return c.memMan
}

func (c *Core) Quanda() *Quanda {
	return c.quanda
}

func (c *Core) Log() util.Logger {
	return c.log
}

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

func NewCore(group *Address, maxMemory int) *Core {
	return NewCoreExt(group, maxMemory, "")
}

func (rc *Core) Stop() {
	rc.quanda.Stop()
}

func (rc *Core) frameLogger(rpc *RPC, frame Frame) {
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

func (rc *Core) destLogger(e *DestinationEvent) {
	if e.Metric.Hops >= 0 {
		rc.log.Printf("Online %s via %s (%s)", e.Dest.String(), e.Route.String(), e.Metric.String())
		go rc.quanda.handleDestOnline(e.Dest, e.Route.(Handler), e.Metric)
	} else {
		rc.log.Printf("Offline %s via %s (%s)", e.Dest.String(), e.Route.String(), e.Metric.String())
		go rc.quanda.handleDestOffline(e.Dest, e.Route.(Handler))
	}
}

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

func (rc *Core) initRPCLogger(rpc *RPC) {
	if LogLevel >= LDetail {
		rpc.log = rc.log.WithPrefix("RPC " + rpc.fullID.String() + " ")
	}
}

func (rc *Core) resolveAddress(dest *Address) *Address {
	if dest.Len() == 0 {
		return BlankAddress
	}
	if dest.StartsWith(&addrLocal) {
		return rc.group.Append(dest.Slice(addrLocal.len, -1))
	}
	return dest
}

func (rc *Core) RouteAndDispatch(ctx context.Context, frame Frame, downstreamHandler Handler) error {
	// TODO this lock is temporary, needed because SetUpstreamHandler is not thread safe
	rc.m.Lock()
	defer rc.m.Unlock()
	if frame.Type() == UpStart {
		var err error
		start := frame.(UpStartFrame)
		resolvedDest := rc.resolveAddress(start.Dest())
		_, handler, err := rc.QueryRoute(ctx, resolvedDest, false)
		if err != nil {
			return RoutingError{start.Dest(), err}
		}

		rpc := New(NewFullID(start.ID(), start.Source(), resolvedDest, []byte{}))
		rc.initRPCLogger(rpc)
		rpc.SetUpstreamHandler(handler)
		rpc.SetDownstreamHandler(downstreamHandler)

		err = rc.Add(rpc)
		if err != nil {
			return err
		}
		lh, ok := downstreamHandler.(*LinkHandler)
		if ok {
			rc.quanda.onDestUsed(resolvedDest, lh, time.Now())
		}
		return rpc.sendUpstream(ctx, frame)
	}
	return rc.Dispatch(ctx, frame, downstreamHandler.ID())
}

func (rc *Core) StartRPC(ctx context.Context, dest *Address, metadata []string, props map[string][]byte, downstreamHandler Handler) error {
	resolvedDest := rc.resolveAddress(dest)
	_, handler, err := rc.QueryRoute(ctx, resolvedDest, true)
	if err != nil {
		return RoutingError{dest, err}
	}
	// TODO this lock is temporary, needed because SetUpstreamHandler is not thread safe
	rc.m.Lock()
	defer rc.m.Unlock()
	rpc := New(RandomFullID(rc.id, resolvedDest))
	rc.initRPCLogger(rpc)

	// TODO could check here if the ID already exists locally at least
	rpc.SetUpstreamHandler(handler)
	rpc.SetDownstreamHandler(downstreamHandler)
	err = rc.Add(rpc)
	if err != nil {
		return err
	}
	lh, ok := downstreamHandler.(*LinkHandler)
	if ok {
		rc.quanda.onDestUsed(resolvedDest, lh, time.Now())
	}
	return rpc.sendUpstream(ctx, NewUpStartFrame(rpc.fullID, metadata, props))
}

// QueryRoute looks up a route by quering the network.
// The ctx is only used for terminating the wait early, the query will remain active and collect results until the duration expires.
func (rc *Core) QueryRoute(ctx context.Context, dest *Address, remoteQuery bool) (*Address, Handler, error) {
	d, id, _ := rc.router.GetNearest(dest)
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
	rq := rc.quanda.runQuery(dest, deadline, nil)
	return rq.await(ctx)
}
