package rpc

import (
	"context"
	"fmt"
	"sync"
)

// The Manager holds all active RPCs.
// Its biggest function is that it works as a demultiplexer by dispatching RPCFrames to the RPC they belong to based on the RPC ID.
// Here one can also register listeners RPC events and track statistics.
// Unless we go for keepalives and auto cleanup, the manager is not responsible for closing RPCs, this is the job of the module who creates the RPC.
type Manager struct {
	rpcs  map[uint64]*RPC
	rpcsm sync.RWMutex

	frameCBs    []FrameCallback
	nextCleanup int
}

var minCleanup int = 128

func NewManager() *Manager {
	return &Manager{
		rpcs:        make(map[uint64]*RPC, 127),
		rpcsm:       sync.RWMutex{},
		frameCBs:    []FrameCallback{},
		nextCleanup: minCleanup,
	}
}

func (t *Manager) AddListener(cb FrameCallback) {
	t.frameCBs = append(t.frameCBs, cb)
}

// Add registers an RPC at the manager. It must be unstarted, no frames have been sent yet.
// The RPC is automatically removed after it finishes.
func (rpcm *Manager) Add(rpc *RPC) error {
	rpcm.rpcsm.Lock()
	defer rpcm.rpcsm.Unlock()
	if orpc := rpcm.rpcs[rpc.fullID.id]; orpc != nil && !orpc.Finished() {
		return IDCollision{rpc.fullID.id}
	}
	if len(rpcm.rpcs) >= rpcm.nextCleanup {
		for id, rpc := range rpcm.rpcs {
			if rpc.Finished() {
				delete(rpcm.rpcs, id)
			}
		}
		t := len(rpcm.rpcs) * 2
		if t < minCleanup {
			t = minCleanup
		}
		rpcm.nextCleanup = t
	}

	rpcm.rpcs[rpc.fullID.id] = rpc
	for _, cb := range rpcm.frameCBs {
		rpc.addFrameCB(cb)
	}
	return nil
}

// GetRPCs returns a snapshot list of current active RPCs. For debugging, slow.
func (rpcm *Manager) GetRPCs() []*RPC {
	rpcm.rpcsm.RLock()
	defer rpcm.rpcsm.RUnlock()
	res := make([]*RPC, 0, len(rpcm.rpcs))
	for _, r := range rpcm.rpcs {
		if !r.Finished() {
			res = append(res, r)
		}
	}
	return res
}

func (rpcm *Manager) Dispatch(ctx context.Context, frame Frame, identity uint64) error {
	rpcm.rpcsm.RLock()
	id := frame.ID()
	rpc := rpcm.rpcs[id]
	rpcm.rpcsm.RUnlock()
	if rpc == nil {
		return IDUnknown{frame}
	}
	if IsUpstream(frame) {
		if rpc.downstreamHandler.ID() != identity {
			return IDMismatch{frame}
		}
		return rpc.sendUpstream(ctx, frame)
	}
	if rpc.upstreamHandler.ID() != identity {
		return IDMismatch{frame}
	}
	return rpc.sendDownstream(ctx, frame)
}

type IDCollision struct {
	ID ShortID
}

func (t IDCollision) Error() string {
	return fmt.Sprintf("RPC ID %d already in use", t.ID)
}

type IDUnknown struct {
	Frame Frame
}

func (t IDUnknown) Error() string {
	return fmt.Sprintf("%s ID not available", t.Frame)
}

type IDMismatch struct {
	Frame Frame
}

func (t IDMismatch) Error() string {
	return fmt.Sprintf("%s ID routing mismatch", t.Frame)
}
