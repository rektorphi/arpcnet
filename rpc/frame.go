package rpc

import (
	"fmt"
	"math/bits"
	"reflect"
	"time"

	pb "github.com/rektorphi/arpcnet/generated/rektorphi/arpcnet/v1"
)

// Chunk is a payload fragment. Large payloads can be split across multiple chunks.
type Chunk struct {
	Data      []byte
	Remaining int
	ticket    MemoryTicket
}

const (
	UpStart      = 2
	UpData       = 3
	UpClose      = 4
	UpCancel     = 5
	DownResponse = 8
	DownData     = 9
	DownFinish   = 10
)

var messageTypeNames []string = []string{"", "", "UpStart", "UpData", "UpClose", "UpCancel", "", "", "DownResponse", "DownData", "DownFinish"}

// Frame is the basic frame for all rpc protocol related messages.
type Frame interface {
	ID() ShortID
	Type() int
	Proto() *pb.RPCFrame
	String() string
}

type ChunkFrame interface {
	Frame
	Chunk() *Chunk
}

// IsUpstream returns true if the frame goes from client to server, false if it goes from server to client (downstream)
func IsUpstream(f Frame) bool {
	return f.Type() < DownResponse
}

type baseFrame struct {
	msg *pb.RPCFrame
}

func (t baseFrame) ID() ShortID {
	return t.msg.Id
}

func (t baseFrame) Proto() *pb.RPCFrame {
	return t.msg
}

func getType(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return "*" + t.Elem().Name()
	} else {
		return t.Name()
	}
}

func (t baseFrame) String() string {
	return fmt.Sprintf("%s %d", getType(t.msg.Type), t.msg.Id)
}

// UpStartFrame initiates an RPC and is upstream.
type UpStartFrame struct {
	baseFrame
	n      *pb.UpStart
	source *Address
	dest   *Address
}

func NewUpStartFrame(fullId *FullID, metadata []string, props map[string][]byte) UpStartFrame {
	n := &pb.UpStart{
		SourceId:      fullId.source.ToProto(),
		DestinationId: fullId.dest.ToProto(),
		Metadata:      &pb.MD{KvPairs: metadata},
	}
	msg := &pb.RPCFrame{Id: fullId.id, Type: &pb.RPCFrame_UpStart{UpStart: n}}
	return UpStartFrame{baseFrame{msg}, n, fullId.source, fullId.dest}
}

func (t UpStartFrame) Type() int {
	return UpStart
}

func (t UpStartFrame) Source() *Address {
	return t.source
}

func (t UpStartFrame) Dest() *Address {
	return t.dest
}

func (t UpStartFrame) Properties() map[string][]byte {
	return t.n.Properties
}

func (t UpStartFrame) Metadata() []string {
	return t.n.Metadata.KvPairs
}

type UpDataFrame struct {
	baseFrame
	chunk *Chunk
}

func NewUpDataFrame(shortID ShortID, chunk *Chunk) UpDataFrame {
	n := &pb.UpData{
		Chunk:     chunk.Data,
		Remaining: uint32(chunk.Remaining),
	}
	msg := &pb.RPCFrame{Id: shortID, Type: &pb.RPCFrame_UpData{UpData: n}}
	return UpDataFrame{baseFrame{msg}, chunk}
}

func (t UpDataFrame) Type() int {
	return UpData
}

func (t UpDataFrame) Chunk() *Chunk {
	return t.chunk
}

// UpCloseFrame indicates upstream that the client is finished sending data.
type UpCloseFrame struct {
	baseFrame
	n *pb.UpClose
}

func NewUpCloseFrame(shortID ShortID) UpCloseFrame {
	n := &pb.UpClose{}
	msg := &pb.RPCFrame{Id: shortID, Type: &pb.RPCFrame_UpClose{UpClose: n}}
	return UpCloseFrame{baseFrame{msg}, n}
}

func (t UpCloseFrame) Type() int {
	return UpClose
}

// UpCancelFrame tells upstream the client has abandoned this RPC.
type UpCancelFrame struct {
	baseFrame
	n *pb.UpCancel
}

func NewUpCancelFrame(shortID ShortID, message string) UpCancelFrame {
	n := &pb.UpCancel{Message: message}
	msg := &pb.RPCFrame{Id: shortID, Type: &pb.RPCFrame_UpCancel{UpCancel: n}}
	return UpCancelFrame{baseFrame{msg}, n}
}

func (t UpCancelFrame) Type() int {
	return UpCancel
}

func (t UpCancelFrame) Message() string {
	return t.n.Message
}

// DownResponseFrame is response metadata sent from the server after starting the RPC, typically before sending data.
type DownResponseFrame struct {
	baseFrame
	n *pb.DownResponse
}

func NewDownResponseFrame(shortID ShortID, md []string) DownResponseFrame {
	n := &pb.DownResponse{Metadata: &pb.MD{KvPairs: md}}
	msg := &pb.RPCFrame{Id: shortID, Type: &pb.RPCFrame_DownResp{DownResp: n}}
	return DownResponseFrame{baseFrame{msg}, n}
}

func (t DownResponseFrame) Type() int {
	return DownResponse
}

func (t DownResponseFrame) Metadata() []string {
	return t.n.Metadata.KvPairs
}

// DownDataFrame is a data chunk sent from the server to the client.
type DownDataFrame struct {
	baseFrame
	chunk *Chunk
}

func NewDownDataFrame(shortID ShortID, chunk *Chunk) DownDataFrame {
	t := &pb.DownData{
		Chunk:     chunk.Data,
		Remaining: uint32(chunk.Remaining),
	}
	msg := &pb.RPCFrame{Id: shortID, Type: &pb.RPCFrame_DownData{DownData: t}}
	return DownDataFrame{baseFrame{msg}, chunk}
}

func (t DownDataFrame) Type() int {
	return DownData
}

func (t DownDataFrame) Chunk() *Chunk {
	return t.chunk
}

// DownFinishFrame is sent by the server when it completes the RPC, either regularly (status==0) or in error.
type DownFinishFrame struct {
	baseFrame
	n *pb.DownFinish
}

func NewDownFinishFrame(shortID ShortID, status int, message string, md []string) DownFinishFrame {
	t := &pb.DownFinish{
		Status:   uint32(status),
		Message:  message,
		Metadata: &pb.MD{KvPairs: md},
	}
	msg := &pb.RPCFrame{Id: shortID, Type: &pb.RPCFrame_DownFinish{DownFinish: t}}
	return DownFinishFrame{baseFrame{msg}, t}
}

func (t DownFinishFrame) Type() int {
	return DownFinish
}

func (t DownFinishFrame) Status() int {
	return int(t.n.Status)
}

func (t DownFinishFrame) Message() string {
	return t.n.Message
}

func (t DownFinishFrame) Metadata() []string {
	return t.n.Metadata.KvPairs
}

func FrameFromProto(msg *pb.RPCFrame, memm MemoryManager) Frame {
	switch m := msg.Type.(type) {
	case *pb.RPCFrame_UpStart:
		return UpStartFrame{baseFrame{msg}, m.UpStart, AddressFromProto(m.UpStart.SourceId), AddressFromProto(m.UpStart.DestinationId)}
	case *pb.RPCFrame_UpData:
		t := memm.Acquire(len(m.UpData.Chunk))
		return UpDataFrame{baseFrame{msg}, &Chunk{m.UpData.Chunk, int(m.UpData.Remaining), t}}
	case *pb.RPCFrame_UpClose:
		return UpCloseFrame{baseFrame{msg}, m.UpClose}
	case *pb.RPCFrame_UpCancel:
		return UpCancelFrame{baseFrame{msg}, m.UpCancel}
	case *pb.RPCFrame_DownResp:
		return DownResponseFrame{baseFrame{msg}, m.DownResp}
	case *pb.RPCFrame_DownData:
		t := memm.Acquire(len(m.DownData.Chunk))
		return DownDataFrame{baseFrame{msg}, &Chunk{m.DownData.Chunk, int(m.DownData.Remaining), t}}
	case *pb.RPCFrame_DownFinish:
		return DownFinishFrame{baseFrame{msg}, m.DownFinish}
	default:
		panic("unknown pbMessage type")
	}
}

func FrameToProtoClearTicket(f Frame) *pb.RPCFrame {
	cf, ok := f.(ChunkFrame)
	if ok {
		cf.Chunk().ticket.Release()
	}
	return f.Proto()
}

func MetricFromProto(m *pb.Metric) Metric {
	return Metric{Hops: int(m.Hops)}
}

func MetricToProto(m Metric) *pb.Metric {
	return &pb.Metric{Hops: int32(m.Hops)}
}

type AnnounceFrame struct {
	m        *pb.Announce
	name     *Address
	metric   Metric
	deadline time.Time
}

func NewAnnounceFrameFromProto(m *pb.Announce) *AnnounceFrame {
	return &AnnounceFrame{m, AddressFromProto(m.Name), MetricFromProto(m.Value), unixMsToTime(m.DeadlineUnixMs)}
}

func NewAnnounceFrame(name *Address, metric Metric, deadline time.Time) *AnnounceFrame {
	return &AnnounceFrame{&pb.Announce{
		Name:           name.ToProto(),
		Value:          MetricToProto(metric),
		DeadlineUnixMs: timeToUnixMS(deadline),
	}, name, metric, deadline}
}

func (f *AnnounceFrame) Name() *Address {
	return f.name
}

func (f *AnnounceFrame) Metric() Metric {
	return f.metric
}

func (f *AnnounceFrame) Deadline() time.Time {
	return f.deadline
}

func (f *AnnounceFrame) Proto() *pb.Announce {
	return f.m
}

var zeroUnixMsTime time.Time = unixMsToTime(0)

func timeToUnixMS(t time.Time) uint64 {
	return uint64(t.Unix()*1000 + int64(t.Nanosecond()/1000000))
}

func unixMsToTime(t uint64) time.Time {
	s, ms := bits.Div64(0, t, 1000)
	return time.Unix(int64(s), int64(ms*1000000))
}

type QueryFrame struct {
	m        *pb.Query
	name     *Address
	deadline time.Time
}

func NewQueryFrameFromProto(m *pb.Query) *QueryFrame {
	return &QueryFrame{m, AddressFromProto(m.Name), unixMsToTime(m.DeadlineUnixMs)}
}

func NewQueryFrame(name *Address, deadline time.Time) *QueryFrame {
	return &QueryFrame{&pb.Query{
		Name:           name.ToProto(),
		DeadlineUnixMs: timeToUnixMS(deadline),
	}, name, deadline}
}

func (f *QueryFrame) Name() *Address {
	return f.name
}

func (f *QueryFrame) Deadline() time.Time {
	return f.deadline
}

func (f *QueryFrame) Proto() *pb.Query {
	return f.m
}
