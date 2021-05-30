package rpc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func NewTestRoute(identity uint64) *MultiRPCHandler {
	return NewMultiRPCHandler(identity, fmt.Sprintf("route%d", identity), func(*RPC, FrameReceiver, FrameSender) {})
}

func TestBasicMapOps(t *testing.T) {
	r1 := NewTestRoute(1)
	r2 := NewTestRoute(2)
	r3 := NewTestRoute(3)

	a1 := MustParseAddress("a.1")
	a2 := MustParseAddress("a.2")
	a3 := MustParseAddress("a.3")

	rm := NewRouteMap()
	events := make([]*DestinationEvent, 0)
	rm.AddListener(func(de *DestinationEvent) { de.Dest.Compact(); events = append(events, de) })

	rm.DestinationUpdate(a1, r1, Metric{10})
	rm.DestinationUpdate(a2, r2, Metric{10})
	rm.DestinationUpdate(a3, r3, Metric{10})

	r := rm.Route(a1)
	if r != r1 {
		t.Fatalf("Unexpected route result: %s", r.String())
	}
	r = rm.Route(a2)
	if r != r2 {
		t.Fatalf("Unexpected route result: %s", r.String())
	}
	r = rm.Route(a3)
	if r != r3 {
		t.Fatalf("Unexpected route result: %s", r.String())
	}

	rm.Remove(r3)
	r = rm.Route(a3)
	if r != nil {
		t.Fatalf("Unexpected route result: %s", r.String())
	}

	rm.DestinationOffline(a2, r2)
	r = rm.Route(a2)
	if r != nil {
		t.Fatalf("Unexpected route result: %s", r.String())
	}

	rm.DestinationUpdate(a1, r1, Metric{-1})
	r = rm.Route(a1)
	if r != nil {
		t.Fatalf("Unexpected route result: %s", r.String())
	}

	expectedEvents := []*DestinationEvent{
		{a1, r1, Metric{10}},
		{a2, r2, Metric{10}},
		{a3, r3, Metric{10}},
		{a3, r3, MetricOffline},
		{a2, r2, MetricOffline},
		{a1, r1, MetricOffline},
	}
	assert.Equal(t, expectedEvents, events)
}

func TestTreeMapOps(t *testing.T) {
	r1 := NewTestRoute(1)
	r2 := NewTestRoute(2)
	r3 := NewTestRoute(3)

	rm := NewRouteMap()
	rm.DestinationUpdate(MustParseAddress("a:b"), r1, Metric{10})
	rm.DestinationUpdate(MustParseAddress("a:2"), r2, Metric{10})

	events := make([]*DestinationEvent, 0)
	rm.AddListener(func(de *DestinationEvent) { de.Dest.Compact(); events = append(events, de) })

	// a.b should be returned because a:b:1 is a child of it
	r := rm.Route(MustParseAddress("a:b:1"))
	if r != r1 {
		t.Fatalf("Unexpected route result: %s", r.String())
	}

	rm.DestinationUpdate(MustParseAddress("a:b:1"), r3, Metric{10})
	// a.b.1 should be returned because it is a closer match than a.b
	r = rm.Route(MustParseAddress("a:b:1"))
	if r != r3 {
		t.Fatalf("Unexpected route result: %s", r.String())
	}
	// nothing is returned because a:b is a child of a, parents are responsible for their children but not vice versa
	r = rm.Route(MustParseAddress("a"))
	if r != nil {
		t.Fatalf("Unexpected route result: %s", r.String())
	}

	expectedEvents := []*DestinationEvent{
		{MustParseAddress("a:b:1"), r3, Metric{10}},
	}
	assert.Equal(t, expectedEvents, events)
}

func TestHeapOps(t *testing.T) {
	a1 := MustParseAddress("a:1")
	r1 := NewTestRoute(1)
	r2 := NewTestRoute(2)
	r3 := NewTestRoute(3)

	rm := NewRouteMap()

	events := make([]*DestinationEvent, 0)
	rm.AddListener(func(de *DestinationEvent) { de.Dest.Compact(); events = append(events, de) })

	// adding the first route
	rm.DestinationUpdate(a1, r1, Metric{10})
	// will be returned
	r := rm.Route(a1)
	if r != r1 {
		t.Fatalf("Unexpected route result: %s", r.String())
	}
	// adding second route, which is worse
	rm.DestinationUpdate(a1, r2, Metric{11})
	// still get the first route
	r = rm.Route(a1)
	if r != r1 {
		t.Fatalf("Unexpected route result: %s", r.String())
	}
	// adding third route, even worse
	rm.DestinationUpdate(a1, r3, Metric{12})
	// downgrading first route
	rm.DestinationUpdate(a1, r1, Metric{20})
	// now the second route should be returned
	r = rm.Route(a1)
	if r != r2 {
		t.Fatalf("Unexpected route result: %s", r.String())
	}
	// taking the second route offline
	rm.DestinationOffline(a1, r2)
	// should return the third route
	r = rm.Route(a1)
	if r != r3 {
		t.Fatalf("Unexpected route result: %s", r.String())
	}
	// removing r3, should return the first
	rm.Remove(r3)
	r = rm.Route(a1)
	if r != r1 {
		t.Fatalf("Unexpected route result: %s", r.String())
	}
	// route 2 comes online again with best metric, should be first
	rm.DestinationUpdate(a1, r2, Metric{9})
	r = rm.Route(a1)
	if r != r2 {
		t.Fatalf("Unexpected route result: %s", r.String())
	}

	expectedEvents := []*DestinationEvent{
		{a1, r1, Metric{10}},
		{a1, r2, Metric{11}},
		{a1, r3, Metric{12}},
		{a1, r1, Metric{20}},
		{a1, r2, Metric{9}},
	}
	assert.Equal(t, expectedEvents, events)
}
