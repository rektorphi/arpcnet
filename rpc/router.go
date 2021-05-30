package rpc

import (
	"container/heap"
	"fmt"
	"sync"
)

type Router interface {
	Route(dest *Address) Identifiable
	GetNearest(dest *Address) (int, Identifiable, Metric)

	Add(r Identifiable) error
	Get(id uint64) Identifiable
	GetAll() []Identifiable

	DestinationOffline(dest *Address, r Identifiable)
	DestinationUpdate(dest *Address, r Identifiable, metric Metric)
	Remove(h Identifiable) bool

	AddListener(listener DestinationEventListener)
}

type DestinationEventListener func(*DestinationEvent)

// RouteMap is a bitchy data structure that basically has a bidrectional mapping.
//
// First, the obvious use case, given a destination as a query, it has to return the best route to that destination.
// So some map lookup structure with addresses as keys and a selection of routes as result, of which we can pick the best.
// Ergo destToRouteMap which has as value a routeHeap where we can retrieve the shortest route.
//
// Second, routes can be updated and a single route can serve multiple destinations, so we need a lookup map with route ids as key to
// get a second map with destination as key to get a handle to the routeHeap element to change.
type RouteMap struct {
	destToRouteMap PrefixTreeMap                // dest -> routeHeap queue -> route + metric
	routeToDestMap map[uint64]routeDestinations // route id -> route, dest map -> heapItemHandle
	rwm            sync.RWMutex
	listeners      []DestinationEventListener
}

// NewRouteMap initializes an empty RouteMap.
func NewRouteMap() *RouteMap {
	return &RouteMap{PrefixTreeMap{}, make(map[uint64]routeDestinations), sync.RWMutex{}, []DestinationEventListener{}}
}

// routeDestinations contains a route and all destination addresses reachable over this route.
// The destinations map uses addresses as keys and as value a heapItemHandle, which can be used to modify the entry in the destToRouteMap.
type routeDestinations struct {
	route        Identifiable
	destinations map[string]heapItemHandle
}

// Metric describes the quality of a route. It has a comparison function to sort metrics from best to worst routes.
type Metric struct {
	Hops int
}

var MetricOffline Metric = Metric{Hops: -1}

func (m Metric) String() string {
	return fmt.Sprintf("%d hops", m.Hops)
}

func (m Metric) compareTo(other Metric) int {
	return m.Hops - other.Hops
}

// Route returns the best route to the destination, or nil if no route is currently available.
func (rmap *RouteMap) Route(dest *Address) Identifiable {
	_, id, _ := rmap.GetNearest(dest)
	return id
}

func (rmap *RouteMap) GetNearest(dest *Address) (int, Identifiable, Metric) {
	rmap.rwm.RLock()
	defer rmap.rwm.RUnlock()
	d, n := rmap.destToRouteMap.GetNearest(dest)
	if n == nil {
		return -1, nil, Metric{}
	}
	var rheap *routeHeap = n.(*routeHeap)
	id, metric := rheap.peek()
	return d, id, metric
}

func (rmap *RouteMap) Get(id uint64) Identifiable {
	rmap.rwm.RLock()
	defer rmap.rwm.RUnlock()
	rd, ok := rmap.routeToDestMap[id]
	if ok {
		return rd.route
	}
	return nil
}

func (rmap *RouteMap) GetAll() (res []Identifiable) {
	rmap.rwm.RLock()
	defer rmap.rwm.RUnlock()
	res = make([]Identifiable, len(rmap.routeToDestMap))
	i := 0
	for _, rd := range rmap.routeToDestMap {
		res[i] = rd.route
		i++
	}
	return
}

// Add puts a route in the list of available routes. Initially no destinations are reachable via this route.
func (rmap *RouteMap) Add(r Identifiable) error {
	rmap.rwm.Lock()
	defer rmap.rwm.Unlock()
	rd, ok := rmap.routeToDestMap[r.ID()]
	if ok {
		return fmt.Errorf("a route with id %d already exists", r.ID())
	}
	rd = routeDestinations{r, make(map[string]heapItemHandle)}
	rmap.routeToDestMap[r.ID()] = rd
	return nil
}

// Remove takes the route and all destinations that were reachable via this route offline.
func (rmap *RouteMap) Remove(r Identifiable) bool {
	rmap.rwm.Lock()
	rd, ok := rmap.routeToDestMap[r.ID()]
	if !ok {
		rmap.rwm.Unlock()
		return false
	}

	// keys are reachable destination addresses, values are heapItemHandle
	events := []*DestinationEvent{}
	for destData, handle := range rd.destinations {
		key := NewAddressFromData(destData)
		// get the heap from the handle
		rq := handle.heap
		// remove handle from heap
		prevBestRoute, _ := rq.peek()
		handle.remove()
		newFirstRoute, newMetric := rq.peek()
		// if heap is empty, set node in destToRouteMap to nil, possibly cleaning up tree
		if rq.Len() == 0 {
			rmap.destToRouteMap.Remove(key)
			// destination offline
			events = append(events, &DestinationEvent{key, prevBestRoute, MetricOffline})
		} else if newFirstRoute != prevBestRoute {
			// first changed
			events = append(events, &DestinationEvent{key, newFirstRoute, newMetric})
		}
	}
	delete(rmap.routeToDestMap, r.ID())
	rmap.rwm.Unlock()
	rmap.dispatchMultiple(events)
	return true
}

// DestinationOffline removes the route as option for the given destination.
func (rmap *RouteMap) DestinationOffline(dest *Address, r Identifiable) {
	var event *DestinationEvent = nil
	rmap.rwm.Lock()
	// get the route for the id
	rd, ok := rmap.routeToDestMap[r.ID()]
	if !ok {
		rmap.rwm.Unlock()
		return
	}
	// Remove the destination from the reachable ones for this route, get the heap handle if present
	handle, ok := rd.destinations[dest.Data()]
	delete(rd.destinations, dest.Data())
	if !ok {
		rmap.rwm.Unlock()
		return
	}
	// Get the heap
	rh := handle.heap
	prevBestRoute, _ := rh.peek()
	handle.remove()
	newFirstRoute, newMetric := rh.peek()

	if rh.Len() == 0 {
		rmap.destToRouteMap.Remove(dest)
		// destination offline
		event = &DestinationEvent{dest, prevBestRoute, MetricOffline}
	} else if newFirstRoute != prevBestRoute {
		// first changed
		event = &DestinationEvent{dest, newFirstRoute, newMetric}
	}
	// unlock before event dispatch
	rmap.rwm.Unlock()
	if event != nil {
		rmap.dispatch(event)
	}
}

// DestinationUpdate updates the route for a destination with a changed metric, possibly changing the best route for the destination.
func (rmap *RouteMap) DestinationUpdate(dest *Address, r Identifiable, metric Metric) {
	if metric.Hops < 0 {
		rmap.DestinationOffline(dest, r)
		return
	}

	var event *DestinationEvent = nil
	rmap.rwm.Lock()
	rd, ok := rmap.routeToDestMap[r.ID()]
	if !ok {
		rd = routeDestinations{r, make(map[string]heapItemHandle)}
		rmap.routeToDestMap[r.ID()] = rd
	}
	var rq *routeHeap
	var handle heapItemHandle
	// keys are reachable destination addresses, values are destMetric
	handle, ok = rd.destinations[dest.Data()]
	if ok {
		// from the handle we can get the heap
		rq = handle.heap
		// from that heap use handle in dm to update queue
		prevBestRoute, _ := rq.peek()
		handle.update(metric)
		newFirstRoute, newMetric := rq.peek()
		if newFirstRoute != prevBestRoute {
			// first changed
			event = &DestinationEvent{dest, newFirstRoute, newMetric}
		}
	} else {
		// there is no heapHandle, ergo this destination was not reachable via this route yet
		rq, ok = rmap.destToRouteMap.Get(dest).(*routeHeap)
		if !ok {
			rq = &routeHeap{}
			rmap.destToRouteMap.Put(dest, rq)
		}
		prevBestRoute, _ := rq.peek()
		handle = rq.add(r, metric)
		rd.destinations[dest.Data()] = handle
		newFirstRoute, newMetric := rq.peek()
		if prevBestRoute == nil {
			// new route to dest
			event = &DestinationEvent{dest, newFirstRoute, newMetric}
		} else if prevBestRoute != newFirstRoute {
			// best route changed
			event = &DestinationEvent{dest, newFirstRoute, newMetric}
		}
	}
	rmap.rwm.Unlock()
	if event != nil {
		rmap.dispatch(event)
	}
}

type DestinationEvent struct {
	Dest   *Address
	Route  Identifiable
	Metric Metric
}

func (e DestinationEvent) String() string {
	return fmt.Sprintf("Name %s via %s (%d)", e.Dest.String(), e.Route.String(), e.Metric.Hops)
}

func (rmap *RouteMap) AddListener(listener DestinationEventListener) {
	rmap.listeners = append(rmap.listeners, listener)
}

func (rmap *RouteMap) dispatch(event *DestinationEvent) {
	ls := rmap.listeners
	for _, l := range ls {
		l(event)
	}
}

func (rmap *RouteMap) dispatchMultiple(events []*DestinationEvent) {
	if len(events) == 0 {
		return
	}
	ls := rmap.listeners
	for _, e := range events {
		for _, l := range ls {
			l(e)
		}
	}
}

// -----------------

// routeHeap is a heap of routes together with a metric, the heap top returns the lowest metric.
type routeHeap struct {
	heap _rh
}

type heapItemHandle struct {
	item *heapItem
	heap *routeHeap
}

func (rh *routeHeap) Len() int {
	return len(rh.heap)
}

func (rh *routeHeap) peek() (route Identifiable, metric Metric) {
	if rh.heap.Len() == 0 {
		return nil, Metric{}
	}
	i := rh.heap[0]
	return i.r, i.m
}

func (rh *routeHeap) add(route Identifiable, metric Metric) (h heapItemHandle) {
	i := &heapItem{route, metric, 0}
	heap.Push(&rh.heap, i)
	return heapItemHandle{i, rh}
}

func (hih *heapItemHandle) remove() {
	heap.Remove(&hih.heap.heap, hih.item.index)
	hih.item.r = nil
	hih.heap = nil
	hih.item = nil
}

func (hih *heapItemHandle) update(metric Metric) {
	hih.item.m = metric
	heap.Fix(&hih.heap.heap, hih.item.index)
}

type heapItem struct {
	r     Identifiable
	m     Metric
	index int
}

type _rh []*heapItem

func (rh _rh) Len() int { return len(rh) }

func (rh _rh) Less(i, j int) bool {
	return rh[i].m.compareTo(rh[j].m) < 0
}

func (rh _rh) Swap(i, j int) {
	rh[i], rh[j] = rh[j], rh[i]
	rh[i].index = i
	rh[j].index = j
}

func (rh *_rh) Push(x interface{}) {
	n := len(*rh)
	item := x.(*heapItem)
	item.index = n
	*rh = append(*rh, item)
}

func (rh *_rh) Pop() interface{} {
	old := *rh
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*rh = old[0 : n-1]
	return item
}
