package rpc

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	pb "github.com/rektorphi/arpcnet/generated/rektorphi/arpcnet/v1"
	"github.com/rektorphi/arpcnet/util"
)

const maxQueryTimeout time.Duration = 5000 * time.Millisecond
const baseAnnounceTimeout time.Duration = 5000 * time.Millisecond
const timeLayout = "15:04:05.000"

// Quanda is a module handling the query and announce protocol.
type Quanda struct {
	router      Router
	querym      sync.Mutex
	queries     PrefixTreeMap // values are runningQuery
	anninm      sync.Mutex
	announceIn  map[string]sortedAnnounceEntries // sorted according to LinkHandler id
	annoutm     sync.Mutex
	announceOut PrefixTreeMap // values are sortedAnnounceEntries and sorted according to LinkHandler id
	sweepTicker *time.Ticker
	stopc       chan struct{}
	log         util.Logger
}

func NewQuanda(router Router, log util.Logger) (quanda *Quanda) {
	quanda = &Quanda{
		router:      router,
		querym:      sync.Mutex{},
		queries:     PrefixTreeMap{},
		anninm:      sync.Mutex{},
		announceIn:  make(map[string]sortedAnnounceEntries),
		annoutm:     sync.Mutex{},
		announceOut: PrefixTreeMap{},
		sweepTicker: time.NewTicker(5 * time.Second),
		stopc:       make(chan struct{}),
		log:         log,
	}
	go func() {
		for {
			select {
			case <-quanda.stopc:
				return
			case t := <-quanda.sweepTicker.C:
				quanda.sweepExpiredAnnounces(t)
			}
		}
	}()
	return
}

func (qa *Quanda) Stop() {
	close(qa.stopc)
}

func (qa *Quanda) sweepExpiredAnnounces(sweeptime time.Time) {
	qa.anninm.Lock()
	for dest, entries := range qa.announceIn {
		i := 0
		for _, entry := range entries {
			if entry.deadline.After(sweeptime) {
				entries[i] = entry
				i++
			} else {
				// remove AND take offline
				name := NewAddressFromData(dest)
				qa.log.Printf("AnnounceIn for %s deadline %s expired", name.String(), entry.deadline.Format(timeLayout))
				qa.router.DestinationOffline(name, entry.link)
			}
		}
		if i == 0 {
			delete(qa.announceIn, dest)
		} else if i != len(entries) {
			entries = entries[:i]
			qa.announceIn[dest] = entries
		}
	}
	qa.anninm.Unlock()
	qa.annoutm.Lock()
	for _, v := range qa.announceOut.GetSubtree(BlankAddress) {
		entries := v.Value.(sortedAnnounceEntries)
		i := 0
		for _, entry := range entries {
			if entry.deadline.After(sweeptime) {
				entries[i] = entry
				i++
			} else {
				_ = i
				// remove only
				qa.log.Printf("AnnounceOut for %s deadline %s expired", v.Key.String(), entry.deadline.Format(timeLayout))
			}
		}
		if i == 0 {
			qa.announceOut.Remove(v.Key)
		} else if i != len(entries) {
			entries = entries[:i]
			qa.announceOut.Put(v.Key, entries)
		}
	}
	qa.annoutm.Unlock()
}

// Called for every rpc, should be faster :-(
func (qa *Quanda) onDestUsed(dest *Address, lh *LinkHandler, time time.Time) {
	qa.annoutm.Lock()
	d, v := qa.announceOut.GetNearest(dest)
	if d != 0 {
		entries := v.(sortedAnnounceEntries)
		i := sort.Search(len(entries), func(i int) bool { return entries[i].link == lh })
		if i < len(entries) {
			if entries[i].deadline.Sub(time) < baseAnnounceTimeout {
				name := dest.Slice(0, d)
				extdl := entries[i].deadline.Add(baseAnnounceTimeout)
				qa.log.Printf("Announceout for %s deadline extended to %s", name, extdl.Format(timeLayout))
				entries[i].deadline = extdl
				qa.announceOut.Put(name, entries)
				qa.annoutm.Unlock()
				lf := &pb.LinkFrame{Type: &pb.LinkFrame_Announce{Announce: NewAnnounceFrame(name, entries[i].metric, extdl).Proto()}}
				lh.Send(lf)
				return
			}
		}
	}
	qa.annoutm.Unlock()
}

func (qa *Quanda) handleDestOnline(dest *Address, handler Handler, metric Metric) {
	lf := &pb.LinkFrame{Type: &pb.LinkFrame_Announce{Announce: NewAnnounceFrame(dest, metric, zeroUnixMsTime).Proto()}}

	// first forward announces that are still alive
	qa.annoutm.Lock()
	entries, ok := qa.announceOut.Get(dest).(sortedAnnounceEntries)
	qa.annoutm.Unlock()
	if ok {
		now := time.Now()
		for _, entry := range entries {
			if entry.deadline.After(now) {
				entry.link.Send(lf)
			}
		}
	}

	// then check if queries are solved, which can add new announces
	// Get all queries that can be solved by this announcement, that is the entire subtree of the announced name.
	qa.querym.Lock()
	solvedQueries := qa.queries.RemoveSubtree(dest)
	if len(solvedQueries) == 0 {
		qa.querym.Unlock()
		return
	}

	qa.log.Printf("Solving queries with %s", dest.String())
	queriers := make([]*LinkHandler, 0, len(solvedQueries))
	for _, entry := range solvedQueries {
		rq := entry.Value.(*runningQuery)
		// Solve the query and collect the queriers
		queriers = append(queriers, rq.solve(dest, handler)...)
	}
	qa.querym.Unlock()

	// forward to all queriers the announcement
	qa.announce(dest, metric, time.Now().Add(2*baseAnnounceTimeout), queriers...)
}

func (qa *Quanda) handleDestOffline(dest *Address, handler Handler) {
	lf := &pb.LinkFrame{Type: &pb.LinkFrame_Announce{Announce: NewAnnounceFrame(dest, MetricOffline, zeroUnixMsTime).Proto()}}

	qa.annoutm.Lock()
	entries, ok := qa.announceOut.Get(dest).(sortedAnnounceEntries)
	qa.annoutm.Unlock()
	if ok {
		now := time.Now()
		for _, entry := range entries {
			if entry.deadline.After(now) {
				entry.link.Send(lf)
			}
		}
	}
}

func (qa *Quanda) announce(name *Address, metric Metric, deadline time.Time, links ...*LinkHandler) {
	if len(links) == 0 {
		return
	}
	qa.annoutm.Lock()
	entries, ok := qa.announceOut.Get(name).(sortedAnnounceEntries)
	if !ok {
		entries = sortedAnnounceEntries{}
	}
	for _, link := range links {
		// this search could be optimized perhaps
		i := sort.Search(len(entries), func(i int) bool { return entries[i].link == link })
		if i == len(entries) {
			entries = append(entries, announceEntry{link, deadline, metric})
			sort.Sort(entries)
		} else if entries[i].deadline.Before(deadline) {
			qa.log.Printf("Announceout for %s deadline extended to %s", name, deadline.Format(timeLayout))
			entries[i].deadline = deadline
		}
	}
	qa.announceOut.Put(name, entries)
	qa.annoutm.Unlock()

	lf := &pb.LinkFrame{Type: &pb.LinkFrame_Announce{Announce: NewAnnounceFrame(name, metric, deadline).Proto()}}
	for _, link := range links {
		link.Send(lf)
	}
}

func (qa *Quanda) HandleAnnounce(announce *AnnounceFrame, handler *LinkHandler) {
	name := announce.Name()
	// The announce can be online, positive or zero hops, or offline, negative hops
	qa.anninm.Lock()
	var entries sortedAnnounceEntries
	if announce.Metric() == MetricOffline {
		// dest offline case
		entries = qa.announceIn[name.Data()]
		i := sort.Search(len(entries), func(i int) bool { return entries[i].link == handler })
		if i < len(entries) {
			entries[i] = entries[len(entries)-1]
			entries = entries[:len(entries)-1]
			sort.Sort(entries)
			qa.announceIn[name.Data()] = entries
		}
		qa.anninm.Unlock()
		qa.router.DestinationOffline(name, handler)
		return
	}
	// dest online case
	// Extend deadline if valid
	if announce.Deadline().After(time.Now()) {
		entries = qa.announceIn[name.Data()]
		i := sort.Search(len(entries), func(i int) bool { return entries[i].link == handler })
		if i < len(entries) {
			qa.log.Printf("Announcein for %s deadline extended to %s", name, announce.Deadline().Format(timeLayout))
			entries[i].deadline = announce.Deadline()
		} else {
			entries = append(entries, announceEntry{handler, announce.Deadline(), announce.Metric()})
			sort.Sort(entries)
		}
		qa.announceIn[name.Data()] = entries
	}
	qa.anninm.Unlock()

	metric := announce.Metric()
	metric.Hops += 1
	qa.router.DestinationUpdate(announce.Name(), handler, metric)
}

func (qa *Quanda) HandleQuery(query *QueryFrame, handler *LinkHandler) {
	if query.Deadline().Before(time.Now()) {
		return
	}
	d, _, metric := qa.router.GetNearest(query.name)
	if d >= 0 {
		foundName := query.name.Slice(0, d)
		qa.log.Printf("Responding to query from %s for %s with %s", handler.String(), query.Name().String(), foundName)
		qa.announce(foundName, metric, time.Now().Add(2*baseAnnounceTimeout), handler)
		return
	}
	// Cap deadline to our max to avoid excessive deadlines from remote
	deadline := query.Deadline()
	maxDeadline := time.Now().Add(maxQueryTimeout)
	if deadline.After(maxDeadline) {
		deadline = maxDeadline
	}
	qa.log.Printf("Forwarding query from %s for %s", handler.String(), query.Name().String())
	qa.runQuery(query.Name(), deadline, handler)
}

func (qa *Quanda) runQuery(dest *Address, deadline time.Time, linkHandler *LinkHandler) *runningQuery {
	var queryId uint64 = HANDLER_UNUSED_ID
	if linkHandler != nil {
		queryId = linkHandler.ID()
	}
	qa.querym.Lock()
	_, n := qa.queries.GetNearest(dest)
	rq, ok := n.(*runningQuery)
	if !ok {
		// no query active, create a new one
		qa.log.Printf("Starting query for %s", dest.String())
		rq = NewRunningQuery()
		qa.queries.Put(dest, rq)
		qa.querym.Unlock()
		// and broadcast query
		qf := &pb.LinkFrame{Type: &pb.LinkFrame_Query{Query: NewQueryFrame(dest, deadline).Proto()}}
		handlers := qa.router.GetAll()
		for _, h := range handlers {
			lh, ok := h.(*LinkHandler)
			if ok && lh.id != queryId {
				lh.Send(qf)
			}
		}
		time.AfterFunc(time.Until(deadline), func() {
			qa.querym.Lock()
			if rq.doneErr == nil && rq.doneHandler == nil {
				qa.log.Printf("Query for %s expired", dest.String())
				rq.fail(fmt.Errorf("query deadline exceeded"))
				qa.queries.Remove(dest)
			}
			qa.querym.Unlock()
		})
	} else if linkHandler != nil {
		rq.addQuerier(linkHandler)
		qa.querym.Unlock()
	} else {
		qa.querym.Unlock()
	}
	return rq
}

func (qa *Quanda) OpenQueryNames() (openQueries []*Address) {
	qa.querym.Lock()
	openQueries = make([]*Address, 0, qa.queries.Size())
	qa.queries.IterateSubtree(BlankAddress, func(pme PrefixMapEntry) bool {
		if pme.Value != nil {
			openQueries = append(openQueries, pme.Key)
		}
		return true
	})
	qa.querym.Unlock()
	return
}

type announceEntry struct {
	link     *LinkHandler
	deadline time.Time
	metric   Metric
}

type sortedAnnounceEntries []announceEntry

func (h sortedAnnounceEntries) Len() int {
	return len(h)
}

func (h sortedAnnounceEntries) Less(i, j int) bool {
	return h[i].link.ID() < h[j].link.ID()
}

func (h sortedAnnounceEntries) Swap(i, j int) {
	t := h[i]
	h[i] = h[j]
	h[j] = t
}

type runningQuery struct {
	queriers    []*LinkHandler
	deadline    time.Time
	done        chan struct{}
	doneName    *Address
	doneHandler Handler
	doneErr     error
}

func NewRunningQuery() *runningQuery {
	return &runningQuery{make([]*LinkHandler, 0), time.Now().Add(maxQueryTimeout), make(chan struct{}), nil, nil, nil}
}

func (rq *runningQuery) fail(err error) {
	rq.doneErr = err
	close(rq.done)
}

func (rq *runningQuery) solve(name *Address, handler Handler) []*LinkHandler {
	rq.doneName = name
	rq.doneHandler = handler
	close(rq.done)
	return rq.queriers
}

func (rq *runningQuery) addQuerier(link *LinkHandler) {
	rq.queriers = append(rq.queriers, link)
}

func (rq *runningQuery) await(ctx context.Context) (doneAddr *Address, doneHandler Handler, err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-rq.done:
		if rq.doneErr != nil {
			err = rq.doneErr
		} else {
			doneAddr, doneHandler = rq.doneName, rq.doneHandler
		}
	}
	return
}
