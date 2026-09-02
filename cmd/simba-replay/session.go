package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/tonymontanov/go-moex/forts"
	"github.com/tonymontanov/go-moex/internal/pcap"
	"github.com/tonymontanov/go-moex/internal/simba"
	"github.com/tonymontanov/go-moex/orderbook"
)

/*
session — drive the PRODUCTION forts.BookSession over a capture for every
instrument and check it the same two ways verify checks the reference
model: each snapshot published after an instrument went live must equal
the session's engine at LastMsgSeqNumProcessed, and BestPrices must match
the engine's top of book after each transaction.

Snapshots describe the book at L, which is up to ~50k packets behind the
live feed, so incremental packets are delivered lazily: before a snapshot
packet with L is handed to the session, every incremental packet with
MsgSeqNum <= L is delivered and nothing beyond. That is a legal ordering
for a client and lets the engine be compared at exactly L. Memory: the
whole capture's incremental payloads are held (~160 MB for 6 minutes of
main session); -max-packets limits it.
*/

func runSession(rd *pcap.Reader, opts runOptions) error {
	// Load both channels in capture order.
	var inc []fixturePacket
	var snap []fixturePacket
	var firstIncSeq uint32
	var err error = forEachPacket(rd, opts.maxPackets, func(pkt pcap.Packet) error {
		var fp fixturePacket = fixturePacket{seq: simba.PacketHeaderSeq(pkt.Payload), payload: append([]byte{}, pkt.Payload...)}
		switch addrOf(pkt) {
		case opts.inc:
			if firstIncSeq == 0 {
				firstIncSeq = fp.seq
			}
			inc = append(inc, fp)
		case opts.snap:
			snap = append(snap, fp)
		}
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("loaded incremental=%d snapshot=%d packets\n", len(inc), len(snap))

	var rng *rand.Rand = rand.New(rand.NewSource(opts.seed))
	var feedCfg forts.FeedConfig = forts.FeedConfig{GapTimeout: 5 * time.Millisecond}
	var replayer *captureReplayer
	if opts.replay {
		replayer = &captureReplayer{packets: make(map[uint32][]byte, len(inc))}
		for _, fp := range inc {
			replayer.packets[fp.seq] = fp.payload
		}
		feedCfg.Replayer = replayer
	}
	var session *forts.BookSession = forts.NewBookSession(forts.BookSessionConfig{Feed: feedCfg})
	// deliver hands one incremental datagram to the session through the
	// simulated legs: each leg drops it with probability opts.loss; with
	// -ab the packet is offered twice in random order.
	var dropped, offered int
	var droppedSeqs map[uint32]bool = make(map[uint32]bool) // lost on every leg
	var deliver = func(payload []byte) {
		var legs int = 1
		if opts.ab {
			legs = 2
		}
		var sent int
		for i := 0; i < legs; i++ {
			offered++
			if opts.loss > 0 && rng.Float64() < opts.loss {
				dropped++
				continue
			}
			session.HandleIncrementalPacket(payload)
			sent++
		}
		if sent == 0 && legs > 0 && opts.loss > 0 {
			droppedSeqs[simba.PacketHeaderSeq(payload)] = true
		}
	}
	// settle waits until the feed layer has nothing held back (gap timers
	// and replays have run), so the engine is exactly at the delivered seq.
	var settle = func() {
		var deadline time.Time = time.Now().Add(5 * time.Second)
		for session.FeedPending() > 0 && time.Now().Before(deadline) {
			time.Sleep(200 * time.Microsecond)
		}
	}
	var engines map[int32]*orderbook.Engine = make(map[int32]*orderbook.Engine)
	var engineOf = func(sec int32) *orderbook.Engine {
		var e *orderbook.Engine = engines[sec]
		if e == nil {
			if opts.maxInstr > 0 && len(engines) >= opts.maxInstr {
				return nil
			}
			e = session.Subscribe(sec)
			engines[sec] = e
		}
		return e
	}
	// Subscribe every instrument that appears in a snapshot or update.
	for _, sp := range snap {
		if _, v, ok := snapshotView(sp.payload); ok {
			engineOf(v.SecurityID)
		}
	}

	var next int
	var pendingBP map[int32]bpExpect = make(map[int32]bpExpect)
	var bpChecks, bpMism, compares, mismatches, deferred, unverifiable int
	// caughtUp reports whether the session has processed every packet up
	// to and including seq. With simulated loss a dropped packet is only
	// detectable once a later one arrives, which lazy delivery withholds —
	// comparing then would blame the session for an unknowable gap.
	var caughtUp = func(seq uint32) bool {
		var exp uint32
		var ok bool
		exp, ok = session.FeedExpected()
		return ok && exp == seq+1
	}
	var examples int
	var deliverInc = func(upTo uint32) {
		for next < len(inc) && (upTo == 0 || inc[next].seq <= upTo) {
			var eot bool
			var p simba.Packet
			p, err = simba.ParsePacket(inc[next].payload, 0)
			if err == nil {
				for {
					var m simba.Message
					var ok bool
					m, ok, err = p.Next()
					if err != nil || !ok {
						break
					}
					switch m.Kind {
					case simba.KindBestPrices:
						if v, ok := m.BestPrices(); ok {
							for i := 0; i < v.Len(); i++ {
								var e simba.BestPricesEntry = v.Entry(i)
								pendingBP[e.SecurityID] = bpExpect{bidPx: e.MktBidPx, askPx: e.MktOfferPx, bidSize: e.MktBidSize, askSize: e.MktOfferSize, msgSeq: inc[next].seq}
							}
						}
					case simba.KindOrderUpdate:
						if u, ok := m.OrderUpdate(); ok {
							engineOf(u.SecurityID)
							if u.MDFlags&mdFlagEndOfTransaction != 0 {
								eot = true
							}
						}
					case simba.KindOrderExecution:
						if e, ok := m.OrderExecution(); ok {
							engineOf(e.SecurityID)
							if e.MDFlags&mdFlagEndOfTransaction != 0 {
								eot = true
							}
						}
					}
				}
			}
			deliver(inc[next].payload)
			next++
			if !eot {
				continue
			}
			if opts.loss > 0 {
				settle()
			}
			if !caughtUp(inc[next-1].seq) {
				// Expectations of transactions we cannot verify must not
				// leak into later checks.
				for sec := range pendingBP {
					delete(pendingBP, sec)
				}
				unverifiable++
				continue
			}
			for sec, e := range pendingBP {
				delete(pendingBP, sec)
				var engine *orderbook.Engine = engines[sec]
				if engine == nil {
					continue
				}
				if st, _ := session.State(sec); st != forts.BookLive {
					continue
				}
				if !opts.replay {
					// Without replay a packet lost on every leg is gone; an
					// instrument whose only update of this transaction was in
					// it cannot know yet (no later RptSeq to jump) — the
					// snapshot repair fixes it a cycle later. Not verifiable here.
					var lost bool
					for q := e.msgSeq; q <= inc[next-1].seq; q++ {
						if droppedSeqs[q] {
							lost = true
						}
					}
					if lost {
						unverifiable++
						continue
					}
				}
				bpChecks++
				var bidPx, bidSize, askPx, askSize int64
				var haveBid, haveAsk bool
				bidPx, bidSize, haveBid = engine.BestBid()
				askPx, askSize, haveAsk = engine.BestAsk()
				var expBid bool = e.bidPx != simba.NullDecimalMantissa
				var expAsk bool = e.askPx != simba.NullDecimalMantissa
				var ok bool = haveBid == expBid && haveAsk == expAsk
				if ok && haveBid && (bidPx != e.bidPx || bidSize != e.bidSize) {
					ok = false
				}
				if ok && haveAsk && (askPx != e.askPx || askSize != e.askSize) {
					ok = false
				}
				if !ok {
					bpMism++
					if examples < opts.examples || opts.verbose {
						examples++
						var lost []uint32
						var lastRpt uint32
						for q := e.msgSeq; q <= inc[next-1].seq; q++ {
							if droppedSeqs[q] {
								lost = append(lost, q)
							}
						}
						// RptSeq of the last message for sec in the transaction's packets.
						for k := next - 1; k >= 0 && inc[k].seq >= e.msgSeq; k-- {
							lastRpt = lastRptFor(inc[k].payload, sec)
							if lastRpt != 0 {
								break
							}
						}
						var engSeq, _ = engine.LastSeq()
						var expSeq, _ = session.FeedExpected()
						fmt.Printf("    engine rpt=%d wire last rpt=%d feed expected=%d state=%v\n", engSeq, lastRpt, expSeq, func() forts.BookState { st, _ := session.State(sec); return st }())
						fmt.Printf("[BestPrices mismatch] sec=%d bpSeq=%d eotSeq=%d dropped=%v expected bid=%d/%d ask=%d/%d  engine bid=%d/%d(%v) ask=%d/%d(%v)\n",
							sec, e.msgSeq, inc[next-1].seq, lost, e.bidPx, e.bidSize, e.askPx, e.askSize, bidPx, bidSize, haveBid, askPx, askSize, haveAsk)
					}
				}
			}
		}
	}

	type partialSnap struct {
		L, R   uint32
		orders map[int64]refOrder
	}
	var partial map[int32]*partialSnap = make(map[int32]*partialSnap)
	var wasLive map[int32]bool = make(map[int32]bool)
	for _, sp := range snap {
		var hdr simba.PacketHeader
		var v simba.SnapshotView
		var ok bool
		hdr, v, ok = snapshotView(sp.payload)
		if !ok {
			session.HandleSnapshotPacket(sp.payload)
			continue
		}
		var engine *orderbook.Engine = engines[v.SecurityID]
		if engine == nil {
			continue
		}
		if v.LastMsgSeqNumProcessed < firstIncSeq {
			// Snapshot older than the capture: neither the session nor the
			// oracle can use it (updates between L and the first packet are
			// lost). Skip it entirely — a live client would hit this only at
			// the very first cycle after start.
			deferred++
			continue
		}
		deliverInc(v.LastMsgSeqNumProcessed)
		if opts.loss > 0 {
			settle()
		}
		if hdr.IsStartOfSnapshot() {
			var st forts.BookState
			st, _ = session.State(v.SecurityID)
			wasLive[v.SecurityID] = st == forts.BookLive
			partial[v.SecurityID] = &partialSnap{L: v.LastMsgSeqNumProcessed, R: v.RptSeq, orders: make(map[int64]refOrder)}
		}
		session.HandleSnapshotPacket(sp.payload)
		var ps *partialSnap = partial[v.SecurityID]
		if ps == nil {
			continue
		}
		for i := 0; i < v.Len(); i++ {
			var e simba.OrderBookSnapshotEntry = v.Entry(i)
			if e.MDEntryType == simba.MDEntryTypeEmptyBook || e.MDEntryID == simba.NullInt64 {
				continue
			}
			ps.orders[e.MDEntryID] = refOrder{side: e.MDEntryType, px: e.MDEntryPx, size: e.MDEntrySize}
		}
		if !hdr.IsEndOfSnapshot() {
			continue
		}
		delete(partial, v.SecurityID)
		if !wasLive[v.SecurityID] {
			continue
		}
		if !caughtUp(ps.L) {
			unverifiable++
			continue
		}
		compares++
		var diff int
		var got map[int64]refOrder = make(map[int64]refOrder)
		engine.ForEachOrder(func(id int64, side orderbook.Side, px, size int64) {
			var s simba.MDEntryType = simba.MDEntryTypeOffer
			if side == orderbook.SideBid {
				s = simba.MDEntryTypeBid
			}
			got[id] = refOrder{side: s, px: px, size: size}
		})
		for id, w := range ps.orders {
			if g, ok := got[id]; !ok || g != w {
				diff++
			}
		}
		for id := range got {
			if _, ok := ps.orders[id]; !ok {
				diff++
			}
		}
		var seq uint64
		seq, _ = engine.LastSeq()
		if diff != 0 || (ps.R != 0 && seq != uint64(ps.R)) {
			mismatches++
			if examples < opts.examples || opts.verbose {
				examples++
				fmt.Printf("[Snapshot mismatch] sec=%d L=%d diffs=%d engineSeq=%d snapRpt=%d engineOrders=%d snapOrders=%d\n", v.SecurityID, ps.L, diff, seq, ps.R, len(got), len(ps.orders))
			}
		}
	}
	deliverInc(0)
	settle()

	var live, empty int
	for sec, e := range engines {
		if st, _ := session.State(sec); st == forts.BookLive {
			live++
		}
		if e.OrderCount() == 0 {
			empty++
		}
	}
	fmt.Println("\n== production BookSession ==")
	if opts.ab || opts.loss > 0 {
		fmt.Printf("simulation: legs=%d loss=%.4f offered=%d dropped=%d replay=%v\n", map[bool]int{false: 1, true: 2}[opts.ab], opts.loss, offered, dropped, opts.replay)
	}
	if replayer != nil {
		fmt.Printf("capture replayer: requests=%d packets_served=%d\n", replayer.requests, replayer.served)
	}
	fmt.Printf("instruments=%d live=%d empty_books=%d\n", len(engines), live, empty)
	fmt.Printf("snapshot oracle: compares=%d mismatches=%d skipped_older_than_capture=%d unverifiable(session not caught up at L)=%d\n", compares, mismatches, deferred, unverifiable)
	fmt.Printf("bestprices oracle: checks=%d mismatches=%d\n", bpChecks, bpMism)
	fmt.Printf("session stats: %s\n", session.Stats())
	if mismatches != 0 || bpMism != 0 {
		return fmt.Errorf("session oracle failed: %d snapshot / %d bestprices mismatches", mismatches, bpMism)
	}
	return nil
}

type fixturePacket struct {
	seq     uint32
	payload []byte
}

func snapshotView(payload []byte) (simba.PacketHeader, simba.SnapshotView, bool) {
	var p simba.Packet
	var err error
	p, err = simba.ParsePacket(payload, 0)
	if err != nil {
		return simba.PacketHeader{}, simba.SnapshotView{}, false
	}
	var m simba.Message
	var ok bool
	m, ok, err = p.Next()
	if err != nil || !ok {
		return p.Header(), simba.SnapshotView{}, false
	}
	var v simba.SnapshotView
	v, ok = m.Snapshot()
	return p.Header(), v, ok
}

// captureReplayer serves TCP-replay requests from the capture (stands in
// for the exchange's Replay service in offline runs).
type captureReplayer struct {
	packets  map[uint32][]byte
	requests int
	served   int
}

func (r *captureReplayer) Replay(ctx context.Context, beg, end uint32, h func([]byte)) (simba.ReplayResult, error) {
	r.requests++
	var res simba.ReplayResult
	for seq := beg; seq <= end; seq++ {
		if p, ok := r.packets[seq]; ok {
			h(p)
			res.Packets++
			r.served++
		}
	}
	return res, nil
}

// lastRptFor returns the RptSeq of the last OrderUpdate/OrderExecution
// for sec in the packet (0 if none).
func lastRptFor(payload []byte, sec int32) uint32 {
	var p simba.Packet
	var err error
	p, err = simba.ParsePacket(payload, 0)
	if err != nil {
		return 0
	}
	var last uint32
	for {
		var m simba.Message
		var ok bool
		m, ok, err = p.Next()
		if err != nil || !ok {
			break
		}
		if u, ok := m.OrderUpdate(); ok && u.SecurityID == sec {
			last = u.RptSeq
		}
		if e, ok := m.OrderExecution(); ok && e.SecurityID == sec {
			last = e.RptSeq
		}
	}
	return last
}
