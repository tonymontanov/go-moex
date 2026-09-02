package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/tonymontanov/go-moex/internal/pcap"
	"github.com/tonymontanov/go-moex/internal/simba"
)

/*
verify — the correctness oracle.

A deliberately naive reference model (maps, no zero-alloc tricks) is fed
the Incremental and Snapshot feeds in capture order and checked two ways:

 1. Snapshot oracle (spec §4.2.5). For every instrument the first complete
    OrderBookSnapshot seeds the book; buffered incrementals with
    MsgSeqNum > LastMsgSeqNumProcessed are then applied. Every LATER
    snapshot of the same instrument must equal the book after applying all
    incrementals with MsgSeqNum ≤ its LastMsgSeqNumProcessed. Any
    difference means the model (or our reading of the spec) is wrong.

 2. BestPrices oracle (spec §1.4.5). BestPrices precedes the order changes
    of a transaction and states the resulting best bid/ask and the total
    size at those prices. After the transaction ends (EndOfTransaction
    flag) the live book's top of book must match.

Two books per instrument: `lazy` is only advanced to the point a snapshot
asks about (so it can be compared against the past), `live` is advanced
immediately (for BestPrices).
*/

type refOrder struct {
	side simba.MDEntryType
	px   int64
	size int64
}

type refBook map[int64]refOrder

func (b refBook) clone() refBook {
	var out refBook = make(refBook, len(b))
	for k, v := range b {
		out[k] = v
	}
	return out
}

// best returns the best price and aggregate size on side; ok=false if empty.
func (b refBook) best(side simba.MDEntryType) (px int64, size int64, ok bool) {
	for _, o := range b {
		if o.side != side {
			continue
		}
		if !ok || (side == simba.MDEntryTypeBid && o.px > px) || (side == simba.MDEntryTypeOffer && o.px < px) {
			px, size, ok = o.px, o.size, true
		} else if o.px == px {
			size += o.size
		}
	}
	return
}

type incMsg struct {
	msgSeq uint32
	rpt    uint32
	exec   bool
	action simba.MDUpdateAction
	side   simba.MDEntryType
	id     int64
	px     int64
	size   int64
	flags  uint64
}

type applyStats struct {
	applied         uint64
	technical       uint64 // OrderExecution with null price — consumes RptSeq, no book effect.
	unknownDelete   uint64 // Delete for an order not in the book.
	unknownChange   uint64 // Execution Change for an order not in the book.
	rptAnomalies    uint64 // rpt != last+1 (rpt==0 unsequenced rebroadcast excluded).
	unsequenced     uint64 // rpt == 0.
	changeSizeGrew  uint64 // execution Change that increased size (should never).
	nonQuoteInBook  uint64 // NonQuote orders inserted (kept; spec says filter them).
	activeNotInBook uint64 // ActiveSide execution referencing an order never seen as OrderUpdate.
}

func (b refBook) apply(m *incMsg, st *applyStats, lastRpt *uint32, haveRpt *bool) {
	if m.rpt == 0 {
		st.unsequenced++
	} else {
		if *haveRpt && m.rpt != *lastRpt+1 {
			st.rptAnomalies++
		}
		*lastRpt, *haveRpt = m.rpt, true
	}
	st.applied++
	if m.exec {
		if m.px == simba.NullDecimalMantissa {
			st.technical++
			return
		}
		var old refOrder
		var ok bool
		old, ok = b[m.id]
		switch m.action {
		case simba.MDUpdateActionChange:
			if !ok {
				st.unknownChange++
				if m.flags&mdFlagActiveSide != 0 {
					st.activeNotInBook++
				}
				return
			}
			if m.size > old.size {
				st.changeSizeGrew++
			}
			old.size = m.size
			b[m.id] = old
		case simba.MDUpdateActionDelete:
			if !ok {
				st.unknownDelete++
				if m.flags&mdFlagActiveSide != 0 {
					st.activeNotInBook++
				}
				return
			}
			delete(b, m.id)
		default:
			// New execution with a price: not described by the spec; treat as upsert.
			b[m.id] = refOrder{side: m.side, px: m.px, size: m.size}
		}
		return
	}
	switch m.action {
	case simba.MDUpdateActionDelete:
		if _, ok := b[m.id]; !ok {
			st.unknownDelete++
			return
		}
		delete(b, m.id)
	default:
		if m.flags&mdFlagNonQuote != 0 {
			st.nonQuoteInBook++
		}
		b[m.id] = refOrder{side: m.side, px: m.px, size: m.size}
	}
}

type snapAsm struct {
	lastMsgSeq uint32
	rpt        uint32
	session    uint32
	entries    []refEntry
	fragments  int
	inconsist  bool
}

type refEntry struct {
	id   int64
	side simba.MDEntryType
	px   int64
	size int64
	nq   bool
}

type bpExpect struct {
	bidPx, askPx     int64
	bidSize, askSize int64
	msgSeq           uint32
}

type instrument struct {
	id      int32
	lazy    refBook
	live    refBook
	synced  bool
	pending []incMsg
	asm     *snapAsm

	lazyStats, liveStats applyStats
	lazyRpt, liveRpt     uint32
	lazyHave, liveHave   bool

	snapshots  int
	compares   int
	mismatches int
	rptMism    int // snapshot RptSeq != lazy book RptSeq after applying ≤ L.
	syncRptGap int // first message after sync has rpt != snapshot.rpt+1.

	bp         *bpExpect
	bpChecks   int
	bpMism     int
	bpSkipTx   uint64
	pendingMax int
}

type verifyState struct {
	opts      runOptions
	inc, snap *channel
	incSeqNow uint32
	instr     map[int32]*instrument
	order     []int32 // instrument ids in first-seen order (for -max-instruments).

	emptyBooks, seqResets int
	firstIncSeq           uint32 // first Incremental MsgSeqNum in the capture.
	afterEmptyBook        bool   // an EmptyBook was observed: books are defined by the Incremental feed alone from here on.
	deferredSyncs         uint64 // snapshots skipped because their L predates the capture (unverifiable).
	snapRptZero           uint64 // completed snapshots with RptSeq == 0.
	orphanFrags           uint64
	snapInconsistent      uint64
	mismatchExamples      int
	bpExamples            int
	firstTS, lastTS       time.Time
	eotPending            map[int32]struct{} // instruments with an outstanding BP expectation.
}

func runVerify(rd *pcap.Reader, opts runOptions) error {
	var vs *verifyState
	var err error
	vs, err = verifyCapture(rd, opts)
	if err != nil {
		return err
	}
	vs.report()
	return nil
}

// verifyCapture runs the oracle and returns its state (tests assert on it).
func verifyCapture(rd *pcap.Reader, opts runOptions) (*verifyState, error) {
	var vs *verifyState = &verifyState{
		opts:       opts,
		inc:        newChannel("incremental " + opts.inc),
		snap:       newChannel("snapshot " + opts.snap),
		instr:      make(map[int32]*instrument),
		eotPending: make(map[int32]struct{}),
	}
	var err error = forEachPacket(rd, opts.maxPackets, func(pkt pcap.Packet) error {
		if vs.firstTS.IsZero() {
			vs.firstTS = pkt.TS
		}
		vs.lastTS = pkt.TS
		switch addrOf(pkt) {
		case opts.inc:
			vs.onIncremental(pkt)
		case opts.snap:
			vs.onSnapshot(pkt)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return vs, nil
}

// totals aggregates per-instrument counters.
func (vs *verifyState) totals() (compares, mismatches, bpChecks, bpMism, rptMism, syncRptGap int, lazy, live applyStats) {
	for _, in := range vs.instr {
		compares += in.compares
		mismatches += in.mismatches
		bpChecks += in.bpChecks
		bpMism += in.bpMism
		rptMism += in.rptMism
		syncRptGap += in.syncRptGap
		addStats(&lazy, &in.lazyStats)
		addStats(&live, &in.liveStats)
	}
	return
}

func (vs *verifyState) get(sec int32) *instrument {
	var in *instrument = vs.instr[sec]
	if in == nil {
		if vs.opts.maxInstr > 0 && len(vs.instr) >= vs.opts.maxInstr {
			return nil
		}
		in = &instrument{id: sec, lazy: make(refBook), live: make(refBook)}
		// After an EmptyBook the Incremental feed alone defines every book
		// (spec §4.2.8): an instrument first seen now starts synced-empty.
		in.synced = vs.afterEmptyBook
		vs.instr[sec] = in
		vs.order = append(vs.order, sec)
	}
	return in
}

func (vs *verifyState) onIncremental(pkt pcap.Packet) {
	var seq uint32
	var eot bool
	var info simba.PacketInfo
	var err error
	info, err = walkDecoded(pkt.Payload, func(i int, hdr simba.SBEHeader, d *simba.Decoded) {
		if i == 0 {
			// Header is known after Walk returns, but we need seq per message; read it up front.
			seq = simba.PacketHeaderSeq(pkt.Payload)
		}
		switch {
		case d.OrderUpdate != nil:
			var u *simba.OrderUpdate = d.OrderUpdate
			vs.push(u.SecurityID, incMsg{msgSeq: seq, rpt: u.RptSeq, action: u.MDUpdateAction, side: u.MDEntryType, id: u.MDEntryID, px: u.MDEntryPx, size: u.MDEntrySize, flags: u.MDFlags})
			if u.MDFlags&mdFlagEndOfTransaction != 0 {
				eot = true
			}
		case d.OrderExecution != nil:
			var e *simba.OrderExecution = d.OrderExecution
			vs.push(e.SecurityID, incMsg{msgSeq: seq, rpt: e.RptSeq, exec: true, action: e.MDUpdateAction, side: e.MDEntryType, id: e.MDEntryID, px: e.MDEntryPx, size: e.MDEntrySize, flags: e.MDFlags})
			if e.MDFlags&mdFlagEndOfTransaction != 0 {
				eot = true
			}
		case d.BestPrices != nil:
			for _, bp := range d.BestPrices.Entries {
				var in *instrument = vs.get(bp.SecurityID)
				if in == nil {
					continue
				}
				if in.bp != nil {
					in.bpSkipTx++ // previous expectation never closed by an EOT.
				}
				in.bp = &bpExpect{bidPx: bp.MktBidPx, askPx: bp.MktOfferPx, bidSize: bp.MktBidSize, askSize: bp.MktOfferSize, msgSeq: seq}
				vs.eotPending[bp.SecurityID] = struct{}{}
			}
		case d.EmptyBook != nil:
			vs.emptyBooks++
			vs.afterEmptyBook = true
			for _, in := range vs.instr {
				in.lazy = make(refBook)
				in.live = make(refBook)
				in.pending = in.pending[:0]
				in.lazyHave, in.liveHave = false, false
				in.synced = true // books are rebuilt from the OrderUpdate rebroadcast, not from a snapshot.
			}
		case d.NewSeqNo != nil:
			vs.seqResets++
			for _, in := range vs.instr {
				in.lazy = make(refBook)
				in.live = make(refBook)
				in.pending = in.pending[:0]
				in.synced = false
				in.asm = nil
				in.lazyHave, in.liveHave = false, false
			}
		}
	})
	if err != nil {
		vs.inc.walkErrors++
		return
	}
	if vs.firstIncSeq == 0 {
		vs.firstIncSeq = info.Header.MsgSeqNum
	}
	vs.inc.observe(info.Header.MsgSeqNum, pkt.TS)
	vs.incSeqNow = info.Header.MsgSeqNum
	if eot {
		vs.checkBestPrices(pkt)
	}
}

func (vs *verifyState) push(sec int32, m incMsg) {
	var in *instrument = vs.get(sec)
	if in == nil {
		return
	}
	in.pending = append(in.pending, m)
	if len(in.pending) > in.pendingMax {
		in.pendingMax = len(in.pending)
	}
	if in.synced {
		in.live.apply(&m, &in.liveStats, &in.liveRpt, &in.liveHave)
	}
}

// checkBestPrices compares every outstanding BestPrices expectation with
// the live book once a transaction has ended.
func (vs *verifyState) checkBestPrices(pkt pcap.Packet) {
	for sec := range vs.eotPending {
		var in *instrument = vs.instr[sec]
		delete(vs.eotPending, sec)
		if in == nil || in.bp == nil {
			continue
		}
		var exp *bpExpect = in.bp
		in.bp = nil
		if !in.synced {
			continue
		}
		in.bpChecks++
		var bidPx, bidSize, askPx, askSize int64
		var haveBid, haveAsk bool
		bidPx, bidSize, haveBid = in.live.best(simba.MDEntryTypeBid)
		askPx, askSize, haveAsk = in.live.best(simba.MDEntryTypeOffer)
		var expBid bool = exp.bidPx != simba.NullDecimalMantissa
		var expAsk bool = exp.askPx != simba.NullDecimalMantissa
		var ok bool = (haveBid == expBid) && (haveAsk == expAsk)
		if ok && haveBid && (bidPx != exp.bidPx || bidSize != exp.bidSize) {
			ok = false
		}
		if ok && haveAsk && (askPx != exp.askPx || askSize != exp.askSize) {
			ok = false
		}
		if !ok {
			in.bpMism++
			if vs.bpExamples < vs.opts.examples || vs.opts.verbose {
				vs.bpExamples++
				fmt.Printf("[BestPrices mismatch] sec=%d ts=%s bpSeq=%d eotSeq=%d expected bid=%d/%d ask=%d/%d  live bid=%d/%d(%v) ask=%d/%d(%v) orders=%d\n",
					sec, pkt.TS.Format("15:04:05.000000"), exp.msgSeq, vs.incSeqNow, exp.bidPx, exp.bidSize, exp.askPx, exp.askSize,
					bidPx, bidSize, haveBid, askPx, askSize, haveAsk, len(in.live))
			}
		}
	}
}

func (vs *verifyState) onSnapshot(pkt pcap.Packet) {
	var snap *simba.OrderBookSnapshot
	var info simba.PacketInfo
	var err error
	info, err = walkDecoded(pkt.Payload, func(i int, hdr simba.SBEHeader, d *simba.Decoded) {
		if d.OrderBookSnapshot != nil {
			snap = d.OrderBookSnapshot
		}
	})
	if err != nil {
		vs.snap.walkErrors++
		return
	}
	vs.snap.observe(info.Header.MsgSeqNum, pkt.TS)
	if snap == nil {
		return
	}
	var in *instrument = vs.get(snap.SecurityID)
	if in == nil {
		return
	}
	var h simba.PacketHeader = info.Header
	if h.IsStartOfSnapshot() {
		in.asm = &snapAsm{lastMsgSeq: snap.LastMsgSeqNumProcessed, rpt: snap.RptSeq, session: snap.ExchangeTradingSessionID}
	}
	if in.asm == nil {
		vs.orphanFrags++
		return
	}
	var a *snapAsm = in.asm
	a.fragments++
	if a.lastMsgSeq != snap.LastMsgSeqNumProcessed || a.rpt != snap.RptSeq {
		a.inconsist = true
	}
	for _, e := range snap.Entries {
		if e.MDEntryType == simba.MDEntryTypeEmptyBook || e.MDEntryID == simba.NullInt64 {
			continue
		}
		a.entries = append(a.entries, refEntry{id: e.MDEntryID, side: e.MDEntryType, px: e.MDEntryPx, size: e.MDEntrySize, nq: e.MDFlags&mdFlagNonQuote != 0})
	}
	if h.IsEndOfSnapshot() {
		in.asm = nil
		if a.inconsist {
			vs.snapInconsistent++
		}
		vs.completeSnapshot(in, a, pkt)
	}
}

func (vs *verifyState) completeSnapshot(in *instrument, a *snapAsm, pkt pcap.Packet) {
	in.snapshots++
	var L uint32 = a.lastMsgSeq
	if a.rpt == 0 {
		vs.snapRptZero++
	}
	if !in.synced {
		if vs.firstIncSeq == 0 || L < vs.firstIncSeq {
			// The snapshot was taken before the capture started: the
			// incrementals between L and the first captured packet are
			// lost, so a book seeded from it cannot be verified. Wait for
			// the next cycle.
			vs.deferredSyncs++
			return
		}
		in.lazy = bookFromEntries(a.entries)
		in.lazyRpt, in.lazyHave = a.rpt, true
		var first bool = true
		var kept int
		for _, m := range in.pending {
			if m.msgSeq <= L {
				continue
			}
			if first {
				first = false
				if m.rpt != 0 && m.rpt != a.rpt+1 {
					in.syncRptGap++
				}
			}
			in.lazy.apply(&m, &in.lazyStats, &in.lazyRpt, &in.lazyHave)
			kept++
		}
		in.pending = in.pending[:0]
		in.live = in.lazy.clone()
		in.liveRpt, in.liveHave = in.lazyRpt, in.lazyHave
		in.liveStats = applyStats{}
		in.synced = true
		return
	}

	// Advance lazy book to L, then compare.
	var i int
	for i < len(in.pending) && in.pending[i].msgSeq <= L {
		in.lazy.apply(&in.pending[i], &in.lazyStats, &in.lazyRpt, &in.lazyHave)
		i++
	}
	in.pending = append(in.pending[:0], in.pending[i:]...)
	in.compares++
	if in.lazyHave && in.lazyRpt != a.rpt {
		in.rptMism++
	}
	var missing, extra, differ []int64
	var want refBook = bookFromEntries(a.entries)
	for id, w := range want {
		var got refOrder
		var ok bool
		got, ok = in.lazy[id]
		if !ok {
			missing = append(missing, id)
		} else if got != w {
			differ = append(differ, id)
		}
	}
	for id := range in.lazy {
		if _, ok := want[id]; !ok {
			extra = append(extra, id)
		}
	}
	if len(missing)+len(extra)+len(differ) == 0 {
		return
	}
	in.mismatches++
	if vs.mismatchExamples < vs.opts.examples || vs.opts.verbose {
		vs.mismatchExamples++
		fmt.Printf("[Snapshot mismatch] sec=%d ts=%s L=%d snapRpt=%d bookRpt=%d snapOrders=%d bookOrders=%d missing=%d extra=%d differ=%d\n",
			in.id, pkt.TS.Format("15:04:05.000000"), L, a.rpt, in.lazyRpt, len(want), len(in.lazy), len(missing), len(extra), len(differ))
		sort.Slice(missing, func(x, y int) bool { return missing[x] < missing[y] })
		for k, id := range missing {
			if k >= 3 {
				break
			}
			var w refOrder = want[id]
			fmt.Printf("    missing id=%d side=%c px=%d sz=%d\n", id, w.side, w.px, w.size)
		}
		for k, id := range extra {
			if k >= 3 {
				break
			}
			var g refOrder = in.lazy[id]
			fmt.Printf("    extra   id=%d side=%c px=%d sz=%d\n", id, g.side, g.px, g.size)
		}
		for k, id := range differ {
			if k >= 3 {
				break
			}
			fmt.Printf("    differ  id=%d book=%+v snap=%+v\n", id, in.lazy[id], want[id])
		}
	}
	// Resync from the snapshot so one divergence does not cascade.
	in.lazy = want
	in.lazyRpt, in.lazyHave = a.rpt, true
	for k := range in.pending {
		in.lazy.apply(&in.pending[k], &in.lazyStats, &in.lazyRpt, &in.lazyHave)
	}
	in.pending = in.pending[:0]
	in.live = in.lazy.clone()
	in.liveRpt, in.liveHave = in.lazyRpt, in.lazyHave
}

func bookFromEntries(entries []refEntry) refBook {
	var b refBook = make(refBook, len(entries))
	for _, e := range entries {
		b[e.id] = refOrder{side: e.side, px: e.px, size: e.size}
	}
	return b
}

func (vs *verifyState) report() {
	fmt.Println("\n== channels ==")
	fmt.Println(vs.inc.summary())
	fmt.Println(vs.snap.summary())
	fmt.Printf("window: %s .. %s (%s)\n", vs.firstTS.Format("15:04:05.000"), vs.lastTS.Format("15:04:05.000"), vs.lastTS.Sub(vs.firstTS).Round(time.Millisecond))
	fmt.Printf("EmptyBook=%d SequenceReset=%d orphan snapshot fragments=%d inconsistent snapshots=%d deferred syncs (snapshot older than capture)=%d snapshots with RptSeq=0: %d\n",
		vs.emptyBooks, vs.seqResets, vs.orphanFrags, vs.snapInconsistent, vs.deferredSyncs, vs.snapRptZero)

	var total, synced, withCompares, mismatched, bpMismatched int
	var compares, mismatches, bpChecks, bpMism, rptMism, syncRptGap int
	var lazy, live applyStats
	var pendingMax int
	var snapOrders int
	for _, in := range vs.instr {
		total++
		if in.synced {
			synced++
			snapOrders += len(in.live)
		}
		if in.compares > 0 {
			withCompares++
		}
		if in.mismatches > 0 {
			mismatched++
		}
		if in.bpMism > 0 {
			bpMismatched++
		}
		compares += in.compares
		mismatches += in.mismatches
		bpChecks += in.bpChecks
		bpMism += in.bpMism
		rptMism += in.rptMism
		syncRptGap += in.syncRptGap
		addStats(&lazy, &in.lazyStats)
		addStats(&live, &in.liveStats)
		if in.pendingMax > pendingMax {
			pendingMax = in.pendingMax
		}
	}
	fmt.Println("\n== reference model ==")
	fmt.Printf("instruments: seen=%d synced=%d with_compares=%d   resting orders in live books at end=%d   max pending per instrument=%d\n", total, synced, withCompares, snapOrders, pendingMax)
	fmt.Printf("snapshot oracle: compares=%d mismatches=%d (instruments affected=%d)   snapshot.RptSeq != book RptSeq at L: %d   first-after-sync RptSeq gaps: %d\n",
		compares, mismatches, mismatched, rptMism, syncRptGap)
	fmt.Printf("bestprices oracle: checks=%d mismatches=%d (instruments affected=%d)\n", bpChecks, bpMism, bpMismatched)
	fmt.Printf("apply (lazy books): %s\n", formatApply(&lazy))
	fmt.Printf("apply (live books): %s\n", formatApply(&live))
}

func addStats(dst, src *applyStats) {
	dst.applied += src.applied
	dst.technical += src.technical
	dst.unknownDelete += src.unknownDelete
	dst.unknownChange += src.unknownChange
	dst.rptAnomalies += src.rptAnomalies
	dst.unsequenced += src.unsequenced
	dst.changeSizeGrew += src.changeSizeGrew
	dst.nonQuoteInBook += src.nonQuoteInBook
	dst.activeNotInBook += src.activeNotInBook
}

func formatApply(s *applyStats) string {
	return fmt.Sprintf("applied=%d technical=%d unknownDelete=%d unknownChange=%d activeNotInBook=%d rptAnomalies=%d unsequenced(rpt=0)=%d changeSizeGrew=%d nonQuoteInserted=%d",
		s.applied, s.technical, s.unknownDelete, s.unknownChange, s.activeNotInBook, s.rptAnomalies, s.unsequenced, s.changeSizeGrew, s.nonQuoteInBook)
}
