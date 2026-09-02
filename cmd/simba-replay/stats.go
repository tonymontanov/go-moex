package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tonymontanov/go-moex/internal/pcap"
	"github.com/tonymontanov/go-moex/internal/simba"
)

// instrSeq — per-instrument RptSeq tracking on the Incremental feed.
type instrSeq struct {
	have    bool
	last    uint32
	lastNQ  bool // last message carried NonQuote.
	updates uint64
}

type snapshotCycle struct {
	start       time.Time
	instruments int
	fragments   int
	maxFrag     int
	incSeqAt    uint32 // Incremental MsgSeqNum observed when the cycle started.
}

type statsState struct {
	opts runOptions
	inc  *channel
	snap *channel

	incSeqNow uint32 // latest Incremental MsgSeqNum seen (for snapshot lag).

	templates    map[uint16]uint64
	msgsPerPkt   *histogram
	payloadBytes *histogram
	lastFrag0    uint64
	possDup      uint64
	packetsMulti uint64

	rptDelta       *histogram // RptSeq delta over all OrderUpdate/OrderExecution.
	rptDeltaNoNQ   *histogram // same, skipping NonQuote messages.
	rptGapExamples int
	instr          map[int32]*instrSeq
	instrNoNQ      map[int32]*instrSeq
	nonQuoteMsgs   uint64
	execNullPx     uint64
	execActive     uint64
	execPassive    uint64
	execActions    *histogram
	updActions     *histogram

	emptyBooks int
	seqResets  int

	// Snapshot feed.
	cycles         []*snapshotCycle
	curCycle       *snapshotCycle
	snapOpen       map[int32]int // instruments with StartOfSnapshot seen but no End yet -> fragments so far.
	snapEntries    *histogram
	snapFragments  *histogram
	snapLag        *histogram // incSeqNow - LastMsgSeqNumProcessed, bucketed by 1000.
	snapMaxLag     int64
	snapJ          uint64
	snapNullID     uint64
	snapNonQuote   uint64
	snapOrphanFrag uint64
	snapSessions   *histogram
}

func runStats(rd *pcap.Reader, opts runOptions) error {
	var st *statsState = &statsState{
		opts:          opts,
		inc:           newChannel("incremental " + opts.inc),
		snap:          newChannel("snapshot " + opts.snap),
		templates:     make(map[uint16]uint64),
		msgsPerPkt:    newHistogram(),
		payloadBytes:  newHistogram(),
		rptDelta:      newHistogram(),
		rptDeltaNoNQ:  newHistogram(),
		instr:         make(map[int32]*instrSeq),
		instrNoNQ:     make(map[int32]*instrSeq),
		execActions:   newHistogram(),
		updActions:    newHistogram(),
		snapOpen:      make(map[int32]int),
		snapEntries:   newHistogram(),
		snapFragments: newHistogram(),
		snapLag:       newHistogram(),
		snapSessions:  newHistogram(),
	}
	var err error = forEachPacket(rd, opts.maxPackets, func(pkt pcap.Packet) error {
		switch addrOf(pkt) {
		case opts.inc:
			st.onIncremental(pkt)
		case opts.snap:
			st.onSnapshot(pkt)
		}
		return nil
	})
	if err != nil {
		return err
	}
	st.report()
	return nil
}

func (st *statsState) onIncremental(pkt pcap.Packet) {
	var msgs []string
	var emptyBookIdx int = -1
	var seqResetIdx int = -1
	var info simba.PacketInfo
	var err error
	info, err = walkDecoded(pkt.Payload, func(i int, hdr simba.SBEHeader, d *simba.Decoded) {
		st.templates[hdr.TemplateID]++
		st.inc.schemas[[2]uint16{hdr.SchemaID, hdr.Version}]++
		if st.opts.verbose || st.emptyBooks < st.opts.examples || st.seqResets < st.opts.examples {
			msgs = append(msgs, describeMessage(hdr, d))
		}
		switch {
		case d.OrderUpdate != nil:
			var u *simba.OrderUpdate = d.OrderUpdate
			st.updActions.add(int64(u.MDUpdateAction))
			st.trackRpt(u.SecurityID, u.RptSeq, u.MDFlags, pkt, i, hdr, d)
		case d.OrderExecution != nil:
			var e *simba.OrderExecution = d.OrderExecution
			st.execActions.add(int64(e.MDUpdateAction))
			if e.MDEntryPx == simba.NullDecimalMantissa {
				st.execNullPx++
			}
			if e.MDFlags&mdFlagActiveSide != 0 {
				st.execActive++
			}
			if e.MDFlags&mdFlagPassiveSide != 0 {
				st.execPassive++
			}
			st.trackRpt(e.SecurityID, e.RptSeq, e.MDFlags, pkt, i, hdr, d)
		case d.EmptyBook != nil:
			emptyBookIdx = i
		case d.NewSeqNo != nil:
			seqResetIdx = i
		}
	})
	if err != nil {
		st.inc.walkErrors++
		if st.inc.walkErrors <= 3 {
			fmt.Printf("walk error (incremental, record %d, seq %d): %v\n", pkt.Index, info.Header.MsgSeqNum, err)
		}
		return
	}
	if info.Truncated {
		st.inc.truncated++
	}
	st.inc.observe(info.Header.MsgSeqNum, pkt.TS)
	st.incSeqNow = info.Header.MsgSeqNum
	st.msgsPerPkt.add(sizeBucket(info.Messages))
	st.payloadBytes.add(int64(len(pkt.Payload)/100) * 100)
	if info.Messages > 1 {
		st.packetsMulti++
	}
	if !info.Header.IsLastFragment() {
		st.lastFrag0++
	}
	if info.Header.IsPossDup() {
		st.possDup++
	}

	if emptyBookIdx >= 0 {
		st.emptyBooks++
		if st.opts.verbose || st.emptyBooks <= st.opts.examples {
			fmt.Printf("\n[EmptyBook #%d] record=%d ts=%s MsgSeqNum=%d flags=%s session=%d transactTime=%d messages=%d position=%d\n",
				st.emptyBooks, pkt.Index, pkt.TS.Format("15:04:05.000000"), info.Header.MsgSeqNum, flagsString(info.Header.MsgFlags),
				info.Incremental.ExchangeTradingSessionID, info.Incremental.TransactTime, info.Messages, emptyBookIdx)
			printMessages(msgs, st.opts.examples+3)
		}
	}
	if seqResetIdx >= 0 {
		st.seqResets++
		if st.opts.verbose || st.seqResets <= st.opts.examples {
			fmt.Printf("\n[SequenceReset #%d] record=%d ts=%s MsgSeqNum=%d flags=%s session=%d messages=%d position=%d\n",
				st.seqResets, pkt.Index, pkt.TS.Format("15:04:05.000000"), info.Header.MsgSeqNum, flagsString(info.Header.MsgFlags),
				info.Incremental.ExchangeTradingSessionID, info.Messages, seqResetIdx)
			printMessages(msgs, st.opts.examples+3)
		}
		// RptSeq numbering restarts per spec §4.2.7.
		st.instr = make(map[int32]*instrSeq)
		st.instrNoNQ = make(map[int32]*instrSeq)
	}
}

func printMessages(msgs []string, limit int) {
	for i, m := range msgs {
		if i >= limit {
			fmt.Printf("    ... (%d more)\n", len(msgs)-i)
			break
		}
		fmt.Printf("    [%d] %s\n", i, m)
	}
}

func (st *statsState) trackRpt(sec int32, rpt uint32, flags uint64, pkt pcap.Packet, i int, hdr simba.SBEHeader, d *simba.Decoded) {
	var nq bool = flags&mdFlagNonQuote != 0
	if nq {
		st.nonQuoteMsgs++
	}
	var s *instrSeq = st.instr[sec]
	if s == nil {
		s = &instrSeq{}
		st.instr[sec] = s
	}
	s.updates++
	if s.have {
		var delta int64 = int64(rpt) - int64(s.last)
		st.rptDelta.add(delta)
		if delta != 1 && st.rptGapExamples < st.opts.examples {
			st.rptGapExamples++
			fmt.Printf("[RptSeq delta %d] record=%d MsgSeqNum=%d msg#%d prevNonQuote=%v: %s\n", delta, pkt.Index, st.incSeqNow, i, s.lastNQ, describeMessage(hdr, d))
		}
	}
	s.have, s.last, s.lastNQ = true, rpt, nq

	if !nq {
		var s2 *instrSeq = st.instrNoNQ[sec]
		if s2 == nil {
			s2 = &instrSeq{}
			st.instrNoNQ[sec] = s2
		}
		if s2.have {
			st.rptDeltaNoNQ.add(int64(rpt) - int64(s2.last))
		}
		s2.have, s2.last = true, rpt
	}
}

func (st *statsState) onSnapshot(pkt pcap.Packet) {
	var snap *simba.OrderBookSnapshot
	var reset bool
	var info simba.PacketInfo
	var err error
	info, err = walkDecoded(pkt.Payload, func(i int, hdr simba.SBEHeader, d *simba.Decoded) {
		st.snap.schemas[[2]uint16{hdr.SchemaID, hdr.Version}]++
		if d.OrderBookSnapshot != nil {
			snap = d.OrderBookSnapshot
		}
		if d.NewSeqNo != nil {
			reset = true
		}
	})
	if err != nil {
		st.snap.walkErrors++
		if st.snap.walkErrors <= 3 {
			fmt.Printf("walk error (snapshot, record %d): %v\n", pkt.Index, err)
		}
		return
	}
	if info.Truncated {
		st.snap.truncated++
	}
	var restart bool = st.snap.observe(info.Header.MsgSeqNum, pkt.TS)
	// A cycle starts with SequenceReset(NewSeqNo=1) followed by the first
	// snapshot packet with MsgSeqNum=1: count that as one boundary.
	if st.curCycle == nil || reset || (restart && st.curCycle.fragments > 0) {
		st.curCycle = &snapshotCycle{start: pkt.TS, incSeqAt: st.incSeqNow}
		st.cycles = append(st.cycles, st.curCycle)
		st.snapOpen = make(map[int32]int)
	}
	if snap == nil {
		return
	}
	var c *snapshotCycle = st.curCycle
	var h simba.PacketHeader = info.Header
	if h.IsStartOfSnapshot() {
		c.instruments++
		st.snapOpen[snap.SecurityID] = 0
	}
	var frag int
	var open bool
	frag, open = st.snapOpen[snap.SecurityID]
	if !open {
		st.snapOrphanFrag++
	}
	frag++
	st.snapOpen[snap.SecurityID] = frag
	c.fragments++
	st.snapEntries.add(int64(len(snap.Entries)))
	for _, e := range snap.Entries {
		if e.MDEntryType == simba.MDEntryTypeEmptyBook {
			st.snapJ++
		}
		if e.MDEntryID == simba.NullInt64 {
			st.snapNullID++
		}
		if e.MDFlags&mdFlagNonQuote != 0 {
			st.snapNonQuote++
		}
	}
	st.snapSessions.add(int64(snap.ExchangeTradingSessionID))
	if h.IsEndOfSnapshot() {
		st.snapFragments.add(int64(frag))
		if frag > c.maxFrag {
			c.maxFrag = frag
		}
		delete(st.snapOpen, snap.SecurityID)
		if st.incSeqNow > 0 && snap.LastMsgSeqNumProcessed <= st.incSeqNow {
			var lag int64 = int64(st.incSeqNow) - int64(snap.LastMsgSeqNumProcessed)
			st.snapLag.add(lag / 1000 * 1000)
			if lag > st.snapMaxLag {
				st.snapMaxLag = lag
			}
		}
	}
}

func (st *statsState) report() {
	fmt.Println("\n== channels ==")
	fmt.Println(st.inc.summary())
	fmt.Println(st.snap.summary())

	fmt.Println("\n== incremental feed ==")
	fmt.Printf("templates: %s\n", formatTemplates(st.templates))
	fmt.Printf("packets with >1 message: %d of %d (%.1f%%)\n", st.packetsMulti, st.inc.packets, 100*float64(st.packetsMulti)/float64(maxU(st.inc.packets, 1)))
	fmt.Printf("messages per packet (bucketed ≤): %s\n", st.msgsPerPkt)
	fmt.Printf("payload bytes (bucketed by 100): %s\n", st.payloadBytes)
	fmt.Printf("LastFragment=0 packets: %d   PossDup packets: %d\n", st.lastFrag0, st.possDup)
	fmt.Printf("OrderUpdate actions: %s\n", st.updActions)
	fmt.Printf("OrderExecution actions: %s   nullPx(technical)=%d active=%d passive=%d\n", st.execActions, st.execNullPx, st.execActive, st.execPassive)
	fmt.Printf("NonQuote (OTC) messages: %d\n", st.nonQuoteMsgs)
	fmt.Printf("instruments seen: %d   busiest (sec=updates): %s\n", len(st.instr), st.topInstruments(8))
	fmt.Printf("RptSeq delta, all messages:      %s\n", st.rptDelta)
	fmt.Printf("RptSeq delta, NonQuote excluded: %s\n", st.rptDeltaNoNQ)
	fmt.Printf("EmptyBook messages: %d   SequenceReset messages: %d\n", st.emptyBooks, st.seqResets)

	fmt.Println("\n== snapshot feed ==")
	fmt.Printf("cycles: %d\n", len(st.cycles))
	for i, c := range st.cycles {
		if i >= st.opts.examples && !st.opts.verbose {
			fmt.Printf("  ... (%d more cycles)\n", len(st.cycles)-i)
			break
		}
		var dur string = "open"
		if i+1 < len(st.cycles) {
			dur = st.cycles[i+1].start.Sub(c.start).Round(time.Millisecond).String()
		}
		fmt.Printf("  cycle %d: start=%s duration=%s instruments=%d packets=%d maxFragments=%d incSeqAtStart=%d\n",
			i, c.start.Format("15:04:05.000"), dur, c.instruments, c.fragments, c.maxFrag, c.incSeqAt)
	}
	fmt.Printf("entries per snapshot packet: %s\n", st.snapEntries)
	fmt.Printf("fragments per instrument snapshot: %s\n", st.snapFragments)
	fmt.Printf("snapshot lag (incremental MsgSeqNum now - LastMsgSeqNumProcessed, bucketed by 1000): %s   max=%d\n", st.snapLag, st.snapMaxLag)
	fmt.Printf("entries: type J=%d nullID=%d NonQuote=%d   orphan fragments (no StartOfSnapshot)=%d   sessions=%s\n",
		st.snapJ, st.snapNullID, st.snapNonQuote, st.snapOrphanFrag, st.snapSessions)
}

func maxU(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// topInstruments lists the n instruments with the most updates.
func (st *statsState) topInstruments(n int) string {
	type kv struct {
		sec int32
		n   uint64
	}
	var all []kv
	for sec, s := range st.instr {
		all = append(all, kv{sec, s.updates})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].n > all[j].n })
	var parts []string
	for i, e := range all {
		if i >= n {
			break
		}
		parts = append(parts, fmt.Sprintf("%d=%d", e.sec, e.n))
	}
	// Also show the median instrument, useful for picking test fixtures.
	if len(all) > 0 {
		var m kv = all[len(all)/2]
		parts = append(parts, fmt.Sprintf("median:%d=%d", m.sec, m.n))
		var q kv = all[len(all)/10]
		parts = append(parts, fmt.Sprintf("p90:%d=%d", q.sec, q.n))
	}
	return strings.Join(parts, " ")
}
