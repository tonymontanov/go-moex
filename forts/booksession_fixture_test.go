package forts

import (
	"encoding/binary"
	"io"
	"testing"

	"github.com/tonymontanov/go-moex/internal/pcap"
	"github.com/tonymontanov/go-moex/internal/simba"
	"github.com/tonymontanov/go-moex/orderbook"
)

/*
Oracle test on production fixtures (internal/simba/testdata, cut from
MOEX's public captures of 2026-05-15): the BookSession must reproduce
every OrderBookSnapshot the exchange published after the instrument went
live, and the top of book after every transaction must equal the
BestPrices message the exchange sent for it.

Snapshots describe the book at LastMsgSeqNumProcessed (L), which on the
wire is up to ~50k packets behind the live feed. To compare exactly, the
test delivers incremental packets lazily: before handing the session a
snapshot packet with a given L, it delivers every incremental packet with
MsgSeqNum <= L and nothing beyond. That is a legal ordering (a client
always has seen packets up to L before the snapshot arrives) and lets the
engine be compared at precisely L.
*/

const (
	fixtureIncGroup  = "239.195.20.81:20081"
	fixtureSnapGroup = "239.195.20.82:20082"
)

type fixturePacket struct {
	seq     uint32
	payload []byte
}

func loadFixture(t *testing.T, name string) (inc []fixturePacket, snap []fixturePacket) {
	t.Helper()
	var src *pcap.Source
	var err error
	src, err = pcap.Open("../internal/simba/testdata/"+name, "")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	for {
		var pkt pcap.Packet
		pkt, err = src.Reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		var fp fixturePacket = fixturePacket{seq: simba.PacketHeaderSeq(pkt.Payload), payload: append([]byte{}, pkt.Payload...)}
		switch pcap.FormatAddr(pkt.DstIP, pkt.DstPort) {
		case fixtureIncGroup:
			inc = append(inc, fp)
		case fixtureSnapGroup:
			snap = append(snap, fp)
		}
	}
	return inc, snap
}

type snapshotContent struct {
	sec     int32
	L, R    uint32
	orders  map[int64][3]int64
	started bool
	done    bool
}

func snapshotOf(t *testing.T, payload []byte) (simba.PacketHeader, simba.SnapshotView, bool) {
	var p simba.Packet
	var err error
	p, err = simba.ParsePacket(payload, 0)
	if err != nil {
		t.Fatal(err)
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

type bpExpectation struct {
	bidPx, askPx, bidSize, askSize int64
}

// bestPricesAndEOT extracts BestPrices expectations and whether the
// packet ends a transaction.
func bestPricesAndEOT(t *testing.T, payload []byte) (bp map[int32]bpExpectation, eot bool) {
	var p simba.Packet
	var err error
	p, err = simba.ParsePacket(payload, 0)
	if err != nil {
		t.Fatal(err)
	}
	for {
		var m simba.Message
		var ok bool
		m, ok, err = p.Next()
		if err != nil || !ok {
			break
		}
		switch m.Kind {
		case simba.KindBestPrices:
			var v simba.BestPricesView
			if v, ok = m.BestPrices(); ok {
				for i := 0; i < v.Len(); i++ {
					var e simba.BestPricesEntry = v.Entry(i)
					if bp == nil {
						bp = make(map[int32]bpExpectation)
					}
					bp[e.SecurityID] = bpExpectation{bidPx: e.MktBidPx, askPx: e.MktOfferPx, bidSize: e.MktBidSize, askSize: e.MktOfferSize}
				}
			}
		case simba.KindOrderUpdate:
			if u, ok := m.OrderUpdate(); ok && u.MDFlags&(1<<12) != 0 {
				eot = true
			}
		case simba.KindOrderExecution:
			if e, ok := m.OrderExecution(); ok && e.MDFlags&(1<<12) != 0 {
				eot = true
			}
		}
	}
	return bp, eot
}

func runFixtureOracle(t *testing.T, name string, secs []int32) {
	var inc, snap = loadFixture(t, name)
	if len(inc) == 0 || len(snap) == 0 {
		t.Fatalf("fixture %s: inc=%d snap=%d", name, len(inc), len(snap))
	}
	var s *BookSession = NewBookSession(BookSessionConfig{})
	var engines map[int32]*orderbook.Engine = make(map[int32]*orderbook.Engine)
	for _, sec := range secs {
		engines[sec] = s.Subscribe(sec)
	}

	var next int // next incremental packet to deliver.
	var pendingBP map[int32]bpExpectation = make(map[int32]bpExpectation)
	var bpChecks, bpMismatches, compares, mismatches int
	var deliverInc = func(upTo uint32) {
		for next < len(inc) && (upTo == 0 || inc[next].seq <= upTo) {
			var bp map[int32]bpExpectation
			var eot bool
			bp, eot = bestPricesAndEOT(t, inc[next].payload)
			for sec, e := range bp {
				pendingBP[sec] = e
			}
			s.HandleIncrementalPacket(inc[next].payload)
			next++
			if !eot {
				continue
			}
			for sec, e := range pendingBP {
				delete(pendingBP, sec)
				var engine *orderbook.Engine = engines[sec]
				if engine == nil {
					continue
				}
				if st, _ := s.State(sec); st != BookLive {
					continue
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
					bpMismatches++
					if bpMismatches <= 3 {
						t.Errorf("BestPrices mismatch sec=%d after packet %d: exp bid=%d/%d ask=%d/%d got bid=%d/%d(%v) ask=%d/%d(%v)", sec, inc[next-1].seq, e.bidPx, e.bidSize, e.askPx, e.askSize, bidPx, bidSize, haveBid, askPx, askSize, haveAsk)
					}
				}
			}
		}
	}

	var partial map[int32]*snapshotContent = make(map[int32]*snapshotContent)
	var syncedAt map[int32]int = make(map[int32]int) // syncCount when first observed live.
	for _, sp := range snap {
		var hdr simba.PacketHeader
		var v simba.SnapshotView
		var ok bool
		hdr, v, ok = snapshotOf(t, sp.payload)
		if !ok {
			s.HandleSnapshotPacket(sp.payload) // SequenceReset / Heartbeat
			continue
		}
		var engine *orderbook.Engine = engines[v.SecurityID]
		if engine == nil {
			continue
		}
		// Deliver incrementals up to L, then the fragment.
		deliverInc(v.LastMsgSeqNumProcessed)
		var wasLive bool
		if st, _ := s.State(v.SecurityID); st == BookLive {
			wasLive = true
		}
		s.HandleSnapshotPacket(sp.payload)

		// Accumulate the snapshot content for the oracle.
		var sc *snapshotContent = partial[v.SecurityID]
		if hdr.IsStartOfSnapshot() || sc == nil {
			sc = &snapshotContent{sec: v.SecurityID, L: v.LastMsgSeqNumProcessed, R: v.RptSeq, orders: make(map[int64][3]int64)}
			partial[v.SecurityID] = sc
		}
		for i := 0; i < v.Len(); i++ {
			var e simba.OrderBookSnapshotEntry = v.Entry(i)
			if e.MDEntryType == simba.MDEntryTypeEmptyBook || e.MDEntryID == simba.NullInt64 {
				continue
			}
			sc.orders[e.MDEntryID] = [3]int64{int64(sideFromMDEntryType(e.MDEntryType)), e.MDEntryPx, e.MDEntrySize}
		}
		if !hdr.IsEndOfSnapshot() {
			continue
		}
		delete(partial, v.SecurityID)
		if !wasLive {
			// This snapshot (or an EmptyBook) synced the instrument: nothing to compare against yet.
			if st, _ := s.State(v.SecurityID); st == BookLive {
				syncedAt[v.SecurityID]++
			}
			continue
		}
		// Live before this snapshot: the engine at L must equal the snapshot.
		compares++
		var got map[int64][3]int64 = bookOrders(engine)
		var diff int
		for id, w := range sc.orders {
			if g, ok := got[id]; !ok || g != w {
				diff++
			}
		}
		for id := range got {
			if _, ok := sc.orders[id]; !ok {
				diff++
			}
		}
		var seq uint64
		seq, _ = engine.LastSeq()
		if diff != 0 || (sc.R != 0 && seq != uint64(sc.R)) {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("snapshot mismatch sec=%d L=%d: %d order diffs, engine seq=%d snapshot rpt=%d (book=%d snap=%d)", sc.sec, sc.L, diff, seq, sc.R, len(got), len(sc.orders))
			}
		}
	}
	deliverInc(0)

	var st BookSessionStats = s.Stats()
	t.Logf("%s: compares=%d mismatches=%d bestprices=%d/%d stats=%s", name, compares, mismatches, bpChecks, bpMismatches, st)
	if compares == 0 {
		t.Fatalf("no snapshot comparisons happened (synced=%v)", syncedAt)
	}
	if mismatches != 0 || bpMismatches != 0 {
		t.Fatalf("oracle failed: snapshot mismatches=%d bestprices mismatches=%d", mismatches, bpMismatches)
	}
	if st.ParseErrors != 0 || st.UnknownOrders != 0 || st.Resyncs != 0 || st.IncGaps != 0 || st.SnapGaps != 0 {
		t.Fatalf("unexpected stats: %s", st)
	}
	for sec, e := range engines {
		if st, _ := s.State(sec); st != BookLive || e.OrderCount() == 0 {
			t.Fatalf("instrument %d not live/non-empty at end: state=%v orders=%d", sec, st, e.OrderCount())
		}
	}
}

func TestBookSessionMainSessionFixture(t *testing.T) {
	runFixtureOracle(t, "main-session-2instr.pcap.gz", []int32{7299709, 3416766})
}

func TestBookSessionTechBreakFixture(t *testing.T) {
	runFixtureOracle(t, "tech-break-2instr.pcap.gz", []int32{7559029, 7293491})
}

// Guard against fixture/decoder drift: the first incremental packet of
// the tech-break fixture is the daily SequenceReset.
func TestTechBreakFixtureStartsWithSequenceReset(t *testing.T) {
	var inc, _ = loadFixture(t, "tech-break-2instr.pcap.gz")
	var p simba.Packet
	var err error
	p, err = simba.ParsePacket(inc[0].payload, 0)
	if err != nil {
		t.Fatal(err)
	}
	var m simba.Message
	m, _, _ = p.Next()
	if m.Kind != simba.KindSequenceReset {
		t.Fatalf("first message %v", m)
	}
	if binary.LittleEndian.Uint32(m.Body()[0:4]) != 1 {
		t.Fatal("NewSeqNo must be 1")
	}
}
