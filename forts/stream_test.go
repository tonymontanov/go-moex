package forts

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/tonymontanov/go-moex/internal/simba"
	"github.com/tonymontanov/go-moex/orderbook"
)

// Wire builders mirroring internal/simba/decode.go layouts.

func sbeHeader(templateID, blockLength uint16) []byte {
	var h [8]byte
	binary.LittleEndian.PutUint16(h[0:2], blockLength)
	binary.LittleEndian.PutUint16(h[2:4], templateID)
	binary.LittleEndian.PutUint16(h[4:6], simba.SchemaID)
	binary.LittleEndian.PutUint16(h[6:8], simba.SchemaVersion9)
	return h[:]
}

func incPacket(seq uint32, flags uint16, msgs ...[]byte) []byte {
	var buf []byte = make([]byte, 28)
	binary.LittleEndian.PutUint32(buf[0:4], seq)
	binary.LittleEndian.PutUint16(buf[6:8], flags|simba.FlagIncrementalPacket)
	binary.LittleEndian.PutUint32(buf[24:28], 7562)
	for _, m := range msgs {
		buf = append(buf, m...)
	}
	binary.LittleEndian.PutUint16(buf[4:6], uint16(len(buf)))
	return buf
}

func orderUpdateMsg(u simba.OrderUpdate) []byte {
	var body [50]byte
	binary.LittleEndian.PutUint64(body[0:8], uint64(u.MDEntryID))
	binary.LittleEndian.PutUint64(body[8:16], uint64(u.MDEntryPx))
	binary.LittleEndian.PutUint64(body[16:24], uint64(u.MDEntrySize))
	binary.LittleEndian.PutUint64(body[24:32], u.MDFlags)
	binary.LittleEndian.PutUint64(body[32:40], u.MDFlags2)
	binary.LittleEndian.PutUint32(body[40:44], uint32(u.SecurityID))
	binary.LittleEndian.PutUint32(body[44:48], u.RptSeq)
	body[48] = byte(u.MDUpdateAction)
	body[49] = byte(u.MDEntryType)
	return append(sbeHeader(simba.TemplateOrderUpdate, 50), body[:]...)
}

func orderExecMsg(e simba.OrderExecution) []byte {
	var body [74]byte
	binary.LittleEndian.PutUint64(body[0:8], uint64(e.MDEntryID))
	binary.LittleEndian.PutUint64(body[8:16], uint64(e.MDEntryPx))
	binary.LittleEndian.PutUint64(body[16:24], uint64(e.MDEntrySize))
	binary.LittleEndian.PutUint64(body[24:32], uint64(e.LastPx))
	binary.LittleEndian.PutUint64(body[32:40], uint64(e.LastQty))
	binary.LittleEndian.PutUint64(body[40:48], uint64(e.TradeID))
	binary.LittleEndian.PutUint64(body[48:56], e.MDFlags)
	binary.LittleEndian.PutUint64(body[56:64], e.MDFlags2)
	binary.LittleEndian.PutUint32(body[64:68], uint32(e.SecurityID))
	binary.LittleEndian.PutUint32(body[68:72], e.RptSeq)
	body[72] = byte(e.MDUpdateAction)
	body[73] = byte(e.MDEntryType)
	return append(sbeHeader(simba.TemplateOrderExecution, 74), body[:]...)
}

func emptyBookMsg() []byte {
	var body [4]byte
	binary.LittleEndian.PutUint32(body[0:4], simba.NullUint32)
	return append(sbeHeader(simba.TemplateEmptyBook, 4), body[:]...)
}

func seqResetMsg(newSeq uint32) []byte {
	var body [4]byte
	binary.LittleEndian.PutUint32(body[0:4], newSeq)
	return append(sbeHeader(simba.TemplateSequenceReset, 4), body[:]...)
}

func snapPacket(seq uint32, flags uint16, sec int32, L, rpt uint32, entries ...simba.OrderBookSnapshotEntry) []byte {
	var body []byte = make([]byte, 16)
	binary.LittleEndian.PutUint32(body[0:4], uint32(sec))
	binary.LittleEndian.PutUint32(body[4:8], L)
	binary.LittleEndian.PutUint32(body[8:12], rpt)
	binary.LittleEndian.PutUint32(body[12:16], 7562)
	body = append(body, 57, 0, byte(len(entries)))
	for _, e := range entries {
		var b [57]byte
		binary.LittleEndian.PutUint64(b[0:8], uint64(e.MDEntryID))
		binary.LittleEndian.PutUint64(b[16:24], uint64(e.MDEntryPx))
		binary.LittleEndian.PutUint64(b[24:32], uint64(e.MDEntrySize))
		binary.LittleEndian.PutUint64(b[40:48], e.MDFlags)
		b[56] = byte(e.MDEntryType)
		body = append(body, b[:]...)
	}
	var buf []byte = make([]byte, 16)
	binary.LittleEndian.PutUint32(buf[0:4], seq)
	binary.LittleEndian.PutUint16(buf[6:8], flags)
	buf = append(buf, sbeHeader(simba.TemplateOrderBookSnapshot, 16)...)
	buf = append(buf, body...)
	binary.LittleEndian.PutUint16(buf[4:6], uint16(len(buf)))
	return buf
}

func upd(sec int32, rpt uint32, id, px, size int64, action simba.MDUpdateAction, side simba.MDEntryType) []byte {
	return orderUpdateMsg(simba.OrderUpdate{MDEntryID: id, MDEntryPx: px, MDEntrySize: size, MDFlags: 1, SecurityID: sec, RptSeq: rpt, MDUpdateAction: action, MDEntryType: side})
}

func newTestSession(t *testing.T) (*BookSession, *[]int32, *[]BookState) {
	t.Helper()
	var changes []int32
	var states []BookState
	var s *BookSession = NewBookSession(BookSessionConfig{
		OnBook:  func(id int32, e *orderbook.Engine) { changes = append(changes, id) },
		OnState: func(id int32, st BookState) { states = append(states, st) },
		Feed:    FeedConfig{GapTimeout: 2 * time.Millisecond},
	})
	return s, &changes, &states
}

func bookOrders(e *orderbook.Engine) map[int64][3]int64 {
	var out map[int64][3]int64 = make(map[int64][3]int64)
	e.ForEachOrder(func(id int64, side orderbook.Side, px, size int64) { out[id] = [3]int64{int64(side), px, size} })
	return out
}

// TestBookSessionLateJoin — spec §4.2.5: buffer while collecting, seed
// from the snapshot, apply only updates with MsgSeqNum > L.
func TestBookSessionLateJoin(t *testing.T) {
	var s, changes, states = newTestSession(t)
	var e *orderbook.Engine = s.Subscribe(42)

	// Updates arriving before the snapshot: seq 100 (included in the
	// snapshot, must be dropped) and 101/102 (must be applied on top).
	s.HandleIncrementalPacket(incPacket(100, simba.FlagLastFragment, upd(42, 10, 1, 100, 5, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	s.HandleIncrementalPacket(incPacket(101, simba.FlagLastFragment, upd(42, 11, 2, 101, 7, simba.MDUpdateActionNew, simba.MDEntryTypeOffer), upd(99, 1, 9, 1, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	s.HandleIncrementalPacket(incPacket(102, simba.FlagLastFragment, upd(42, 12, 1, 0, 0, simba.MDUpdateActionDelete, simba.MDEntryTypeBid)))
	if e.OrderCount() != 0 || len(*changes) != 0 {
		t.Fatalf("collecting book must not change: orders=%d changes=%v", e.OrderCount(), *changes)
	}

	// Snapshot in two fragments, taken at L=100 (contains order 1 already), RptSeq=10.
	s.HandleSnapshotPacket(snapPacket(1, simba.FlagStartOfSnapshot, 42, 100, 10, simba.OrderBookSnapshotEntry{MDEntryID: 1, MDEntryPx: 100, MDEntrySize: 5, MDEntryType: simba.MDEntryTypeBid}))
	if st, _ := s.State(42); st != BookCollecting {
		t.Fatal("first fragment must not complete the snapshot")
	}
	s.HandleSnapshotPacket(snapPacket(2, simba.FlagEndOfSnapshot, 42, 100, 10, simba.OrderBookSnapshotEntry{MDEntryID: 7, MDEntryPx: 90, MDEntrySize: 1, MDEntryType: simba.MDEntryTypeBid}))

	if st, _ := s.State(42); st != BookLive {
		t.Fatalf("state %v, want live", st)
	}
	var got map[int64][3]int64 = bookOrders(e)
	if len(got) != 2 || got[7] != [3]int64{int64(orderbook.SideBid), 90, 1} || got[2] != [3]int64{int64(orderbook.SideAsk), 101, 7} {
		t.Fatalf("book after sync: %v", got)
	}
	if seq, _ := e.LastSeq(); seq != 12 {
		t.Fatalf("LastSeq=%d want 12", seq)
	}
	if len(*states) != 1 || (*states)[0] != BookLive || len(*changes) != 1 {
		t.Fatalf("callbacks: states=%v changes=%v", *states, *changes)
	}

	// Live: applied directly; a later snapshot is ignored.
	s.HandleIncrementalPacket(incPacket(103, simba.FlagLastFragment, upd(42, 13, 3, 102, 1, simba.MDUpdateActionNew, simba.MDEntryTypeOffer)))
	s.HandleSnapshotPacket(snapPacket(3, simba.FlagStartOfSnapshot|simba.FlagEndOfSnapshot, 42, 103, 13))
	if e.OrderCount() != 3 {
		t.Fatalf("live book orders=%d", e.OrderCount())
	}
	if s.Stats().Syncs != 1 {
		t.Fatalf("stats %s", s.Stats())
	}
}

// TestBookSessionTechnicalTradeAdvancesSeq — production numbers technical
// trades (null price) and NonQuote records; skipping them made the next
// update look like a gap.
func TestBookSessionTechnicalTradeAdvancesSeq(t *testing.T) {
	var s, _, _ = newTestSession(t)
	var e *orderbook.Engine = s.Subscribe(42)
	s.HandleIncrementalPacket(incPacket(1, simba.FlagLastFragment, emptyBookMsg())) // instruments become live-empty.
	s.HandleIncrementalPacket(incPacket(2, simba.FlagLastFragment, upd(42, 1, 1, 100, 5, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	s.HandleIncrementalPacket(incPacket(3, simba.FlagLastFragment,
		orderExecMsg(simba.OrderExecution{MDEntryID: 555, MDEntryPx: simba.NullDecimalMantissa, MDEntrySize: simba.NullInt64, LastPx: 1, LastQty: 1, MDFlags: 4, SecurityID: 42, RptSeq: 2, MDUpdateAction: simba.MDUpdateActionNew, MDEntryType: simba.MDEntryTypeBid}),
		orderUpdateMsg(simba.OrderUpdate{MDEntryID: 556, MDEntryPx: 1, MDEntrySize: 1, MDFlags: 4, SecurityID: 42, RptSeq: 3, MDUpdateAction: simba.MDUpdateActionNew, MDEntryType: simba.MDEntryTypeBid}), // NonQuote: not in book.
		upd(42, 4, 2, 101, 1, simba.MDUpdateActionNew, simba.MDEntryTypeOffer),
	))
	if st, _ := s.State(42); st != BookLive {
		t.Fatalf("technical trade caused a resync: %s", s.Stats())
	}
	if e.OrderCount() != 2 {
		t.Fatalf("orders=%d want 2 (NonQuote filtered)", e.OrderCount())
	}
	if seq, _ := e.LastSeq(); seq != 4 {
		t.Fatalf("LastSeq=%d", seq)
	}
}

// TestBookSessionEmptyBookRebroadcast — daily reset: SequenceReset,
// EmptyBook, then books re-broadcast with RptSeq=0 and PossDupFlag.
func TestBookSessionEmptyBookRebroadcast(t *testing.T) {
	var s, _, _ = newTestSession(t)
	var e *orderbook.Engine = s.Subscribe(42)
	// Before the reset the feed was at some high number.
	s.HandleIncrementalPacket(incPacket(7_000_000, simba.FlagLastFragment, upd(42, 500, 77, 1, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	// Wire order (production capture): SequenceReset(NewSeqNo=1) in a
	// packet numbered 1, then EmptyBook ALSO numbered 1, then 2, 3, ...
	s.HandleIncrementalPacket(incPacket(1, simba.FlagLastFragment, seqResetMsg(1)))
	s.HandleIncrementalPacket(incPacket(1, simba.FlagLastFragment, emptyBookMsg()))
	if st, _ := s.State(42); st != BookLive {
		t.Fatal("after EmptyBook the incremental feed defines the book: must be live")
	}
	if e.OrderCount() != 0 {
		t.Fatal("EmptyBook must clear the book")
	}
	var e2 *orderbook.Engine = s.Subscribe(43) // subscribed after EmptyBook: live from the start.
	if st, _ := s.State(43); st != BookLive {
		t.Fatal("instrument subscribed after EmptyBook must start live")
	}
	s.HandleIncrementalPacket(incPacket(2, simba.FlagPossDupFlag,
		upd(42, 0, 1, 100, 5, simba.MDUpdateActionNew, simba.MDEntryTypeBid),
		upd(42, 0, 2, 100, 3, simba.MDUpdateActionNew, simba.MDEntryTypeBid),
		upd(43, 0, 9, 50, 1, simba.MDUpdateActionNew, simba.MDEntryTypeOffer),
	))
	s.HandleIncrementalPacket(incPacket(3, simba.FlagLastFragment|simba.FlagPossDupFlag, upd(42, 0, 3, 99, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	if e.OrderCount() != 3 || e2.OrderCount() != 1 {
		t.Fatalf("rebroadcast not applied: %d/%d", e.OrderCount(), e2.OrderCount())
	}
	if px, size, ok := e.BestBid(); !ok || px != 100 || size != 8 {
		t.Fatalf("best bid %d/%d %v", px, size, ok)
	}
	// First real update restarts numbering at 1.
	s.HandleIncrementalPacket(incPacket(4, simba.FlagLastFragment, upd(42, 1, 1, 0, 0, simba.MDUpdateActionDelete, simba.MDEntryTypeBid)))
	if st, _ := s.State(42); st != BookLive || e.OrderCount() != 2 {
		t.Fatalf("first sequenced update after rebroadcast: state=%v orders=%d stats=%s", st, e.OrderCount(), s.Stats())
	}
	var st BookSessionStats = s.Stats()
	if st.EmptyBooks != 1 || st.SequenceResets != 1 || st.Resyncs != 0 || st.IncGaps != 0 || st.IncDuplicates != 0 {
		t.Fatalf("stats %s", st)
	}
}

// TestBookSessionGapResync — an RptSeq gap sends the instrument back to
// Collecting; the update that revealed the gap is kept for the merge.
func TestBookSessionGapResync(t *testing.T) {
	var s, _, states = newTestSession(t)
	var e *orderbook.Engine = s.Subscribe(42)
	s.HandleIncrementalPacket(incPacket(1, simba.FlagLastFragment, emptyBookMsg()))
	s.HandleIncrementalPacket(incPacket(2, simba.FlagLastFragment, upd(42, 1, 1, 100, 5, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	// Packet 3 (rpt 2) lost on both legs; packet 4 (rpt 3) arrives. The feed
	// layer holds it for GapTimeout, then declares the gap and applies it —
	// the RptSeq jump sends the instrument back to Collecting.
	s.HandleIncrementalPacket(incPacket(4, simba.FlagLastFragment, upd(42, 3, 3, 102, 1, simba.MDUpdateActionNew, simba.MDEntryTypeOffer)))
	waitFor(t, func() bool { st, _ := s.State(42); return st == BookCollecting })
	if e.OrderCount() != 0 {
		t.Fatalf("gap must clear the book: orders=%d", e.OrderCount())
	}
	// Duplicate of an already-applied packet is ignored at feed level.
	s.HandleIncrementalPacket(incPacket(4, simba.FlagLastFragment, upd(42, 3, 3, 102, 1, simba.MDUpdateActionNew, simba.MDEntryTypeOffer)))
	// Snapshot taken at L=3 (after rpt 2, before rpt 3): includes orders 1 and 2.
	s.HandleSnapshotPacket(snapPacket(1, simba.FlagStartOfSnapshot|simba.FlagEndOfSnapshot, 42, 3, 2,
		simba.OrderBookSnapshotEntry{MDEntryID: 1, MDEntryPx: 100, MDEntrySize: 5, MDEntryType: simba.MDEntryTypeBid},
		simba.OrderBookSnapshotEntry{MDEntryID: 2, MDEntryPx: 101, MDEntrySize: 5, MDEntryType: simba.MDEntryTypeBid},
	))
	if st, _ := s.State(42); st != BookLive || e.OrderCount() != 3 {
		t.Fatalf("after resync: state=%v orders=%d stats=%s", st, e.OrderCount(), s.Stats())
	}
	if seq, _ := e.LastSeq(); seq != 3 {
		t.Fatalf("LastSeq=%d", seq)
	}
	var st BookSessionStats = s.Stats()
	if st.Resyncs != 1 || st.Syncs != 1 || st.IncGaps != 1 || st.IncDuplicates != 1 {
		t.Fatalf("stats %s", st)
	}
	if len(*states) != 3 { // live (EmptyBook), collecting (gap), live (snapshot)
		t.Fatalf("states %v", *states)
	}
}

func TestBookSessionSnapshotFragmentLoss(t *testing.T) {
	var s, _, _ = newTestSession(t)
	s.Subscribe(42)
	s.HandleSnapshotPacket(snapPacket(1, simba.FlagStartOfSnapshot, 42, 10, 5, simba.OrderBookSnapshotEntry{MDEntryID: 1, MDEntryPx: 100, MDEntrySize: 5, MDEntryType: simba.MDEntryTypeBid}))
	// Fragment 2 lost; 3 arrives with EndOfSnapshot: partial snapshot must be discarded.
	s.HandleSnapshotPacket(snapPacket(3, simba.FlagEndOfSnapshot, 42, 10, 5, simba.OrderBookSnapshotEntry{MDEntryID: 2, MDEntryPx: 101, MDEntrySize: 5, MDEntryType: simba.MDEntryTypeBid}))
	if st, _ := s.State(42); st != BookCollecting {
		t.Fatal("incomplete snapshot must not sync")
	}
	if s.Stats().SnapshotsDiscarded != 1 || s.Stats().SnapGaps != 1 {
		t.Fatalf("stats %s", s.Stats())
	}
	// Next cycle delivers it whole.
	s.HandleSnapshotPacket(snapPacket(1, simba.FlagStartOfSnapshot|simba.FlagEndOfSnapshot, 42, 10, 5, simba.OrderBookSnapshotEntry{MDEntryID: 1, MDEntryPx: 100, MDEntrySize: 5, MDEntryType: simba.MDEntryTypeBid}))
	if st, _ := s.State(42); st != BookLive {
		t.Fatal("full snapshot must sync")
	}
}

func TestSideFromMDEntryType(t *testing.T) {
	if sideFromMDEntryType(simba.MDEntryTypeBid) != orderbook.SideBid || sideFromMDEntryType(simba.MDEntryTypeOffer) != orderbook.SideAsk {
		t.Fatal("side mapping")
	}
}

func TestLevelsToDecimal(t *testing.T) {
	var engine *orderbook.Engine = orderbook.NewEngine()
	if err := engine.ApplyDelta(1, orderbook.SideBid, 9500000, 10, orderbook.ActionUpsert, 1); err != nil {
		t.Fatal(err)
	}
	var out = levelsToDecimal(engine.Levels(orderbook.SideBid, 0))
	if len(out) != 1 {
		t.Fatalf("levelsToDecimal returned %d levels, want 1", len(out))
	}
	if !out[0].Price.Equal(decStr(t, "95")) {
		t.Errorf("Price = %s, want 95", out[0].Price)
	}
	if !out[0].Size.Equal(decStr(t, "10")) {
		t.Errorf("Size = %s, want 10", out[0].Size)
	}
}
