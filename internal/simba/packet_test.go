package simba

import (
	"encoding/binary"
	"testing"
)

func snapshotPacket(seq uint32, flags uint16, sec int32, L, rpt, session uint32, entries ...[]byte) []byte {
	var body []byte = make([]byte, 16)
	binary.LittleEndian.PutUint32(body[0:4], uint32(sec))
	binary.LittleEndian.PutUint32(body[4:8], L)
	binary.LittleEndian.PutUint32(body[8:12], rpt)
	binary.LittleEndian.PutUint32(body[12:16], session)
	body = append(body, byte(orderBookSnapshotEntrySize), 0, byte(len(entries)))
	for _, e := range entries {
		body = append(body, e...)
	}
	var out []byte = make([]byte, 16)
	binary.LittleEndian.PutUint32(out[0:4], seq)
	binary.LittleEndian.PutUint16(out[6:8], flags)
	out = append(out, sbeMessage(TemplateOrderBookSnapshot, orderBookSnapshotRootSize, body)...)
	binary.LittleEndian.PutUint16(out[4:6], uint16(len(out)))
	return out
}

func snapshotEntry(id int64, px, size int64, entryType MDEntryType) []byte {
	var b [orderBookSnapshotEntrySize]byte
	binary.LittleEndian.PutUint64(b[0:8], uint64(id))
	binary.LittleEndian.PutUint64(b[16:24], uint64(px))
	binary.LittleEndian.PutUint64(b[24:32], uint64(size))
	var nullID int64 = NullInt64
	binary.LittleEndian.PutUint64(b[32:40], uint64(nullID))
	b[56] = byte(entryType)
	return b[:]
}

func TestParsePacketIteratesAllMessages(t *testing.T) {
	var packet []byte = incrementalPacket(105806, FlagLastFragment,
		sbeMessage(TemplateOrderUpdate, orderUpdateSize, orderUpdateBody(1439162, 60142, 1892945606659163299, 77667, 26, MDUpdateActionNew, MDEntryTypeBid)),
		sbeMessage(TemplateOrderExecution, orderExecutionSize, orderExecutionBody(1439162, 60144, 1892945606659163299, 77667, 0, 77664, 26, 5, 0x20000000002, MDUpdateActionDelete, MDEntryTypeBid)),
		sbeMessage(TemplateOrderExecution, orderExecutionSize, orderExecutionBody(1439162, 60145, 1892945606659163300, 77664, 0, 77664, 26, 5, 0x40000000001, MDUpdateActionDelete, MDEntryTypeOffer)),
	)
	var p Packet
	var err error
	p, err = ParsePacket(packet, 0)
	if err != nil {
		t.Fatal(err)
	}
	if p.Header().MsgSeqNum != 105806 || p.Version() != 9 {
		t.Fatalf("header %+v version %d", p.Header(), p.Version())
	}
	var inc IncrementalHeader
	var isInc bool
	inc, isInc = p.Incremental()
	if !isInc || inc.ExchangeTradingSessionID != 6144 {
		t.Fatalf("incremental header %+v %v", inc, isInc)
	}
	var kinds []Kind
	var rpts []uint32
	for {
		var m Message
		var ok bool
		m, ok, err = p.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		kinds = append(kinds, m.Kind)
		if u, ok := m.OrderUpdate(); ok {
			rpts = append(rpts, u.RptSeq)
		}
		if e, ok := m.OrderExecution(); ok {
			rpts = append(rpts, e.RptSeq)
			if e.MDFlags&(1<<41) == 0 && e.MDFlags&(1<<42) == 0 {
				t.Errorf("execution flags lost: %x", e.MDFlags)
			}
		}
	}
	if len(kinds) != 3 || kinds[0] != KindOrderUpdate || kinds[1] != KindOrderExecution || kinds[2] != KindOrderExecution {
		t.Fatalf("kinds %v", kinds)
	}
	if len(rpts) != 3 || rpts[0] != 60142 || rpts[1] != 60144 || rpts[2] != 60145 {
		t.Fatalf("rpts %v", rpts)
	}
	if p.Messages() != 3 {
		t.Fatalf("Messages()=%d", p.Messages())
	}
}

func TestParsePacketGuards(t *testing.T) {
	var good []byte = incrementalPacket(1, FlagLastFragment, sbeMessage(TemplateOrderUpdate, orderUpdateSize, orderUpdateBody(1, 1, 1, 1, 1, MDUpdateActionNew, MDEntryTypeBid)))

	var bad []byte = append([]byte{}, good...)
	binary.LittleEndian.PutUint16(bad[4:6], uint16(len(bad)-1))
	if _, err := ParsePacket(bad, 0); err != ErrMsgSize {
		t.Fatalf("MsgSize guard: %v", err)
	}

	bad = append([]byte{}, good...)
	binary.LittleEndian.PutUint16(bad[28+4:28+6], 12345)
	if _, err := ParsePacket(bad, 0); err != ErrSchemaID {
		t.Fatalf("SchemaID guard: %v", err)
	}

	bad = append([]byte{}, good...)
	binary.LittleEndian.PutUint16(bad[28+6:28+8], 10)
	if _, err := ParsePacket(bad, 0); err != ErrSchemaVersion {
		t.Fatalf("unknown version guard: %v", err)
	}
	if _, err := ParsePacket(good, SchemaVersion8); err != ErrSchemaVersion {
		t.Fatalf("pinned version guard: %v", err)
	}
	if _, err := ParsePacket(good, SchemaVersion9); err != nil {
		t.Fatalf("pinned matching version: %v", err)
	}
	if _, err := ParsePacket(good[:10], 0); err != ErrShortPacket {
		t.Fatalf("short packet: %v", err)
	}
}

// TestKindAcrossSchemaVersions: SecurityDefinition is template 21 on
// version 8 (what production carried in May 2026) and 27 on version 9.
func TestKindAcrossSchemaVersions(t *testing.T) {
	if kindOf(8, 21) != KindSecurityDefinition || kindOf(9, 27) != KindSecurityDefinition {
		t.Fatal("SecurityDefinition mapping")
	}
	if kindOf(8, 27) != KindUnknown || kindOf(9, 21) != KindUnknown {
		t.Fatal("cross-version ids must not map")
	}
	if kindOf(8, 9) != KindSecurityStatus || kindOf(9, 28) != KindSecurityStatus {
		t.Fatal("SecurityStatus mapping")
	}
	for _, id := range []uint16{1, 2, 4, 14, 15, 16, 17} {
		if kindOf(8, id) != kindOf(9, id) || kindOf(8, id) == KindUnknown {
			t.Fatalf("order-log template %d must be stable across versions", id)
		}
	}
}

func TestSnapshotView(t *testing.T) {
	var packet []byte = snapshotPacket(7, FlagStartOfSnapshot|FlagEndOfSnapshot|FlagLastFragment, 42, 1000, 55, 7562,
		snapshotEntry(11, 100, 5, MDEntryTypeBid),
		snapshotEntry(12, 101, 7, MDEntryTypeOffer),
	)
	var p Packet
	var err error
	p, err = ParsePacket(packet, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, isInc := p.Incremental(); isInc {
		t.Fatal("snapshot packet must not be incremental")
	}
	var m Message
	var ok bool
	m, ok, err = p.Next()
	if err != nil || !ok || m.Kind != KindOrderBookSnapshot {
		t.Fatalf("Next: %v %v %v", m, ok, err)
	}
	var v SnapshotView
	v, ok = m.Snapshot()
	if !ok || v.SecurityID != 42 || v.LastMsgSeqNumProcessed != 1000 || v.RptSeq != 55 || v.ExchangeTradingSessionID != 7562 || v.Len() != 2 {
		t.Fatalf("view %+v ok=%v", v, ok)
	}
	if e := v.Entry(1); e.MDEntryID != 12 || e.MDEntryPx != 101 || e.MDEntrySize != 7 || e.MDEntryType != MDEntryTypeOffer || e.TradeID != NullInt64 {
		t.Fatalf("entry %+v", e)
	}
	if sec, ok := m.SecurityID(); !ok || sec != 42 {
		t.Fatal("SecurityID fast path")
	}
	if _, ok, _ = p.Next(); ok {
		t.Fatal("snapshot packet carries exactly one message")
	}
}

func TestBestPricesViewAndTrailingMessage(t *testing.T) {
	var bp []byte = []byte{36, 0, 2}
	var e1, e2 [36]byte
	binary.LittleEndian.PutUint64(e1[0:8], 77650)
	binary.LittleEndian.PutUint64(e1[8:16], uint64(NullDecimalMantissa))
	binary.LittleEndian.PutUint32(e1[32:36], 1439162)
	binary.LittleEndian.PutUint32(e2[32:36], 7)
	bp = append(bp, e1[:]...)
	bp = append(bp, e2[:]...)
	var packet []byte = incrementalPacket(3, FlagLastFragment,
		sbeMessage(TemplateBestPrices, 0, bp),
		sbeMessage(TemplateOrderUpdate, orderUpdateSize, orderUpdateBody(7, 1, 1, 1, 1, MDUpdateActionNew, MDEntryTypeBid)),
	)
	var p Packet
	var err error
	p, err = ParsePacket(packet, 0)
	if err != nil {
		t.Fatal(err)
	}
	var m Message
	m, _, err = p.Next()
	if err != nil {
		t.Fatal(err)
	}
	var v BestPricesView
	var ok bool
	v, ok = m.BestPrices()
	if !ok || v.Len() != 2 || v.Entry(0).MktBidPx != 77650 || v.Entry(0).MktOfferPx != NullDecimalMantissa || v.Entry(1).SecurityID != 7 {
		t.Fatalf("bestprices view %+v ok=%v", v, ok)
	}
	m, ok, err = p.Next()
	if err != nil || !ok || m.Kind != KindOrderUpdate {
		t.Fatalf("message after group: %v %v %v", m, ok, err)
	}
}

// TestParsePacketZeroAlloc — the hot path must not touch the heap.
func TestParsePacketZeroAlloc(t *testing.T) {
	var packet []byte = incrementalPacket(105806, FlagLastFragment,
		sbeMessage(TemplateOrderUpdate, orderUpdateSize, orderUpdateBody(1439162, 60142, 1, 77667, 26, MDUpdateActionNew, MDEntryTypeBid)),
		sbeMessage(TemplateOrderExecution, orderExecutionSize, orderExecutionBody(1439162, 60144, 1, 77667, 0, 77664, 26, 5, 2, MDUpdateActionDelete, MDEntryTypeBid)),
	)
	var snap []byte = snapshotPacket(7, FlagStartOfSnapshot|FlagEndOfSnapshot|FlagLastFragment, 42, 1000, 55, 7562,
		snapshotEntry(11, 100, 5, MDEntryTypeBid), snapshotEntry(12, 101, 7, MDEntryTypeOffer))
	var sink int64
	var allocs float64 = testing.AllocsPerRun(1000, func() {
		var p Packet
		var err error
		p, err = ParsePacket(packet, 0)
		if err != nil {
			panic(err)
		}
		for {
			var m Message
			var ok bool
			m, ok, err = p.Next()
			if err != nil {
				panic(err)
			}
			if !ok {
				break
			}
			if u, ok := m.OrderUpdate(); ok {
				sink += u.MDEntrySize
			}
			if e, ok := m.OrderExecution(); ok {
				sink += e.LastQty
			}
		}
		p, _ = ParsePacket(snap, 0)
		var m Message
		m, _, _ = p.Next()
		var v SnapshotView
		v, _ = m.Snapshot()
		for i := 0; i < v.Len(); i++ {
			sink += v.Entry(i).MDEntrySize
		}
	})
	if allocs != 0 {
		t.Fatalf("hot path allocates: %.1f allocs/op", allocs)
	}
	if sink == 0 {
		t.Fatal("sink")
	}
}
