package simba

import (
	"encoding/binary"
	"testing"
)

// sbeMessage assembles messageHeader + body.
func sbeMessage(templateID uint16, blockLength uint16, body []byte) []byte {
	var h [8]byte
	binary.LittleEndian.PutUint16(h[0:2], blockLength)
	binary.LittleEndian.PutUint16(h[2:4], templateID)
	binary.LittleEndian.PutUint16(h[4:6], 19780)
	binary.LittleEndian.PutUint16(h[6:8], 9)
	return append(h[:], body...)
}

// incrementalPacket wraps messages in MarketDataPacketHeader +
// IncrementalPacketHeader with a correct MsgSize.
func incrementalPacket(seq uint32, flags uint16, msgs ...[]byte) []byte {
	var out []byte = make([]byte, 28)
	binary.LittleEndian.PutUint32(out[0:4], seq)
	binary.LittleEndian.PutUint16(out[6:8], flags|FlagIncrementalPacket)
	binary.LittleEndian.PutUint64(out[8:16], 1700000000000000000)
	binary.LittleEndian.PutUint64(out[16:24], 1700000000000000001)
	binary.LittleEndian.PutUint32(out[24:28], 6144)
	for _, m := range msgs {
		out = append(out, m...)
	}
	binary.LittleEndian.PutUint16(out[4:6], uint16(len(out)))
	return out
}

func orderUpdateBody(sec int32, rpt uint32, id int64, px, size int64, action MDUpdateAction, side MDEntryType) []byte {
	var b [orderUpdateSize]byte
	binary.LittleEndian.PutUint64(b[0:8], uint64(id))
	binary.LittleEndian.PutUint64(b[8:16], uint64(px))
	binary.LittleEndian.PutUint64(b[16:24], uint64(size))
	binary.LittleEndian.PutUint64(b[24:32], 2) // IOC
	binary.LittleEndian.PutUint32(b[40:44], uint32(sec))
	binary.LittleEndian.PutUint32(b[44:48], rpt)
	b[48] = byte(action)
	b[49] = byte(side)
	return b[:]
}

func orderExecutionBody(sec int32, rpt uint32, id int64, px, size, lastPx, lastQty, tradeID int64, flags uint64, action MDUpdateAction, side MDEntryType) []byte {
	var b [orderExecutionSize]byte
	binary.LittleEndian.PutUint64(b[0:8], uint64(id))
	binary.LittleEndian.PutUint64(b[8:16], uint64(px))
	binary.LittleEndian.PutUint64(b[16:24], uint64(size))
	binary.LittleEndian.PutUint64(b[24:32], uint64(lastPx))
	binary.LittleEndian.PutUint64(b[32:40], uint64(lastQty))
	binary.LittleEndian.PutUint64(b[40:48], uint64(tradeID))
	binary.LittleEndian.PutUint64(b[48:56], flags)
	binary.LittleEndian.PutUint32(b[64:68], uint32(sec))
	binary.LittleEndian.PutUint32(b[68:72], rpt)
	b[72] = byte(action)
	b[73] = byte(side)
	return b[:]
}

// TestWalkSpecExample_4_2_1 replays the second packet of spec §4.2.1: one
// OrderUpdate followed by two OrderExecution messages in ONE datagram.
// DecodePacket (v1.0 API) sees only the first; Walk must deliver all three.
func TestWalkSpecExample_4_2_1(t *testing.T) {
	var packet []byte = incrementalPacket(105806, FlagLastFragment,
		sbeMessage(TemplateOrderUpdate, orderUpdateSize, orderUpdateBody(1439162, 60142, 1892945606659163299, 77667, 26, MDUpdateActionNew, MDEntryTypeBid)),
		sbeMessage(TemplateOrderExecution, orderExecutionSize, orderExecutionBody(1439162, 60144, 1892945606659163299, 77667, 0, 77664, 26, 1892945606658296055, 0x20000000002, MDUpdateActionDelete, MDEntryTypeBid)),
		sbeMessage(TemplateOrderExecution, orderExecutionSize, orderExecutionBody(1439162, 60145, 1892945606659163300, 77664, 0, 77664, 26, 1892945606658296055, 0x40000000001, MDUpdateActionDelete, MDEntryTypeOffer)),
	)

	var got []uint16
	var rpts []uint32
	var info PacketInfo
	var err error
	info, err = Walk(packet, func(hdr SBEHeader, body []byte) bool {
		got = append(got, hdr.TemplateID)
		var d Decoded
		if e := DecodeMessage(hdr, body, &d); e != nil {
			t.Fatalf("DecodeMessage: %v", e)
		}
		switch {
		case d.OrderUpdate != nil:
			rpts = append(rpts, d.OrderUpdate.RptSeq)
		case d.OrderExecution != nil:
			rpts = append(rpts, d.OrderExecution.RptSeq)
		}
		return true
	})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if info.Header.MsgSeqNum != 105806 || !info.Header.IsIncremental() || info.Incremental.ExchangeTradingSessionID != 6144 {
		t.Fatalf("headers: %+v", info)
	}
	if info.Messages != 3 || info.Truncated {
		t.Fatalf("Messages=%d Truncated=%v, want 3/false", info.Messages, info.Truncated)
	}
	if len(got) != 3 || got[0] != TemplateOrderUpdate || got[1] != TemplateOrderExecution || got[2] != TemplateOrderExecution {
		t.Fatalf("templates %v", got)
	}
	if len(rpts) != 3 || rpts[0] != 60142 || rpts[1] != 60144 || rpts[2] != 60145 {
		t.Fatalf("RptSeq %v", rpts)
	}

	// The legacy single-message API still returns the first message only.
	var d Decoded
	d, err = DecodePacket(packet)
	if err != nil || d.OrderUpdate == nil || d.OrderExecution != nil {
		t.Fatalf("DecodePacket: %+v err=%v", d, err)
	}
}

// TestWalkEmptyBookFirst replays spec §4.2.2: EmptyBook as the first of
// several messages in one packet — a first-message-only decoder would
// clear the book and lose the updates that follow.
func TestWalkEmptyBookFirst(t *testing.T) {
	var eb [4]byte
	binary.LittleEndian.PutUint32(eb[0:4], NullUint32)
	var packet []byte = incrementalPacket(105806, FlagLastFragment,
		sbeMessage(TemplateEmptyBook, 4, eb[:]),
		sbeMessage(TemplateOrderUpdate, orderUpdateSize, orderUpdateBody(1439162, 60142, 1, 77667, 26, MDUpdateActionNew, MDEntryTypeBid)),
	)
	var seen int
	var info PacketInfo
	var err error
	info, err = Walk(packet, func(hdr SBEHeader, body []byte) bool {
		seen++
		return true
	})
	if err != nil || info.Messages != 2 || seen != 2 {
		t.Fatalf("Messages=%d seen=%d err=%v", info.Messages, seen, err)
	}
}

// TestWalkGroupsAndTruncation checks that repeating groups are stepped
// over correctly (BestPrices with two entries followed by an OrderUpdate)
// and that a truncated last message is delivered with Truncated set.
func TestWalkGroupsAndTruncation(t *testing.T) {
	var bp []byte = []byte{36, 0, 2} // groupSize: blockLength=36, numInGroup=2
	bp = append(bp, make([]byte, 72)...)
	var packet []byte = incrementalPacket(7, FlagLastFragment,
		sbeMessage(TemplateBestPrices, 0, bp),
		sbeMessage(TemplateOrderUpdate, orderUpdateSize, orderUpdateBody(5, 1, 1, 100, 1, MDUpdateActionNew, MDEntryTypeBid)),
	)
	var templates []uint16
	var info PacketInfo
	var err error
	info, err = Walk(packet, func(hdr SBEHeader, body []byte) bool {
		templates = append(templates, hdr.TemplateID)
		return true
	})
	if err != nil || info.Truncated || len(templates) != 2 || templates[1] != TemplateOrderUpdate {
		t.Fatalf("templates=%v truncated=%v err=%v", templates, info.Truncated, err)
	}

	// Cut the BestPrices group short: the walker cannot size it, so it must
	// be delivered as the (truncated) last message.
	var short []byte = incrementalPacket(8, FlagLastFragment, sbeMessage(TemplateBestPrices, 0, bp[:3+36]))
	info, err = Walk(short, func(hdr SBEHeader, body []byte) bool { return true })
	if err != nil || !info.Truncated || info.Messages != 1 {
		t.Fatalf("truncated: Messages=%d Truncated=%v err=%v", info.Messages, info.Truncated, err)
	}
}
