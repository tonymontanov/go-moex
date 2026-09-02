/*
FILE: internal/simba/decode.go

DESCRIPTION:
Zero-copy-ish binary decoders for SIMBA SPECTRA packets. Every decode
function takes the raw UDP payload as received and returns a Go struct;
no intermediate []byte copies beyond what encoding/binary.LittleEndian
requires (fixed-width reads directly off the slice). This matters because
SIMBA runs on the market-data hot path — one packet per order-book event
during the trading session.

PACKET STRUCTURE (§2.3 of spectra_simba_en.pdf, confirmed byte-for-byte
against simba_spectra-9.0.xml):

	[MarketDataPacketHeader: 16 bytes]
	[IncrementalPacketHeader: 12 bytes]   <- only if MsgFlags.IncrementalPacket
	[messageHeader (SBE header): 8 bytes]
	[message root block, fixed size per template]
	[repeating group, if the template has one]

DecodePacket dispatches on SBEHeader.TemplateID after parsing the two
outer headers, returning a typed payload via the Decoded struct (a closed
sum type — Go has no union types, so a struct-of-optional-fields plus a
Kind discriminator is the idiomatic zero-alloc alternative to interface{}).
*/
package simba

import (
	"encoding/binary"
	"fmt"
)

const (
	packetHeaderSize      = 16
	incrementalHeaderSize = 12
	sbeHeaderSize         = 8
)

// ParsePacketHeader decodes the 16-byte MarketDataPacketHeader from the
// front of buf and returns the remaining bytes.
func ParsePacketHeader(buf []byte) (PacketHeader, []byte, error) {
	if len(buf) < packetHeaderSize {
		return PacketHeader{}, nil, fmt.Errorf("simba: packet too short for MarketDataPacketHeader: %d bytes", len(buf))
	}
	var h PacketHeader
	h.MsgSeqNum = binary.LittleEndian.Uint32(buf[0:4])
	h.MsgSize = binary.LittleEndian.Uint16(buf[4:6])
	h.MsgFlags = binary.LittleEndian.Uint16(buf[6:8])
	h.SendingTime = binary.LittleEndian.Uint64(buf[8:16])
	return h, buf[packetHeaderSize:], nil
}

// parseIncrementalHeader decodes the 12-byte IncrementalPacketHeader.
func parseIncrementalHeader(buf []byte) (IncrementalHeader, []byte, error) {
	if len(buf) < incrementalHeaderSize {
		return IncrementalHeader{}, nil, fmt.Errorf("simba: packet too short for IncrementalPacketHeader: %d bytes", len(buf))
	}
	var h IncrementalHeader
	h.TransactTime = binary.LittleEndian.Uint64(buf[0:8])
	h.ExchangeTradingSessionID = binary.LittleEndian.Uint32(buf[8:12])
	return h, buf[incrementalHeaderSize:], nil
}

// parseSBEHeader decodes the 8-byte messageHeader.
func parseSBEHeader(buf []byte) (SBEHeader, []byte, error) {
	if len(buf) < sbeHeaderSize {
		return SBEHeader{}, nil, fmt.Errorf("simba: packet too short for SBE messageHeader: %d bytes", len(buf))
	}
	var h SBEHeader
	h.BlockLength = binary.LittleEndian.Uint16(buf[0:2])
	h.TemplateID = binary.LittleEndian.Uint16(buf[2:4])
	h.SchemaID = binary.LittleEndian.Uint16(buf[4:6])
	h.Version = binary.LittleEndian.Uint16(buf[6:8])
	return h, buf[sbeHeaderSize:], nil
}

// Decoded — decoded application payload of one SIMBA packet. Kind
// discriminates which of the pointer fields is populated (only one, or
// none for Heartbeat/SequenceReset which carry no useful payload beyond
// the headers themselves).
type Decoded struct {
	PacketHeader PacketHeader
	// Incremental — non-nil iff PacketHeader.IsIncremental().
	Incremental *IncrementalHeader
	SBE         SBEHeader

	BestPrices         *BestPrices
	EmptyBook          *EmptyBook
	OrderUpdate        *OrderUpdate
	OrderExecution     *OrderExecution
	OrderBookSnapshot  *OrderBookSnapshot
	SecurityDefinition *SecurityDefinition
	// NewSeqNo — populated for SequenceReset(2); the session-level
	// resync value the caller should adopt for gap tracking.
	NewSeqNo *uint32
}

// DecodePacket parses one full UDP datagram payload and decodes its FIRST
// SBE message into the pointer-based Decoded. Kept for v1.0 callers; hot
// paths use ParsePacket/Packet.Next (no allocation, every message).
func DecodePacket(buf []byte) (Decoded, error) {
	var out Decoded
	var first bool = true
	var decodeErr error
	var info PacketInfo
	var err error
	info, err = Walk(buf, func(hdr SBEHeader, body []byte) bool {
		if !first {
			return false
		}
		first = false
		out.SBE = hdr
		decodeErr = DecodeMessage(hdr, body, &out)
		return false
	})
	out.PacketHeader = info.Header
	if info.Header.IsIncremental() {
		var ih IncrementalHeader = info.Incremental
		out.Incremental = &ih
	}
	if err != nil {
		return out, err
	}
	if first {
		return out, fmt.Errorf("simba: packet too short for SBE messageHeader: %d bytes", len(buf)-packetHeaderSize)
	}
	return out, decodeErr
}

const bestPricesEntrySize = 8 + 8 + 8 + 8 + 4 // 36

func decodeBestPrices(buf []byte, rootBlockLength uint16) (*BestPrices, error) {
	// BestPrices has an empty root block (all fields live in the repeating
	// group) — rootBlockLength should be 0, but skip it defensively in case
	// a future schema version adds root fields.
	if len(buf) < int(rootBlockLength) {
		return nil, fmt.Errorf("simba: BestPrices root block truncated")
	}
	buf = buf[rootBlockLength:]

	if len(buf) < 3 {
		return nil, fmt.Errorf("simba: BestPrices missing group dimensions")
	}
	var groupBlockLen uint16 = binary.LittleEndian.Uint16(buf[0:2])
	var numInGroup uint8 = buf[2]
	buf = buf[3:]

	var out *BestPrices = &BestPrices{Entries: make([]BestPricesEntry, 0, numInGroup)}
	for i := 0; i < int(numInGroup); i++ {
		if len(buf) < int(groupBlockLen) {
			return nil, fmt.Errorf("simba: BestPrices entry %d truncated", i)
		}
		var e BestPricesEntry
		e.MktBidPx = int64(binary.LittleEndian.Uint64(buf[0:8]))
		e.MktOfferPx = int64(binary.LittleEndian.Uint64(buf[8:16]))
		e.MktBidSize = int64(binary.LittleEndian.Uint64(buf[16:24]))
		e.MktOfferSize = int64(binary.LittleEndian.Uint64(buf[24:32]))
		e.SecurityID = int32(binary.LittleEndian.Uint32(buf[32:36]))
		out.Entries = append(out.Entries, e)
		buf = buf[groupBlockLen:]
	}
	return out, nil
}

func decodeEmptyBook(buf []byte) (*EmptyBook, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("simba: EmptyBook body too short")
	}
	return &EmptyBook{LastMsgSeqNumProcessed: binary.LittleEndian.Uint32(buf[0:4])}, nil
}

const orderUpdateSize = 8 + 8 + 8 + 8 + 8 + 4 + 4 + 1 + 1 // 50

func decodeOrderUpdate(buf []byte) (*OrderUpdate, error) {
	if len(buf) < orderUpdateSize {
		return nil, fmt.Errorf("simba: OrderUpdate body too short: %d bytes, want >= %d", len(buf), orderUpdateSize)
	}
	var m OrderUpdate
	m.MDEntryID = int64(binary.LittleEndian.Uint64(buf[0:8]))
	m.MDEntryPx = int64(binary.LittleEndian.Uint64(buf[8:16]))
	m.MDEntrySize = int64(binary.LittleEndian.Uint64(buf[16:24]))
	m.MDFlags = binary.LittleEndian.Uint64(buf[24:32])
	m.MDFlags2 = binary.LittleEndian.Uint64(buf[32:40])
	m.SecurityID = int32(binary.LittleEndian.Uint32(buf[40:44]))
	m.RptSeq = binary.LittleEndian.Uint32(buf[44:48])
	m.MDUpdateAction = MDUpdateAction(buf[48])
	m.MDEntryType = MDEntryType(buf[49])
	return &m, nil
}

const orderExecutionSize = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 4 + 4 + 1 + 1 // 74

func decodeOrderExecution(buf []byte) (*OrderExecution, error) {
	if len(buf) < orderExecutionSize {
		return nil, fmt.Errorf("simba: OrderExecution body too short: %d bytes, want >= %d", len(buf), orderExecutionSize)
	}
	var m OrderExecution
	m.MDEntryID = int64(binary.LittleEndian.Uint64(buf[0:8]))
	m.MDEntryPx = int64(binary.LittleEndian.Uint64(buf[8:16]))
	m.MDEntrySize = int64(binary.LittleEndian.Uint64(buf[16:24]))
	m.LastPx = int64(binary.LittleEndian.Uint64(buf[24:32]))
	m.LastQty = int64(binary.LittleEndian.Uint64(buf[32:40]))
	m.TradeID = int64(binary.LittleEndian.Uint64(buf[40:48]))
	m.MDFlags = binary.LittleEndian.Uint64(buf[48:56])
	m.MDFlags2 = binary.LittleEndian.Uint64(buf[56:64])
	m.SecurityID = int32(binary.LittleEndian.Uint32(buf[64:68]))
	m.RptSeq = binary.LittleEndian.Uint32(buf[68:72])
	m.MDUpdateAction = MDUpdateAction(buf[72])
	m.MDEntryType = MDEntryType(buf[73])
	return &m, nil
}

const orderBookSnapshotRootSize = 4 + 4 + 4 + 4                  // 16
const orderBookSnapshotEntrySize = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 1 // 57

// decodeSecurityDefinitionPrefix reads only the TotNumReports(uint32) +
// Symbol(String25, fixed-width, NUL/space-padded) + SecurityID(Int32)
// prefix of the SecurityDefinition(27) root block — see the
// SecurityDefinition doc comment in types.go for why a fixed-offset
// prefix read is safe without decoding the rest of this large message.
func decodeSecurityDefinitionPrefix(buf []byte) (*SecurityDefinition, error) {
	const symbolOffset = 4
	const symbolLen = 25
	const securityIDOffset = symbolOffset + symbolLen // 29
	if len(buf) < securityIDOffset+4 {
		return nil, fmt.Errorf("simba: SecurityDefinition body too short for the Symbol/SecurityID prefix")
	}
	var symbolRaw []byte = buf[symbolOffset : symbolOffset+symbolLen]
	var end int = len(symbolRaw)
	for end > 0 && (symbolRaw[end-1] == 0 || symbolRaw[end-1] == ' ') {
		end--
	}
	return &SecurityDefinition{
		Symbol:     string(symbolRaw[:end]),
		SecurityID: int32(binary.LittleEndian.Uint32(buf[securityIDOffset : securityIDOffset+4])),
	}, nil
}

func decodeOrderBookSnapshot(buf []byte, rootBlockLength uint16) (*OrderBookSnapshot, error) {
	if len(buf) < int(rootBlockLength) || rootBlockLength < orderBookSnapshotRootSize {
		return nil, fmt.Errorf("simba: OrderBookSnapshot root block too short")
	}
	var m OrderBookSnapshot
	m.SecurityID = int32(binary.LittleEndian.Uint32(buf[0:4]))
	m.LastMsgSeqNumProcessed = binary.LittleEndian.Uint32(buf[4:8])
	m.RptSeq = binary.LittleEndian.Uint32(buf[8:12])
	m.ExchangeTradingSessionID = binary.LittleEndian.Uint32(buf[12:16])
	buf = buf[rootBlockLength:]

	if len(buf) < 3 {
		return nil, fmt.Errorf("simba: OrderBookSnapshot missing group dimensions")
	}
	var groupBlockLen uint16 = binary.LittleEndian.Uint16(buf[0:2])
	var numInGroup uint8 = buf[2]
	buf = buf[3:]

	m.Entries = make([]OrderBookSnapshotEntry, 0, numInGroup)
	for i := 0; i < int(numInGroup); i++ {
		if len(buf) < int(groupBlockLen) {
			return nil, fmt.Errorf("simba: OrderBookSnapshot entry %d truncated", i)
		}
		var e OrderBookSnapshotEntry
		e.MDEntryID = int64(binary.LittleEndian.Uint64(buf[0:8]))
		e.TransactTime = binary.LittleEndian.Uint64(buf[8:16])
		e.MDEntryPx = int64(binary.LittleEndian.Uint64(buf[16:24]))
		e.MDEntrySize = int64(binary.LittleEndian.Uint64(buf[24:32]))
		e.TradeID = int64(binary.LittleEndian.Uint64(buf[32:40]))
		e.MDFlags = binary.LittleEndian.Uint64(buf[40:48])
		e.MDFlags2 = binary.LittleEndian.Uint64(buf[48:56])
		e.MDEntryType = MDEntryType(buf[56])
		m.Entries = append(m.Entries, e)
		buf = buf[groupBlockLen:]
	}
	return &m, nil
}
