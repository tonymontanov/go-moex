/*
FILE: internal/simba/packet.go

DESCRIPTION:
Allocation-free view of one SIMBA datagram. ParsePacket validates the
outer headers (MsgSize must equal the datagram length; the first SBE
header must carry SchemaID 19780 and a supported Version) and Packet.Next
steps over the SBE messages inside — one or more per Incremental packet
(spec §2.3.1), exactly one per Snapshot packet (§2.3.2). Message accessors
(OrderUpdate, OrderExecution, BestPrices, Snapshot, ...) decode fixed
offsets off the datagram buffer into value structs: no pointers, no
slices, no per-message heap traffic. Repeating groups are exposed as
Len()/Entry(i) views instead of materialised slices.

This is the hot-path API; Walk/DecodeMessage/DecodePacket are thin
wrappers kept for the harness and v1.0 callers.

Sizing rule: root block = SBEHeader.BlockLength (read from the wire, so
a version that appends root fields still parses); groups and var-data are
skipped per kindShape. Messages this package cannot size (DiscreteAuction:
var-data nested inside a group) are only accepted as the last message of
a packet.
*/
package simba

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Parse errors. Callers on a live feed should count them and treat a
// stream of them as corruption/schema drift, not skip silently.
var (
	ErrShortPacket    = errors.New("simba: packet shorter than its headers")
	ErrMsgSize        = errors.New("simba: MsgSize does not match datagram length")
	ErrSchemaID       = errors.New("simba: unexpected SchemaID")
	ErrSchemaVersion  = errors.New("simba: unsupported schema version")
	ErrTruncatedGroup = errors.New("simba: repeating group or var-data truncated")
	ErrUnsizedMiddle  = errors.New("simba: unsizable message followed by more bytes")
	ErrBodyTooShort   = errors.New("simba: message body shorter than its root block")
)

// Packet — parsed headers plus an iterator over the SBE messages of one
// datagram. Zero-value is not usable; obtain via ParsePacket.
type Packet struct {
	buf     []byte
	hdr     PacketHeader
	inc     IncrementalHeader
	off     int    // offset of the next SBE header in buf.
	version uint16 // schema version of the first message (all messages share it).
	n       int    // messages yielded so far.
}

// Message — one SBE message inside a Packet. Body aliases the datagram
// buffer (root block + groups + var-data). Accessors decode on demand.
type Message struct {
	Header SBEHeader
	Kind   Kind
	body   []byte
}

// Body returns the raw message body (root block + groups + var-data).
func (m Message) Body() []byte { return m.body }

// ParsePacket validates the datagram headers. It accepts any schema
// version listed by SupportedVersion; pass a non-zero requireVersion to
// pin one.
func ParsePacket(buf []byte, requireVersion uint16) (Packet, error) {
	var p Packet
	if len(buf) < packetHeaderSize {
		return p, ErrShortPacket
	}
	p.buf = buf
	p.hdr.MsgSeqNum = binary.LittleEndian.Uint32(buf[0:4])
	p.hdr.MsgSize = binary.LittleEndian.Uint16(buf[4:6])
	p.hdr.MsgFlags = binary.LittleEndian.Uint16(buf[6:8])
	p.hdr.SendingTime = binary.LittleEndian.Uint64(buf[8:16])
	if int(p.hdr.MsgSize) != len(buf) {
		return p, ErrMsgSize
	}
	p.off = packetHeaderSize
	if p.hdr.IsIncremental() {
		if len(buf) < packetHeaderSize+incrementalHeaderSize {
			return p, ErrShortPacket
		}
		p.inc.TransactTime = binary.LittleEndian.Uint64(buf[16:24])
		p.inc.ExchangeTradingSessionID = binary.LittleEndian.Uint32(buf[24:28])
		p.off += incrementalHeaderSize
	}
	if len(buf) < p.off+sbeHeaderSize {
		// A packet with headers only (never seen on the wire) is not an
		// error per se; Next just yields nothing.
		return p, nil
	}
	var schemaID uint16 = binary.LittleEndian.Uint16(buf[p.off+4 : p.off+6])
	var version uint16 = binary.LittleEndian.Uint16(buf[p.off+6 : p.off+8])
	if schemaID != SchemaID {
		return p, ErrSchemaID
	}
	if !SupportedVersion(version) || (requireVersion != 0 && version != requireVersion) {
		return p, ErrSchemaVersion
	}
	p.version = version
	return p, nil
}

// Header returns the MarketDataPacketHeader.
func (p *Packet) Header() PacketHeader { return p.hdr }

// Incremental returns the IncrementalPacketHeader; ok is false for
// Snapshot-type packets.
func (p *Packet) Incremental() (IncrementalHeader, bool) { return p.inc, p.hdr.IsIncremental() }

// Version returns the schema version carried by the packet's messages.
func (p *Packet) Version() uint16 { return p.version }

// Messages returns how many messages Next has yielded so far.
func (p *Packet) Messages() int { return p.n }

// Next yields the next SBE message. ok is false when the packet is
// exhausted; err is non-nil on a malformed message (iteration stops).
func (p *Packet) Next() (m Message, ok bool, err error) {
	if p.off >= len(p.buf) {
		return m, false, nil
	}
	var rest []byte = p.buf[p.off:]
	if len(rest) < sbeHeaderSize {
		return m, false, ErrShortPacket
	}
	m.Header.BlockLength = binary.LittleEndian.Uint16(rest[0:2])
	m.Header.TemplateID = binary.LittleEndian.Uint16(rest[2:4])
	m.Header.SchemaID = binary.LittleEndian.Uint16(rest[4:6])
	m.Header.Version = binary.LittleEndian.Uint16(rest[6:8])
	m.Kind = kindOf(m.Header.Version, m.Header.TemplateID)
	rest = rest[sbeHeaderSize:]

	var size int
	size, err = messageSizeKind(m.Kind, m.Header.BlockLength, rest)
	if err != nil {
		return m, false, err
	}
	if size < 0 {
		// Unsizable: must be the last message; takes the rest.
		m.body = rest
		p.off = len(p.buf)
		p.n++
		return m, true, nil
	}
	m.body = rest[:size]
	p.off += sbeHeaderSize + size
	p.n++
	return m, true, nil
}

// messageSizeKind returns the body size of a message of kind k, or -1 if
// it cannot be derived from the schema.
func messageSizeKind(k Kind, blockLength uint16, body []byte) (int, error) {
	var shape templateShape
	var known bool
	shape, known = kindShape(k)
	if !known || shape.unsizable {
		if int(blockLength) > len(body) {
			return 0, ErrBodyTooShort
		}
		return -1, nil
	}
	var off int = int(blockLength)
	if off > len(body) {
		return 0, ErrBodyTooShort
	}
	for _, dim := range shape.groupDims {
		if len(body) < off+dim {
			return 0, ErrTruncatedGroup
		}
		var blockLen int = int(binary.LittleEndian.Uint16(body[off : off+2]))
		var num int
		if dim == 3 {
			num = int(body[off+2])
		} else {
			num = int(binary.LittleEndian.Uint16(body[off+2 : off+4]))
		}
		off += dim + blockLen*num
		if off > len(body) {
			return 0, ErrTruncatedGroup
		}
	}
	for vi := 0; vi < shape.varData; vi++ {
		if len(body) < off+2 {
			return 0, ErrTruncatedGroup
		}
		off += 2 + int(binary.LittleEndian.Uint16(body[off:off+2]))
		if off > len(body) {
			return 0, ErrTruncatedGroup
		}
	}
	return off, nil
}

// ---- message accessors (value types, no allocation) ----

// SequenceReset returns NewSeqNo. Only valid for KindSequenceReset.
func (m Message) SequenceReset() (newSeqNo uint32, ok bool) {
	if m.Kind != KindSequenceReset || len(m.body) < 4 {
		return 0, false
	}
	return binary.LittleEndian.Uint32(m.body[0:4]), true
}

// EmptyBook decodes EmptyBook(4).
func (m Message) EmptyBook() (EmptyBook, bool) {
	if m.Kind != KindEmptyBook || len(m.body) < 4 {
		return EmptyBook{}, false
	}
	return EmptyBook{LastMsgSeqNumProcessed: binary.LittleEndian.Uint32(m.body[0:4])}, true
}

// OrderUpdate decodes OrderUpdate(15).
func (m Message) OrderUpdate() (OrderUpdate, bool) {
	var u OrderUpdate
	if m.Kind != KindOrderUpdate || len(m.body) < orderUpdateSize {
		return u, false
	}
	var b []byte = m.body
	u.MDEntryID = int64(binary.LittleEndian.Uint64(b[0:8]))
	u.MDEntryPx = int64(binary.LittleEndian.Uint64(b[8:16]))
	u.MDEntrySize = int64(binary.LittleEndian.Uint64(b[16:24]))
	u.MDFlags = binary.LittleEndian.Uint64(b[24:32])
	u.MDFlags2 = binary.LittleEndian.Uint64(b[32:40])
	u.SecurityID = int32(binary.LittleEndian.Uint32(b[40:44]))
	u.RptSeq = binary.LittleEndian.Uint32(b[44:48])
	u.MDUpdateAction = MDUpdateAction(b[48])
	u.MDEntryType = MDEntryType(b[49])
	return u, true
}

// OrderExecution decodes OrderExecution(16).
func (m Message) OrderExecution() (OrderExecution, bool) {
	var e OrderExecution
	if m.Kind != KindOrderExecution || len(m.body) < orderExecutionSize {
		return e, false
	}
	var b []byte = m.body
	e.MDEntryID = int64(binary.LittleEndian.Uint64(b[0:8]))
	e.MDEntryPx = int64(binary.LittleEndian.Uint64(b[8:16]))
	e.MDEntrySize = int64(binary.LittleEndian.Uint64(b[16:24]))
	e.LastPx = int64(binary.LittleEndian.Uint64(b[24:32]))
	e.LastQty = int64(binary.LittleEndian.Uint64(b[32:40]))
	e.TradeID = int64(binary.LittleEndian.Uint64(b[40:48]))
	e.MDFlags = binary.LittleEndian.Uint64(b[48:56])
	e.MDFlags2 = binary.LittleEndian.Uint64(b[56:64])
	e.SecurityID = int32(binary.LittleEndian.Uint32(b[64:68]))
	e.RptSeq = binary.LittleEndian.Uint32(b[68:72])
	e.MDUpdateAction = MDUpdateAction(b[72])
	e.MDEntryType = MDEntryType(b[73])
	return e, true
}

// SecurityID of an OrderUpdate/OrderExecution without decoding the rest
// (for fast per-instrument routing). ok is false for other kinds.
func (m Message) SecurityID() (int32, bool) {
	switch m.Kind {
	case KindOrderUpdate:
		if len(m.body) >= 44 {
			return int32(binary.LittleEndian.Uint32(m.body[40:44])), true
		}
	case KindOrderExecution:
		if len(m.body) >= 68 {
			return int32(binary.LittleEndian.Uint32(m.body[64:68])), true
		}
	case KindOrderBookSnapshot:
		if len(m.body) >= 4 {
			return int32(binary.LittleEndian.Uint32(m.body[0:4])), true
		}
	}
	return 0, false
}

// GroupView — a repeating group: entries of a fixed wire size.
type GroupView struct {
	entries  []byte
	blockLen int
	n        int
}

// Len returns numInGroup.
func (g GroupView) Len() int { return g.n }

func (g GroupView) entry(i int) []byte { return g.entries[i*g.blockLen : (i+1)*g.blockLen] }

// groupAfter parses group dimensions at body[root:].
func groupAfter(body []byte, root int) (GroupView, bool) {
	if len(body) < root+3 {
		return GroupView{}, false
	}
	var g GroupView
	g.blockLen = int(binary.LittleEndian.Uint16(body[root : root+2]))
	g.n = int(body[root+2])
	var start int = root + 3
	if g.blockLen*g.n > len(body)-start {
		return GroupView{}, false
	}
	g.entries = body[start : start+g.blockLen*g.n]
	return g, true
}

// BestPricesView — BestPrices(14) with Entry(i) access.
type BestPricesView struct{ g GroupView }

// BestPrices returns a view over the BestPrices group.
func (m Message) BestPrices() (BestPricesView, bool) {
	if m.Kind != KindBestPrices {
		return BestPricesView{}, false
	}
	var g GroupView
	var ok bool
	g, ok = groupAfter(m.body, int(m.Header.BlockLength))
	if !ok || g.blockLen < bestPricesEntrySize {
		return BestPricesView{}, false
	}
	return BestPricesView{g: g}, true
}

// Len returns the number of entries.
func (v BestPricesView) Len() int { return v.g.Len() }

// Entry decodes entry i (0 <= i < Len()).
func (v BestPricesView) Entry(i int) BestPricesEntry {
	var b []byte = v.g.entry(i)
	return BestPricesEntry{
		MktBidPx:     int64(binary.LittleEndian.Uint64(b[0:8])),
		MktOfferPx:   int64(binary.LittleEndian.Uint64(b[8:16])),
		MktBidSize:   int64(binary.LittleEndian.Uint64(b[16:24])),
		MktOfferSize: int64(binary.LittleEndian.Uint64(b[24:32])),
		SecurityID:   int32(binary.LittleEndian.Uint32(b[32:36])),
	}
}

// SnapshotView — OrderBookSnapshot(17): root fields + Entry(i) access.
type SnapshotView struct {
	SecurityID               int32
	LastMsgSeqNumProcessed   uint32
	RptSeq                   uint32
	ExchangeTradingSessionID uint32
	g                        GroupView
}

// Snapshot returns a view over an OrderBookSnapshot message.
func (m Message) Snapshot() (SnapshotView, bool) {
	var v SnapshotView
	if m.Kind != KindOrderBookSnapshot || m.Header.BlockLength < orderBookSnapshotRootSize || len(m.body) < orderBookSnapshotRootSize {
		return v, false
	}
	var b []byte = m.body
	v.SecurityID = int32(binary.LittleEndian.Uint32(b[0:4]))
	v.LastMsgSeqNumProcessed = binary.LittleEndian.Uint32(b[4:8])
	v.RptSeq = binary.LittleEndian.Uint32(b[8:12])
	v.ExchangeTradingSessionID = binary.LittleEndian.Uint32(b[12:16])
	var ok bool
	v.g, ok = groupAfter(b, int(m.Header.BlockLength))
	if !ok || v.g.blockLen < orderBookSnapshotEntrySize {
		return SnapshotView{}, false
	}
	return v, true
}

// Len returns the number of entries in this fragment.
func (v SnapshotView) Len() int { return v.g.Len() }

// Entry decodes entry i.
func (v SnapshotView) Entry(i int) OrderBookSnapshotEntry {
	var b []byte = v.g.entry(i)
	return OrderBookSnapshotEntry{
		MDEntryID:    int64(binary.LittleEndian.Uint64(b[0:8])),
		TransactTime: binary.LittleEndian.Uint64(b[8:16]),
		MDEntryPx:    int64(binary.LittleEndian.Uint64(b[16:24])),
		MDEntrySize:  int64(binary.LittleEndian.Uint64(b[24:32])),
		TradeID:      int64(binary.LittleEndian.Uint64(b[32:40])),
		MDFlags:      binary.LittleEndian.Uint64(b[40:48]),
		MDFlags2:     binary.LittleEndian.Uint64(b[48:56]),
		MDEntryType:  MDEntryType(b[56]),
	}
}

// SecurityDefinitionPrefix — the Symbol/SecurityID prefix of
// SecurityDefinition (TotNumReports uint32 + Symbol String25 + SecurityID
// Int32, offsets 0/4/29 — identical in schema versions 8 and 9).
type SecurityDefinitionPrefix struct {
	TotNumReports uint32
	SecurityID    int32
	symbol        [25]byte
	symbolLen     int
}

// Symbol returns the trimmed symbol bytes (aliases the struct, no alloc).
func (s *SecurityDefinitionPrefix) Symbol() []byte { return s.symbol[:s.symbolLen] }

// SymbolString allocates a string for the symbol.
func (s *SecurityDefinitionPrefix) SymbolString() string { return string(s.symbol[:s.symbolLen]) }

// SecurityDefinition decodes the prefix of a SecurityDefinition message.
func (m Message) SecurityDefinition() (SecurityDefinitionPrefix, bool) {
	var s SecurityDefinitionPrefix
	if m.Kind != KindSecurityDefinition || len(m.body) < 33 {
		return s, false
	}
	s.TotNumReports = binary.LittleEndian.Uint32(m.body[0:4])
	copy(s.symbol[:], m.body[4:29])
	s.symbolLen = 25
	for s.symbolLen > 0 && (s.symbol[s.symbolLen-1] == 0 || s.symbol[s.symbolLen-1] == ' ') {
		s.symbolLen--
	}
	s.SecurityID = int32(binary.LittleEndian.Uint32(m.body[29:33]))
	return s, true
}

// String renders a message for logs/diagnostics (allocates).
func (m Message) String() string {
	return fmt.Sprintf("%s(template=%d v=%d block=%d body=%d)", m.Kind, m.Header.TemplateID, m.Header.Version, m.Header.BlockLength, len(m.body))
}

// SequenceResetIn reports whether the datagram carries a SequenceReset
// message (a single-message packet on the wire) and its NewSeqNo. Cheap:
// header parse plus one message header.
func SequenceResetIn(buf []byte) (newSeqNo uint32, ok bool) {
	var p Packet
	var err error
	p, err = ParsePacket(buf, 0)
	if err != nil {
		return 0, false
	}
	var m Message
	m, ok, err = p.Next()
	if err != nil || !ok {
		return 0, false
	}
	return m.SequenceReset()
}
