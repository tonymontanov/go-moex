/*
FILE: internal/simba/walk.go

DESCRIPTION:
Callback-style wrappers over Packet for the offline harness
(cmd/simba-replay) and for v1.0 callers of DecodePacket. Hot-path code
uses Packet/Message from packet.go directly.

Walk is tolerant where ParsePacket is strict: it accepts a wrong MsgSize
(synthetic test packets) and a truncated last message (reported through
PacketInfo.Truncated) so that the harness can describe malformed traffic
instead of stopping on it.
*/
package simba

import (
	"encoding/binary"
	"fmt"
)

// templateShape — how to skip a message body after its root block.
type templateShape struct {
	groupDims []int // per repeating group: 3 for groupSize (uint16+uint8), 4 for groupSize2 (uint16+uint16).
	varData   int   // number of trailing var-length fields (uint16 length prefix each).
	unsizable bool  // shape has data nested inside a group; cannot be walked past.
}

// Template IDs of schema version 9 (simba_spectra-9.0.xml). Version 8
// differs for SecurityDefinition (21) and SecurityStatus (9) — see
// kindOf; hot-path code should switch on Message.Kind, not on these.
const (
	TemplateSecurityDefinitionUpdate uint16 = 10
	TemplateSecurityMassStatus       uint16 = 19
	TemplateSecurityGroupStatus      uint16 = 22
	TemplateDiscreteAuction          uint16 = 24
	TemplateTradingSessionStatus     uint16 = 26
	TemplateSecurityStatus           uint16 = 28
	TemplateLogon                    uint16 = 1000
	TemplateLogout                   uint16 = 1001
	TemplateMarketDataRequest        uint16 = 1002
	TemplateMarketDataDummyMessage   uint16 = 1003
)

// PacketInfo — the outer headers of one datagram plus walk diagnostics.
type PacketInfo struct {
	Header      PacketHeader
	Incremental IncrementalHeader // valid iff Header.IsIncremental().
	// Messages — number of SBE messages delivered to fn.
	Messages int
	// Truncated — the last message could not be sized from the schema
	// (body shorter than its declared groups/var-data): it was delivered
	// as the remainder of the packet and per-message decoders apply their
	// own bounds checks. A harness should count these; a live feed should
	// treat them as corruption.
	Truncated bool
	// Version — schema version of the messages (0 if none).
	Version uint16
}

// Walk parses the packet headers of buf and calls fn for every SBE message
// in order with its header and body (root block + groups + var-data, sized
// per the schema; for an unsizable/truncated last message, body is the
// rest of the packet). fn returns false to stop early. Body slices alias
// buf.
func Walk(buf []byte, fn func(hdr SBEHeader, body []byte) bool) (PacketInfo, error) {
	var info PacketInfo
	var rest []byte
	var err error
	info.Header, rest, err = ParsePacketHeader(buf)
	if err != nil {
		return info, err
	}
	if info.Header.IsIncremental() {
		info.Incremental, rest, err = parseIncrementalHeader(rest)
		if err != nil {
			return info, err
		}
	}
	for len(rest) > 0 {
		var hdr SBEHeader
		hdr, rest, err = parseSBEHeader(rest)
		if err != nil {
			return info, err
		}
		if info.Version == 0 {
			info.Version = hdr.Version
		}
		var size int
		size, err = messageSizeKind(kindOf(hdr.Version, hdr.TemplateID), hdr.BlockLength, rest)
		if err != nil {
			info.Truncated = true
			size = -1
		}
		var body []byte
		if size < 0 {
			body = rest
			rest = nil
		} else {
			body = rest[:size]
			rest = rest[size:]
		}
		info.Messages++
		if !fn(hdr, body) {
			return info, nil
		}
	}
	return info, nil
}

// PacketHeaderSeq returns MsgSeqNum of a datagram without full parsing
// (0 if the buffer is too short).
func PacketHeaderSeq(buf []byte) uint32 {
	if len(buf) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(buf[0:4])
}

// DecodeMessage decodes one SBE message (as delivered by Walk) into the
// typed payload fields of Decoded. Headers in out are left untouched.
// Allocates (pointer fields); harness/compat use only.
func DecodeMessage(hdr SBEHeader, body []byte, out *Decoded) error {
	var m Message = Message{Header: hdr, Kind: kindOf(hdr.Version, hdr.TemplateID), body: body}
	switch m.Kind {
	case KindHeartbeat:
	case KindSequenceReset:
		var v uint32
		var ok bool
		v, ok = m.SequenceReset()
		if !ok {
			return fmt.Errorf("simba: SequenceReset body too short")
		}
		out.NewSeqNo = &v
	case KindBestPrices:
		var bp *BestPrices
		var err error
		bp, err = decodeBestPrices(body, hdr.BlockLength)
		if err != nil {
			return err
		}
		out.BestPrices = bp
	case KindEmptyBook:
		var eb *EmptyBook
		var err error
		eb, err = decodeEmptyBook(body)
		if err != nil {
			return err
		}
		out.EmptyBook = eb
	case KindOrderUpdate:
		var ou *OrderUpdate
		var err error
		ou, err = decodeOrderUpdate(body)
		if err != nil {
			return err
		}
		out.OrderUpdate = ou
	case KindOrderExecution:
		var oe *OrderExecution
		var err error
		oe, err = decodeOrderExecution(body)
		if err != nil {
			return err
		}
		out.OrderExecution = oe
	case KindOrderBookSnapshot:
		var obs *OrderBookSnapshot
		var err error
		obs, err = decodeOrderBookSnapshot(body, hdr.BlockLength)
		if err != nil {
			return err
		}
		out.OrderBookSnapshot = obs
	case KindSecurityDefinition:
		var sd *SecurityDefinition
		var err error
		sd, err = decodeSecurityDefinitionPrefix(body)
		if err != nil {
			return err
		}
		out.SecurityDefinition = sd
	default:
		// Unknown/unsupported kind — not an error, just not decoded.
	}
	return nil
}
