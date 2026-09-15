/*
FILE: internal/simba/secdef.go

DESCRIPTION:
Zero-allocation views over the Instruments feed messages (FUT-INFO /
OPT-INFO groups): SecurityDefinition, SecurityStatus,
TradingSessionStatus and SecurityDefinitionUpdateReport. These carry the
reference data the order-book feed does not: numeric SecurityID, price
limits, tick, margins, trading status, expiry events, legs.

SCHEMA VERSIONS. Production ran schema 8 in May 2026 while the published
XML is 9.0. The two differ in SecurityDefinition only:

	                       v8 (PythonSimbaClient-8.0)   v9 (simba_spectra-9.0.xml)
	root block             326                          342
	0..182                 identical head               identical head
	182                    MaturityDate uint32          Flags uint64
	186                    MaturityTime uint32          —
	190                    Flags uint64                 (Flags is at 182)
	after Flags            same fields, +8 offset       —
	SettlPrice             float64 @290                 Decimal5NULL @282
	318                    TradePeriodAccess uint64     TradePeriodAccess @310
	318/326..              —                            HighLimitPxWeekend, LowLimitPxWeekend, ClearingSettlPrice

SecurityStatus v8 (70 bytes) is a prefix of v9 (86: two weekend limits
appended). TradingSessionStatus (48) and SecurityDefinitionUpdateReport
(28) are identical. The v8 layout comes from the exchange's own
PythonSimbaClient-8.0 (ftp.moex.com/pub/SIMBA/Spectra/prod/backup/) and
was confirmed by decoding production captures (docs/pcap-findings, N15).

Every accessor bounds-checks against the root block length carried in
the wire header, so a longer root block of a future version is tolerated
and a shorter one yields the null value. Groups are walked sequentially
after the root block (five groups, then two var-data fields).
*/
package simba

import (
	"encoding/binary"
	"math"
)

// Null sentinels of the optional scalar types that do not declare an
// explicit nullValue in the schema (SBE defaults).
const (
	NullInt32  int32  = -2147483648
	NullUint8  uint8  = 255
	NullUint16 uint16 = 65535
)

// SecurityTradingStatus — enum SecurityTradingStatus (uInt8NULL).
type SecurityTradingStatus uint8

const (
	TradingStatusTradingHalt          SecurityTradingStatus = 2
	TradingStatusReadyToTrade         SecurityTradingStatus = 17
	TradingStatusNotAvailableForTrade SecurityTradingStatus = 18
	TradingStatusNotTradedOnThisMkt   SecurityTradingStatus = 19
	TradingStatusUnknownOrInvalid     SecurityTradingStatus = 20
	TradingStatusPreOpen              SecurityTradingStatus = 21
	TradingStatusDiscreteAuctionOpen  SecurityTradingStatus = 119
	TradingStatusDiscreteAuctionClose SecurityTradingStatus = 121
	TradingStatusInstrumentHalt       SecurityTradingStatus = 122
	TradingStatusInstrumentReadyPromo SecurityTradingStatus = 123
	TradingStatusInstrumentHaltPromo  SecurityTradingStatus = 124
	TradingStatusNull                 SecurityTradingStatus = 255
)

func (s SecurityTradingStatus) String() string {
	switch s {
	case TradingStatusTradingHalt:
		return "TradingHalt"
	case TradingStatusReadyToTrade:
		return "ReadyToTrade"
	case TradingStatusNotAvailableForTrade:
		return "NotAvailableForTrade"
	case TradingStatusNotTradedOnThisMkt:
		return "NotTradedOnThisMarket"
	case TradingStatusUnknownOrInvalid:
		return "UnknownOrInvalid"
	case TradingStatusPreOpen:
		return "PreOpen"
	case TradingStatusDiscreteAuctionOpen:
		return "DiscreteAuctionOpen"
	case TradingStatusDiscreteAuctionClose:
		return "DiscreteAuctionClose"
	case TradingStatusInstrumentHalt:
		return "InstrumentHalt"
	case TradingStatusInstrumentReadyPromo:
		return "InstrumentReadyPromo"
	case TradingStatusInstrumentHaltPromo:
		return "InstrumentHaltPromo"
	case TradingStatusNull:
		return "null"
	default:
		return "unknown"
	}
}

// Tradable — the instrument accepts orders in this status.
func (s SecurityTradingStatus) Tradable() bool {
	return s == TradingStatusReadyToTrade || s == TradingStatusInstrumentReadyPromo || s == TradingStatusDiscreteAuctionOpen
}

// TradSesStatus — enum TradSesStatus (uint8) of TradingSessionStatus.
type TradSesStatus uint8

const (
	TradSesStatusHalted  TradSesStatus = 1
	TradSesStatusOpen    TradSesStatus = 2
	TradSesStatusClosed  TradSesStatus = 3
	TradSesStatusPreOpen TradSesStatus = 4
)

// Layout of SecurityDefinition by schema version (see file header).
type secDefLayout struct {
	flags     int // offset of Flags
	tailShift int // added to every v9 offset after Flags
	settlF64  bool
}

func secDefLayoutFor(version uint16) secDefLayout {
	if version <= SchemaVersion8 {
		return secDefLayout{flags: 190, tailShift: 8, settlF64: true}
	}
	return secDefLayout{flags: 182}
}

// SecurityDefinitionView — SecurityDefinition (template 21 on v8, 27 on
// v9). Aliases the packet buffer; valid until the next packet.
type SecurityDefinitionView struct {
	body   []byte
	root   int
	lay    secDefLayout
	groups [5]GroupView
	desc   []byte
	quotes []byte
	ok     bool
}

// SecurityDefinitionView decodes the whole message (root block by the
// wire block length, five repeating groups, two var-data fields).
func (m Message) SecurityDefinitionView() (SecurityDefinitionView, bool) {
	var v SecurityDefinitionView
	if m.Kind != KindSecurityDefinition {
		return v, false
	}
	v.body = m.body
	v.root = int(m.Header.BlockLength)
	v.lay = secDefLayoutFor(m.Header.Version)
	if v.root > len(m.body) || v.root < 182 {
		return v, false
	}
	var off int = v.root
	for i := range v.groups {
		var g GroupView
		var ok bool
		g, ok = groupAfter(m.body, off)
		if !ok {
			return v, false
		}
		v.groups[i] = g
		off += 3 + g.blockLen*g.n
	}
	var vd []byte
	var ok bool
	if vd, off, ok = varDataAt(m.body, off); !ok {
		return v, false
	}
	v.desc = vd
	if vd, _, ok = varDataAt(m.body, off); !ok {
		return v, false
	}
	v.quotes = vd
	v.ok = true
	return v, true
}

func varDataAt(body []byte, off int) ([]byte, int, bool) {
	if len(body) < off+2 {
		return nil, off, false
	}
	var n int = int(binary.LittleEndian.Uint16(body[off : off+2]))
	off += 2
	if len(body) < off+n {
		return nil, off, false
	}
	return body[off : off+n], off + n, true
}

func (v SecurityDefinitionView) has(off, size int) bool { return off+size <= v.root }

func (v SecurityDefinitionView) i32(off int) int32 {
	if !v.has(off, 4) {
		return NullInt32
	}
	return int32(binary.LittleEndian.Uint32(v.body[off:]))
}

func (v SecurityDefinitionView) u32(off int) uint32 {
	if !v.has(off, 4) {
		return NullUint32
	}
	return binary.LittleEndian.Uint32(v.body[off:])
}

func (v SecurityDefinitionView) i64(off int) int64 {
	if !v.has(off, 8) {
		return NullDecimalMantissa
	}
	return int64(binary.LittleEndian.Uint64(v.body[off:]))
}

func (v SecurityDefinitionView) u64(off int) uint64 {
	if !v.has(off, 8) {
		return 0
	}
	return binary.LittleEndian.Uint64(v.body[off:])
}

func (v SecurityDefinitionView) u8(off int) uint8 {
	if !v.has(off, 1) {
		return NullUint8
	}
	return v.body[off]
}

func (v SecurityDefinitionView) chars(off, n int) []byte {
	if !v.has(off, n) {
		return nil
	}
	return trimChars(v.body[off : off+n])
}

func trimChars(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == 0 || b[len(b)-1] == ' ') {
		b = b[:len(b)-1]
	}
	return b
}

// Head fields — identical across versions.

func (v SecurityDefinitionView) TotNumReports() uint32 { return v.u32(0) }
func (v SecurityDefinitionView) Symbol() []byte        { return v.chars(4, 25) }
func (v SecurityDefinitionView) SecurityID() int32     { return v.i32(29) }
func (v SecurityDefinitionView) SecurityAltID() []byte { return v.chars(33, 25) }

// SecurityAltIDSource — '4' ISIN, '8' exchange symbol.
func (v SecurityDefinitionView) SecurityAltIDSource() byte { return v.u8(58) }
func (v SecurityDefinitionView) SecurityType() []byte      { return v.chars(59, 4) }
func (v SecurityDefinitionView) CFICode() []byte           { return v.chars(63, 6) }
func (v SecurityDefinitionView) StrikePrice() int64        { return v.i64(69) }
func (v SecurityDefinitionView) ContractMultiplier() int32 { return v.i32(77) }
func (v SecurityDefinitionView) TradingStatus() SecurityTradingStatus {
	return SecurityTradingStatus(v.u8(81))
}
func (v SecurityDefinitionView) Currency() []byte                { return v.chars(82, 3) }
func (v SecurityDefinitionView) MarketSegmentID() byte           { return v.u8(85) }
func (v SecurityDefinitionView) TradingSessionID() uint8         { return v.u8(86) }
func (v SecurityDefinitionView) ExchangeTradingSessionID() int32 { return v.i32(87) }
func (v SecurityDefinitionView) Volatility() int64               { return v.i64(91) }
func (v SecurityDefinitionView) HighLimitPx() int64              { return v.i64(99) }
func (v SecurityDefinitionView) LowLimitPx() int64               { return v.i64(107) }
func (v SecurityDefinitionView) MinPriceIncrement() int64        { return v.i64(115) }
func (v SecurityDefinitionView) MinPriceIncrementAmount() int64  { return v.i64(123) }
func (v SecurityDefinitionView) InitialMarginOnBuy() int64       { return v.i64(131) } // Decimal2 mantissa
func (v SecurityDefinitionView) InitialMarginOnSell() int64      { return v.i64(139) } // Decimal2 mantissa
func (v SecurityDefinitionView) InitialMarginSyntetic() int64    { return v.i64(147) } // Decimal2 mantissa
func (v SecurityDefinitionView) TheorPrice() int64               { return v.i64(155) }
func (v SecurityDefinitionView) TheorPriceLimit() int64          { return v.i64(163) }
func (v SecurityDefinitionView) UnderlyingQty() int64            { return v.i64(171) }
func (v SecurityDefinitionView) UnderlyingCurrency() []byte      { return v.chars(179, 3) }

// MaturityDate / MaturityTime — schema 8 only (YYYYMMDD, HHMMSSsss);
// NullUint32 on schema 9, where expiry comes from the Events group.
func (v SecurityDefinitionView) MaturityDate() uint32 {
	if v.lay.tailShift == 0 {
		return NullUint32
	}
	return v.u32(182)
}

func (v SecurityDefinitionView) MaturityTime() uint32 {
	if v.lay.tailShift == 0 {
		return NullUint32
	}
	return v.u32(186)
}

// Tail fields — offsets are the v9 ones shifted by the version layout.

func (v SecurityDefinitionView) Flags() uint64 { return v.u64(v.lay.flags) }
func (v SecurityDefinitionView) MinPriceIncrementAmountCurr() int64 {
	return v.i64(190 + v.lay.tailShift)
}
func (v SecurityDefinitionView) SettlPriceOpen() int64   { return v.i64(198 + v.lay.tailShift) }
func (v SecurityDefinitionView) ValuationMethod() []byte { return v.chars(206+v.lay.tailShift, 4) }
func (v SecurityDefinitionView) SettlCurrency() []byte   { return v.chars(234+v.lay.tailShift, 3) }
func (v SecurityDefinitionView) NegativePrices() uint8   { return v.u8(237 + v.lay.tailShift) }
func (v SecurityDefinitionView) DerivativeContractMultiplier() int32 {
	return v.i32(238 + v.lay.tailShift)
}
func (v SecurityDefinitionView) TradeModeID() int32        { return v.i32(290 + v.lay.tailShift) }
func (v SecurityDefinitionView) GroupMask() int64          { return v.i64(294 + v.lay.tailShift) }
func (v SecurityDefinitionView) SectionID() int32          { return v.i32(302 + v.lay.tailShift) }
func (v SecurityDefinitionView) BaseContractID() int32     { return v.i32(306 + v.lay.tailShift) }
func (v SecurityDefinitionView) TradePeriodAccess() uint64 { return v.u64(310 + v.lay.tailShift) }

// SettlPrice — Decimal5 mantissa. Schema 8 carries a float64 here (per
// the exchange's v8 client); it is converted to a Decimal5 mantissa.
func (v SecurityDefinitionView) SettlPrice() int64 {
	var off int = 282 + v.lay.tailShift
	if !v.has(off, 8) {
		return NullDecimalMantissa
	}
	if !v.lay.settlF64 {
		return v.i64(off)
	}
	var f float64 = math.Float64frombits(binary.LittleEndian.Uint64(v.body[off:]))
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return NullDecimalMantissa
	}
	return int64(math.Round(f * 100_000))
}

// Weekend limits and ClearingSettlPrice — schema 9 only (null on v8).
func (v SecurityDefinitionView) HighLimitPxWeekend() int64 { return v.tailOnly9(318) }
func (v SecurityDefinitionView) LowLimitPxWeekend() int64  { return v.tailOnly9(326) }
func (v SecurityDefinitionView) ClearingSettlPrice() int64 { return v.tailOnly9(334) }

func (v SecurityDefinitionView) tailOnly9(off int) int64 {
	if v.lay.tailShift != 0 {
		return NullDecimalMantissa
	}
	return v.i64(off)
}

// Var-data fields.

// SecurityDesc — UTF-8 human-readable name (aliases the buffer).
func (v SecurityDefinitionView) SecurityDesc() []byte { return v.desc }

// QuotationList — ASCII quotation list (aliases the buffer).
func (v SecurityDefinitionView) QuotationList() []byte { return v.quotes }

// Groups.

// MDFeedTypeEntry — NoMDFeedTypes entry (33 bytes).
type MDFeedTypeEntry struct {
	MDFeedType  []byte
	MarketDepth uint32
	MDBookType  uint32
}

// UnderlyingEntry — NoUnderlyings entry (37 bytes).
type UnderlyingEntry struct {
	UnderlyingSymbol     []byte
	UnderlyingBoard      []byte
	UnderlyingSecurityID int32
	UnderlyingFutureID   int32
}

// LegEntry — NoLegs entry (33 bytes).
type LegEntry struct {
	LegSymbol     []byte
	LegSecurityID int32
	LegRatioQty   int32
}

// InstrAttribEntry — NoInstrAttrib entry (35 bytes).
type InstrAttribEntry struct {
	InstrAttribType  int32
	InstrAttribValue []byte
}

// EventEntry — NoEvents entry (16 bytes). EventDate is YYYYMMDD,
// EventTime is nanoseconds since the Unix epoch (UTC) or an HHMMSSsss
// local time depending on EventType — the spec describes them per type.
type EventEntry struct {
	EventType int32
	EventDate uint32
	EventTime uint64
}

func (v SecurityDefinitionView) MDFeedTypesLen() int  { return v.groups[0].n }
func (v SecurityDefinitionView) UnderlyingsLen() int  { return v.groups[1].n }
func (v SecurityDefinitionView) LegsLen() int         { return v.groups[2].n }
func (v SecurityDefinitionView) InstrAttribsLen() int { return v.groups[3].n }
func (v SecurityDefinitionView) EventsLen() int       { return v.groups[4].n }

func (v SecurityDefinitionView) MDFeedType(i int) MDFeedTypeEntry {
	var b []byte = v.groups[0].entry(i)
	var e MDFeedTypeEntry
	if len(b) < 33 {
		return e
	}
	e.MDFeedType = trimChars(b[0:25])
	e.MarketDepth = binary.LittleEndian.Uint32(b[25:29])
	e.MDBookType = binary.LittleEndian.Uint32(b[29:33])
	return e
}

func (v SecurityDefinitionView) Underlying(i int) UnderlyingEntry {
	var b []byte = v.groups[1].entry(i)
	var e UnderlyingEntry
	if len(b) < 37 {
		return e
	}
	e.UnderlyingSymbol = trimChars(b[0:25])
	e.UnderlyingBoard = trimChars(b[25:29])
	e.UnderlyingSecurityID = int32(binary.LittleEndian.Uint32(b[29:33]))
	e.UnderlyingFutureID = int32(binary.LittleEndian.Uint32(b[33:37]))
	return e
}

func (v SecurityDefinitionView) Leg(i int) LegEntry {
	var b []byte = v.groups[2].entry(i)
	var e LegEntry
	if len(b) < 33 {
		return e
	}
	e.LegSymbol = trimChars(b[0:25])
	e.LegSecurityID = int32(binary.LittleEndian.Uint32(b[25:29]))
	e.LegRatioQty = int32(binary.LittleEndian.Uint32(b[29:33]))
	return e
}

// InstrAttrib — the XML (v9) puts InstrAttribType first; the v8 client
// puts the 31-byte value first. Both are 35 bytes; the version decides.
func (v SecurityDefinitionView) InstrAttrib(i int) InstrAttribEntry {
	var b []byte = v.groups[3].entry(i)
	var e InstrAttribEntry
	if len(b) < 35 {
		return e
	}
	if v.lay.tailShift != 0 {
		e.InstrAttribValue = trimChars(b[0:31])
		e.InstrAttribType = int32(binary.LittleEndian.Uint32(b[31:35]))
		return e
	}
	e.InstrAttribType = int32(binary.LittleEndian.Uint32(b[0:4]))
	e.InstrAttribValue = trimChars(b[4:35])
	return e
}

func (v SecurityDefinitionView) Event(i int) EventEntry {
	var b []byte = v.groups[4].entry(i)
	var e EventEntry
	if len(b) < 16 {
		return e
	}
	e.EventType = int32(binary.LittleEndian.Uint32(b[0:4]))
	e.EventDate = binary.LittleEndian.Uint32(b[4:8])
	e.EventTime = binary.LittleEndian.Uint64(b[8:16])
	return e
}

// SecurityStatusView — SecurityStatus (template 9 on v8, 28 on v9).
//
//	off  0  SecurityID              Int32
//	off  4  Symbol                  String25
//	off 29  SecurityTradingStatus   uInt8NULL
//	off 30  HighLimitPx             Decimal5NULL
//	off 38  LowLimitPx              Decimal5NULL
//	off 46  InitialMarginOnBuy      Decimal2NULL
//	off 54  InitialMarginOnSell     Decimal2NULL
//	off 62  InitialMarginSyntetic   Decimal2NULL
//	off 70  HighLimitPxWeekend      Decimal5NULL (v9)
//	off 78  LowLimitPxWeekend       Decimal5NULL (v9)
type SecurityStatusView struct {
	body []byte
	root int
}

func (m Message) SecurityStatusView() (SecurityStatusView, bool) {
	if m.Kind != KindSecurityStatus || int(m.Header.BlockLength) > len(m.body) || m.Header.BlockLength < 70 {
		return SecurityStatusView{}, false
	}
	return SecurityStatusView{body: m.body, root: int(m.Header.BlockLength)}, true
}

func (v SecurityStatusView) i64(off int) int64 {
	if off+8 > v.root {
		return NullDecimalMantissa
	}
	return int64(binary.LittleEndian.Uint64(v.body[off:]))
}

func (v SecurityStatusView) SecurityID() int32 { return int32(binary.LittleEndian.Uint32(v.body[0:4])) }
func (v SecurityStatusView) Symbol() []byte    { return trimChars(v.body[4:29]) }
func (v SecurityStatusView) TradingStatus() SecurityTradingStatus {
	return SecurityTradingStatus(v.body[29])
}
func (v SecurityStatusView) HighLimitPx() int64           { return v.i64(30) }
func (v SecurityStatusView) LowLimitPx() int64            { return v.i64(38) }
func (v SecurityStatusView) InitialMarginOnBuy() int64    { return v.i64(46) }
func (v SecurityStatusView) InitialMarginOnSell() int64   { return v.i64(54) }
func (v SecurityStatusView) InitialMarginSyntetic() int64 { return v.i64(62) }
func (v SecurityStatusView) HighLimitPxWeekend() int64    { return v.i64(70) }
func (v SecurityStatusView) LowLimitPxWeekend() int64     { return v.i64(78) }

// TradingSessionStatus — template 26 (identical on v8/v9, 48 bytes).
//
//	off  0  TradSesOpenTime           uint64 (ns UTC)
//	off  8  TradSesCloseTime          uint64
//	off 16  TradingSessionID          uInt8NULL
//	off 17  ExchangeTradingSessionID  Int32NULL
//	off 21  TradSesStatus             uint8
//	off 22  MarketSegmentID           char
//	off 23  TradSesEvent              uInt8NULL
//	off 24  TradePeriodID             int64
//	off 32  SettlSessBegin            uint64
//	off 40  ClrSessBegin              uint64
type TradingSessionStatus struct {
	TradSesOpenTime          uint64
	TradSesCloseTime         uint64
	TradingSessionID         uint8
	ExchangeTradingSessionID int32
	TradSesStatus            TradSesStatus
	MarketSegmentID          byte
	TradSesEvent             uint8
	TradePeriodID            int64
	SettlSessBegin           uint64
	ClrSessBegin             uint64
}

func (m Message) TradingSessionStatus() (TradingSessionStatus, bool) {
	if m.Kind != KindTradingSessionStatus || len(m.body) < 48 {
		return TradingSessionStatus{}, false
	}
	var b []byte = m.body
	return TradingSessionStatus{
		TradSesOpenTime:          binary.LittleEndian.Uint64(b[0:8]),
		TradSesCloseTime:         binary.LittleEndian.Uint64(b[8:16]),
		TradingSessionID:         b[16],
		ExchangeTradingSessionID: int32(binary.LittleEndian.Uint32(b[17:21])),
		TradSesStatus:            TradSesStatus(b[21]),
		MarketSegmentID:          b[22],
		TradSesEvent:             b[23],
		TradePeriodID:            int64(binary.LittleEndian.Uint64(b[24:32])),
		SettlSessBegin:           binary.LittleEndian.Uint64(b[32:40]),
		ClrSessBegin:             binary.LittleEndian.Uint64(b[40:48]),
	}, true
}

// SecurityDefinitionUpdateReport — template 10 (28 bytes): intraday
// theoretical price / volatility refresh on the OPT-INFO incremental feed.
type SecurityDefinitionUpdateReport struct {
	SecurityID      int32
	Volatility      int64
	TheorPrice      int64
	TheorPriceLimit int64
}

func (m Message) SecurityDefinitionUpdateReport() (SecurityDefinitionUpdateReport, bool) {
	if m.Kind != KindSecurityDefinitionUpdateReport || len(m.body) < 28 {
		return SecurityDefinitionUpdateReport{}, false
	}
	var b []byte = m.body
	return SecurityDefinitionUpdateReport{
		SecurityID:      int32(binary.LittleEndian.Uint32(b[0:4])),
		Volatility:      int64(binary.LittleEndian.Uint64(b[4:12])),
		TheorPrice:      int64(binary.LittleEndian.Uint64(b[12:20])),
		TheorPriceLimit: int64(binary.LittleEndian.Uint64(b[20:28])),
	}, true
}
