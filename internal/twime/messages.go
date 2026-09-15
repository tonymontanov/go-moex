/*
FILE: internal/twime/messages.go

DESCRIPTION:
Wire layouts of every TWIME SPECTRA 7.7 message, hand-transcribed from
twime_spectra-7.7.xml. SBE root blocks are packed little-endian with no
padding (FAQ §3.2: structure alignment is 1 byte), so a field's offset is
the sum of the widths before it; each type's doc comment lists the
offset table so a schema bump can be diffed against the XML.

Outbound (client-to-server) messages implement Marshaler: Append writes
messageHeader + root block to the caller's buffer. Inbound
(server-to-client) messages have Decode* functions that read a root
block by fixed offsets. Neither direction allocates.

Optional integer fields follow the schema's nullValue convention
(schema.go Null*): a "not set" ExpireDate, ClOrdLinkID, OrderID etc.
must be encoded as the sentinel, never as zero — zero is a legal value
for most of them.
*/
package twime

import (
	"fmt"
	"time"

	"github.com/tonymontanov/go-moex/internal/sbe"
)

// Marshaler — outbound message. Append writes the full frame
// (messageHeader + root block) to dst and returns the extended slice.
type Marshaler interface {
	Template() uint16
	Append(dst []byte) []byte
}

// Root block lengths (bytes) per template, derived from the field lists
// in the XML. The server-side encoder uses the same values (verified
// against the exchange's Python serializer in codec_golden_test.go).
const (
	BlockLengthEstablish             = 32
	BlockLengthEstablishmentAck      = 20
	BlockLengthEstablishmentReject   = 9
	BlockLengthTerminate             = 1
	BlockLengthRetransmitRequest     = 20
	BlockLengthRetransmission        = 20
	BlockLengthSequence              = 8
	BlockLengthFloodReject           = 16
	BlockLengthSessionReject         = 13
	BlockLengthBusinessMessageReject = 20

	BlockLengthNewOrderSingle                  = 47
	BlockLengthOrderMassCancelRequest          = 50
	BlockLengthOrderMassCancelByBFLimitRequest = 15
	BlockLengthOrderCancelRequest              = 28
	BlockLengthOrderReplaceRequest             = 46
	BlockLengthNewOrderIceberg                 = 54
	BlockLengthOrderIcebergCancelRequest       = 28
	BlockLengthOrderIcebergReplaceRequest      = 41
	BlockLengthNewOrderIcebergX                = 55

	BlockLengthOrderMassCancelResponse = 20
	BlockLengthEmptyBook               = 12
	BlockLengthSystemEvent             = 21
	BlockLengthNewOrderSingleResponse  = 74
	BlockLengthNewOrderIcebergResponse = 90
	BlockLengthOrderCancelResponse     = 52
	BlockLengthOrderReplaceResponse    = 69
	BlockLengthExecutionSingleReport   = 77
	BlockLengthExecutionMultilegReport = 85
)

// MaxBlockLength — largest root block in the schema; sizes the session's
// read buffer.
const MaxBlockLength = BlockLengthNewOrderIcebergResponse

// TimestampOf converts t to the schema's TimeStamp (ns since Unix epoch,
// UTC).
func TimestampOf(t time.Time) uint64 { return uint64(t.UnixNano()) }

// TimeOf converts a TimeStamp back; the zero time for NullTimestamp.
func TimeOf(ts uint64) time.Time {
	if ts == NullTimestamp {
		return time.Time{}
	}
	return time.Unix(0, int64(ts)).UTC()
}

// ErrShortBlock — root block shorter than the template requires.
type ErrShortBlock struct {
	Template uint16
	Got      int
	Want     int
}

func (e *ErrShortBlock) Error() string {
	return fmt.Sprintf("twime: %s(%d) root block %d bytes, need %d", TemplateName(e.Template), e.Template, e.Got, e.Want)
}

func checkLen(template uint16, body []byte, want int) error {
	if len(body) < want {
		return &ErrShortBlock{Template: template, Got: len(body), Want: want}
	}
	return nil
}

// appendFrame reserves header + blockLength bytes in dst, writes the
// header and returns (dst, body) with body zeroed and ready for Put*.
func appendFrame(dst []byte, template uint16, blockLength int) ([]byte, []byte) {
	dst = sbe.AppendHeader(dst, sbe.MessageHeader{
		BlockLength: uint16(blockLength),
		TemplateID:  template,
		SchemaID:    SchemaID,
		Version:     SchemaVersion,
	})
	start := len(dst)
	for i := 0; i < blockLength; i++ {
		dst = append(dst, 0)
	}
	return dst, dst[start : start+blockLength]
}

// ---------------------------------------------------------------------
// Session layer
// ---------------------------------------------------------------------

// Establish(5000) — session logon.
//
//	off  0  Timestamp          TimeStamp (uint64)
//	off  8  KeepaliveInterval  DeltaMillisecs (uint32, 1000..60000)
//	off 12  Credentials        String20 (login)
type Establish struct {
	Timestamp         uint64
	KeepaliveInterval uint32
	Credentials       string
}

func (Establish) Template() uint16 { return TemplateEstablish }

func (m Establish) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateEstablish, BlockLengthEstablish)
	sbe.PutU64(b, 0, m.Timestamp)
	sbe.PutU32(b, 8, m.KeepaliveInterval)
	sbe.PutChars(b, 12, CredentialsLen, m.Credentials)
	return dst
}

// EstablishmentAck(5001).
//
//	off  0  RequestTimestamp   TimeStamp
//	off  8  KeepaliveInterval  DeltaMillisecs — the SERVER's interval
//	off 12  NextSeqNo          UInt64 — next server-to-client application seq
type EstablishmentAck struct {
	RequestTimestamp  uint64
	KeepaliveInterval uint32
	NextSeqNo         uint64
}

func DecodeEstablishmentAck(body []byte) (EstablishmentAck, error) {
	if err := checkLen(TemplateEstablishmentAck, body, BlockLengthEstablishmentAck); err != nil {
		return EstablishmentAck{}, err
	}
	return EstablishmentAck{
		RequestTimestamp:  sbe.U64(body, 0),
		KeepaliveInterval: sbe.U32(body, 8),
		NextSeqNo:         sbe.U64(body, 12),
	}, nil
}

// EstablishmentReject(5002).
//
//	off 0  RequestTimestamp         TimeStamp
//	off 8  EstablishmentRejectCode  uint8
type EstablishmentReject struct {
	RequestTimestamp uint64
	Code             EstablishmentRejectCode
}

func DecodeEstablishmentReject(body []byte) (EstablishmentReject, error) {
	if err := checkLen(TemplateEstablishmentReject, body, BlockLengthEstablishmentReject); err != nil {
		return EstablishmentReject{}, err
	}
	return EstablishmentReject{
		RequestTimestamp: sbe.U64(body, 0),
		Code:             EstablishmentRejectCode(sbe.U8(body, 8)),
	}, nil
}

// Terminate(5003) — sent by either side; the initiator waits for the
// peer's Terminate before closing TCP.
//
//	off 0  TerminationCode  uint8
type Terminate struct {
	Code TerminationCode
}

func (Terminate) Template() uint16 { return TemplateTerminate }

func (m Terminate) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateTerminate, BlockLengthTerminate)
	sbe.PutU8(b, 0, uint8(m.Code))
	return dst
}

func DecodeTerminate(body []byte) (Terminate, error) {
	if err := checkLen(TemplateTerminate, body, BlockLengthTerminate); err != nil {
		return Terminate{}, err
	}
	return Terminate{Code: TerminationCode(sbe.U8(body, 0))}, nil
}

// RetransmitRequest(5004).
//
//	off  0  Timestamp  TimeStamp
//	off  8  FromSeqNo  UInt64
//	off 16  Count      UInt32 (<= 10 on the transactional gateway)
type RetransmitRequest struct {
	Timestamp uint64
	FromSeqNo uint64
	Count     uint32
}

func (RetransmitRequest) Template() uint16 { return TemplateRetransmitRequest }

func (m RetransmitRequest) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateRetransmitRequest, BlockLengthRetransmitRequest)
	sbe.PutU64(b, 0, m.Timestamp)
	sbe.PutU64(b, 8, m.FromSeqNo)
	sbe.PutU32(b, 16, m.Count)
	return dst
}

// Retransmission(5005) — header of a retransmit reply; Count application
// messages follow it on the stream.
//
//	off  0  NextSeqNo         UInt64 — seq of the first retransmitted message
//	off  8  RequestTimestamp  TimeStamp
//	off 16  Count             UInt32
type Retransmission struct {
	NextSeqNo        uint64
	RequestTimestamp uint64
	Count            uint32
}

func DecodeRetransmission(body []byte) (Retransmission, error) {
	if err := checkLen(TemplateRetransmission, body, BlockLengthRetransmission); err != nil {
		return Retransmission{}, err
	}
	return Retransmission{
		NextSeqNo:        sbe.U64(body, 0),
		RequestTimestamp: sbe.U64(body, 8),
		Count:            sbe.U32(body, 16),
	}, nil
}

// Sequence(5006) — heartbeat. Client-to-server carries NextSeqNo =
// NullUint64; server-to-client carries the next application seq.
//
//	off 0  NextSeqNo  UInt64
type Sequence struct {
	NextSeqNo uint64
}

func (Sequence) Template() uint16 { return TemplateSequence }

func (m Sequence) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateSequence, BlockLengthSequence)
	sbe.PutU64(b, 0, m.NextSeqNo)
	return dst
}

func DecodeSequence(body []byte) (Sequence, error) {
	if err := checkLen(TemplateSequence, body, BlockLengthSequence); err != nil {
		return Sequence{}, err
	}
	return Sequence{NextSeqNo: sbe.U64(body, 0)}, nil
}

// FloodReject(5007) — the trading message with ClOrdID was dropped by the
// per-second limiter. Not numbered, not recoverable.
//
//	off  0  ClOrdID        UInt64
//	off  8  QueueSize      UInt32 — messages received during the last second
//	off 12  PenaltyRemain  UInt32 — microseconds until the gateway accepts again
type FloodReject struct {
	ClOrdID       uint64
	QueueSize     uint32
	PenaltyRemain uint32
}

// Penalty — PenaltyRemain as a Duration.
func (m FloodReject) Penalty() time.Duration {
	return time.Duration(m.PenaltyRemain) * time.Microsecond
}

func DecodeFloodReject(body []byte) (FloodReject, error) {
	if err := checkLen(TemplateFloodReject, body, BlockLengthFloodReject); err != nil {
		return FloodReject{}, err
	}
	return FloodReject{
		ClOrdID:       sbe.U64(body, 0),
		QueueSize:     sbe.U32(body, 8),
		PenaltyRemain: sbe.U32(body, 12),
	}, nil
}

// SessionReject(5008) — malformed field / duplicate ClOrdID. Not
// numbered, not recoverable.
//
//	off  0  ClOrdID              UInt64
//	off  8  RefTagID             UInt32 — FIX tag of the offending field
//	off 12  SessionRejectReason  uint8
type SessionReject struct {
	ClOrdID  uint64
	RefTagID uint32
	Reason   SessionRejectReason
}

func DecodeSessionReject(body []byte) (SessionReject, error) {
	if err := checkLen(TemplateSessionReject, body, BlockLengthSessionReject); err != nil {
		return SessionReject{}, err
	}
	return SessionReject{
		ClOrdID:  sbe.U64(body, 0),
		RefTagID: sbe.U32(body, 8),
		Reason:   SessionRejectReason(sbe.U8(body, 12)),
	}, nil
}

// BusinessMessageReject(5009) — application-level rejection of an order
// request. Session layer by template id: not numbered, not recoverable.
//
//	off  0  ClOrdID       UInt64
//	off  8  Timestamp     TimeStamp
//	off 16  OrdRejReason  Int32 — code from the spec's "List of return codes"
type BusinessMessageReject struct {
	ClOrdID      uint64
	Timestamp    uint64
	OrdRejReason int32
}

func DecodeBusinessMessageReject(body []byte) (BusinessMessageReject, error) {
	if err := checkLen(TemplateBusinessMessageReject, body, BlockLengthBusinessMessageReject); err != nil {
		return BusinessMessageReject{}, err
	}
	return BusinessMessageReject{
		ClOrdID:      sbe.U64(body, 0),
		Timestamp:    sbe.U64(body, 8),
		OrdRejReason: sbe.I32(body, 16),
	}, nil
}

// ---------------------------------------------------------------------
// Application layer: client-to-server
// ---------------------------------------------------------------------

// NewOrderSingle(6000).
//
//	off  0  ClOrdID       UInt64
//	off  8  ExpireDate    TimeStamp (NullTimestamp unless GTD)
//	off 16  Price         Decimal5 mantissa (int64)
//	off 24  SecurityID    Int32
//	off 28  ClOrdLinkID   Int32 (NullInt32 if unused)
//	off 32  OrderQty      UInt32
//	off 36  ComplianceID  char
//	off 37  TimeInForce   uint8
//	off 38  Side          uint8
//	off 39  ClientFlags   uint8 bitmask
//	off 40  Account       String7
type NewOrderSingle struct {
	ClOrdID      uint64
	ExpireDate   uint64
	Price        int64
	SecurityID   int32
	ClOrdLinkID  int32
	OrderQty     uint32
	ComplianceID ComplianceID
	TimeInForce  TimeInForce
	Side         Side
	ClientFlags  ClientFlags
	Account      string
}

func (NewOrderSingle) Template() uint16 { return TemplateNewOrderSingle }

func (m NewOrderSingle) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateNewOrderSingle, BlockLengthNewOrderSingle)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutU64(b, 8, m.ExpireDate)
	sbe.PutI64(b, 16, m.Price)
	sbe.PutI32(b, 24, m.SecurityID)
	sbe.PutI32(b, 28, m.ClOrdLinkID)
	sbe.PutU32(b, 32, m.OrderQty)
	sbe.PutU8(b, 36, uint8(m.ComplianceID))
	sbe.PutU8(b, 37, uint8(m.TimeInForce))
	sbe.PutU8(b, 38, uint8(m.Side))
	sbe.PutU8(b, 39, uint8(m.ClientFlags))
	sbe.PutChars(b, 40, AccountLen, m.Account)
	return dst
}

// NewOrderIceberg(6008) — no TimeInForce (always Day).
//
//	off  0  ClOrdID             UInt64
//	off  8  ExpireDate          TimeStamp
//	off 16  Price               Decimal5
//	off 24  SecurityID          Int32
//	off 28  ClOrdLinkID         Int32
//	off 32  DisplayQty          UInt32
//	off 36  DisplayVarianceQty  UInt32
//	off 40  OrderQty            UInt32
//	off 44  ComplianceID        char
//	off 45  Side                uint8
//	off 46  ClientFlags         uint8
//	off 47  Account             String7
type NewOrderIceberg struct {
	ClOrdID            uint64
	ExpireDate         uint64
	Price              int64
	SecurityID         int32
	ClOrdLinkID        int32
	DisplayQty         uint32
	DisplayVarianceQty uint32
	OrderQty           uint32
	ComplianceID       ComplianceID
	Side               Side
	ClientFlags        ClientFlags
	Account            string
}

func (NewOrderIceberg) Template() uint16 { return TemplateNewOrderIceberg }

func (m NewOrderIceberg) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateNewOrderIceberg, BlockLengthNewOrderIceberg)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutU64(b, 8, m.ExpireDate)
	sbe.PutI64(b, 16, m.Price)
	sbe.PutI32(b, 24, m.SecurityID)
	sbe.PutI32(b, 28, m.ClOrdLinkID)
	sbe.PutU32(b, 32, m.DisplayQty)
	sbe.PutU32(b, 36, m.DisplayVarianceQty)
	sbe.PutU32(b, 40, m.OrderQty)
	sbe.PutU8(b, 44, uint8(m.ComplianceID))
	sbe.PutU8(b, 45, uint8(m.Side))
	sbe.PutU8(b, 46, uint8(m.ClientFlags))
	sbe.PutChars(b, 47, AccountLen, m.Account)
	return dst
}

// NewOrderIcebergX(6011) — iceberg with TimeInForce.
//
//	off  0  ClOrdID             UInt64
//	off  8  ExpireDate          TimeStamp
//	off 16  Price               Decimal5
//	off 24  SecurityID          Int32
//	off 28  ClOrdLinkID         Int32
//	off 32  DisplayQty          UInt32
//	off 36  DisplayVarianceQty  UInt32
//	off 40  OrderQty            UInt32
//	off 44  ComplianceID        char
//	off 45  TimeInForce         uint8
//	off 46  Side                uint8
//	off 47  ClientFlags         uint8
//	off 48  Account             String7
type NewOrderIcebergX struct {
	ClOrdID            uint64
	ExpireDate         uint64
	Price              int64
	SecurityID         int32
	ClOrdLinkID        int32
	DisplayQty         uint32
	DisplayVarianceQty uint32
	OrderQty           uint32
	ComplianceID       ComplianceID
	TimeInForce        TimeInForce
	Side               Side
	ClientFlags        ClientFlags
	Account            string
}

func (NewOrderIcebergX) Template() uint16 { return TemplateNewOrderIcebergX }

func (m NewOrderIcebergX) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateNewOrderIcebergX, BlockLengthNewOrderIcebergX)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutU64(b, 8, m.ExpireDate)
	sbe.PutI64(b, 16, m.Price)
	sbe.PutI32(b, 24, m.SecurityID)
	sbe.PutI32(b, 28, m.ClOrdLinkID)
	sbe.PutU32(b, 32, m.DisplayQty)
	sbe.PutU32(b, 36, m.DisplayVarianceQty)
	sbe.PutU32(b, 40, m.OrderQty)
	sbe.PutU8(b, 44, uint8(m.ComplianceID))
	sbe.PutU8(b, 45, uint8(m.TimeInForce))
	sbe.PutU8(b, 46, uint8(m.Side))
	sbe.PutU8(b, 47, uint8(m.ClientFlags))
	sbe.PutChars(b, 48, AccountLen, m.Account)
	return dst
}

// OrderCancelRequest(6006). OrderIcebergCancelRequest(6009) has the
// identical layout — see OrderIcebergCancelRequest.
//
//	off  0  ClOrdID      UInt64
//	off  8  OrderID      Int64
//	off 16  SecurityID   Int32
//	off 20  ClientFlags  uint8
//	off 21  Account      String7
type OrderCancelRequest struct {
	ClOrdID     uint64
	OrderID     int64
	SecurityID  int32
	ClientFlags ClientFlags
	Account     string
}

func (OrderCancelRequest) Template() uint16 { return TemplateOrderCancelRequest }

func (m OrderCancelRequest) Append(dst []byte) []byte {
	return appendCancel(dst, TemplateOrderCancelRequest, m)
}

// OrderIcebergCancelRequest(6009) — same fields as OrderCancelRequest.
type OrderIcebergCancelRequest OrderCancelRequest

func (OrderIcebergCancelRequest) Template() uint16 { return TemplateOrderIcebergCancelRequest }

func (m OrderIcebergCancelRequest) Append(dst []byte) []byte {
	return appendCancel(dst, TemplateOrderIcebergCancelRequest, OrderCancelRequest(m))
}

func appendCancel(dst []byte, template uint16, m OrderCancelRequest) []byte {
	dst, b := appendFrame(dst, template, BlockLengthOrderCancelRequest)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutI64(b, 8, m.OrderID)
	sbe.PutI32(b, 16, m.SecurityID)
	sbe.PutU8(b, 20, uint8(m.ClientFlags))
	sbe.PutChars(b, 21, AccountLen, m.Account)
	return dst
}

// OrderReplaceRequest(6007).
//
//	off  0  ClOrdID       UInt64
//	off  8  OrderID       Int64
//	off 16  Price         Decimal5
//	off 24  OrderQty      UInt32
//	off 28  ClOrdLinkID   Int32
//	off 32  SecurityID    Int32
//	off 36  ComplianceID  char
//	off 37  Mode          uint8
//	off 38  ClientFlags   uint8
//	off 39  Account       String7
type OrderReplaceRequest struct {
	ClOrdID      uint64
	OrderID      int64
	Price        int64
	OrderQty     uint32
	ClOrdLinkID  int32
	SecurityID   int32
	ComplianceID ComplianceID
	Mode         ReplaceMode
	ClientFlags  ClientFlags
	Account      string
}

func (OrderReplaceRequest) Template() uint16 { return TemplateOrderReplaceRequest }

func (m OrderReplaceRequest) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateOrderReplaceRequest, BlockLengthOrderReplaceRequest)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutI64(b, 8, m.OrderID)
	sbe.PutI64(b, 16, m.Price)
	sbe.PutU32(b, 24, m.OrderQty)
	sbe.PutI32(b, 28, m.ClOrdLinkID)
	sbe.PutI32(b, 32, m.SecurityID)
	sbe.PutU8(b, 36, uint8(m.ComplianceID))
	sbe.PutU8(b, 37, uint8(m.Mode))
	sbe.PutU8(b, 38, uint8(m.ClientFlags))
	sbe.PutChars(b, 39, AccountLen, m.Account)
	return dst
}

// OrderIcebergReplaceRequest(6010) — no OrderQty/Mode.
//
//	off  0  ClOrdID       UInt64
//	off  8  OrderID       Int64
//	off 16  Price         Decimal5
//	off 24  ClOrdLinkID   Int32
//	off 28  SecurityID    Int32
//	off 32  ComplianceID  char
//	off 33  ClientFlags   uint8
//	off 34  Account       String7
type OrderIcebergReplaceRequest struct {
	ClOrdID      uint64
	OrderID      int64
	Price        int64
	ClOrdLinkID  int32
	SecurityID   int32
	ComplianceID ComplianceID
	ClientFlags  ClientFlags
	Account      string
}

func (OrderIcebergReplaceRequest) Template() uint16 { return TemplateOrderIcebergReplaceRequest }

func (m OrderIcebergReplaceRequest) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateOrderIcebergReplaceRequest, BlockLengthOrderIcebergReplaceRequest)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutI64(b, 8, m.OrderID)
	sbe.PutI64(b, 16, m.Price)
	sbe.PutI32(b, 24, m.ClOrdLinkID)
	sbe.PutI32(b, 28, m.SecurityID)
	sbe.PutU8(b, 32, uint8(m.ComplianceID))
	sbe.PutU8(b, 33, uint8(m.ClientFlags))
	sbe.PutChars(b, 34, AccountLen, m.Account)
	return dst
}

// OrderMassCancelRequest(6004) — filtered mass cancel, one transaction.
// Filters (spec §4.1.10): ClOrdLinkID != 0 overrides Side/SecurityGroup/
// SecurityID; SecurityID 0 or null = any; Side AllOrders = both sides;
// Account ending in "%%%" = every account of the login; SecurityGroup
// empty or "%" = every contract.
//
//	off  0  ClOrdID        UInt64
//	off  8  ClOrdLinkID    Int32
//	off 12  SecurityID     Int32
//	off 16  SecurityType   uint8 bitmask
//	off 17  Side           uint8
//	off 18  Account        String7
//	off 25  SecurityGroup  String25
type OrderMassCancelRequest struct {
	ClOrdID       uint64
	ClOrdLinkID   int32
	SecurityID    int32
	SecurityType  SecurityType
	Side          Side
	Account       string
	SecurityGroup string
}

func (OrderMassCancelRequest) Template() uint16 { return TemplateOrderMassCancelRequest }

func (m OrderMassCancelRequest) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateOrderMassCancelRequest, BlockLengthOrderMassCancelRequest)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutI32(b, 8, m.ClOrdLinkID)
	sbe.PutI32(b, 12, m.SecurityID)
	sbe.PutU8(b, 16, uint8(m.SecurityType))
	sbe.PutU8(b, 17, uint8(m.Side))
	sbe.PutChars(b, 18, AccountLen, m.Account)
	sbe.PutChars(b, 25, SecurityGroupLen, m.SecurityGroup)
	return dst
}

// OrderMassCancelByBFLimitRequest(6005).
//
//	off 0  ClOrdID  UInt64
//	off 8  Account  String7
type OrderMassCancelByBFLimitRequest struct {
	ClOrdID uint64
	Account string
}

func (OrderMassCancelByBFLimitRequest) Template() uint16 {
	return TemplateOrderMassCancelByBFLimitRequest
}

func (m OrderMassCancelByBFLimitRequest) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateOrderMassCancelByBFLimitRequest, BlockLengthOrderMassCancelByBFLimitRequest)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutChars(b, 8, AccountLen, m.Account)
	return dst
}

// ---------------------------------------------------------------------
// Application layer: server-to-client
// ---------------------------------------------------------------------

// NewOrderSingleResponse(7015) — order accepted.
//
//	off  0  ClOrdID           UInt64
//	off  8  Timestamp         TimeStamp
//	off 16  ExpireDate        TimeStamp
//	off 24  OrderID           Int64
//	off 32  Flags             uint64 bitmask
//	off 40  Flags2            uint64 bitmask
//	off 48  Price             Decimal5
//	off 56  SecurityID        Int32
//	off 60  OrderQty          UInt32
//	off 64  TradingSessionID  Int32
//	off 68  ClOrdLinkID       Int32
//	off 72  Side              uint8
//	off 73  ComplianceID      char
type NewOrderSingleResponse struct {
	ClOrdID          uint64
	Timestamp        uint64
	ExpireDate       uint64
	OrderID          int64
	Flags            Flags
	Flags2           uint64
	Price            int64
	SecurityID       int32
	OrderQty         uint32
	TradingSessionID int32
	ClOrdLinkID      int32
	Side             Side
	ComplianceID     ComplianceID
}

func DecodeNewOrderSingleResponse(body []byte) (NewOrderSingleResponse, error) {
	if err := checkLen(TemplateNewOrderSingleResponse, body, BlockLengthNewOrderSingleResponse); err != nil {
		return NewOrderSingleResponse{}, err
	}
	return NewOrderSingleResponse{
		ClOrdID:          sbe.U64(body, 0),
		Timestamp:        sbe.U64(body, 8),
		ExpireDate:       sbe.U64(body, 16),
		OrderID:          sbe.I64(body, 24),
		Flags:            Flags(sbe.U64(body, 32)),
		Flags2:           sbe.U64(body, 40),
		Price:            sbe.I64(body, 48),
		SecurityID:       sbe.I32(body, 56),
		OrderQty:         sbe.U32(body, 60),
		TradingSessionID: sbe.I32(body, 64),
		ClOrdLinkID:      sbe.I32(body, 68),
		Side:             Side(sbe.U8(body, 72)),
		ComplianceID:     ComplianceID(sbe.U8(body, 73)),
	}, nil
}

// NewOrderIcebergResponse(7016).
//
//	off  0  ClOrdID             UInt64
//	off  8  Timestamp           TimeStamp
//	off 16  ExpireDate          TimeStamp
//	off 24  OrderID             Int64
//	off 32  DisplayOrderID      Int64
//	off 40  Flags               uint64
//	off 48  Flags2              uint64
//	off 56  Price               Decimal5
//	off 64  SecurityID          Int32
//	off 68  OrderQty            UInt32
//	off 72  DisplayQty          UInt32
//	off 76  DisplayVarianceQty  UInt32
//	off 80  TradingSessionID    Int32
//	off 84  ClOrdLinkID         Int32
//	off 88  Side                uint8
//	off 89  ComplianceID        char
type NewOrderIcebergResponse struct {
	ClOrdID            uint64
	Timestamp          uint64
	ExpireDate         uint64
	OrderID            int64
	DisplayOrderID     int64
	Flags              Flags
	Flags2             uint64
	Price              int64
	SecurityID         int32
	OrderQty           uint32
	DisplayQty         uint32
	DisplayVarianceQty uint32
	TradingSessionID   int32
	ClOrdLinkID        int32
	Side               Side
	ComplianceID       ComplianceID
}

func DecodeNewOrderIcebergResponse(body []byte) (NewOrderIcebergResponse, error) {
	if err := checkLen(TemplateNewOrderIcebergResponse, body, BlockLengthNewOrderIcebergResponse); err != nil {
		return NewOrderIcebergResponse{}, err
	}
	return NewOrderIcebergResponse{
		ClOrdID:            sbe.U64(body, 0),
		Timestamp:          sbe.U64(body, 8),
		ExpireDate:         sbe.U64(body, 16),
		OrderID:            sbe.I64(body, 24),
		DisplayOrderID:     sbe.I64(body, 32),
		Flags:              Flags(sbe.U64(body, 40)),
		Flags2:             sbe.U64(body, 48),
		Price:              sbe.I64(body, 56),
		SecurityID:         sbe.I32(body, 64),
		OrderQty:           sbe.U32(body, 68),
		DisplayQty:         sbe.U32(body, 72),
		DisplayVarianceQty: sbe.U32(body, 76),
		TradingSessionID:   sbe.I32(body, 80),
		ClOrdLinkID:        sbe.I32(body, 84),
		Side:               Side(sbe.U8(body, 88)),
		ComplianceID:       ComplianceID(sbe.U8(body, 89)),
	}, nil
}

// OrderCancelResponse(7017) — order (fully) cancelled: by request, by
// mass cancel, by COD/UKS/cross rules, by Mode 2/3 replace, or by the
// exchange (ClOrdID == NullUint64 marks an unsolicited cancel).
//
//	off  0  ClOrdID           UInt64
//	off  8  Timestamp         TimeStamp
//	off 16  OrderID           Int64
//	off 24  Flags             uint64
//	off 32  Flags2            uint64
//	off 40  OrderQty          UInt32 — quantity cancelled (was left)
//	off 44  TradingSessionID  Int32
//	off 48  ClOrdLinkID       Int32
type OrderCancelResponse struct {
	ClOrdID          uint64
	Timestamp        uint64
	OrderID          int64
	Flags            Flags
	Flags2           uint64
	OrderQty         uint32
	TradingSessionID int32
	ClOrdLinkID      int32
}

// Unsolicited — cancel not caused by a request of this session.
func (m OrderCancelResponse) Unsolicited() bool { return m.ClOrdID == NullUint64 }

func DecodeOrderCancelResponse(body []byte) (OrderCancelResponse, error) {
	if err := checkLen(TemplateOrderCancelResponse, body, BlockLengthOrderCancelResponse); err != nil {
		return OrderCancelResponse{}, err
	}
	return OrderCancelResponse{
		ClOrdID:          sbe.U64(body, 0),
		Timestamp:        sbe.U64(body, 8),
		OrderID:          sbe.I64(body, 16),
		Flags:            Flags(sbe.U64(body, 24)),
		Flags2:           sbe.U64(body, 32),
		OrderQty:         sbe.U32(body, 40),
		TradingSessionID: sbe.I32(body, 44),
		ClOrdLinkID:      sbe.I32(body, 48),
	}, nil
}

// OrderReplaceResponse(7018) — replace produced a new OrderID.
//
//	off  0  ClOrdID           UInt64
//	off  8  Timestamp         TimeStamp
//	off 16  OrderID           Int64 — new
//	off 24  PrevOrderID       Int64 — replaced
//	off 32  Flags             uint64
//	off 40  Flags2            uint64
//	off 48  Price             Decimal5
//	off 56  OrderQty          UInt32
//	off 60  TradingSessionID  Int32
//	off 64  ClOrdLinkID       Int32
//	off 68  ComplianceID      char
type OrderReplaceResponse struct {
	ClOrdID          uint64
	Timestamp        uint64
	OrderID          int64
	PrevOrderID      int64
	Flags            Flags
	Flags2           uint64
	Price            int64
	OrderQty         uint32
	TradingSessionID int32
	ClOrdLinkID      int32
	ComplianceID     ComplianceID
}

func DecodeOrderReplaceResponse(body []byte) (OrderReplaceResponse, error) {
	if err := checkLen(TemplateOrderReplaceResponse, body, BlockLengthOrderReplaceResponse); err != nil {
		return OrderReplaceResponse{}, err
	}
	return OrderReplaceResponse{
		ClOrdID:          sbe.U64(body, 0),
		Timestamp:        sbe.U64(body, 8),
		OrderID:          sbe.I64(body, 16),
		PrevOrderID:      sbe.I64(body, 24),
		Flags:            Flags(sbe.U64(body, 32)),
		Flags2:           sbe.U64(body, 40),
		Price:            sbe.I64(body, 48),
		OrderQty:         sbe.U32(body, 56),
		TradingSessionID: sbe.I32(body, 60),
		ClOrdLinkID:      sbe.I32(body, 64),
		ComplianceID:     ComplianceID(sbe.U8(body, 68)),
	}, nil
}

// OrderMassCancelResponse(7007) — summary after the per-order
// OrderCancelResponse messages.
//
//	off  0  ClOrdID              UInt64
//	off  8  Timestamp            TimeStamp
//	off 16  TotalAffectedOrders  Int32
type OrderMassCancelResponse struct {
	ClOrdID             uint64
	Timestamp           uint64
	TotalAffectedOrders int32
}

func DecodeOrderMassCancelResponse(body []byte) (OrderMassCancelResponse, error) {
	if err := checkLen(TemplateOrderMassCancelResponse, body, BlockLengthOrderMassCancelResponse); err != nil {
		return OrderMassCancelResponse{}, err
	}
	return OrderMassCancelResponse{
		ClOrdID:             sbe.U64(body, 0),
		Timestamp:           sbe.U64(body, 8),
		TotalAffectedOrders: sbe.I32(body, 16),
	}, nil
}

// ExecutionSingleReport(7019) — one fill. OrderQty is the quantity LEFT
// in the order after this fill (0 = fully filled).
//
//	off  0  ClOrdID           UInt64
//	off  8  Timestamp         TimeStamp
//	off 16  OrderID           Int64
//	off 24  TrdMatchID        Int64
//	off 32  Flags             uint64
//	off 40  Flags2            uint64
//	off 48  LastPx            Decimal5
//	off 56  LastQty           UInt32
//	off 60  OrderQty          UInt32
//	off 64  TradingSessionID  Int32
//	off 68  ClOrdLinkID       Int32
//	off 72  SecurityID        Int32
//	off 76  Side              uint8
type ExecutionSingleReport struct {
	ClOrdID          uint64
	Timestamp        uint64
	OrderID          int64
	TrdMatchID       int64
	Flags            Flags
	Flags2           uint64
	LastPx           int64
	LastQty          uint32
	OrderQty         uint32
	TradingSessionID int32
	ClOrdLinkID      int32
	SecurityID       int32
	Side             Side
}

func DecodeExecutionSingleReport(body []byte) (ExecutionSingleReport, error) {
	if err := checkLen(TemplateExecutionSingleReport, body, BlockLengthExecutionSingleReport); err != nil {
		return ExecutionSingleReport{}, err
	}
	return ExecutionSingleReport{
		ClOrdID:          sbe.U64(body, 0),
		Timestamp:        sbe.U64(body, 8),
		OrderID:          sbe.I64(body, 16),
		TrdMatchID:       sbe.I64(body, 24),
		Flags:            Flags(sbe.U64(body, 32)),
		Flags2:           sbe.U64(body, 40),
		LastPx:           sbe.I64(body, 48),
		LastQty:          sbe.U32(body, 56),
		OrderQty:         sbe.U32(body, 60),
		TradingSessionID: sbe.I32(body, 64),
		ClOrdLinkID:      sbe.I32(body, 68),
		SecurityID:       sbe.I32(body, 72),
		Side:             Side(sbe.U8(body, 76)),
	}, nil
}

// ExecutionMultilegReport(7020) — leg fill of a multileg instrument.
//
//	off  0  ClOrdID           UInt64
//	off  8  Timestamp         TimeStamp
//	off 16  OrderID           Int64
//	off 24  TrdMatchID        Int64
//	off 32  Flags             uint64
//	off 40  Flags2            uint64
//	off 48  LastPx            Decimal5
//	off 56  LegPrice          Decimal5
//	off 64  LastQty           UInt32
//	off 68  OrderQty          UInt32
//	off 72  TradingSessionID  Int32
//	off 76  ClOrdLinkID       Int32
//	off 80  SecurityID        Int32
//	off 84  Side              uint8
type ExecutionMultilegReport struct {
	ClOrdID          uint64
	Timestamp        uint64
	OrderID          int64
	TrdMatchID       int64
	Flags            Flags
	Flags2           uint64
	LastPx           int64
	LegPrice         int64
	LastQty          uint32
	OrderQty         uint32
	TradingSessionID int32
	ClOrdLinkID      int32
	SecurityID       int32
	Side             Side
}

func DecodeExecutionMultilegReport(body []byte) (ExecutionMultilegReport, error) {
	if err := checkLen(TemplateExecutionMultilegReport, body, BlockLengthExecutionMultilegReport); err != nil {
		return ExecutionMultilegReport{}, err
	}
	return ExecutionMultilegReport{
		ClOrdID:          sbe.U64(body, 0),
		Timestamp:        sbe.U64(body, 8),
		OrderID:          sbe.I64(body, 16),
		TrdMatchID:       sbe.I64(body, 24),
		Flags:            Flags(sbe.U64(body, 32)),
		Flags2:           sbe.U64(body, 40),
		LastPx:           sbe.I64(body, 48),
		LegPrice:         sbe.I64(body, 56),
		LastQty:          sbe.U32(body, 64),
		OrderQty:         sbe.U32(body, 68),
		TradingSessionID: sbe.I32(body, 72),
		ClOrdLinkID:      sbe.I32(body, 76),
		SecurityID:       sbe.I32(body, 80),
		Side:             Side(sbe.U8(body, 84)),
	}, nil
}

// EmptyBook(7010) — main clearing started: every order of the given
// trading session is gone; the client must drop its local copies and
// must NOT send cancels for them (they would be rejected).
//
//	off 0  Timestamp         TimeStamp
//	off 8  TradingSessionID  Int32
type EmptyBook struct {
	Timestamp        uint64
	TradingSessionID int32
}

func DecodeEmptyBook(body []byte) (EmptyBook, error) {
	if err := checkLen(TemplateEmptyBook, body, BlockLengthEmptyBook); err != nil {
		return EmptyBook{}, err
	}
	return EmptyBook{
		Timestamp:        sbe.U64(body, 0),
		TradingSessionID: sbe.I32(body, 8),
	}, nil
}

// SystemEvent(7014) — trading-session lifecycle event.
//
//	off  0  Timestamp         TimeStamp
//	off  8  EventId           Int64
//	off 16  TradingSessionID  Int32
//	off 20  TradSesEvent      uint8
type SystemEvent struct {
	Timestamp        uint64
	EventID          int64
	TradingSessionID int32
	TradSesEvent     TradSesEvent
}

func DecodeSystemEvent(body []byte) (SystemEvent, error) {
	if err := checkLen(TemplateSystemEvent, body, BlockLengthSystemEvent); err != nil {
		return SystemEvent{}, err
	}
	return SystemEvent{
		Timestamp:        sbe.U64(body, 0),
		EventID:          sbe.I64(body, 8),
		TradingSessionID: sbe.I32(body, 16),
		TradSesEvent:     TradSesEvent(sbe.U8(body, 20)),
	}, nil
}
