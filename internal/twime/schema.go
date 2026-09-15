/*
FILE: internal/twime/schema.go

DESCRIPTION:
Schema identification, template ids, enums, sets and null sentinels of
TWIME SPECTRA (FORTS order entry over TCP), transcribed from the official
SBE schema twime_spectra-7.7.xml
(https://ftp.moex.com/pub/TWIME/Spectra/prod/doc/twime_spectra-7.7.xml,
package moex_spectra_twime, id=19781, version=7, byteOrder=littleEndian).

Every constant below carries the XML name so a future schema bump can be
diffed mechanically against the file. Message layouts (field offsets and
block lengths) live in messages.go.
*/
package twime

// SchemaID — sbe:messageSchema id of TWIME SPECTRA.
const SchemaID uint16 = 19781

// SchemaVersion — sbe:messageSchema version the codec was written
// against. The wire header of every message repeats it; Decoder rejects
// other schema ids but tolerates other versions by block length (SBE
// forward-compat rule: unknown trailing bytes of a root block are
// skipped).
const SchemaVersion uint16 = 7

// Template ids (sbe:message id=...). 5xxx — session layer, 6xxx —
// client-to-server application, 7xxx — server-to-client application.
const (
	TemplateEstablish             uint16 = 5000
	TemplateEstablishmentAck      uint16 = 5001
	TemplateEstablishmentReject   uint16 = 5002
	TemplateTerminate             uint16 = 5003
	TemplateRetransmitRequest     uint16 = 5004
	TemplateRetransmission        uint16 = 5005
	TemplateSequence              uint16 = 5006
	TemplateFloodReject           uint16 = 5007
	TemplateSessionReject         uint16 = 5008
	TemplateBusinessMessageReject uint16 = 5009

	TemplateNewOrderSingle                  uint16 = 6000
	TemplateOrderMassCancelRequest          uint16 = 6004
	TemplateOrderMassCancelByBFLimitRequest uint16 = 6005
	TemplateOrderCancelRequest              uint16 = 6006
	TemplateOrderReplaceRequest             uint16 = 6007
	TemplateNewOrderIceberg                 uint16 = 6008
	TemplateOrderIcebergCancelRequest       uint16 = 6009
	TemplateOrderIcebergReplaceRequest      uint16 = 6010
	TemplateNewOrderIcebergX                uint16 = 6011

	TemplateOrderMassCancelResponse uint16 = 7007
	TemplateEmptyBook               uint16 = 7010
	TemplateSystemEvent             uint16 = 7014
	TemplateNewOrderSingleResponse  uint16 = 7015
	TemplateNewOrderIcebergResponse uint16 = 7016
	TemplateOrderCancelResponse     uint16 = 7017
	TemplateOrderReplaceResponse    uint16 = 7018
	TemplateExecutionSingleReport   uint16 = 7019
	TemplateExecutionMultilegReport uint16 = 7020
)

// TemplateName — XML message name for logs/metrics labels; "unknown"
// for ids outside the schema.
func TemplateName(id uint16) string {
	switch id {
	case TemplateEstablish:
		return "Establish"
	case TemplateEstablishmentAck:
		return "EstablishmentAck"
	case TemplateEstablishmentReject:
		return "EstablishmentReject"
	case TemplateTerminate:
		return "Terminate"
	case TemplateRetransmitRequest:
		return "RetransmitRequest"
	case TemplateRetransmission:
		return "Retransmission"
	case TemplateSequence:
		return "Sequence"
	case TemplateFloodReject:
		return "FloodReject"
	case TemplateSessionReject:
		return "SessionReject"
	case TemplateBusinessMessageReject:
		return "BusinessMessageReject"
	case TemplateNewOrderSingle:
		return "NewOrderSingle"
	case TemplateOrderMassCancelRequest:
		return "OrderMassCancelRequest"
	case TemplateOrderMassCancelByBFLimitRequest:
		return "OrderMassCancelByBFLimitRequest"
	case TemplateOrderCancelRequest:
		return "OrderCancelRequest"
	case TemplateOrderReplaceRequest:
		return "OrderReplaceRequest"
	case TemplateNewOrderIceberg:
		return "NewOrderIceberg"
	case TemplateOrderIcebergCancelRequest:
		return "OrderIcebergCancelRequest"
	case TemplateOrderIcebergReplaceRequest:
		return "OrderIcebergReplaceRequest"
	case TemplateNewOrderIcebergX:
		return "NewOrderIcebergX"
	case TemplateOrderMassCancelResponse:
		return "OrderMassCancelResponse"
	case TemplateEmptyBook:
		return "EmptyBook"
	case TemplateSystemEvent:
		return "SystemEvent"
	case TemplateNewOrderSingleResponse:
		return "NewOrderSingleResponse"
	case TemplateNewOrderIcebergResponse:
		return "NewOrderIcebergResponse"
	case TemplateOrderCancelResponse:
		return "OrderCancelResponse"
	case TemplateOrderReplaceResponse:
		return "OrderReplaceResponse"
	case TemplateExecutionSingleReport:
		return "ExecutionSingleReport"
	case TemplateExecutionMultilegReport:
		return "ExecutionMultilegReport"
	default:
		return "unknown"
	}
}

// IsSessionTemplate — session-layer message (5000..5009). Session-layer
// messages are not numbered by the server and do not advance NextSeqNo.
func IsSessionTemplate(id uint16) bool { return id >= 5000 && id < 6000 }

// Null sentinels (schema <types>: every optional integer type declares
// nullValue = maxValue+1; TimeStamp is UInt64-null; Decimal5 mantissa is
// presence="required" and has no null).
const (
	NullInt8      int8   = 127
	NullInt16     int16  = 32767
	NullInt32     int32  = 2147483647
	NullInt64     int64  = 9223372036854775807
	NullUint8     uint8  = 255
	NullUint16    uint16 = 65535
	NullUint32    uint32 = 4294967295
	NullUint64    uint64 = 18446744073709551615
	NullTimestamp uint64 = NullUint64
)

// KeepaliveInterval bounds — type DeltaMillisecs minValue/maxValue.
const (
	MinKeepaliveMillis uint32 = 1000
	MaxKeepaliveMillis uint32 = 60000
)

// Fixed char-array widths.
const (
	CredentialsLen   = 20 // String20 — Establish.Credentials
	AccountLen       = 7  // String7 — Account
	SecurityGroupLen = 25 // String25 — OrderMassCancelRequest.SecurityGroup
)

// TerminationCode — enum TerminationCodeEnum (uint8).
type TerminationCode uint8

const (
	TerminationFinished              TerminationCode = 0
	TerminationUnspecifiedError      TerminationCode = 1
	TerminationReRequestOutOfBounds  TerminationCode = 2
	TerminationReRequestInProgress   TerminationCode = 3
	TerminationTooFastClient         TerminationCode = 4
	TerminationTooSlowClient         TerminationCode = 5
	TerminationMissedHeartbeat       TerminationCode = 6
	TerminationInvalidMessage        TerminationCode = 7
	TerminationTCPFailure            TerminationCode = 8
	TerminationInvalidSequenceNumber TerminationCode = 9
	TerminationServerShutdown        TerminationCode = 10
	TerminationSequenceReset         TerminationCode = 11
)

func (c TerminationCode) String() string {
	switch c {
	case TerminationFinished:
		return "Finished"
	case TerminationUnspecifiedError:
		return "UnspecifiedError"
	case TerminationReRequestOutOfBounds:
		return "ReRequestOutOfBounds"
	case TerminationReRequestInProgress:
		return "ReRequestInProgress"
	case TerminationTooFastClient:
		return "TooFastClient"
	case TerminationTooSlowClient:
		return "TooSlowClient"
	case TerminationMissedHeartbeat:
		return "MissedHeartbeat"
	case TerminationInvalidMessage:
		return "InvalidMessage"
	case TerminationTCPFailure:
		return "TCPFailure"
	case TerminationInvalidSequenceNumber:
		return "InvalidSequenceNumber"
	case TerminationServerShutdown:
		return "ServerShutdown"
	case TerminationSequenceReset:
		return "SequenceReset"
	default:
		return "unknown"
	}
}

// EstablishmentRejectCode — enum EstablishmentRejectCodeEnum (uint8).
type EstablishmentRejectCode uint8

const (
	EstablishRejectUnnegotiated       EstablishmentRejectCode = 0
	EstablishRejectAlreadyEstablished EstablishmentRejectCode = 1
	EstablishRejectSessionBlocked     EstablishmentRejectCode = 2
	EstablishRejectKeepaliveInterval  EstablishmentRejectCode = 3
	EstablishRejectCredentials        EstablishmentRejectCode = 4
	EstablishRejectUnspecified        EstablishmentRejectCode = 5
	EstablishRejectTooFastReconnect   EstablishmentRejectCode = 6
)

func (c EstablishmentRejectCode) String() string {
	switch c {
	case EstablishRejectUnnegotiated:
		return "Unnegotiated"
	case EstablishRejectAlreadyEstablished:
		return "AlreadyEstablished"
	case EstablishRejectSessionBlocked:
		return "SessionBlocked"
	case EstablishRejectKeepaliveInterval:
		return "KeepaliveInterval"
	case EstablishRejectCredentials:
		return "Credentials"
	case EstablishRejectUnspecified:
		return "Unspecified"
	case EstablishRejectTooFastReconnect:
		return "TooFastReconnect"
	default:
		return "unknown"
	}
}

// SessionRejectReason — enum SessionRejectReasonEnum (uint8).
type SessionRejectReason uint8

const (
	SessionRejectValueIsIncorrect    SessionRejectReason = 5
	SessionRejectOther               SessionRejectReason = 99
	SessionRejectSystemIsUnavailable SessionRejectReason = 100
	SessionRejectClOrdIDIsNotUnique  SessionRejectReason = 101
)

func (r SessionRejectReason) String() string {
	switch r {
	case SessionRejectValueIsIncorrect:
		return "ValueIsIncorrect"
	case SessionRejectOther:
		return "Other"
	case SessionRejectSystemIsUnavailable:
		return "SystemIsUnavailable"
	case SessionRejectClOrdIDIsNotUnique:
		return "ClOrdIdIsNotUnique"
	default:
		return "unknown"
	}
}

// TimeInForce — enum TimeInForceEnum (uint8). BOC = Book-or-Cancel
// (post-only: rejected instead of matching as an aggressor).
type TimeInForce uint8

const (
	TimeInForceDay TimeInForce = 0
	TimeInForceIOC TimeInForce = 3
	TimeInForceFOK TimeInForce = 4
	TimeInForceGTD TimeInForce = 6
	TimeInForceBOC TimeInForce = 122
)

func (t TimeInForce) String() string {
	switch t {
	case TimeInForceDay:
		return "Day"
	case TimeInForceIOC:
		return "IOC"
	case TimeInForceFOK:
		return "FOK"
	case TimeInForceGTD:
		return "GTD"
	case TimeInForceBOC:
		return "BOC"
	default:
		return "unknown"
	}
}

// Side — enum SideEnum (uint8). AllOrders is valid only as a mass-cancel
// filter.
type Side uint8

const (
	SideBuy       Side = 1
	SideSell      Side = 2
	SideAllOrders Side = 89
)

func (s Side) String() string {
	switch s {
	case SideBuy:
		return "Buy"
	case SideSell:
		return "Sell"
	case SideAllOrders:
		return "AllOrders"
	default:
		return "unknown"
	}
}

// ReplaceMode — enum ModeEnum (uint8), OrderReplaceRequest.Mode.
type ReplaceMode uint8

const (
	ReplaceModeDontChangeOrderQty          ReplaceMode = 0
	ReplaceModeChangeOrderQty              ReplaceMode = 1
	ReplaceModeCheckOrderQtyAndCancelOrder ReplaceMode = 2
	ReplaceModeFixStyleReplace             ReplaceMode = 3
)

// SecurityType — set SecurityTypeSet (uint8 bitmask).
type SecurityType uint8

const (
	SecurityTypeFuture   SecurityType = 1 << 0
	SecurityTypeOption   SecurityType = 1 << 1
	SecurityTypeMultileg SecurityType = 1 << 2
)

// TradSesEvent — enum TradSesEventEnum (uint8), SystemEvent.TradSesEvent.
type TradSesEvent uint8

const (
	TradSesEventSessionDataReady            TradSesEvent = 101
	TradSesEventClearingStarted             TradSesEvent = 105
	TradSesEventExtensionOfLimitsFinished   TradSesEvent = 106
	TradSesEventBrokerRecalcFinished        TradSesEvent = 108
	TradSesEventAuctionFinished             TradSesEvent = 122
	TradSesEventAuctionCollectOrderStarted  TradSesEvent = 123
	TradSesEventAuctionCollectOrderFinished TradSesEvent = 124
)

func (e TradSesEvent) String() string {
	switch e {
	case TradSesEventSessionDataReady:
		return "SessionDataReady"
	case TradSesEventClearingStarted:
		return "ClearingStarted"
	case TradSesEventExtensionOfLimitsFinished:
		return "ExtensionOfLimitsFinished"
	case TradSesEventBrokerRecalcFinished:
		return "BrokerRecalcFinished"
	case TradSesEventAuctionFinished:
		return "AuctionFinished"
	case TradSesEventAuctionCollectOrderStarted:
		return "AuctionCollectOrderStarted"
	case TradSesEventAuctionCollectOrderFinished:
		return "AuctionCollectOrderFinished"
	default:
		return "unknown"
	}
}

// ComplianceID — enum ComplianceIDEnum (char). Algorithmic orders must
// carry ComplianceAlgorithm ('R').
type ComplianceID byte

const (
	ComplianceNotAvailable ComplianceID = ' '
	ComplianceManual       ComplianceID = 'M'
	ComplianceStopLoss     ComplianceID = 'S'
	ComplianceAlgorithm    ComplianceID = 'R'
	ComplianceAutofollow   ComplianceID = 'A'
	ComplianceMarginCall   ComplianceID = 'D'
)

// ClientFlags — set ClientFlagsSet (uint8 bitmask), request-side flags.
type ClientFlags uint8

const (
	ClientFlagDontCheckLimits ClientFlags = 1 << 0
	ClientFlagNccRequest      ClientFlags = 1 << 1
)

// Flags — set FlagsSet (uint64 bitmask), response-side order/trade flags.
// Bit numbers are the XML <choice> values.
type Flags uint64

const (
	FlagDay                   Flags = 1 << 0
	FlagIOC                   Flags = 1 << 1
	FlagOTC                   Flags = 1 << 2
	FlagPosTransfer           Flags = 1 << 3
	FlagCollateral            Flags = 1 << 4
	FlagDontCheckLimits       Flags = 1 << 9
	FlagDueToCrossCancel      Flags = 1 << 13
	FlagFOK                   Flags = 1 << 19
	FlagReplace               Flags = 1 << 20
	FlagCancel                Flags = 1 << 21
	FlagMassCancel            Flags = 1 << 22
	FlagClearing              Flags = 1 << 25
	FlagNegotiated            Flags = 1 << 26
	FlagMultiLeg              Flags = 1 << 27
	FlagCrossTrade            Flags = 1 << 29
	FlagNegotiatedMatchByRef  Flags = 1 << 31
	FlagCOD                   Flags = 1 << 32
	FlagUKS                   Flags = 1 << 37
	FlagNCCRequest            Flags = 1 << 38
	FlagNCC                   Flags = 1 << 39
	FlagLiqNettingRF          Flags = 1 << 40
	FlagActiveSide            Flags = 1 << 41
	FlagPassiveSide           Flags = 1 << 42
	FlagSynthetic             Flags = 1 << 45
	FlagIceberg               Flags = 1 << 47
	FlagDisclosedIceberg      Flags = 1 << 53
	FlagBOC                   Flags = 1 << 60
	FlagDuringDiscreteAuction Flags = 1 << 62
)

// Has reports whether every bit of mask is set.
func (f Flags) Has(mask Flags) bool { return f&mask == mask }

// IsCancelled — the response records a cancellation of any kind (user
// cancel, mass cancel, cross cancel, COD, UKS, NCC).
func (f Flags) IsCancelled() bool {
	return f&(FlagCancel|FlagMassCancel|FlagDueToCrossCancel|FlagCOD|FlagUKS|FlagNCC) != 0
}
