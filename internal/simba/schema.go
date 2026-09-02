/*
FILE: internal/simba/schema.go

DESCRIPTION:
Schema identification and template-id normalisation across SIMBA schema
versions. The wire carries (SchemaID, Version) in every SBE header and
MOEX renumbers templates between versions (history section of
spectra_simba_en.pdf): SecurityDefinition was 18 → 20 → 21 → 27,
SecurityStatus 9 → 28, TradingSessionStatus 11 → 23 → 26, ... Production
captures from 2026-05-15 still carried Version=8 (SecurityDefinition=21)
while the published schema was already 9.0 — a decoder that hard-codes
one version silently ignores whole message classes on the other.

Kind is the version-independent message identity used by the rest of the
package; kindOf maps a wire template id to it for every supported version.
Order-log templates (Heartbeat 1, SequenceReset 2, EmptyBook 4, BestPrices
14, OrderUpdate 15, OrderExecution 16, OrderBookSnapshot 17) have been
stable since version 3 (when MDFlags2 was added) and are identical in 8/9.
*/
package simba

import "fmt"

// SchemaID — sbe:messageSchema id of SIMBA SPECTRA (all versions).
const SchemaID uint16 = 19780

// Supported schema versions (wire `version` field). Both are accepted by
// default; production ran 8 in May 2026 while the FTP schema was 9.0.
const (
	SchemaVersion8 uint16 = 8
	SchemaVersion9 uint16 = 9
)

// Kind — version-independent message identity.
type Kind uint8

const (
	KindUnknown Kind = iota
	KindHeartbeat
	KindSequenceReset
	KindEmptyBook
	KindBestPrices
	KindOrderUpdate
	KindOrderExecution
	KindOrderBookSnapshot
	KindSecurityDefinition
	KindSecurityStatus
	KindSecurityDefinitionUpdateReport
	KindTradingSessionStatus
	KindDiscreteAuction
	KindSecurityMassStatus
	KindSecurityGroupStatus
	KindLogon
	KindLogout
	KindMarketDataRequest
	KindMarketDataDummyMessage
)

func (k Kind) String() string {
	switch k {
	case KindHeartbeat:
		return "Heartbeat"
	case KindSequenceReset:
		return "SequenceReset"
	case KindEmptyBook:
		return "EmptyBook"
	case KindBestPrices:
		return "BestPrices"
	case KindOrderUpdate:
		return "OrderUpdate"
	case KindOrderExecution:
		return "OrderExecution"
	case KindOrderBookSnapshot:
		return "OrderBookSnapshot"
	case KindSecurityDefinition:
		return "SecurityDefinition"
	case KindSecurityStatus:
		return "SecurityStatus"
	case KindSecurityDefinitionUpdateReport:
		return "SecurityDefinitionUpdateReport"
	case KindTradingSessionStatus:
		return "TradingSessionStatus"
	case KindDiscreteAuction:
		return "DiscreteAuction"
	case KindSecurityMassStatus:
		return "SecurityMassStatus"
	case KindSecurityGroupStatus:
		return "SecurityGroupStatus"
	case KindLogon:
		return "Logon"
	case KindLogout:
		return "Logout"
	case KindMarketDataRequest:
		return "MarketDataRequest"
	case KindMarketDataDummyMessage:
		return "MarketDataDummyMessage"
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

// kindOf maps a wire template id to Kind for the given schema version.
// Returns KindUnknown for ids the version does not define (or a version
// this package does not know).
func kindOf(version uint16, templateID uint16) Kind {
	switch templateID {
	case 1:
		return KindHeartbeat
	case 2:
		return KindSequenceReset
	case 4:
		return KindEmptyBook
	case 14:
		return KindBestPrices
	case 15:
		return KindOrderUpdate
	case 16:
		return KindOrderExecution
	case 17:
		return KindOrderBookSnapshot
	case 10:
		return KindSecurityDefinitionUpdateReport
	case 19:
		return KindSecurityMassStatus
	case 22:
		return KindSecurityGroupStatus
	case 24:
		return KindDiscreteAuction
	case 26:
		return KindTradingSessionStatus
	case 1000:
		return KindLogon
	case 1001:
		return KindLogout
	case 1002:
		return KindMarketDataRequest
	case 1003:
		return KindMarketDataDummyMessage
	}
	switch version {
	case SchemaVersion8:
		switch templateID {
		case 21:
			return KindSecurityDefinition
		case 9:
			return KindSecurityStatus
		}
	case SchemaVersion9:
		switch templateID {
		case 27:
			return KindSecurityDefinition
		case 28:
			return KindSecurityStatus
		}
	}
	return KindUnknown
}

// SupportedVersion reports whether this package knows the template table
// of a schema version.
func SupportedVersion(v uint16) bool { return v == SchemaVersion8 || v == SchemaVersion9 }

// kindShape — how to skip a message body after its root block (see
// messageSize). Keyed by Kind so it is version-independent.
func kindShape(k Kind) (shape templateShape, known bool) {
	switch k {
	case KindHeartbeat, KindSequenceReset, KindEmptyBook, KindOrderUpdate, KindOrderExecution,
		KindSecurityStatus, KindSecurityDefinitionUpdateReport, KindTradingSessionStatus, KindSecurityGroupStatus,
		KindLogon, KindLogout, KindMarketDataRequest, KindMarketDataDummyMessage:
		return templateShape{}, true
	case KindBestPrices, KindOrderBookSnapshot:
		return templateShape{groupDims: []int{3}}, true
	case KindSecurityDefinition:
		return templateShape{groupDims: []int{3, 3, 3, 3, 3}, varData: 2}, true
	case KindSecurityMassStatus:
		return templateShape{groupDims: []int{4}}, true
	case KindDiscreteAuction:
		return templateShape{unsizable: true}, true
	}
	return templateShape{}, false
}
