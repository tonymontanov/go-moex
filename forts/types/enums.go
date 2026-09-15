/*
FILE: forts/types/enums.go

DESCRIPTION:
FORTS FIX Gate protocol enums (spectra_fixgate_en.pdf, MOEX, rev. 20.11.2025).
Deliberately NOT in the shared root types/ package: unlike OKX (one v5
protocol for both spot and swap), MOEX Shares (v2.0) will speak a different
wire protocol (ASTS FIX/TWIME) with its own enum semantics — e.g. the
Equities market supports true Market orders, which FORTS does not. Putting
these values in forts/types keeps that section-specific meaning local and
avoids ever tempting a future shares/ package to "just reuse" a FORTS enum
that happens to look similar (see repository handoff doc, architecture
principle: no cross-section reuse, only a shared transport layer).

Values are the literal FIX tag values used on the wire (single ASCII
character in most cases) — this allows direct encode/decode without a
translation table, exactly like go-okx's InstType mirrors the OKX v5 wire
values.

IMPORTANT — FORTS FIX Gate has NO native "Market" order type:
tag 40 (OrdType) accepts only '2' (Limit) per §4.1.1 "New Order Single".
A market-like execution is achieved by sending a marketable limit order
with TimeInForceIOC/FOK at an aggressive price (see forts/trading.go
CreateOrder — it rejects OrderTypeMarket with ErrorKindInvalidRequest and
documents the IOC/FOK workaround rather than silently faking it).

Some enum values below (OrdType='2', Side, OrdStatus, ExecType core set,
TimeInForce=0/3/6) are confirmed directly against the FIX Gate spec text.
TimeInForce=4 (FOK) follows the FIX 4.4 standard enumeration and is marked
TODO pending confirmation against the live test circuit — see
docs/handoff.md "Open questions".
*/
package types

// Side — order direction (FIX tag 54).
type Side string

const (
	SideBuy  Side = "1"
	SideSell Side = "2"
)

// OrdType — order type (FIX tag 40). FORTS FIX Gate supports ONLY Limit.
type OrdType string

const (
	// OrdTypeLimit — the only OrdType FORTS FIX Gate accepts (§4.1.1).
	OrdTypeLimit OrdType = "2"
)

// TimeInForce — time in force (FIX tag 59).
type TimeInForce string

const (
	// TimeInForceDay — resting order for the current trading day. MOEX calls
	// this the default ("quote order") when tag 59 is omitted entirely; the
	// SDK always sends it explicitly for clarity.
	TimeInForceDay TimeInForce = "0"
	// TimeInForceIOC — Immediate-Or-Cancel. Confirmed by spec §4.2.1
	// ("An order containing TimeInForce=3(IOC) can be completely matched,
	// cancelled or partly matched with its leftover part cancelled").
	TimeInForceIOC TimeInForce = "3"
	// TimeInForceFOK — Fill-Or-Kill. Standard FIX 4.4 value; TODO: confirm
	// against the live FORTS test circuit (not explicitly enumerated in the
	// extracted spec excerpt available at SDK authoring time).
	TimeInForceFOK TimeInForce = "4"
	// TimeInForceGTD — Good-Till-Date. Requires ExpireDate (tag 432).
	TimeInForceGTD TimeInForce = "6"
	// TimeInForceBOC — Book-Or-Cancel (post-only: rejected instead of
	// matching as the aggressor). TWIME only (TimeInForceEnum 122); FIX
	// Gate rejects it.
	TimeInForceBOC TimeInForce = "122"
)

// OrdStatus — current order status (FIX tag 39), carried on every
// Execution Report.
type OrdStatus string

const (
	OrdStatusNew             OrdStatus = "0"
	OrdStatusPartiallyFilled OrdStatus = "1"
	OrdStatusFilled          OrdStatus = "2"
	OrdStatusCanceled        OrdStatus = "4"
	OrdStatusPendingCancel   OrdStatus = "6"
	OrdStatusRejected        OrdStatus = "8"
	OrdStatusPendingReplace  OrdStatus = "E"
)

// ExecType — reason the Execution Report was sent (FIX tag 150).
type ExecType string

const (
	ExecTypeNew         ExecType = "0"
	ExecTypeCanceled    ExecType = "4"
	ExecTypeReplaced    ExecType = "5"
	ExecTypeRejected    ExecType = "8"
	ExecTypeOrderStatus ExecType = "I"
	ExecTypeTrade       ExecType = "F"
)

// MarketSegmentID — FIX tag 1300, distinguishes futures from options within
// the FORTS (futures) engine/market pair used by ISS (see internal/iss).
type MarketSegmentID string

const (
	MarketSegmentFutures MarketSegmentID = "F"
	MarketSegmentOptions MarketSegmentID = "O"
)
