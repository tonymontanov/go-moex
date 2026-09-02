/*
FILE: config.go

DESCRIPTION:
Public SDK configuration (spec §5.5). Unlike go-okx/go-bybit, MOEX has no
single "base URL + WS URL" pair — v1.0 uses three independent transports,
each with its own connectivity model:

  - ISS  (internal/iss)   — public HTTPS REST, reference data & candles.
  - FIX  (internal/fix)   — TCP FIX 4.4 session to FIX Gate, order entry.
  - SIMBA(internal/simba) — UDP multicast + TCP replay, market data (order
    book). Requires exchange colocation; connectivity parameters (multicast
    groups, replay host) are issued per-client by MOEX and are therefore
    left empty by DefaultConfig() — see docs/handoff.md "Open questions".

MAIN FUNCTIONS:
  - DefaultConfig(): production ISS/FIX endpoints (per moex.com/a8530) with
    sane transport-level defaults. SIMBA multicast addresses are NOT filled
    (client-specific, provided by MOEX/broker after colocation setup).
  - (Config).withDefaults(): fills empty fields from DefaultConfig(). The
    SDK always runs the user-supplied Config through this first.

DEPENDENCIES:
  - time: timeouts, heartbeat/reconnect intervals.
*/
package moex

import "time"

// Production FIX Gate endpoints for the Derivatives market (Spectra),
// per https://www.moex.com/a8530. Two independent data centers with
// distinct TargetCompID per service; the client must implement failover
// between them itself (FIX Gate servers do not share MsgSeqNum state).
var (
	// DefaultFIXHostDSP — DSP data center address.
	DefaultFIXHostDSP string = "91.203.252.32"
	// DefaultFIXHostM1 — M1 data center address.
	DefaultFIXHostM1 string = "91.203.254.32"
	// DefaultFIXPortTransactional — order entry port (both data centers).
	DefaultFIXPortTransactional int = 6001
	// DefaultFIXPortDropCopy — drop copy (execution mirror) port.
	DefaultFIXPortDropCopy int = 6002
	// DefaultFIXTargetCompIDDSPTransactional — TargetCompID for DSP transactional.
	DefaultFIXTargetCompIDDSPTransactional string = "FD"
	// DefaultFIXTargetCompIDDSPDropCopy — TargetCompID for DSP drop copy.
	DefaultFIXTargetCompIDDSPDropCopy string = "DD"
	// DefaultFIXTargetCompIDM1Transactional — TargetCompID for M1 transactional.
	DefaultFIXTargetCompIDM1Transactional string = "FG"
	// DefaultFIXTargetCompIDM1DropCopy — TargetCompID for M1 drop copy.
	DefaultFIXTargetCompIDM1DropCopy string = "DC"

	// DefaultISSBaseURL — production ISS REST endpoint.
	DefaultISSBaseURL string = "https://iss.moex.com/iss"
)

// Config — public SDK configuration. Passed to NewClient.
type Config struct {
	// ISS — reference data / candles / history transport settings.
	ISS ISSConfig
	// FIX — order entry transport settings (FIX Gate, Derivatives market).
	FIX FIXConfig
	// SIMBA — market data transport settings (order book). Empty by default:
	// multicast groups are issued per-client by MOEX after colocation setup.
	SIMBA SIMBAConfig
	// Orderbook — orderbook engine settings shared by every section.
	Orderbook OrderbookConfig

	// Logger — optional logger. If nil, NoopLogger() is used.
	Logger Logger
	// Metrics — optional counter factory. If nil, NoopMetrics() is used.
	Metrics CounterFactory

	// UserAgent — User-Agent value for ISS REST requests. Default: "go-moex".
	UserAgent string
}

// ISSConfig — MOEX Informational & Statistical Server (ISS) transport.
type ISSConfig struct {
	// BaseURL — ISS REST base URL. Default: DefaultISSBaseURL.
	BaseURL string
	// Login / Password — ISS subscription credentials (paid real-time
	// access; see moex.com/a2193). ISS authenticates via a session cookie
	// obtained from a dedicated passport endpoint, not a request header —
	// see internal/iss/auth.go. Both empty ⇒ SDK falls back to the free,
	// delayed (~15 min), rate-limited (~10 req/s) tier.
	Login    string
	Password string
	// RequestTimeout — timeout for a single ISS request. Default: 10s.
	RequestTimeout time.Duration
	// MaxIdleConns / MaxIdleConnsPerHost / IdleConnTimeout — http.Transport pool.
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration
	// RequestsPerSecond — client-side throttle applied before every ISS
	// call (token bucket, see internal/iss/limiter.go). Default: 10 (the
	// documented free-tier ceiling); raise it once the paid subscription's
	// actual limit is confirmed with MOEX/broker. 0 disables throttling.
	RequestsPerSecond float64
}

// FIXConfig — FIX Gate (Derivatives market, order entry) transport.
type FIXConfig struct {
	// Host / Port — FIX Gate address. Defaults point at the DSP transactional
	// service (DefaultFIXHostDSP:DefaultFIXPortTransactional); pass the M1
	// values for the secondary data center (see Config docs for failover).
	Host string
	Port int
	// SenderCompID — assigned by the exchange per trading firm (broker), NOT
	// a secret: the FIX Gate spec confirms auth happens at the network/IP
	// level (via the broker's dedicated circuit), the Logon message itself
	// carries no Username/Password fields. Required.
	SenderCompID string
	// TargetCompID — FIX Gate identifier for the chosen service/data center.
	// Default: DefaultFIXTargetCompIDDSPTransactional.
	TargetCompID string
	// HeartBtInt — heartbeat interval requested in Logon (tag 108). Default: 30s.
	HeartBtInt time.Duration
	// ResetSeqNumFlag — send Logon with tag 141=Y to reset MsgSeqNum to 1.
	// Per spec: MUST be true for the first Logon of every trading day,
	// see internal/fix session recovery logic which sets this automatically
	// on a fresh (no persisted log) session.
	ResetSeqNumFlag bool
	// DialTimeout — TCP connect timeout. Default: 5s.
	DialTimeout time.Duration
	// ReconnectInitialBackoff / ReconnectMaxBackoff / ReconnectJitter — see
	// spec §11 retry policy. Defaults: 200ms / 10s / 0.2. Note: spec
	// mandates NOT reconnecting within 30s of the previous session ending
	// (FIX Gate silently drops earlier attempts) — internal/fix enforces
	// this floor regardless of the computed backoff.
	ReconnectInitialBackoff time.Duration
	ReconnectMaxBackoff     time.Duration
	ReconnectJitter         float64
}

// SIMBAConfig — SIMBA SPECTRA (Derivatives market, market data) transport.
// All fields are client-specific and issued by MOEX/broker after colocation
// is provisioned; see docs/handoff.md "Open questions" for what is still
// missing to fill this in for the target account.
type SIMBAConfig struct {
	// IncrementalGroupA / IncrementalGroupB — multicast group:port for the
	// two redundant incremental (order log) feeds.
	IncrementalGroupA string
	IncrementalGroupB string
	// SnapshotGroupA / SnapshotGroupB — multicast group:port for the two
	// redundant snapshot (active orders) feeds.
	SnapshotGroupA string
	SnapshotGroupB string
	// InstrumentsGroupA / InstrumentsGroupB — multicast group:port for the
	// two redundant "Instruments" feeds (SecurityDefinition and friends —
	// used by forts.Client.ResolveSecurityID to map a human symbol like
	// "Si-12.25" to the numeric SecurityID SIMBA order/trade messages key
	// on). Distinct multicast groups from Incremental/Snapshot per §1.2 of
	// the connection procedure ("Start listening to the Instruments
	// Incremental stream...").
	InstrumentsGroupA string
	InstrumentsGroupB string
	// ReplayHost — TCP Replay service address (recovery of missed packets
	// within the current trading session), e.g. "91.203.255.244:7029".
	ReplayHost string
	// SourceIPA / SourceIPB — unicast source of every A (resp. B) group as
	// listed in configuration.xml (production: 91.203.253.244 /
	// 91.203.255.244). When set, joins are source-specific (SSM), which is
	// what MOEX's reference client does; empty = any-source join.
	SourceIPA string
	SourceIPB string
	// NetworkInterface — local interface name used to join the multicast
	// groups (e.g. "eth0"). Required in a colocation environment with
	// multiple NICs; empty ⇒ OS default interface selection.
	NetworkInterface string
}

// OrderbookConfig — orderbook engine settings (spec §5.3), shared by every
// section via the orderbook package.
type OrderbookConfig struct {
	// MaxDepth — depth of the local order book (levels per side). 0 = unlimited.
	MaxDepth int
	// GapResyncEnabled — automatically request a fresh
	// OrderBookSnapshot/EmptyBook cycle on an RptSeq gap. Default: true.
	GapResyncEnabled bool
}

// DefaultConfig returns a Config with all sensible defaults (production ISS
// + FIX endpoints, standard transport timeouts). SIMBA is intentionally left
// empty — see SIMBAConfig doc.
func DefaultConfig() Config {
	return Config{
		ISS: ISSConfig{
			BaseURL:             DefaultISSBaseURL,
			RequestTimeout:      10 * time.Second,
			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 50,
			IdleConnTimeout:     90 * time.Second,
			RequestsPerSecond:   10,
		},
		FIX: FIXConfig{
			Host:                    DefaultFIXHostDSP,
			Port:                    DefaultFIXPortTransactional,
			TargetCompID:            DefaultFIXTargetCompIDDSPTransactional,
			HeartBtInt:              30 * time.Second,
			DialTimeout:             5 * time.Second,
			ReconnectInitialBackoff: 200 * time.Millisecond,
			ReconnectMaxBackoff:     10 * time.Second,
			ReconnectJitter:         0.2,
		},
		Orderbook: OrderbookConfig{
			MaxDepth:         0,
			GapResyncEnabled: true,
		},
		Logger:    NoopLogger(),
		Metrics:   NoopMetrics(),
		UserAgent: "go-moex",
	}
}

// withDefaults returns a Config where all empty fields are filled with
// values from DefaultConfig(). Used inside NewClient — the user-supplied
// Config is never mutated.
func (c Config) withDefaults() Config {
	var def Config = DefaultConfig()

	if c.ISS.BaseURL == "" {
		c.ISS.BaseURL = def.ISS.BaseURL
	}
	if c.ISS.RequestTimeout == 0 {
		c.ISS.RequestTimeout = def.ISS.RequestTimeout
	}
	if c.ISS.MaxIdleConns == 0 {
		c.ISS.MaxIdleConns = def.ISS.MaxIdleConns
	}
	if c.ISS.MaxIdleConnsPerHost == 0 {
		c.ISS.MaxIdleConnsPerHost = def.ISS.MaxIdleConnsPerHost
	}
	if c.ISS.IdleConnTimeout == 0 {
		c.ISS.IdleConnTimeout = def.ISS.IdleConnTimeout
	}
	if c.ISS.RequestsPerSecond == 0 {
		c.ISS.RequestsPerSecond = def.ISS.RequestsPerSecond
	}

	if c.FIX.Host == "" {
		c.FIX.Host = def.FIX.Host
	}
	if c.FIX.Port == 0 {
		c.FIX.Port = def.FIX.Port
	}
	if c.FIX.TargetCompID == "" {
		c.FIX.TargetCompID = def.FIX.TargetCompID
	}
	if c.FIX.HeartBtInt == 0 {
		c.FIX.HeartBtInt = def.FIX.HeartBtInt
	}
	if c.FIX.DialTimeout == 0 {
		c.FIX.DialTimeout = def.FIX.DialTimeout
	}
	if c.FIX.ReconnectInitialBackoff == 0 {
		c.FIX.ReconnectInitialBackoff = def.FIX.ReconnectInitialBackoff
	}
	if c.FIX.ReconnectMaxBackoff == 0 {
		c.FIX.ReconnectMaxBackoff = def.FIX.ReconnectMaxBackoff
	}
	if c.FIX.ReconnectJitter == 0 {
		c.FIX.ReconnectJitter = def.FIX.ReconnectJitter
	}

	if c.Logger == nil {
		c.Logger = NoopLogger()
	}
	if c.Metrics == nil {
		c.Metrics = NoopMetrics()
	}
	if c.UserAgent == "" {
		c.UserAgent = def.UserAgent
	}

	return c
}

// validate checks the minimal set of fields required to construct a Client.
// FIX.SenderCompID is intentionally NOT required here: a Client with public
// ISS access only (no trading) is a valid, useful configuration (e.g. for
// market-data-only tooling) — forts.TradingClient methods return
// ErrorKindInvalidRequest lazily if SenderCompID is missing when a FIX
// session is actually dialed.
func (c Config) validate() error {
	if c.ISS.BaseURL == "" {
		return NewError(TransportUnknown, ErrorKindInvalidRequest, "", "config: ISS.BaseURL is empty", nil)
	}
	return nil
}
