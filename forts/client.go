/*
FILE: forts/client.go

DESCRIPTION:
FORTS (Derivatives market — futures) section client. Owns the transports
(ISS, FIX Gate or TWIME for order entry, SIMBA) and the shared state that
ties them together: open-order cache, position tracker, per-instrument
order-book engines. Registers itself with the root moex.Client via
moex.RegisterFortsFactory in init() — see client.go in the repository
root for why this indirection exists (avoids an import cycle).

CONNECTION LIFECYCLE:
  - ISS is stateless HTTP — the iss.Client is created eagerly in NewClient.
  - Order entry requires an explicit Client.Connect(ctx) call before any
    TradingClient method works — Logon/Establish is a network operation
    that can fail, so it is never triggered implicitly from a lazy getter
    (unlike go-okx's Swap()/Spot(), which don't need a handshake at all).
    Connect picks TWIME when Config.TWIME.Addr is set, FIX Gate otherwise;
    the TradingClient API is the same on both (see forts/twime.go for what
    differs underneath).
  - SIMBA listeners are started per-instrument by
    MarketDataClient.WatchOrderBook, not eagerly — a process that only
    trades without market data has no reason to join any multicast group.
*/
package forts

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	moex "github.com/tonymontanov/go-moex"
	"github.com/tonymontanov/go-moex/forts/types"
	"github.com/tonymontanov/go-moex/internal/fix"
	"github.com/tonymontanov/go-moex/internal/iss"
	"github.com/tonymontanov/go-moex/orderbook"
)

func init() {
	moex.RegisterFortsFactory(func(c *moex.Client) any { return NewClient(c) })
}

// Client — FORTS section client.
type Client struct {
	root   *moex.Client
	cfg    moex.Config
	logger moex.Logger

	issClient *iss.Client

	fixMu      sync.RWMutex
	fixSession *fix.Session
	clOrdIDGen *clOrdIDGenerator

	twimeMu      sync.Mutex
	twimeLeg     *twimeLeg
	twimeNextSeq uint64        // carried across TWIME sessions for recovery.
	twimeClOrdID atomic.Uint64 // last ClOrdID issued on TWIME.
	closerOnce   sync.Once

	orderCorrelator      *correlator[*types.OrderInfo]
	cancelCorrelator     *correlator[*types.OrderInfo]
	massCancelCorrelator *correlator[massCancelResult]

	openOrdersMu sync.RWMutex
	openOrders   map[int64]*types.OrderInfo
	clOrdToOrder map[string]int64 // ClOrdID -> OrderID, once known.

	orderWatchMu  sync.Mutex
	orderWatchers []chan *types.OrderInfo

	positions *positionTracker

	simbaMu            sync.Mutex
	engines            map[int32]*orderbook.Engine
	symbolToSecurityID map[string]int32
	securityIDToSymbol map[int32]string

	market  *MarketDataClient
	trading *TradingClient
	account *AccountClient
}

// NewClient constructs the FORTS client from the root moex.Client. Exposed
// (rather than only reachable via moex.Client.Forts()) so tests and
// advanced users can construct it directly against a *moex.Client without
// a type assertion.
func NewClient(root *moex.Client) *Client {
	var cfg moex.Config = root.Config()
	var logger moex.Logger = root.Logger()

	var issClient *iss.Client
	var err error
	issClient, err = iss.NewClient(iss.Config{
		BaseURL:             cfg.ISS.BaseURL,
		Login:               cfg.ISS.Login,
		Password:            cfg.ISS.Password,
		RequestTimeout:      cfg.ISS.RequestTimeout,
		MaxIdleConns:        cfg.ISS.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.ISS.MaxIdleConnsPerHost,
		IdleConnTimeout:     cfg.ISS.IdleConnTimeout,
		RequestsPerSecond:   cfg.ISS.RequestsPerSecond,
		UserAgent:           cfg.UserAgent,
		Logger:              logger,
		Metrics:             cfg.Metrics,
	})
	if err != nil {
		// iss.NewClient only fails on cookiejar construction, which never
		// happens with a nil PublicSuffixList — kept as an error return for
		// API hygiene, degrade to a nil client + logged error rather than
		// panicking a lazily-constructed sub-client.
		logger.Error("forts: failed to construct ISS client", moex.Err(err))
	}

	var c *Client = &Client{
		root:                 root,
		cfg:                  cfg,
		logger:               logger,
		issClient:            issClient,
		clOrdIDGen:           newClOrdIDGenerator("GM"),
		orderCorrelator:      newCorrelator[*types.OrderInfo](),
		cancelCorrelator:     newCorrelator[*types.OrderInfo](),
		massCancelCorrelator: newCorrelator[massCancelResult](),
		openOrders:           make(map[int64]*types.OrderInfo),
		clOrdToOrder:         make(map[string]int64),
		positions:            newPositionTracker(),
		engines:              make(map[int32]*orderbook.Engine),
		symbolToSecurityID:   make(map[string]int32),
		securityIDToSymbol:   make(map[int32]string),
	}
	// TWIME ClOrdID must be unique per login for the trading session (and
	// for the life of a multi-day order). Seeding from the clock keeps ids
	// unique across process restarts within a day without persistence.
	c.twimeClOrdID.Store(uint64(time.Now().UnixMicro()))
	c.market = &MarketDataClient{c: c}
	c.trading = &TradingClient{c: c}
	c.account = &AccountClient{c: c}
	return c
}

// MarketData returns the reference-data/candles/order-book sub-client.
func (c *Client) MarketData() *MarketDataClient { return c.market }

// Trading returns the order-entry sub-client. Panics-free but every method
// returns ErrorKindInvalidRequest until Connect has succeeded.
func (c *Client) Trading() *TradingClient { return c.trading }

// Account returns the position-tracking sub-client (see
// forts/types/position-info.go for its accuracy caveat).
func (c *Client) Account() *AccountClient { return c.account }

// OrderEntryTransport — which transport Trading() uses: TransportTWIME
// when Config.TWIME.Addr is set, TransportFIX otherwise.
func (c *Client) OrderEntryTransport() moex.Transport { return c.orderEntryTransport() }

func (c *Client) orderEntryTransport() moex.Transport {
	if c.cfg.TWIME.Enabled() {
		return moex.TransportTWIME
	}
	return moex.TransportFIX
}

// SetSecurityID seeds the Symbol <-> SecurityID mapping. TWIME addresses
// instruments by the numeric SecurityID only; the SDK learns ids from the
// SIMBA Instruments feed when Config.SIMBA is set (MarketData().
// ResolveSecurityID) — without colocation multicast the consumer seeds
// them here (from its own reference data) before placing orders.
func (c *Client) SetSecurityID(symbol string, securityID int32) {
	c.rememberSecurityID(symbol, securityID)
}

// SecurityIDFor — cached SecurityID for symbol, if known.
func (c *Client) SecurityIDFor(symbol string) (int32, bool) {
	c.simbaMu.Lock()
	defer c.simbaMu.Unlock()
	id, ok := c.symbolToSecurityID[symbol]
	return id, ok
}

func (c *Client) rememberSecurityID(symbol string, securityID int32) {
	c.simbaMu.Lock()
	c.symbolToSecurityID[symbol] = securityID
	c.securityIDToSymbol[securityID] = symbol
	c.simbaMu.Unlock()
}

func (c *Client) symbolFor(securityID int32) string {
	c.simbaMu.Lock()
	defer c.simbaMu.Unlock()
	return c.securityIDToSymbol[securityID]
}

// Connect dials the order-entry session used by Trading(): TWIME
// (Establish) when configured, FIX Gate (Logon) otherwise. Safe to call
// once; subsequent calls return nil while the session is alive. After a
// TWIME session ends (gateway Terminate, network failure) Connect may be
// called again: the new session recovers the server messages missed in
// between (see moex.TWIMEConfig.MaxRecoverMessages); respect
// twime.MinReconnectInterval (1s) between attempts.
func (c *Client) Connect(ctx context.Context) error {
	if c.cfg.TWIME.Enabled() {
		return c.connectTWIME(ctx)
	}
	return c.connectFIX(ctx)
}

func (c *Client) connectFIX(ctx context.Context) error {
	c.fixMu.Lock()
	defer c.fixMu.Unlock()
	if c.fixSession != nil {
		return nil
	}
	if c.cfg.FIX.SenderCompID == "" {
		return moex.NewError(moex.TransportFIX, moex.ErrorKindInvalidRequest, "", "forts: Config.FIX.SenderCompID is required to connect (see moex.FIXConfig doc)", nil)
	}

	var session *fix.Session
	var err error
	session, err = fix.Dial(fix.Config{
		Host:                    c.cfg.FIX.Host,
		Port:                    c.cfg.FIX.Port,
		SenderCompID:            c.cfg.FIX.SenderCompID,
		TargetCompID:            c.cfg.FIX.TargetCompID,
		HeartBtInt:              c.cfg.FIX.HeartBtInt,
		ResetSeqNumFlag:         c.cfg.FIX.ResetSeqNumFlag,
		DialTimeout:             c.cfg.FIX.DialTimeout,
		ReconnectInitialBackoff: c.cfg.FIX.ReconnectInitialBackoff,
		ReconnectMaxBackoff:     c.cfg.FIX.ReconnectMaxBackoff,
		ReconnectJitter:         c.cfg.FIX.ReconnectJitter,
	})
	if err != nil {
		return moex.NewError(moex.TransportFIX, moex.ErrorKindNetwork, "", "forts: dial FIX Gate", err)
	}
	session.SetAppHandler(c.handleFIXAppMessage)

	if err = session.Logon(ctx); err != nil {
		_ = session.Close()
		return moex.NewError(moex.TransportFIX, moex.ErrorKindNetwork, "", "forts: FIX Logon", err)
	}

	c.fixSession = session
	c.registerCloser()
	return nil
}

func (c *Client) registerCloser() {
	c.closerOnce.Do(func() { c.root.RegisterCloser(c.Close) })
}

// Close shuts down the order-entry session (best-effort FIX Logout or
// TWIME Terminate handshake). SIMBA listeners started by WatchOrderBook
// are stopped via their own ctx cancellation, not here — see stream.go.
func (c *Client) Close() error {
	var err error
	c.fixMu.Lock()
	if c.fixSession != nil {
		err = c.fixSession.Close()
		c.fixSession = nil
	}
	c.fixMu.Unlock()

	c.twimeMu.Lock()
	leg := c.twimeLeg
	c.twimeLeg = nil
	c.twimeMu.Unlock()
	if leg != nil {
		if terr := leg.close(); terr != nil && err == nil {
			err = terr
		}
	}
	return err
}

func (c *Client) session() (*fix.Session, error) {
	c.fixMu.RLock()
	defer c.fixMu.RUnlock()
	if c.fixSession == nil {
		return nil, moex.NewError(moex.TransportFIX, moex.ErrorKindInvalidRequest, "", "forts: not connected — call Client.Connect(ctx) before using Trading()", nil)
	}
	return c.fixSession, nil
}

// twime returns the live TWIME leg, or nil when order entry goes over FIX.
// When TWIME is configured but the session is not (or no longer) up, the
// returned leg is a stub whose methods fail with "not connected".
func (c *Client) twime() *twimeLeg {
	if !c.cfg.TWIME.Enabled() {
		return nil
	}
	c.twimeMu.Lock()
	defer c.twimeMu.Unlock()
	if c.twimeLeg == nil {
		return &twimeLeg{c: c}
	}
	return c.twimeLeg
}
