/*
FILE: forts/instruments.go

DESCRIPTION:
InstrumentSession — a permanent listener of the SIMBA Instruments feeds
(FUT-INFO / OPT-INFO) that keeps an in-memory reference-data cache:
SecurityID <-> Symbol, tick, price limits, margins, trading status,
expiry events, legs. It replaces the one-shot 30-second
MarketDataClient.ResolveSecurityID for anything that must be current
during the day (limits and status change intraday, and a resting order
outside the limits is rejected).

WIRE FACTS (production captures, schema 8):
  - The Replay group re-broadcasts every SecurityDefinition in a cycle
    that starts with SequenceReset; FUT-INFO cycles in ~7 s (745
    instruments), OPT-INFO in ~165 s (36k). TotNumReports carries the
    cycle size. Packets are not fragmented, one message per packet.
  - The Incremental group carries SecurityStatus (status/limit changes),
    SecurityDefinitionUpdateReport (theor price / volatility, OPT-INFO)
    and TradingSessionStatus; it is almost silent on FUT-INFO.
  - SecurityGroupStatus (template 22) is frequent on the Replay group but
    its schema is not decoded yet; it is counted and ignored.

Ready() is closed once a full Replay cycle has been observed, i.e. every
instrument of the feed is in the cache.
*/
package forts

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/shopspring/decimal"
	moex "github.com/tonymontanov/go-moex"
	"github.com/tonymontanov/go-moex/internal/simba"
)

// Instrument — decoded SecurityDefinition, refreshed by SecurityStatus
// and SecurityDefinitionUpdateReport. Null wire values decode to the
// zero value (decimal zero, empty string, simba.NullInt32 for ids).
type Instrument struct {
	SecurityID               int32
	Symbol                   string
	SecurityAltID            string
	SecurityAltIDSource      byte // '4' ISIN, '8' exchange symbol
	SecurityType             string
	CFICode                  string
	StrikePrice              decimal.Decimal
	ContractMultiplier       int32
	TradingStatus            simba.SecurityTradingStatus
	Currency                 string
	MarketSegmentID          byte
	TradingSessionID         uint8
	ExchangeTradingSessionID int32
	Volatility               decimal.Decimal
	HighLimitPx              decimal.Decimal
	LowLimitPx               decimal.Decimal
	MinPriceIncrement        decimal.Decimal
	MinPriceIncrementAmount  decimal.Decimal
	InitialMarginOnBuy       decimal.Decimal
	InitialMarginOnSell      decimal.Decimal
	InitialMarginSyntetic    decimal.Decimal
	TheorPrice               decimal.Decimal
	TheorPriceLimit          decimal.Decimal
	UnderlyingQty            decimal.Decimal
	UnderlyingCurrency       string
	MaturityDate             uint32 // YYYYMMDD on schema 8; 0 on schema 9 (see Events)
	Flags                    uint64
	SettlPrice               decimal.Decimal
	TradeModeID              int32
	GroupMask                int64
	SectionID                int32
	BaseContractID           int32
	TradePeriodAccess        uint64
	Underlyings              []InstrumentUnderlying
	Legs                     []InstrumentLeg
	Events                   []InstrumentEvent
	Description              string
	SchemaVersion            uint16
	// UpdatedAtNs — SendingTime of the packet that last touched the entry.
	UpdatedAtNs uint64
}

// InstrumentUnderlying — NoUnderlyings entry.
type InstrumentUnderlying struct {
	Symbol     string
	Board      string
	SecurityID int32
	FutureID   int32
}

// InstrumentLeg — NoLegs entry (multileg instruments).
type InstrumentLeg struct {
	Symbol     string
	SecurityID int32
	RatioQty   int32
}

// InstrumentEvent — NoEvents entry (expiry and settlement dates).
type InstrumentEvent struct {
	Type int32
	Date uint32 // YYYYMMDD
	Time uint64
}

// InstrumentSessionConfig — construction parameters.
type InstrumentSessionConfig struct {
	// SIMBA — InstrumentsGroupA (+B) required; InstrumentsIncrementalGroupA
	// (+B) optional.
	SIMBA moex.SIMBAConfig
	// OnDefinition — called after every SecurityDefinition (each Replay
	// cycle repeats them all). The pointer is a private copy.
	OnDefinition func(*Instrument)
	// OnStatus — called when SecurityStatus changed an instrument's
	// trading status or limits/margins; prev is the status before.
	OnStatus func(inst *Instrument, prev simba.SecurityTradingStatus)
	// OnSessionStatus — TradingSessionStatus messages.
	OnSessionStatus func(simba.TradingSessionStatus)
	Metrics         moex.CounterFactory
	Logger          moex.Logger
}

// InstrumentSessionStats — counters (read with Stats()).
type InstrumentSessionStats struct {
	Packets, ParseErrors, UnknownMessages   uint64
	Definitions, StatusUpdates, TheorUpdate uint64
	SessionStatuses, GroupStatuses          uint64
	Cycles                                  uint64
	TotNumReports                           uint32
	Instruments                             int
}

// InstrumentSession — see file header.
type InstrumentSession struct {
	cfg    InstrumentSessionConfig
	logger moex.Logger
	met    instrumentMetrics

	mu        sync.Mutex
	byID      map[int32]*Instrument
	bySymbol  map[string]int32
	cycleSeen map[int32]struct{}
	prevCycle map[int32]struct{}
	inCycle   bool
	totNum    uint32
	stats     InstrumentSessionStats
	ready     chan struct{}
	readyDone atomic.Bool
}

type instrumentMetrics struct {
	packets, parseErrors, definitions, statusUpdates, cycles moex.Counter
}

// NewInstrumentSession constructs a session; call Run to start.
func NewInstrumentSession(cfg InstrumentSessionConfig) *InstrumentSession {
	var logger moex.Logger = cfg.Logger
	if logger == nil {
		logger = moex.NoopLogger()
	}
	var s *InstrumentSession = &InstrumentSession{
		cfg:       cfg,
		logger:    logger,
		byID:      make(map[int32]*Instrument),
		bySymbol:  make(map[string]int32),
		cycleSeen: make(map[int32]struct{}),
		ready:     make(chan struct{}),
	}
	if cfg.Metrics != nil {
		s.met = instrumentMetrics{
			packets:       cfg.Metrics.Counter("moex_simba_info_packets_total"),
			parseErrors:   cfg.Metrics.Counter("moex_simba_info_parse_errors_total"),
			definitions:   cfg.Metrics.Counter("moex_simba_info_definitions_total"),
			statusUpdates: cfg.Metrics.Counter("moex_simba_info_status_updates_total"),
			cycles:        cfg.Metrics.Counter("moex_simba_info_cycles_total"),
		}
	}
	return s
}

func metricInc(c moex.Counter) {
	if c != nil {
		c.Inc()
	}
}

// Ready is closed after the first complete Replay cycle.
func (s *InstrumentSession) Ready() <-chan struct{} { return s.ready }

// Stats — counters snapshot.
func (s *InstrumentSession) Stats() InstrumentSessionStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st InstrumentSessionStats = s.stats
	st.Instruments = len(s.byID)
	return st
}

// Get returns a copy of the instrument by SecurityID.
func (s *InstrumentSession) Get(securityID int32) (Instrument, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var in *Instrument = s.byID[securityID]
	if in == nil {
		return Instrument{}, false
	}
	return *in, true
}

// BySymbol returns a copy of the instrument by Symbol.
func (s *InstrumentSession) BySymbol(symbol string) (Instrument, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var id, ok = s.bySymbol[symbol]
	if !ok {
		return Instrument{}, false
	}
	return *s.byID[id], true
}

// All returns copies of every cached instrument (unspecified order).
func (s *InstrumentSession) All() []Instrument {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Instrument = make([]Instrument, 0, len(s.byID))
	for _, in := range s.byID {
		out = append(out, *in)
	}
	return out
}

// Run joins the Instruments Replay group(s) and, when configured, the
// Instruments Incremental group(s); serves until ctx is done.
func (s *InstrumentSession) Run(ctx context.Context) error {
	if s.cfg.SIMBA.InstrumentsGroupA == "" {
		return moex.NewError(moex.TransportSIMBA, moex.ErrorKindInvalidRequest, "", "forts: Config.SIMBA.InstrumentsGroupA is required for InstrumentSession", nil)
	}
	var legs []*simba.Listener
	var closeLegs = func() {
		for _, l := range legs {
			_ = l.Close()
		}
	}
	var listen = func(group, source string) error {
		if group == "" {
			return nil
		}
		var l *simba.Listener
		var err error
		l, err = simba.Listen(simba.ListenerConfig{GroupAddr: group, SourceIP: source, Interface: s.cfg.SIMBA.NetworkInterface, Logger: s.logger})
		if err != nil {
			return moex.NewError(moex.TransportSIMBA, moex.ErrorKindNetwork, "", "forts: Instruments multicast group "+group, err)
		}
		legs = append(legs, l)
		return nil
	}
	var c moex.SIMBAConfig = s.cfg.SIMBA
	for _, g := range []struct{ group, src string }{
		{c.InstrumentsGroupA, c.SourceIPA}, {c.InstrumentsGroupB, c.SourceIPB},
		{c.InstrumentsIncrementalGroupA, c.SourceIPA}, {c.InstrumentsIncrementalGroupB, c.SourceIPB},
	} {
		if err := listen(g.group, g.src); err != nil {
			closeLegs()
			return err
		}
	}
	defer closeLegs()

	var runCtx context.Context
	var cancel context.CancelFunc
	runCtx, cancel = context.WithCancel(ctx)
	defer cancel()
	var errCh chan error = make(chan error, len(legs))
	for _, l := range legs {
		go func(l *simba.Listener) { errCh <- l.Run(runCtx, s.HandlePacket) }(l)
	}
	var err error = <-errCh
	cancel()
	for i := 1; i < len(legs); i++ {
		<-errCh
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		return nil
	}
	return err
}

// HandlePacket decodes one Instruments-feed datagram (Replay or
// Incremental). Exported for tests and capture replay.
func (s *InstrumentSession) HandlePacket(buf []byte) {
	metricInc(s.met.packets)
	var p simba.Packet
	var err error
	p, err = simba.ParsePacket(buf, 0)
	if err != nil {
		s.mu.Lock()
		s.stats.Packets++
		s.stats.ParseErrors++
		s.mu.Unlock()
		metricInc(s.met.parseErrors)
		return
	}
	s.mu.Lock()
	s.stats.Packets++
	s.mu.Unlock()
	var sendingTime uint64 = p.Header().SendingTime
	for {
		var m simba.Message
		var ok bool
		m, ok, err = p.Next()
		if err != nil {
			s.mu.Lock()
			s.stats.ParseErrors++
			s.mu.Unlock()
			metricInc(s.met.parseErrors)
			return
		}
		if !ok {
			return
		}
		switch m.Kind {
		case simba.KindSequenceReset:
			s.onSequenceReset()
		case simba.KindSecurityDefinition:
			var v simba.SecurityDefinitionView
			if v, ok = m.SecurityDefinitionView(); ok {
				s.onDefinition(v, m.Header.Version, sendingTime)
			} else {
				s.mu.Lock()
				s.stats.ParseErrors++
				s.mu.Unlock()
				metricInc(s.met.parseErrors)
			}
		case simba.KindSecurityStatus:
			var v simba.SecurityStatusView
			if v, ok = m.SecurityStatusView(); ok {
				s.onStatus(v, sendingTime)
			}
		case simba.KindSecurityDefinitionUpdateReport:
			var u simba.SecurityDefinitionUpdateReport
			if u, ok = m.SecurityDefinitionUpdateReport(); ok {
				s.onTheorUpdate(u, sendingTime)
			}
		case simba.KindTradingSessionStatus:
			var t simba.TradingSessionStatus
			if t, ok = m.TradingSessionStatus(); ok {
				s.mu.Lock()
				s.stats.SessionStatuses++
				s.mu.Unlock()
				if s.cfg.OnSessionStatus != nil {
					s.cfg.OnSessionStatus(t)
				}
			}
		case simba.KindSecurityGroupStatus:
			s.mu.Lock()
			s.stats.GroupStatuses++
			s.mu.Unlock()
		case simba.KindHeartbeat:
		default:
			s.mu.Lock()
			s.stats.UnknownMessages++
			s.mu.Unlock()
		}
	}
}

// onSequenceReset closes a Replay cycle. The cache is complete when the
// finished cycle delivered TotNumReports definitions, or — when the feed
// is filtered (a broker relay, a capture cut to a few instruments) — when
// two consecutive cycles carried the same set of ids.
func (s *InstrumentSession) onSequenceReset() {
	s.mu.Lock()
	var complete bool
	if s.inCycle && len(s.cycleSeen) > 0 {
		s.stats.Cycles++
		metricInc(s.met.cycles)
		complete = s.totNum != 0 && uint32(len(s.cycleSeen)) >= s.totNum
		if !complete && s.prevCycle != nil && len(s.prevCycle) == len(s.cycleSeen) {
			complete = true
			for id := range s.cycleSeen {
				if _, ok := s.prevCycle[id]; !ok {
					complete = false
					break
				}
			}
		}
		s.prevCycle = s.cycleSeen
	}
	s.inCycle = true
	s.cycleSeen = make(map[int32]struct{}, len(s.cycleSeen))
	s.mu.Unlock()
	if complete {
		s.markReady()
	}
}

func (s *InstrumentSession) markReady() {
	if s.readyDone.CompareAndSwap(false, true) {
		s.mu.Lock()
		var n int = len(s.byID)
		s.mu.Unlock()
		close(s.ready)
		s.logger.Info("forts: instruments cache complete", moex.Int("instruments", int64(n)))
	}
}

func decimal2FromMantissa(m int64) decimal.Decimal {
	if m == simba.NullDecimalMantissa {
		return decimal.Zero
	}
	return decimal.New(m, -2)
}

func decimal5(m int64) decimal.Decimal {
	if m == simba.NullDecimalMantissa {
		return decimal.Zero
	}
	return decimalFromSIMBAMantissa(m)
}

func (s *InstrumentSession) onDefinition(v simba.SecurityDefinitionView, version uint16, sendingTime uint64) {
	var in Instrument = Instrument{
		SecurityID:               v.SecurityID(),
		Symbol:                   string(v.Symbol()),
		SecurityAltID:            string(v.SecurityAltID()),
		SecurityAltIDSource:      v.SecurityAltIDSource(),
		SecurityType:             string(v.SecurityType()),
		CFICode:                  string(v.CFICode()),
		StrikePrice:              decimal5(v.StrikePrice()),
		ContractMultiplier:       v.ContractMultiplier(),
		TradingStatus:            v.TradingStatus(),
		Currency:                 string(v.Currency()),
		MarketSegmentID:          v.MarketSegmentID(),
		TradingSessionID:         v.TradingSessionID(),
		ExchangeTradingSessionID: v.ExchangeTradingSessionID(),
		Volatility:               decimal5(v.Volatility()),
		HighLimitPx:              decimal5(v.HighLimitPx()),
		LowLimitPx:               decimal5(v.LowLimitPx()),
		MinPriceIncrement:        decimal5(v.MinPriceIncrement()),
		MinPriceIncrementAmount:  decimal5(v.MinPriceIncrementAmount()),
		InitialMarginOnBuy:       decimal2FromMantissa(v.InitialMarginOnBuy()),
		InitialMarginOnSell:      decimal2FromMantissa(v.InitialMarginOnSell()),
		InitialMarginSyntetic:    decimal2FromMantissa(v.InitialMarginSyntetic()),
		TheorPrice:               decimal5(v.TheorPrice()),
		TheorPriceLimit:          decimal5(v.TheorPriceLimit()),
		UnderlyingQty:            decimal5(v.UnderlyingQty()),
		UnderlyingCurrency:       string(v.UnderlyingCurrency()),
		Flags:                    v.Flags(),
		SettlPrice:               decimal5(v.SettlPrice()),
		TradeModeID:              v.TradeModeID(),
		GroupMask:                v.GroupMask(),
		SectionID:                v.SectionID(),
		BaseContractID:           v.BaseContractID(),
		TradePeriodAccess:        v.TradePeriodAccess(),
		Description:              string(v.SecurityDesc()),
		SchemaVersion:            version,
		UpdatedAtNs:              sendingTime,
	}
	if md := v.MaturityDate(); md != simba.NullUint32 {
		in.MaturityDate = md
	}
	for i := 0; i < v.UnderlyingsLen(); i++ {
		var e simba.UnderlyingEntry = v.Underlying(i)
		in.Underlyings = append(in.Underlyings, InstrumentUnderlying{Symbol: string(e.UnderlyingSymbol), Board: string(e.UnderlyingBoard), SecurityID: e.UnderlyingSecurityID, FutureID: e.UnderlyingFutureID})
	}
	for i := 0; i < v.LegsLen(); i++ {
		var e simba.LegEntry = v.Leg(i)
		in.Legs = append(in.Legs, InstrumentLeg{Symbol: string(e.LegSymbol), SecurityID: e.LegSecurityID, RatioQty: e.LegRatioQty})
	}
	for i := 0; i < v.EventsLen(); i++ {
		var e simba.EventEntry = v.Event(i)
		in.Events = append(in.Events, InstrumentEvent{Type: e.EventType, Date: e.EventDate, Time: e.EventTime})
	}

	s.mu.Lock()
	s.stats.Definitions++
	s.stats.TotNumReports = v.TotNumReports()
	s.totNum = v.TotNumReports()
	var stored *Instrument = &in
	if old, ok := s.byID[in.SecurityID]; ok && old.Symbol != in.Symbol {
		delete(s.bySymbol, old.Symbol)
	}
	s.byID[in.SecurityID] = stored
	s.bySymbol[in.Symbol] = in.SecurityID
	if s.inCycle {
		s.cycleSeen[in.SecurityID] = struct{}{}
	}
	var complete bool = s.inCycle && s.totNum != 0 && uint32(len(s.cycleSeen)) >= s.totNum
	var cp Instrument = in
	s.mu.Unlock()
	metricInc(s.met.definitions)
	if s.cfg.OnDefinition != nil {
		s.cfg.OnDefinition(&cp)
	}
	if complete {
		s.markReady()
	}
}

func (s *InstrumentSession) onStatus(v simba.SecurityStatusView, sendingTime uint64) {
	s.mu.Lock()
	var in *Instrument = s.byID[v.SecurityID()]
	if in == nil {
		in = &Instrument{SecurityID: v.SecurityID(), Symbol: string(v.Symbol())}
		s.byID[in.SecurityID] = in
		s.bySymbol[in.Symbol] = in.SecurityID
	}
	var prev simba.SecurityTradingStatus = in.TradingStatus
	if st := v.TradingStatus(); st != simba.TradingStatusNull {
		in.TradingStatus = st
	}
	if m := v.HighLimitPx(); m != simba.NullDecimalMantissa {
		in.HighLimitPx = decimal5(m)
	}
	if m := v.LowLimitPx(); m != simba.NullDecimalMantissa {
		in.LowLimitPx = decimal5(m)
	}
	if m := v.InitialMarginOnBuy(); m != simba.NullDecimalMantissa {
		in.InitialMarginOnBuy = decimal2FromMantissa(m)
	}
	if m := v.InitialMarginOnSell(); m != simba.NullDecimalMantissa {
		in.InitialMarginOnSell = decimal2FromMantissa(m)
	}
	if m := v.InitialMarginSyntetic(); m != simba.NullDecimalMantissa {
		in.InitialMarginSyntetic = decimal2FromMantissa(m)
	}
	in.UpdatedAtNs = sendingTime
	s.stats.StatusUpdates++
	var cp Instrument = *in
	s.mu.Unlock()
	metricInc(s.met.statusUpdates)
	if s.cfg.OnStatus != nil {
		s.cfg.OnStatus(&cp, prev)
	}
}

func (s *InstrumentSession) onTheorUpdate(u simba.SecurityDefinitionUpdateReport, sendingTime uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.TheorUpdate++
	var in *Instrument = s.byID[u.SecurityID]
	if in == nil {
		return
	}
	in.Volatility = decimal5(u.Volatility)
	in.TheorPrice = decimal5(u.TheorPrice)
	in.TheorPriceLimit = decimal5(u.TheorPriceLimit)
	in.UpdatedAtNs = sendingTime
}

// WatchInstruments starts an InstrumentSession on the client's SIMBA
// configuration in a background goroutine and feeds every definition
// into the client's Symbol <-> SecurityID cache (used by the TWIME order
// leg and ResolveSecurityID). cfg.SIMBA is taken from the client when
// empty. The session stops when ctx is done; its Run error is logged.
func (mc *MarketDataClient) WatchInstruments(ctx context.Context, cfg InstrumentSessionConfig) (*InstrumentSession, error) {
	if cfg.SIMBA.InstrumentsGroupA == "" {
		cfg.SIMBA = mc.c.cfg.SIMBA
	}
	if cfg.SIMBA.InstrumentsGroupA == "" {
		return nil, moex.NewError(moex.TransportSIMBA, moex.ErrorKindInvalidRequest, "", "forts: Config.SIMBA.InstrumentsGroupA is not set — cannot watch instruments without the Instruments multicast feed", nil)
	}
	if cfg.Logger == nil {
		cfg.Logger = mc.c.logger
	}
	if cfg.Metrics == nil {
		cfg.Metrics = mc.c.cfg.Metrics
	}
	var user = cfg.OnDefinition
	var c *Client = mc.c
	cfg.OnDefinition = func(in *Instrument) {
		c.rememberSecurityID(in.Symbol, in.SecurityID)
		if user != nil {
			user(in)
		}
	}
	var s *InstrumentSession = NewInstrumentSession(cfg)
	go func() {
		if err := s.Run(ctx); err != nil {
			c.logger.Error("forts: InstrumentSession stopped", moex.Err(err))
		}
	}()
	return s, nil
}
