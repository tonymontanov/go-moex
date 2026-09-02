/*
FILE: forts/stream.go

DESCRIPTION:
Market-data entry points of MarketDataClient over SIMBA SPECTRA:
ResolveSecurityID (symbol -> SecurityID via the Instruments feed) and
WatchOrderBook (channel adapter over BookSession for one instrument).
The synchronisation logic itself lives in booksession.go.

Requires exchange colocation — see moex.SIMBAConfig doc and
docs/handoff.md "Open questions".
*/
package forts

import (
	"context"
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	moex "github.com/tonymontanov/go-moex"
	"github.com/tonymontanov/go-moex/internal/simba"
	"github.com/tonymontanov/go-moex/orderbook"
	rootTypes "github.com/tonymontanov/go-moex/types"
)

// OrderBookUpdate — a snapshot-on-change notification delivered by
// WatchOrderBook. Levels are already converted to decimal.Decimal (the
// consumer should not need to know about the underlying Decimal5
// mantissa/exponent encoding). State tells whether the book is in sync
// with the exchange (BookLive) or being rebuilt (BookCollecting — Bids/
// Asks are empty then).
type OrderBookUpdate struct {
	Symbol string
	State  BookState
	Bids   []rootTypes.OrderBookLevel
	Asks   []rootTypes.OrderBookLevel
}

// NewBookSession returns a session bound to this client's SIMBA config
// and logger. Subscribe instruments, then Run.
func (mc *MarketDataClient) NewBookSession(onBook func(securityID int32, engine *orderbook.Engine), onState func(securityID int32, state BookState)) *BookSession {
	return NewBookSession(BookSessionConfig{
		SIMBA:   mc.c.cfg.SIMBA,
		OnBook:  onBook,
		OnState: onState,
		Logger:  mc.c.logger,
	})
}

// ResolveSecurityID maps a human symbol to the numeric SecurityID SIMBA
// keys on, by briefly listening to the "Instruments" multicast feed for a
// matching SecurityDefinition message (template 21 on schema version 8,
// 27 on version 9 — both handled). Requires
// Config.SIMBA.InstrumentsGroupA to be set; results are cached for the
// lifetime of the Client.
func (mc *MarketDataClient) ResolveSecurityID(ctx context.Context, symbol string) (int32, error) {
	mc.c.simbaMu.Lock()
	if id, ok := mc.c.symbolToSecurityID[symbol]; ok {
		mc.c.simbaMu.Unlock()
		return id, nil
	}
	mc.c.simbaMu.Unlock()

	var cfg moex.SIMBAConfig = mc.c.cfg.SIMBA
	if cfg.InstrumentsGroupA == "" {
		return 0, moex.NewError(moex.TransportSIMBA, moex.ErrorKindInvalidRequest, "", "forts: Config.SIMBA.InstrumentsGroupA is not set — cannot resolve SecurityID without colocation multicast access", nil)
	}

	var listener *simba.Listener
	var err error
	listener, err = simba.Listen(simba.ListenerConfig{
		GroupAddr: cfg.InstrumentsGroupA,
		Interface: cfg.NetworkInterface,
		Logger:    mc.c.logger,
	})
	if err != nil {
		return 0, moex.NewError(moex.TransportSIMBA, moex.ErrorKindNetwork, "", "forts: join Instruments multicast group", err)
	}
	defer listener.Close()

	var runCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var found int32 = -1
	var runErr error = listener.Run(runCtx, func(buf []byte) {
		var p simba.Packet
		var perr error
		p, perr = simba.ParsePacket(buf, 0)
		if perr != nil {
			return
		}
		for {
			var m simba.Message
			var ok bool
			m, ok, perr = p.Next()
			if perr != nil || !ok {
				return
			}
			var sd simba.SecurityDefinitionPrefix
			sd, ok = m.SecurityDefinition()
			if !ok {
				continue
			}
			var sym string = sd.SymbolString()
			mc.c.simbaMu.Lock()
			mc.c.symbolToSecurityID[sym] = sd.SecurityID
			mc.c.simbaMu.Unlock()
			if sym == symbol {
				found = sd.SecurityID
				cancel()
				return
			}
		}
	})
	_ = runErr // context deadline/cancel is the expected exit path, not an error to surface.

	if found < 0 {
		return 0, moex.NewError(moex.TransportSIMBA, moex.ErrorKindInvalidRequest, "", fmt.Sprintf("forts: SecurityID for %q not found on the Instruments feed within the timeout", symbol), nil)
	}
	return found, nil
}

// WatchOrderBook maintains the book of securityID (snapshot-synchronised
// via BookSession) and streams depth-limited updates until ctx is
// canceled. depth <= 0 streams the full book on every change. The channel
// is best-effort: a slow consumer drops intermediate updates, the next
// one still reflects the latest state.
func (mc *MarketDataClient) WatchOrderBook(ctx context.Context, securityID int32, symbolLabel string, depth int) (<-chan OrderBookUpdate, error) {
	var cfg moex.SIMBAConfig = mc.c.cfg.SIMBA
	if cfg.IncrementalGroupA == "" || cfg.SnapshotGroupA == "" {
		return nil, moex.NewError(moex.TransportSIMBA, moex.ErrorKindInvalidRequest, "", "forts: Config.SIMBA.IncrementalGroupA and SnapshotGroupA are not set — market data requires exchange colocation, see docs/handoff.md", nil)
	}

	var out chan OrderBookUpdate = make(chan OrderBookUpdate, 32)
	// Both callbacks run under the session mutex on the read goroutine:
	// track the state here instead of calling back into the session.
	var live bool
	var session *BookSession = mc.NewBookSession(func(id int32, engine *orderbook.Engine) {
		var update OrderBookUpdate = OrderBookUpdate{Symbol: symbolLabel, State: BookCollecting}
		if live {
			update.State = BookLive
			update.Bids = levelsToDecimal(engine.Levels(orderbook.SideBid, depth))
			update.Asks = levelsToDecimal(engine.Levels(orderbook.SideAsk, depth))
		}
		select {
		case out <- update:
		default:
		}
	}, func(id int32, state BookState) { live = state == BookLive })
	session.Subscribe(securityID)

	go func() {
		defer close(out)
		var err error = session.Run(ctx)
		if err != nil {
			mc.c.logger.Warn("forts: SIMBA book session stopped", moex.Err(err))
		}
	}()
	return out, nil
}

func sideFromMDEntryType(t simba.MDEntryType) orderbook.Side {
	if t == simba.MDEntryTypeBid {
		return orderbook.SideBid
	}
	return orderbook.SideAsk
}

func levelsToDecimal(levels []orderbook.Level) []rootTypes.OrderBookLevel {
	var out []rootTypes.OrderBookLevel = make([]rootTypes.OrderBookLevel, len(levels))
	for i, l := range levels {
		out[i] = rootTypes.OrderBookLevel{Price: decimalFromSIMBAMantissa(l.PriceMantissa), Size: decimal.NewFromInt(l.Size)}
	}
	return out
}
