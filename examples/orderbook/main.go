// Command orderbook demonstrates the SIMBA SPECTRA order-book stream:
// resolve a human symbol to its numeric SecurityID via the Instruments
// feed, then watch order book updates.
//
// REQUIRES EXCHANGE COLOCATION — SIMBA multicast groups are only reachable
// from MOEX's own network segment or a broker-provided low-latency
// circuit (see docs/handoff.md "Open questions"). Outside of that
// environment this will simply time out with no data, which is expected.
//
// Set the environment variables below (issued per-client by MOEX/broker):
//
//	MOEX_SIMBA_INSTRUMENTS_GROUP_A  e.g. "239.195.1.1:15000"
//	MOEX_SIMBA_INCREMENTAL_GROUP_A  e.g. "239.195.2.1:15000"
//	MOEX_SIMBA_INTERFACE            local NIC name, e.g. "eth0"
//	MOEX_FORTS_SYMBOL               default: "Si-12.26"
//
// Run:
//
//	MOEX_SIMBA_INSTRUMENTS_GROUP_A=... MOEX_SIMBA_INCREMENTAL_GROUP_A=... go run ./examples/orderbook
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	moex "github.com/tonymontanov/go-moex"
	"github.com/tonymontanov/go-moex/forts"
)

func main() {
	// Production "primary" (A) groups from configuration.xml on
	// ftp.moex.com/pub/SIMBA/Spectra/prod/primary/ — override for B/test.
	var instrumentsGroup string = envOr("MOEX_SIMBA_INSTRUMENTS_GROUP_A", "239.195.20.83:20083")
	var incrementalGroup string = envOr("MOEX_SIMBA_INCREMENTAL_GROUP_A", "239.195.20.81:20081")
	var snapshotGroup string = envOr("MOEX_SIMBA_SNAPSHOT_GROUP_A", "239.195.20.82:20082")
	var symbol string = envOr("MOEX_FORTS_SYMBOL", "Si-12.26")

	var cfg moex.Config = moex.DefaultConfig()
	cfg.SIMBA.InstrumentsGroupA = instrumentsGroup
	cfg.SIMBA.IncrementalGroupA = incrementalGroup
	cfg.SIMBA.SnapshotGroupA = snapshotGroup
	cfg.SIMBA.NetworkInterface = os.Getenv("MOEX_SIMBA_INTERFACE")

	var client, err = moex.NewClient(cfg)
	if err != nil {
		log.Fatalf("moex.NewClient: %v", err)
	}
	defer client.Close()

	var fortsClient *forts.Client = client.Forts().(*forts.Client)

	var ctx, cancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var resolveCtx, resolveCancel = context.WithTimeout(ctx, 30*time.Second)
	defer resolveCancel()
	var securityID int32
	securityID, err = fortsClient.MarketData().ResolveSecurityID(resolveCtx, symbol)
	if err != nil {
		log.Fatalf("ResolveSecurityID(%s): %v", symbol, err)
	}
	log.Printf("%s resolved to SecurityID=%d", symbol, securityID)

	var updates, watchErr = fortsClient.MarketData().WatchOrderBook(ctx, securityID, symbol, 5)
	if watchErr != nil {
		log.Fatalf("WatchOrderBook: %v", watchErr)
	}

	log.Println("streaming order book updates, press Ctrl+C to stop")
	for update := range updates {
		if len(update.Bids) == 0 && len(update.Asks) == 0 {
			continue
		}
		var bestBid, bestAsk string = "-", "-"
		if len(update.Bids) > 0 {
			bestBid = update.Bids[0].Price.String()
		}
		if len(update.Asks) > 0 {
			bestAsk = update.Asks[0].Price.String()
		}
		log.Printf("%s: bestBid=%s bestAsk=%s (%d/%d levels)", update.Symbol, bestBid, bestAsk, len(update.Bids), len(update.Asks))
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
