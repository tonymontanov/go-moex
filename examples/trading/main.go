// Command trading demonstrates the FORTS order-entry flow: Connect,
// CreateOrder, watch its acknowledgement via WatchOpenOrders, then
// CancelOrder. The same code runs over TWIME (when MOEX_TWIME_ADDR is
// set) or FIX Gate (otherwise) — the TradingClient API does not change.
//
// Requires exchange/broker-issued credentials — will not run against a
// public endpoint. Environment:
//
//	MOEX_FORTS_SYMBOL        (default: "Si-12.26")
//
//	TWIME (preferred when set):
//	MOEX_TWIME_ADDR          host:port of the transactional gateway
//	MOEX_TWIME_LOGIN         TWIME login (Establish.Credentials)
//	MOEX_TWIME_ACCOUNT       7-symbol client account
//	MOEX_TWIME_SECURITY_ID   numeric SecurityID of MOEX_FORTS_SYMBOL (TWIME
//	                         addresses instruments by id; without the SIMBA
//	                         Instruments feed the id must be seeded by hand)
//
//	FIX Gate (fallback):
//	MOEX_FIX_HOST            (default: moex.DefaultFIXHostDSP)
//	MOEX_FIX_SENDER_COMP_ID  (required)
//	MOEX_FIX_ACCOUNT         (required — FORTS trading account code)
//
// Run:
//
//	MOEX_TWIME_ADDR=... MOEX_TWIME_LOGIN=... MOEX_TWIME_ACCOUNT=... MOEX_TWIME_SECURITY_ID=... go run ./examples/trading
//	MOEX_FIX_SENDER_COMP_ID=... MOEX_FIX_ACCOUNT=... go run ./examples/trading
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/shopspring/decimal"

	moex "github.com/tonymontanov/go-moex"
	"github.com/tonymontanov/go-moex/forts"
	"github.com/tonymontanov/go-moex/forts/types"
)

func main() {
	var symbol string = envOr("MOEX_FORTS_SYMBOL", "Si-12.26")
	var cfg moex.Config = moex.DefaultConfig()
	var account string
	var securityID int64

	if addr := os.Getenv("MOEX_TWIME_ADDR"); addr != "" {
		account = os.Getenv("MOEX_TWIME_ACCOUNT")
		var err error
		securityID, err = strconv.ParseInt(os.Getenv("MOEX_TWIME_SECURITY_ID"), 10, 32)
		if os.Getenv("MOEX_TWIME_LOGIN") == "" || account == "" || err != nil {
			log.Fatal("MOEX_TWIME_LOGIN, MOEX_TWIME_ACCOUNT and MOEX_TWIME_SECURITY_ID are required with MOEX_TWIME_ADDR")
		}
		cfg.TWIME.Addr = addr
		cfg.TWIME.Credentials = os.Getenv("MOEX_TWIME_LOGIN")
		cfg.TWIME.Account = account
	} else {
		var senderCompID string = os.Getenv("MOEX_FIX_SENDER_COMP_ID")
		account = os.Getenv("MOEX_FIX_ACCOUNT")
		if senderCompID == "" || account == "" {
			log.Fatal("set MOEX_TWIME_* or MOEX_FIX_SENDER_COMP_ID and MOEX_FIX_ACCOUNT (see file header)")
		}
		cfg.FIX.SenderCompID = senderCompID
		if host := os.Getenv("MOEX_FIX_HOST"); host != "" {
			cfg.FIX.Host = host
		}
	}

	var client, err = moex.NewClient(cfg)
	if err != nil {
		log.Fatalf("moex.NewClient: %v", err)
	}
	defer client.Close()

	var fortsClient *forts.Client = client.Forts().(*forts.Client)
	if securityID != 0 {
		fortsClient.SetSecurityID(symbol, int32(securityID))
	}

	var ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err = fortsClient.Connect(ctx); err != nil {
		log.Fatalf("Connect: %v", err)
	}
	log.Printf("connected over %s", fortsClient.OrderEntryTransport())

	var watchCtx, watchCancel = context.WithCancel(context.Background())
	defer watchCancel()
	var updates = fortsClient.Trading().WatchOpenOrders(watchCtx)
	go func() {
		for order := range updates {
			log.Printf("order update: id=%d status=%s leaves=%d cum=%d", order.OrderID, order.Status, order.LeavesQty, order.CumQty)
		}
	}()

	// FORTS has no Market OrdType — send a deliberately non-aggressive
	// limit far from the market so this demo order rests instead of
	// filling immediately (see CreateOrder doc).
	var info, cErr = fortsClient.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol:      symbol,
		Side:        types.SideBuy,
		Quantity:    1,
		Price:       decimal.NewFromInt(1), // intentionally far off-market; cancel below.
		Account:     account,
		TimeInForce: types.TimeInForceDay,
	})
	if cErr != nil {
		log.Fatalf("CreateOrder: %v", cErr)
	}
	log.Printf("order accepted: id=%d status=%s", info.OrderID, info.Status)

	time.Sleep(2 * time.Second)

	var cancelInfo, cancelErr = fortsClient.Trading().CancelOrder(ctx, types.CancelOrderRequest{
		OrderID:  info.OrderID,
		Symbol:   symbol,
		Side:     types.SideBuy,
		Quantity: 1,
	})
	if cancelErr != nil {
		log.Fatalf("CancelOrder: %v", cancelErr)
	}
	log.Printf("order canceled: id=%d status=%s", cancelInfo.OrderID, cancelInfo.Status)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
