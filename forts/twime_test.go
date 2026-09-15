package forts

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	moex "github.com/tonymontanov/go-moex"
	"github.com/tonymontanov/go-moex/forts/types"
	"github.com/tonymontanov/go-moex/internal/twime"
	"github.com/tonymontanov/go-moex/internal/twime/twimetest"
)

const (
	twSymbol     = "BTU6"
	twSecurityID = 305203
	twAccount    = "5000061"
)

func newTWIMEClient(t *testing.T, g *twimetest.Gateway, mutate func(*moex.Config)) *Client {
	t.Helper()
	cfg := moex.DefaultConfig()
	cfg.TWIME.Addr = g.Addr()
	cfg.TWIME.Credentials = "TEST"
	cfg.TWIME.Account = twAccount
	cfg.TWIME.KeepaliveInterval = time.Second
	if mutate != nil {
		mutate(&cfg)
	}
	root, err := moex.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(root)
	c.SetSecurityID(twSymbol, twSecurityID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	g.Expect(twime.TemplateEstablish, time.Second)
	return c
}

func nextUpdate(t *testing.T, ch <-chan *types.OrderInfo) *types.OrderInfo {
	t.Helper()
	select {
	case o := <-ch:
		return o
	case <-time.After(2 * time.Second):
		t.Fatal("no order update")
		return nil
	}
}

func TestTWIMECreateFillCancel(t *testing.T) {
	g := twimetest.New(t)
	c := newTWIMEClient(t, g, nil)
	if c.OrderEntryTransport() != moex.TransportTWIME {
		t.Fatalf("transport %v", c.OrderEntryTransport())
	}
	ctx := context.Background()
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	updates := c.Trading().WatchOpenOrders(watchCtx)

	info, err := c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideBuy, Price: decimal.RequireFromString("33191.5"), Quantity: 5,
	})
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	sent := g.Expect(twime.TemplateNewOrderSingle, time.Second)
	if twimetest.ClOrdID(sent) == 0 || sent.Body[40] != '5' {
		t.Fatalf("wire NewOrderSingle: clordid %d account %q", twimetest.ClOrdID(sent), sent.Body[40:47])
	}
	if info.Status != types.OrdStatusNew || info.Symbol != twSymbol || info.Quantity != 5 || info.LeavesQty != 5 ||
		info.Account != twAccount || !info.Price.Equal(decimal.RequireFromString("33191.5")) || info.Side != types.SideBuy {
		t.Fatalf("OrderInfo %+v", info)
	}
	if u := nextUpdate(t, updates); u.OrderID != info.OrderID || u.Status != types.OrdStatusNew {
		t.Fatalf("update %+v", u)
	}
	if got := c.Trading().GetOpenOrders(twSymbol); len(got) != 1 || got[0].OrderID != info.OrderID {
		t.Fatalf("open orders %+v", got)
	}

	// Partial fill from the gateway.
	g.Push(twime.ExecutionSingleReport{ClOrdID: twime.NullUint64, OrderID: info.OrderID, TrdMatchID: 1,
		Flags: twime.FlagPassiveSide, LastPx: 3319150000, LastQty: 2, OrderQty: 3, SecurityID: twSecurityID,
		Side: twime.SideBuy, Timestamp: twime.TimestampOf(time.Now())})
	u := nextUpdate(t, updates)
	if u.Status != types.OrdStatusPartiallyFilled || u.CumQty != 2 || u.LeavesQty != 3 || !u.AvgPx.Equal(decimal.RequireFromString("33191.5")) {
		t.Fatalf("after fill %+v", u)
	}
	if pos := c.Account().GetSymbolPosition(twSymbol); pos == nil || pos.Quantity != 2 {
		t.Fatalf("position %+v", pos)
	}

	// Cancel the remainder by OrderID.
	canceled, err := c.Trading().CancelOrder(ctx, types.CancelOrderRequest{OrderID: info.OrderID})
	if err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	cr := g.Expect(twime.TemplateOrderCancelRequest, time.Second)
	if int64(cr.Body[8]) != info.OrderID&0xff {
		t.Fatalf("cancel wire OrderID byte %d", cr.Body[8])
	}
	if canceled.Status != types.OrdStatusCanceled || canceled.LeavesQty != 0 || canceled.CumQty != 2 {
		t.Fatalf("canceled %+v", canceled)
	}
	nextUpdate(t, updates)
	if got := c.Trading().GetOpenOrders(""); len(got) != 0 {
		t.Fatalf("open orders after cancel %+v", got)
	}
}

func TestTWIMECancelByOrigClientOrderID(t *testing.T) {
	g := twimetest.New(t)
	c := newTWIMEClient(t, g, nil)
	ctx := context.Background()
	info, err := c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideSell, Price: decimal.NewFromInt(1), Quantity: 1, ClientOrderID: "4242",
	})
	if err != nil {
		t.Fatal(err)
	}
	if info.ClientOrderID != "4242" {
		t.Fatalf("ClientOrderID %q", info.ClientOrderID)
	}
	canceled, err := c.Trading().CancelOrder(ctx, types.CancelOrderRequest{OrigClientOrderID: "4242"})
	if err != nil {
		t.Fatalf("CancelOrder by OrigClientOrderID: %v", err)
	}
	if canceled.OrderID != info.OrderID || canceled.Status != types.OrdStatusCanceled {
		t.Fatalf("canceled %+v", canceled)
	}
	if _, err := c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideSell, Price: decimal.NewFromInt(1), Quantity: 1, ClientOrderID: "GM1",
	}); err == nil || !moex.IsInvalidRequest(err) {
		t.Fatalf("non-numeric ClientOrderID accepted: %v", err)
	}
}

func TestTWIMEModify(t *testing.T) {
	g := twimetest.New(t)
	c := newTWIMEClient(t, g, nil)
	ctx := context.Background()
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	updates := c.Trading().WatchOpenOrders(watchCtx)
	info, err := c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideBuy, Price: decimal.NewFromInt(100), Quantity: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	g.Expect(twime.TemplateNewOrderSingle, time.Second)
	nextUpdate(t, updates)
	replaced, err := c.Trading().ModifyOrder(ctx, types.ModifyOrderRequest{
		OrderID: info.OrderID, NewPrice: decimal.NewFromInt(101), NewQuantity: 9,
	})
	if err != nil {
		t.Fatalf("ModifyOrder: %v", err)
	}
	rr := g.Expect(twime.TemplateOrderReplaceRequest, time.Second)
	if rr.Body[37] != uint8(twime.ReplaceModeChangeOrderQty) {
		t.Fatalf("replace Mode on the wire %d", rr.Body[37])
	}
	if replaced.OrderID != info.OrderID+1 || replaced.Status != types.OrdStatusNew || replaced.Quantity != 9 ||
		!replaced.Price.Equal(decimal.NewFromInt(101)) || replaced.Symbol != twSymbol || replaced.Side != types.SideBuy {
		t.Fatalf("replaced %+v", replaced)
	}
	old := nextUpdate(t, updates)
	if old.OrderID != info.OrderID || old.Status != types.OrdStatusCanceled {
		t.Fatalf("old order update %+v", old)
	}
	if nu := nextUpdate(t, updates); nu.OrderID != replaced.OrderID {
		t.Fatalf("new order update %+v", nu)
	}
	if got := c.Trading().GetOpenOrders(""); len(got) != 1 || got[0].OrderID != replaced.OrderID {
		t.Fatalf("open orders %+v", got)
	}
}

func TestTWIMECancelAll(t *testing.T) {
	g := twimetest.New(t)
	c := newTWIMEClient(t, g, nil)
	ctx := context.Background()
	if err := c.Trading().CancelAllOrders(ctx, twSymbol, types.SideBuy, ""); err != nil {
		t.Fatalf("CancelAllOrders: %v", err)
	}
	f := g.Expect(twime.TemplateOrderMassCancelRequest, time.Second)
	if f.Body[17] != uint8(twime.SideBuy) || f.Body[16] != 0b111 {
		t.Fatalf("mass cancel wire side %d type %b", f.Body[17], f.Body[16])
	}
	if err := c.Trading().CancelAllOrders(ctx, "", "", ""); err != nil {
		t.Fatalf("CancelAllOrders all: %v", err)
	}
	f = g.Expect(twime.TemplateOrderMassCancelRequest, time.Second)
	if f.Body[17] != uint8(twime.SideAllOrders) {
		t.Fatalf("mass cancel all side %d", f.Body[17])
	}
}

func TestTWIMERejectsBecomeErrors(t *testing.T) {
	g := twimetest.New(t)
	c := newTWIMEClient(t, g, nil)
	ctx := context.Background()
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	updates := c.Trading().WatchOpenOrders(watchCtx)

	g.SetScript(func(g *twimetest.Gateway, f twime.Frame) bool {
		if f.Template() != twime.TemplateNewOrderSingle {
			return false
		}
		g.Write(twime.BusinessMessageReject{ClOrdID: twimetest.ClOrdID(f), OrdRejReason: 4103}.Append(nil))
		return true
	})
	_, err := c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideBuy, Price: decimal.NewFromInt(1), Quantity: 1,
	})
	var me *moex.Error
	if !errors.As(err, &me) || me.Transport != moex.TransportTWIME || me.Code != "4103" || !moex.IsExchange(err) {
		t.Fatalf("business reject: %v", err)
	}
	if u := nextUpdate(t, updates); u.Status != types.OrdStatusRejected || u.Symbol != twSymbol {
		t.Fatalf("rejected update %+v", u)
	}

	g.SetScript(func(g *twimetest.Gateway, f twime.Frame) bool {
		if f.Template() != twime.TemplateNewOrderSingle {
			return false
		}
		g.Write(twime.SessionReject{ClOrdID: twimetest.ClOrdID(f), RefTagID: 44, Reason: twime.SessionRejectValueIsIncorrect}.Append(nil))
		return true
	})
	_, err = c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideBuy, Price: decimal.NewFromInt(1), Quantity: 1,
	})
	if !moex.IsInvalidRequest(err) {
		t.Fatalf("session reject: %v", err)
	}
	nextUpdate(t, updates)

	g.SetScript(nil)
	g.SetFloodEach(1)
	_, err = c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideBuy, Price: decimal.NewFromInt(1), Quantity: 1,
	})
	if !moex.IsRateLimit(err) {
		t.Fatalf("flood reject: %v", err)
	}
}

func TestTWIMEValidation(t *testing.T) {
	g := twimetest.New(t)
	c := newTWIMEClient(t, g, func(cfg *moex.Config) { cfg.TWIME.Account = "" })
	ctx := context.Background()
	_, err := c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: "UNKNOWN", Side: types.SideBuy, Price: decimal.NewFromInt(1), Quantity: 1, Account: twAccount,
	})
	if !moex.IsInvalidRequest(err) {
		t.Fatalf("unknown symbol: %v", err)
	}
	_, err = c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideBuy, Price: decimal.NewFromInt(1), Quantity: 1,
	})
	if !moex.IsInvalidRequest(err) {
		t.Fatalf("missing account: %v", err)
	}
	_, err = c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideBuy, Price: decimal.NewFromInt(1), Quantity: 1, Account: twAccount,
		TimeInForce: types.TimeInForceGTD, ExpireDateYYYYMMDD: "2026-09-18",
	})
	if !moex.IsInvalidRequest(err) {
		t.Fatalf("bad expire date: %v", err)
	}
	info, err := c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideBuy, Price: decimal.NewFromInt(1), Quantity: 1, Account: twAccount,
		TimeInForce: types.TimeInForceGTD, ExpireDateYYYYMMDD: "20260918",
	})
	if err != nil {
		t.Fatalf("GTD order: %v", err)
	}
	f := g.Expect(twime.TemplateNewOrderSingle, time.Second)
	if f.Body[37] != uint8(twime.TimeInForceGTD) || f.Body[8] == 0xff {
		t.Fatalf("GTD on the wire: tif %d expire %x", f.Body[37], f.Body[8:16])
	}
	_ = info
}

func TestTWIMEEmptyBookDropsOpenOrders(t *testing.T) {
	g := twimetest.New(t)
	c := newTWIMEClient(t, g, nil)
	ctx := context.Background()
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	updates := c.Trading().WatchOpenOrders(watchCtx)
	if _, err := c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideBuy, Price: decimal.NewFromInt(1), Quantity: 1,
	}); err != nil {
		t.Fatal(err)
	}
	nextUpdate(t, updates)
	g.Push(twime.EmptyBook{TradingSessionID: 6543, Timestamp: twime.TimestampOf(time.Now())})
	if u := nextUpdate(t, updates); u.Status != types.OrdStatusCanceled {
		t.Fatalf("after EmptyBook %+v", u)
	}
	if got := c.Trading().GetOpenOrders(""); len(got) != 0 {
		t.Fatalf("open orders after EmptyBook %+v", got)
	}
}

func TestTWIMESessionLossFailsInFlightAndReconnects(t *testing.T) {
	g := twimetest.New(t)
	c := newTWIMEClient(t, g, nil)
	ctx := context.Background()

	// A request whose reply never comes; the gateway drops the connection.
	g.SetScript(func(g *twimetest.Gateway, f twime.Frame) bool {
		if f.Template() == twime.TemplateNewOrderSingle {
			go func() { time.Sleep(50 * time.Millisecond); g.Drop() }()
			return true
		}
		return false
	})
	_, err := c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideBuy, Price: decimal.NewFromInt(1), Quantity: 1,
	})
	if !moex.IsNetwork(err) {
		t.Fatalf("in-flight call after drop: %v", err)
	}
	g.Expect(twime.TemplateNewOrderSingle, time.Second)
	<-g.Closed()
	if _, err := c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideBuy, Price: decimal.NewFromInt(1), Quantity: 1,
	}); !moex.IsInvalidRequest(err) {
		t.Fatalf("call while disconnected: %v", err)
	}

	// Reconnect recovers what the gateway logged meanwhile.
	g.SetScript(nil)
	g.Record(twime.SystemEvent{EventID: 1, TradSesEvent: twime.TradSesEventSessionDataReady})
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Connect(cctx); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	g.Expect(twime.TemplateEstablish, time.Second)
	g.Expect(twime.TemplateRetransmitRequest, 2*time.Second)
	info, err := c.Trading().CreateOrder(ctx, types.CreateOrderRequest{
		Symbol: twSymbol, Side: types.SideBuy, Price: decimal.NewFromInt(1), Quantity: 1,
	})
	if err != nil || info.Status != types.OrdStatusNew {
		t.Fatalf("after reconnect: %+v %v", info, err)
	}
}

func TestTWIMEConfigValidation(t *testing.T) {
	cfg := moex.DefaultConfig()
	cfg.TWIME.Addr = "127.0.0.1:1"
	if _, err := moex.NewClient(cfg); !moex.IsInvalidRequest(err) {
		t.Fatalf("TWIME without credentials: %v", err)
	}
	cfg.TWIME.Credentials = "TEST"
	cfg.TWIME.Account = "12345678"
	if _, err := moex.NewClient(cfg); !moex.IsInvalidRequest(err) {
		t.Fatalf("8-char account: %v", err)
	}
}
