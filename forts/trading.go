/*
FILE: forts/trading.go

DESCRIPTION:
Order entry via FIX Gate: CreateOrder/CancelOrder/ModifyOrder (New Order
Single / Order Cancel Request / Order Cancel-Replace Request, §4.1.1/
4.1.2/4.1.4), GetOpenOrders (from the local cache built off Execution
Reports — FIX Gate has no "list open orders" query, see
forts/types/order-info.go and the Order Status Request note below), and
WatchOpenOrders (fan-out of every Execution Report as it arrives).

Order Status Request (§4.1.5) exists on the wire and is wired up as
QueryOrderStatus for completeness, but GetOpenOrders does NOT use it in a
loop (there is no "list all my orders" FIX message) — it only works for an
order whose OrderID/ClOrdID you already know.
*/
package forts

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
	moex "github.com/tonymontanov/go-moex"
	"github.com/tonymontanov/go-moex/forts/types"
	"github.com/tonymontanov/go-moex/internal/fix"
)

// TradingClient — FORTS order entry.
type TradingClient struct{ c *Client }

// massCancelResult — outcome of an Order Mass Cancel Request (§4.1.3/4.1.8).
type massCancelResult struct {
	Accepted            bool
	TotalAffectedOrders int64
	RejectReasonText    string
}

// CreateOrder sends a New Order Single and waits for the first Execution
// Report (ExecType=New or Rejected) — or, on TWIME, for
// NewOrderSingleResponse / BusinessMessageReject. Further fills are
// delivered via WatchOpenOrders / GetOpenOrders, not this call's return
// value.
func (tc *TradingClient) CreateOrder(ctx context.Context, req types.CreateOrderRequest) (*types.OrderInfo, error) {
	if req.Quantity <= 0 {
		return nil, moex.NewError(tc.c.orderEntryTransport(), moex.ErrorKindInvalidRequest, "", "forts: CreateOrderRequest.Quantity must be > 0", nil)
	}
	if req.Price.IsZero() {
		return nil, moex.NewError(tc.c.orderEntryTransport(), moex.ErrorKindInvalidRequest, "", "forts: CreateOrderRequest.Price is required — FORTS has no Market OrdType, send an aggressive limit price with TimeInForceIOC/FOK instead", nil)
	}
	if leg := tc.c.twime(); leg != nil {
		return leg.createOrder(ctx, req)
	}

	var session *fix.Session
	var err error
	session, err = tc.c.session()
	if err != nil {
		return nil, err
	}

	var clOrdID string = req.ClientOrderID
	if clOrdID == "" {
		clOrdID = tc.c.clOrdIDGen.Next()
	}
	var tif types.TimeInForce = req.TimeInForce
	if tif == "" {
		tif = types.TimeInForceDay
	}

	var msg *fix.Message = fix.NewMessage(fix.MsgTypeNewOrderSingle)
	msg.SetStr(fix.TagClOrdID, clOrdID)
	msg.SetStr(fix.TagOrdType, string(types.OrdTypeLimit))
	msg.SetStr(fix.TagSymbol, req.Symbol)
	msg.SetStr(fix.TagAccount, req.Account)
	msg.SetStr(fix.TagTimeInForce, string(tif))
	msg.SetStr(fix.TagSide, string(req.Side))
	msg.SetInt(fix.TagOrderQty, req.Quantity)
	msg.SetStr(fix.TagPrice, req.Price.StringFixed(5))
	msg.SetStr(fix.TagTransactTime, fixNowTimestamp())
	if tif == types.TimeInForceGTD {
		msg.SetStr(fix.TagExpireDate, req.ExpireDateYYYYMMDD)
	}

	var ch chan result[*types.OrderInfo] = tc.c.orderCorrelator.Register(clOrdID)
	if _, err = session.SendApp(msg); err != nil {
		tc.c.orderCorrelator.Resolve(clOrdID, nil, nil) // best-effort cleanup of the registration.
		return nil, moex.NewError(moex.TransportFIX, moex.ErrorKindNetwork, "", "forts: send New Order Single", err)
	}
	return tc.c.orderCorrelator.Wait(ctx, clOrdID, ch)
}

// CancelOrder sends an Order Cancel Request and waits for the resulting
// Execution Report (ExecType=Canceled) or Order Cancel Reject.
func (tc *TradingClient) CancelOrder(ctx context.Context, req types.CancelOrderRequest) (*types.OrderInfo, error) {
	if leg := tc.c.twime(); leg != nil {
		return leg.cancelOrder(ctx, req)
	}
	var session *fix.Session
	var err error
	session, err = tc.c.session()
	if err != nil {
		return nil, err
	}
	if req.OrderID == 0 && req.OrigClientOrderID == "" {
		return nil, moex.NewError(moex.TransportFIX, moex.ErrorKindInvalidRequest, "", "forts: CancelOrderRequest requires OrderID or OrigClientOrderID", nil)
	}

	var clOrdID string = req.ClientOrderID
	if clOrdID == "" {
		clOrdID = tc.c.clOrdIDGen.Next()
	}

	var msg *fix.Message = fix.NewMessage(fix.MsgTypeOrderCancelRequest)
	msg.SetStr(fix.TagClOrdID, clOrdID)
	if req.OrderID != 0 {
		msg.SetInt(fix.TagOrderID, req.OrderID)
	}
	if req.OrigClientOrderID != "" {
		msg.SetStr(fix.TagOrigClOrdID, req.OrigClientOrderID)
	}
	msg.SetStr(fix.TagSymbol, req.Symbol)
	msg.SetStr(fix.TagSide, string(req.Side))
	msg.SetInt(fix.TagOrderQty, req.Quantity)
	msg.SetStr(fix.TagTransactTime, fixNowTimestamp())

	var ch chan result[*types.OrderInfo] = tc.c.cancelCorrelator.Register(clOrdID)
	if _, err = session.SendApp(msg); err != nil {
		tc.c.cancelCorrelator.Resolve(clOrdID, nil, nil)
		return nil, moex.NewError(moex.TransportFIX, moex.ErrorKindNetwork, "", "forts: send Order Cancel Request", err)
	}
	return tc.c.cancelCorrelator.Wait(ctx, clOrdID, ch)
}

// ModifyOrder sends an Order Cancel/Replace Request (price/quantity only —
// FORTS does not support side/symbol replacement).
func (tc *TradingClient) ModifyOrder(ctx context.Context, req types.ModifyOrderRequest) (*types.OrderInfo, error) {
	if leg := tc.c.twime(); leg != nil {
		return leg.modifyOrder(ctx, req)
	}
	var session *fix.Session
	var err error
	session, err = tc.c.session()
	if err != nil {
		return nil, err
	}
	if req.OrderID == 0 && req.OrigClientOrderID == "" {
		return nil, moex.NewError(moex.TransportFIX, moex.ErrorKindInvalidRequest, "", "forts: ModifyOrderRequest requires OrderID or OrigClientOrderID", nil)
	}

	var clOrdID string = req.ClientOrderID
	if clOrdID == "" {
		clOrdID = tc.c.clOrdIDGen.Next()
	}

	var msg *fix.Message = fix.NewMessage(fix.MsgTypeOrderCancelReplace)
	msg.SetStr(fix.TagClOrdID, clOrdID)
	if req.OrderID != 0 {
		msg.SetInt(fix.TagOrderID, req.OrderID)
	}
	if req.OrigClientOrderID != "" {
		msg.SetStr(fix.TagOrigClOrdID, req.OrigClientOrderID)
	}
	msg.SetInt(fix.TagOrderQty, req.NewQuantity)
	msg.SetStr(fix.TagPrice, req.NewPrice.StringFixed(5))
	msg.SetStr(fix.TagSymbol, req.Symbol)
	msg.SetStr(fix.TagSide, string(req.Side))
	msg.SetStr(fix.TagTransactTime, fixNowTimestamp())

	var ch chan result[*types.OrderInfo] = tc.c.orderCorrelator.Register(clOrdID)
	if _, err = session.SendApp(msg); err != nil {
		tc.c.orderCorrelator.Resolve(clOrdID, nil, nil)
		return nil, moex.NewError(moex.TransportFIX, moex.ErrorKindNetwork, "", "forts: send Order Cancel/Replace Request", err)
	}
	return tc.c.orderCorrelator.Wait(ctx, clOrdID, ch)
}

// CancelAllOrders sends an Order Mass Cancel Request (§4.1.3) scoped by
// symbol/side/account as provided (zero values mean "all", matching the
// FIX Gate default semantics — see fields.go doc on MassCancelRequestType).
func (tc *TradingClient) CancelAllOrders(ctx context.Context, symbol string, side types.Side, account string) error {
	if leg := tc.c.twime(); leg != nil {
		return leg.cancelAll(ctx, symbol, side, account)
	}
	var session *fix.Session
	var err error
	session, err = tc.c.session()
	if err != nil {
		return err
	}

	var clOrdID string = tc.c.clOrdIDGen.Next()
	var msg *fix.Message = fix.NewMessage(fix.MsgTypeOrderMassCancelReq)
	msg.SetStr(fix.TagClOrdID, clOrdID)
	if symbol != "" {
		msg.SetStr(fix.TagMassCancelReqType, "1") // cancel by instrument.
		msg.SetStr(fix.TagSymbol, symbol)
	} else {
		msg.SetStr(fix.TagMassCancelReqType, "7") // cancel all orders for the trading member (generic).
	}
	if side != "" {
		msg.SetStr(fix.TagSide, string(side))
	}
	if account != "" {
		msg.SetStr(fix.TagAccount, account)
	}
	msg.SetStr(fix.TagTransactTime, fixNowTimestamp())

	var ch chan result[massCancelResult] = tc.c.massCancelCorrelator.Register(clOrdID)
	if _, err = session.SendApp(msg); err != nil {
		tc.c.massCancelCorrelator.Resolve(clOrdID, massCancelResult{}, nil)
		return moex.NewError(moex.TransportFIX, moex.ErrorKindNetwork, "", "forts: send Order Mass Cancel Request", err)
	}
	var res massCancelResult
	res, err = tc.c.massCancelCorrelator.Wait(ctx, clOrdID, ch)
	if err != nil {
		return err
	}
	if !res.Accepted {
		return moex.NewError(moex.TransportFIX, moex.ErrorKindExchange, "", fmt.Sprintf("forts: Order Mass Cancel rejected: %s", res.RejectReasonText), nil)
	}
	return nil
}

// GetOpenOrders returns the local snapshot of tracked orders. FIX Gate has
// no "list all open orders" request — this reflects every Execution Report
// observed since Connect(). Call it AFTER Connect, before assuming it is
// complete for orders placed in a previous process lifetime (see
// forts/types/position-info.go for the analogous, more severe caveat on
// positions).
func (tc *TradingClient) GetOpenOrders(symbol string) []*types.OrderInfo {
	tc.c.openOrdersMu.RLock()
	defer tc.c.openOrdersMu.RUnlock()
	var out []*types.OrderInfo
	for _, o := range tc.c.openOrders {
		if o.Status == types.OrdStatusFilled || o.Status == types.OrdStatusCanceled || o.Status == types.OrdStatusRejected {
			continue
		}
		if symbol != "" && o.Symbol != symbol {
			continue
		}
		out = append(out, o)
	}
	return out
}

// WatchOpenOrders returns a channel that receives every OrderInfo update
// (new/filled/canceled/rejected) as Execution Reports arrive. The channel
// is closed when ctx is done; callers must drain it to avoid blocking the
// FIX read loop (buffered, size 64 — a slow consumer drops the connection's
// throughput, not correctness, since GetOpenOrders always reflects the
// latest state regardless of channel backpressure).
func (tc *TradingClient) WatchOpenOrders(ctx context.Context) <-chan *types.OrderInfo {
	var ch chan *types.OrderInfo = make(chan *types.OrderInfo, 64)
	tc.c.orderWatchMu.Lock()
	tc.c.orderWatchers = append(tc.c.orderWatchers, ch)
	tc.c.orderWatchMu.Unlock()

	go func() {
		<-ctx.Done()
		tc.c.orderWatchMu.Lock()
		for i, w := range tc.c.orderWatchers {
			if w == ch {
				tc.c.orderWatchers = append(tc.c.orderWatchers[:i], tc.c.orderWatchers[i+1:]...)
				break
			}
		}
		tc.c.orderWatchMu.Unlock()
		close(ch)
	}()
	return ch
}

// handleFIXAppMessage is the fix.Session AppHandler — dispatches Execution
// Report / Order Cancel Reject / Order Mass Cancel Report to the correlator
// waiting on the message's ClOrdID (if any), updates the open-orders cache,
// feeds fills into the position tracker, and fans out to WatchOpenOrders
// subscribers. Runs on the FIX session's readLoop goroutine — must stay
// fast (see fix.AppHandler doc).
func (c *Client) handleFIXAppMessage(msg *fix.Message) {
	switch msg.MsgType() {
	case fix.MsgTypeExecutionReport:
		c.handleExecutionReport(msg)
	case fix.MsgTypeOrderCancelReject:
		c.handleOrderCancelReject(msg)
	case fix.MsgTypeOrderMassCancelRept:
		c.handleOrderMassCancelReport(msg)
	default:
		c.logger.Warn("forts: unhandled FIX application message", moex.Str("msg_type", msg.MsgType()))
	}
}

func (c *Client) handleExecutionReport(msg *fix.Message) {
	var info *types.OrderInfo = orderInfoFromExecutionReport(msg)

	c.openOrdersMu.Lock()
	c.openOrders[info.OrderID] = info
	if info.ClientOrderID != "" {
		c.clOrdToOrder[info.ClientOrderID] = info.OrderID
	}
	c.openOrdersMu.Unlock()

	if execType, _ := msg.GetStr(fix.TagExecType); execType == string(types.ExecTypeTrade) {
		c.positions.ApplyFill(info.Symbol, info.Side, tradeQtyOf(msg), tradePxOf(msg), info.TransactTimeMs)
	}

	var clOrdID, _ = msg.GetStr(fix.TagClOrdID)
	var execType, _ = msg.GetStr(fix.TagExecType)
	switch execType {
	case string(types.ExecTypeCanceled):
		c.cancelCorrelator.Resolve(clOrdID, info, nil)
	default:
		c.orderCorrelator.Resolve(clOrdID, info, nil)
	}

	c.fanOutOrder(info)
}

func (c *Client) handleOrderCancelReject(msg *fix.Message) {
	var clOrdID, _ = msg.GetStr(fix.TagClOrdID)
	var reasonStr, _ = msg.GetStr(fix.TagCxlRejReason)
	var reasonCode int64
	reasonCode, _ = strconv.ParseInt(reasonStr, 10, 64)
	var text, _ = msg.GetStr(fix.TagText)

	var kind moex.ErrorKind = moex.MapFIXCxlRejReason(int(reasonCode))
	var err error = moex.NewError(moex.TransportFIX, kind, reasonStr, fmt.Sprintf("forts: Order Cancel Reject: %s", text), nil)

	if !c.cancelCorrelator.Resolve(clOrdID, nil, err) {
		c.orderCorrelator.Resolve(clOrdID, nil, err)
	}
}

func (c *Client) handleOrderMassCancelReport(msg *fix.Message) {
	var clOrdID, _ = msg.GetStr(fix.TagClOrdID)
	var response, _ = msg.GetStr(fix.TagMassCancelResponse)
	var total, _ = msg.GetInt(fix.TagTotalAffectedOrders)
	var text, _ = msg.GetStr(fix.TagText)

	c.massCancelCorrelator.Resolve(clOrdID, massCancelResult{
		Accepted:            response != "0",
		TotalAffectedOrders: total,
		RejectReasonText:    text,
	}, nil)
}

func (c *Client) fanOutOrder(info *types.OrderInfo) {
	c.orderWatchMu.Lock()
	defer c.orderWatchMu.Unlock()
	for _, ch := range c.orderWatchers {
		select {
		case ch <- info:
		default:
			c.logger.Warn("forts: WatchOpenOrders subscriber is too slow, dropping update", moex.Int("order_id", info.OrderID))
		}
	}
}

func orderInfoFromExecutionReport(msg *fix.Message) *types.OrderInfo {
	var info types.OrderInfo
	info.OrderID, _ = msg.GetInt(fix.TagOrderID)
	info.ClientOrderID, _ = msg.GetStr(fix.TagClOrdID)
	info.Symbol, _ = msg.GetStr(fix.TagSymbol)
	var sideStr, _ = msg.GetStr(fix.TagSide)
	info.Side = types.Side(sideStr)
	info.Price = decimalFromFIXStr(msg, fix.TagPrice)
	info.Quantity, _ = msg.GetInt(fix.TagOrderQty)
	info.LeavesQty, _ = msg.GetInt(fix.TagLeavesQty)
	info.CumQty, _ = msg.GetInt(fix.TagCumQty)
	info.AvgPx = decimalFromFIXStr(msg, fix.TagAvgPx)
	var statusStr, _ = msg.GetStr(fix.TagOrdStatus)
	info.Status = types.OrdStatus(statusStr)
	info.Account, _ = msg.GetStr(fix.TagAccount)
	var transactTime, _ = msg.GetStr(fix.TagTransactTime)
	info.TransactTimeMs = parseFIXTimestampMs(transactTime)
	info.RejectReasonText, _ = msg.GetStr(fix.TagText)
	return &info
}

func decimalFromFIXStr(msg *fix.Message, tag int) decimal.Decimal {
	var s, ok = msg.GetStr(tag)
	if !ok || s == "" {
		return decimal.Zero
	}
	var d, err = decimal.NewFromString(s)
	if err != nil {
		return decimal.Zero
	}
	return d
}

func tradeQtyOf(msg *fix.Message) int64 {
	v, _ := msg.GetInt(fix.TagLastQty)
	return v
}

func tradePxOf(msg *fix.Message) decimal.Decimal {
	return decimalFromFIXStr(msg, fix.TagLastPx)
}

// fixNowTimestamp formats the current UTC time as a FIX UTCTimestamp (tag
// 60, TransactTime — required on every trading request per §4.1.x).
func fixNowTimestamp() string {
	return time.Now().UTC().Format(fixTimestampLayout)
}
