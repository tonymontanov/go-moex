/*
FILE: forts/twime.go

DESCRIPTION:
Order entry over TWIME SPECTRA — the second (and preferred) leg of
TradingClient next to FIX Gate. The public API is identical; underneath
the differences are:

  - Instruments are addressed by numeric SecurityID, not Symbol. The leg
    resolves Symbol -> SecurityID from the Client cache, filling it from
    the SIMBA Instruments feed when configured (MarketData().
    ResolveSecurityID) or from Client.SetSecurityID seeds otherwise.
  - ClOrdID is a uint64: user-supplied ClientOrderID must be decimal;
    generated ones are clock-seeded and monotonic (see NewClient). The
    string form (decimal) is what OrderInfo.ClientOrderID carries.
  - There is no OrdStatus on the wire. Status is derived: New on
    NewOrderSingleResponse with quantity left, Filled when nothing is
    left, PartiallyFilled/Filled from ExecutionSingleReport, Canceled from
    OrderCancelResponse (solicited or not), and the replaced order is
    marked Canceled when OrderReplaceResponse creates its successor.
  - Rejections arrive as separate messages (BusinessMessageReject,
    SessionReject, FloodReject) and are returned as *moex.Error with
    TransportTWIME and the exchange code, not as an OrderInfo with
    Status=Rejected as FIX does. Watchers still get a Rejected OrderInfo
    for new-order rejects so the two legs look alike downstream.
  - Every request carries an Account (String7): from the request or
    Config.TWIME.Account.
  - Prices are Decimal5 mantissas on the wire; conversion goes through
    forts/mapping.go like SIMBA.

ASSUMPTIONS TO VERIFY ON THE TEST CIRCUIT (no live TWIME yet):
  - NewOrderSingleResponse.OrderQty is the quantity LEFT after immediate
    matching (the field is named the same in ExecutionSingleReport where
    the spec says "left"); OrderQty == 0 is treated as fully filled.
  - OrderReplaceResponse.OrderQty is likewise the quantity left on the
    new OrderID.
  - An IOC/FOK remainder that could not match yields OrderCancelResponse
    for the same ClOrdID (Status Canceled).
*/
package forts

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	moex "github.com/tonymontanov/go-moex"
	"github.com/tonymontanov/go-moex/forts/types"
	"github.com/tonymontanov/go-moex/internal/twime"
)

// twimeLeg — one TWIME session plus the per-request context needed to
// turn wire responses (which carry no Symbol/Account) into OrderInfo.
type twimeLeg struct {
	c    *Client
	sess *twime.Session // nil = not connected (stub returned by Client.twime).

	pendingMu sync.Mutex
	pending   map[uint64]twimePending
}

type twimeReqKind uint8

const (
	twimeReqNew twimeReqKind = iota + 1
	twimeReqCancel
	twimeReqReplace
	twimeReqMassCancel
)

type twimePending struct {
	kind    twimeReqKind
	symbol  string
	side    types.Side
	account string
	price   decimal.Decimal
	qty     int64
	orderID int64
}

func (c *Client) connectTWIME(ctx context.Context) error {
	c.twimeMu.Lock()
	defer c.twimeMu.Unlock()
	if c.twimeLeg != nil {
		return nil
	}
	tc := c.cfg.TWIME
	leg := &twimeLeg{c: c, pending: make(map[uint64]twimePending)}
	sess, err := twime.Dial(ctx, twime.Config{
		Addr:               tc.Addr,
		Credentials:        tc.Credentials,
		KeepaliveInterval:  tc.KeepaliveInterval,
		DialTimeout:        tc.DialTimeout,
		EstablishTimeout:   tc.EstablishTimeout,
		TradingRate:        tc.TradingRate,
		NextSeqNo:          c.twimeNextSeq,
		MaxRecoverMessages: tc.MaxRecoverMessages,
		Handler:            leg.handleFrame,
		Logger:             c.logger,
		Metrics:            c.cfg.Metrics,
	})
	if err != nil {
		var ee *twime.EstablishError
		kind := moex.ErrorKindNetwork
		if errors.As(err, &ee) {
			kind = moex.ErrorKindAuth
			if ee.Code == twime.EstablishRejectKeepaliveInterval {
				kind = moex.ErrorKindInvalidRequest
			}
		}
		return moex.NewError(moex.TransportTWIME, kind, "", "forts: TWIME Establish", err)
	}
	leg.sess = sess
	c.twimeLeg = leg
	c.registerCloser()
	go leg.watch()
	return nil
}

// watch runs until the session ends: remembers the sequence number for
// the next Connect, fails in-flight calls and detaches the leg so that
// Connect can be called again.
func (l *twimeLeg) watch() {
	<-l.sess.Done()
	err := l.sess.Err()
	l.c.twimeMu.Lock()
	l.c.twimeNextSeq = l.sess.NextSeqNo()
	if l.c.twimeLeg == l {
		l.c.twimeLeg = nil
	}
	l.c.twimeMu.Unlock()

	if err != nil {
		l.c.logger.Warn("forts: TWIME session ended", moex.Err(err), moex.Int("next_seq_no", int64(l.sess.NextSeqNo())))
	} else {
		l.c.logger.Info("forts: TWIME session terminated", moex.Int("next_seq_no", int64(l.sess.NextSeqNo())))
	}
	fail := moex.NewError(moex.TransportTWIME, moex.ErrorKindNetwork, "", "forts: TWIME session ended before the reply arrived", err)
	l.c.orderCorrelator.failAll(fail)
	l.c.cancelCorrelator.failAll(fail)
	l.c.massCancelCorrelator.failAll(fail)
	l.pendingMu.Lock()
	l.pending = make(map[uint64]twimePending)
	l.pendingMu.Unlock()
}

func (l *twimeLeg) close() error {
	if l.sess == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := l.sess.Terminate(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return nil // socket was closed by Terminate's fallback.
	}
	return err
}

func (l *twimeLeg) notConnected() error {
	return moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", "forts: TWIME not connected — call Client.Connect(ctx) before using Trading()", nil)
}

// --- request side --------------------------------------------------------

func (l *twimeLeg) nextClOrdID(requested string) (uint64, error) {
	if requested == "" {
		return l.c.twimeClOrdID.Add(1), nil
	}
	id, err := strconv.ParseUint(requested, 10, 64)
	if err != nil || id == twime.NullUint64 {
		return 0, moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", fmt.Sprintf("forts: ClientOrderID %q is not a valid TWIME ClOrdID (decimal uint64)", requested), nil)
	}
	return id, nil
}

func clOrdKey(id uint64) string { return strconv.FormatUint(id, 10) }

func (l *twimeLeg) account(requested string) (string, error) {
	acc := requested
	if acc == "" {
		acc = l.c.cfg.TWIME.Account
	}
	if acc == "" {
		return "", moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", "forts: TWIME requires an Account on every request — set it on the request or in Config.TWIME.Account", nil)
	}
	if len(acc) > twime.AccountLen {
		return "", moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", fmt.Sprintf("forts: Account %q is longer than %d bytes", acc, twime.AccountLen), nil)
	}
	return acc, nil
}

func (l *twimeLeg) securityID(ctx context.Context, symbol string) (int32, error) {
	if symbol == "" {
		return 0, moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", "forts: Symbol is required", nil)
	}
	if id, ok := l.c.SecurityIDFor(symbol); ok {
		return id, nil
	}
	if l.c.cfg.SIMBA.InstrumentsGroupA != "" {
		return l.c.market.ResolveSecurityID(ctx, symbol)
	}
	return 0, moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", fmt.Sprintf("forts: SecurityID for %q is unknown — seed it with Client.SetSecurityID or configure Config.SIMBA.InstrumentsGroupA", symbol), nil)
}

func twimeSide(s types.Side) (twime.Side, error) {
	switch s {
	case types.SideBuy:
		return twime.SideBuy, nil
	case types.SideSell:
		return twime.SideSell, nil
	}
	return 0, moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", fmt.Sprintf("forts: Side %q is required (Buy/Sell)", s), nil)
}

func sideFromTWIME(s twime.Side) types.Side {
	switch s {
	case twime.SideBuy:
		return types.SideBuy
	case twime.SideSell:
		return types.SideSell
	}
	return ""
}

func twimeTIF(t types.TimeInForce) (twime.TimeInForce, error) {
	switch t {
	case "", types.TimeInForceDay:
		return twime.TimeInForceDay, nil
	case types.TimeInForceIOC:
		return twime.TimeInForceIOC, nil
	case types.TimeInForceFOK:
		return twime.TimeInForceFOK, nil
	case types.TimeInForceGTD:
		return twime.TimeInForceGTD, nil
	case types.TimeInForceBOC:
		return twime.TimeInForceBOC, nil
	}
	return 0, moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", fmt.Sprintf("forts: unsupported TimeInForce %q", t), nil)
}

// twimeExpireDate — GTD expiry: YYYYMMDD interpreted as the end of that
// day in Moscow time (the exchange's clock), as a TWIME TimeStamp.
func twimeExpireDate(yyyymmdd string) (uint64, error) {
	loc, err := time.LoadLocation("Europe/Moscow")
	if err != nil {
		loc = time.FixedZone("MSK", 3*3600)
	}
	t, err := time.ParseInLocation("20060102", yyyymmdd, loc)
	if err != nil {
		return 0, moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", fmt.Sprintf("forts: ExpireDateYYYYMMDD %q: %v", yyyymmdd, err), nil)
	}
	return twime.TimestampOf(t.Add(24*time.Hour - time.Second)), nil
}

func (l *twimeLeg) sendErr(err error) error {
	var be *twime.BudgetError
	switch {
	case errors.Is(err, twime.ErrBatchTooLarge), errors.As(err, &be):
		return moex.NewError(moex.TransportTWIME, moex.ErrorKindRateLimit, "", "forts: TWIME trading budget", err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	}
	return moex.NewError(moex.TransportTWIME, moex.ErrorKindNetwork, "", "forts: TWIME send", err)
}

func (l *twimeLeg) setPending(id uint64, p twimePending) {
	l.pendingMu.Lock()
	l.pending[id] = p
	l.pendingMu.Unlock()
}

func (l *twimeLeg) takePending(id uint64) (twimePending, bool) {
	l.pendingMu.Lock()
	defer l.pendingMu.Unlock()
	p, ok := l.pending[id]
	if ok {
		delete(l.pending, id)
	}
	return p, ok
}

func (l *twimeLeg) createOrder(ctx context.Context, req types.CreateOrderRequest) (*types.OrderInfo, error) {
	if l.sess == nil {
		return nil, l.notConnected()
	}
	sid, err := l.securityID(ctx, req.Symbol)
	if err != nil {
		return nil, err
	}
	side, err := twimeSide(req.Side)
	if err != nil {
		return nil, err
	}
	tif, err := twimeTIF(req.TimeInForce)
	if err != nil {
		return nil, err
	}
	account, err := l.account(req.Account)
	if err != nil {
		return nil, err
	}
	expire := twime.NullTimestamp
	if tif == twime.TimeInForceGTD {
		if expire, err = twimeExpireDate(req.ExpireDateYYYYMMDD); err != nil {
			return nil, err
		}
	}
	if req.Quantity > int64(twime.NullUint32-1) {
		return nil, moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", "forts: Quantity exceeds uint32", nil)
	}
	clOrdID, err := l.nextClOrdID(req.ClientOrderID)
	if err != nil {
		return nil, err
	}
	msg := twime.NewOrderSingle{
		ClOrdID:      clOrdID,
		ExpireDate:   expire,
		Price:        mantissaFromDecimal(req.Price),
		SecurityID:   sid,
		ClOrdLinkID:  0,
		OrderQty:     uint32(req.Quantity),
		ComplianceID: twime.ComplianceID(l.c.cfg.TWIME.ComplianceID),
		TimeInForce:  tif,
		Side:         side,
		Account:      account,
	}
	key := clOrdKey(clOrdID)
	l.setPending(clOrdID, twimePending{kind: twimeReqNew, symbol: req.Symbol, side: req.Side, account: account, price: req.Price, qty: req.Quantity})
	ch := l.c.orderCorrelator.Register(key)
	if err := l.sess.Send(ctx, msg); err != nil {
		l.c.orderCorrelator.Resolve(key, nil, nil)
		l.takePending(clOrdID)
		return nil, l.sendErr(err)
	}
	return l.c.orderCorrelator.Wait(ctx, key, ch)
}

// orderByRequest — locate the tracked order for a cancel/modify request.
func (l *twimeLeg) orderByRequest(orderID int64, origClOrdID string) (*types.OrderInfo, int64, error) {
	l.c.openOrdersMu.RLock()
	defer l.c.openOrdersMu.RUnlock()
	if orderID == 0 && origClOrdID != "" {
		id, ok := l.c.clOrdToOrder[origClOrdID]
		if !ok {
			return nil, 0, moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", fmt.Sprintf("forts: OrigClientOrderID %q is not a known order — TWIME cancels by OrderID; pass OrderID for orders placed by another process", origClOrdID), nil)
		}
		orderID = id
	}
	if orderID == 0 {
		return nil, 0, moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", "forts: request requires OrderID or OrigClientOrderID", nil)
	}
	return l.c.openOrders[orderID], orderID, nil
}

func (l *twimeLeg) cancelOrder(ctx context.Context, req types.CancelOrderRequest) (*types.OrderInfo, error) {
	if l.sess == nil {
		return nil, l.notConnected()
	}
	known, orderID, err := l.orderByRequest(req.OrderID, req.OrigClientOrderID)
	if err != nil {
		return nil, err
	}
	symbol := req.Symbol
	account := ""
	if known != nil {
		if symbol == "" {
			symbol = known.Symbol
		}
		account = known.Account
	}
	sid, err := l.securityID(ctx, symbol)
	if err != nil {
		return nil, err
	}
	if account, err = l.account(account); err != nil {
		return nil, err
	}
	clOrdID, err := l.nextClOrdID(req.ClientOrderID)
	if err != nil {
		return nil, err
	}
	msg := twime.OrderCancelRequest{ClOrdID: clOrdID, OrderID: orderID, SecurityID: sid, Account: account}
	key := clOrdKey(clOrdID)
	l.setPending(clOrdID, twimePending{kind: twimeReqCancel, symbol: symbol, side: req.Side, account: account, orderID: orderID})
	ch := l.c.cancelCorrelator.Register(key)
	if err := l.sess.Send(ctx, msg); err != nil {
		l.c.cancelCorrelator.Resolve(key, nil, nil)
		l.takePending(clOrdID)
		return nil, l.sendErr(err)
	}
	return l.c.cancelCorrelator.Wait(ctx, key, ch)
}

func (l *twimeLeg) modifyOrder(ctx context.Context, req types.ModifyOrderRequest) (*types.OrderInfo, error) {
	if l.sess == nil {
		return nil, l.notConnected()
	}
	known, orderID, err := l.orderByRequest(req.OrderID, req.OrigClientOrderID)
	if err != nil {
		return nil, err
	}
	symbol := req.Symbol
	account := ""
	if known != nil {
		if symbol == "" {
			symbol = known.Symbol
		}
		account = known.Account
	}
	sid, err := l.securityID(ctx, symbol)
	if err != nil {
		return nil, err
	}
	if account, err = l.account(account); err != nil {
		return nil, err
	}
	if req.NewPrice.IsZero() {
		return nil, moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", "forts: ModifyOrderRequest.NewPrice is required", nil)
	}
	mode := twime.ReplaceModeDontChangeOrderQty
	qty := uint32(0)
	if req.NewQuantity > 0 {
		if req.NewQuantity > int64(twime.NullUint32-1) {
			return nil, moex.NewError(moex.TransportTWIME, moex.ErrorKindInvalidRequest, "", "forts: NewQuantity exceeds uint32", nil)
		}
		mode = twime.ReplaceModeChangeOrderQty
		qty = uint32(req.NewQuantity)
	}
	clOrdID, err := l.nextClOrdID(req.ClientOrderID)
	if err != nil {
		return nil, err
	}
	msg := twime.OrderReplaceRequest{
		ClOrdID:      clOrdID,
		OrderID:      orderID,
		Price:        mantissaFromDecimal(req.NewPrice),
		OrderQty:     qty,
		ClOrdLinkID:  0,
		SecurityID:   sid,
		ComplianceID: twime.ComplianceID(l.c.cfg.TWIME.ComplianceID),
		Mode:         mode,
		Account:      account,
	}
	key := clOrdKey(clOrdID)
	l.setPending(clOrdID, twimePending{kind: twimeReqReplace, symbol: symbol, side: req.Side, account: account, price: req.NewPrice, qty: req.NewQuantity, orderID: orderID})
	ch := l.c.orderCorrelator.Register(key)
	if err := l.sess.Send(ctx, msg); err != nil {
		l.c.orderCorrelator.Resolve(key, nil, nil)
		l.takePending(clOrdID)
		return nil, l.sendErr(err)
	}
	return l.c.orderCorrelator.Wait(ctx, key, ch)
}

func (l *twimeLeg) cancelAll(ctx context.Context, symbol string, side types.Side, account string) error {
	if l.sess == nil {
		return l.notConnected()
	}
	sid := twime.NullInt32
	if symbol != "" {
		id, err := l.securityID(ctx, symbol)
		if err != nil {
			return err
		}
		sid = id
	}
	ts := twime.SideAllOrders
	if side != "" {
		s, err := twimeSide(side)
		if err != nil {
			return err
		}
		ts = s
	}
	account, err := l.account(account)
	if err != nil {
		return err
	}
	clOrdID, err := l.nextClOrdID("")
	if err != nil {
		return err
	}
	msg := twime.OrderMassCancelRequest{
		ClOrdID:      clOrdID,
		ClOrdLinkID:  0,
		SecurityID:   sid,
		SecurityType: twime.SecurityTypeFuture | twime.SecurityTypeOption | twime.SecurityTypeMultileg,
		Side:         ts,
		Account:      account,
	}
	key := clOrdKey(clOrdID)
	l.setPending(clOrdID, twimePending{kind: twimeReqMassCancel, symbol: symbol, side: side, account: account})
	ch := l.c.massCancelCorrelator.Register(key)
	if err := l.sess.Send(ctx, msg); err != nil {
		l.c.massCancelCorrelator.Resolve(key, massCancelResult{}, nil)
		l.takePending(clOrdID)
		return l.sendErr(err)
	}
	res, err := l.c.massCancelCorrelator.Wait(ctx, key, ch)
	if err != nil {
		return err
	}
	if !res.Accepted {
		return moex.NewError(moex.TransportTWIME, moex.ErrorKindExchange, "", fmt.Sprintf("forts: Order Mass Cancel rejected: %s", res.RejectReasonText), nil)
	}
	return nil
}

// --- response side -------------------------------------------------------

// handleFrame is the twime.Handler: runs on the session's read goroutine,
// must not block. Frame.Body aliases the read buffer, so every branch
// decodes before doing anything else.
func (l *twimeLeg) handleFrame(f twime.Frame) {
	switch f.Template() {
	case twime.TemplateNewOrderSingleResponse:
		if r, err := f.NewOrderSingleResponse(); err == nil {
			l.onNewOrder(r)
		}
	case twime.TemplateExecutionSingleReport:
		if r, err := f.ExecutionSingleReport(); err == nil {
			l.onFill(r.ClOrdID, r.OrderID, r.SecurityID, r.Side, r.LastPx, r.LastQty, r.OrderQty, r.Timestamp)
		}
	case twime.TemplateExecutionMultilegReport:
		if r, err := f.ExecutionMultilegReport(); err == nil {
			l.onFill(r.ClOrdID, r.OrderID, r.SecurityID, r.Side, r.LastPx, r.LastQty, r.OrderQty, r.Timestamp)
		}
	case twime.TemplateOrderCancelResponse:
		if r, err := f.OrderCancelResponse(); err == nil {
			l.onCancel(r)
		}
	case twime.TemplateOrderReplaceResponse:
		if r, err := f.OrderReplaceResponse(); err == nil {
			l.onReplace(r)
		}
	case twime.TemplateOrderMassCancelResponse:
		if r, err := f.OrderMassCancelResponse(); err == nil {
			l.takePending(r.ClOrdID)
			l.c.massCancelCorrelator.Resolve(clOrdKey(r.ClOrdID), massCancelResult{Accepted: true, TotalAffectedOrders: int64(r.TotalAffectedOrders)}, nil)
		}
	case twime.TemplateBusinessMessageReject:
		if r, err := f.BusinessMessageReject(); err == nil {
			l.onReject(r.ClOrdID, moex.NewError(moex.TransportTWIME, moex.ErrorKindExchange, strconv.Itoa(int(r.OrdRejReason)),
				fmt.Sprintf("forts: TWIME BusinessMessageReject OrdRejReason=%d", r.OrdRejReason), nil))
		}
	case twime.TemplateSessionReject:
		if r, err := f.SessionReject(); err == nil {
			kind := moex.ErrorKindInvalidRequest
			if r.Reason == twime.SessionRejectSystemIsUnavailable {
				kind = moex.ErrorKindExchange
			}
			l.onReject(r.ClOrdID, moex.NewError(moex.TransportTWIME, kind, strconv.Itoa(int(r.Reason)),
				fmt.Sprintf("forts: TWIME SessionReject %s (field tag %d)", r.Reason, r.RefTagID), nil))
		}
	case twime.TemplateFloodReject:
		if r, err := f.FloodReject(); err == nil {
			l.onReject(r.ClOrdID, moex.NewError(moex.TransportTWIME, moex.ErrorKindRateLimit, "FloodReject",
				fmt.Sprintf("forts: TWIME FloodReject: %d messages in the last second, penalty %v", r.QueueSize, r.Penalty()), nil))
		}
	case twime.TemplateEmptyBook:
		if r, err := f.EmptyBook(); err == nil {
			l.onEmptyBook(r)
		}
	case twime.TemplateSystemEvent:
		if r, err := f.SystemEvent(); err == nil {
			l.c.logger.Info("forts: TWIME system event", moex.Str("event", r.TradSesEvent.String()),
				moex.Int("trading_session_id", int64(r.TradingSessionID)))
		}
	default:
		l.c.logger.Debug("forts: unhandled TWIME frame", moex.Str("template", twime.TemplateName(f.Template())))
	}
}

func twimeMs(ts uint64) int64 {
	if ts == twime.NullTimestamp {
		return 0
	}
	return int64(ts / 1_000_000)
}

func (l *twimeLeg) onNewOrder(r twime.NewOrderSingleResponse) {
	key := clOrdKey(r.ClOrdID)
	p, _ := l.takePending(r.ClOrdID)
	symbol := l.c.symbolFor(r.SecurityID)
	if symbol == "" {
		symbol = p.symbol
	}
	info := &types.OrderInfo{
		OrderID:        r.OrderID,
		ClientOrderID:  key,
		Symbol:         symbol,
		Side:           sideFromTWIME(r.Side),
		Price:          decimalFromSIMBAMantissa(r.Price),
		Quantity:       p.qty,
		LeavesQty:      int64(r.OrderQty),
		Account:        p.account,
		TransactTimeMs: twimeMs(r.Timestamp),
	}
	if info.Quantity < info.LeavesQty {
		info.Quantity = info.LeavesQty
	}
	info.CumQty = info.Quantity - info.LeavesQty
	if info.LeavesQty > 0 {
		info.Status = types.OrdStatusNew
	} else {
		info.Status = types.OrdStatusFilled
	}
	l.c.openOrdersMu.Lock()
	if prev, ok := l.c.openOrders[r.OrderID]; ok && prev.CumQty > info.CumQty {
		// Fills for this order were reported before its acceptance.
		info.CumQty, info.AvgPx = prev.CumQty, prev.AvgPx
		if info.LeavesQty == 0 {
			info.Status = types.OrdStatusFilled
		} else {
			info.Status = types.OrdStatusPartiallyFilled
		}
	}
	l.c.openOrders[r.OrderID] = info
	l.c.clOrdToOrder[key] = r.OrderID
	l.c.openOrdersMu.Unlock()
	l.c.orderCorrelator.Resolve(key, info, nil)
	l.c.fanOutOrder(info)
}

func (l *twimeLeg) onFill(clOrdID uint64, orderID int64, securityID int32, side twime.Side, lastPx int64, lastQty, left uint32, ts uint64) {
	px := decimalFromSIMBAMantissa(lastPx)
	qty := int64(lastQty)
	l.c.openOrdersMu.Lock()
	info, ok := l.c.openOrders[orderID]
	if !ok {
		info = &types.OrderInfo{OrderID: orderID, Symbol: l.c.symbolFor(securityID), Side: sideFromTWIME(side)}
		if clOrdID != twime.NullUint64 {
			info.ClientOrderID = clOrdKey(clOrdID)
			l.c.clOrdToOrder[info.ClientOrderID] = orderID
		}
		l.c.openOrders[orderID] = info
	}
	newCum := info.CumQty + qty
	if newCum > 0 {
		info.AvgPx = info.AvgPx.Mul(decimal.NewFromInt(info.CumQty)).Add(px.Mul(decimal.NewFromInt(qty))).Div(decimal.NewFromInt(newCum))
	}
	info.CumQty = newCum
	info.LeavesQty = int64(left)
	if info.Quantity < info.CumQty+info.LeavesQty {
		info.Quantity = info.CumQty + info.LeavesQty
	}
	if info.LeavesQty == 0 {
		info.Status = types.OrdStatusFilled
	} else {
		info.Status = types.OrdStatusPartiallyFilled
	}
	info.TransactTimeMs = twimeMs(ts)
	snapshot := *info
	l.c.openOrdersMu.Unlock()

	l.c.positions.ApplyFill(snapshot.Symbol, snapshot.Side, qty, px, snapshot.TransactTimeMs)
	if clOrdID != twime.NullUint64 {
		// A fill can precede NewOrderSingleResponse; whichever comes first
		// answers CreateOrder.
		l.c.orderCorrelator.Resolve(clOrdKey(clOrdID), &snapshot, nil)
	}
	l.c.fanOutOrder(&snapshot)
}

func (l *twimeLeg) onCancel(r twime.OrderCancelResponse) {
	var key string
	var p twimePending
	if !r.Unsolicited() {
		key = clOrdKey(r.ClOrdID)
		p, _ = l.takePending(r.ClOrdID)
	}
	l.c.openOrdersMu.Lock()
	info, ok := l.c.openOrders[r.OrderID]
	if !ok {
		info = &types.OrderInfo{OrderID: r.OrderID, Symbol: p.symbol, Side: p.side, Account: p.account}
		l.c.openOrders[r.OrderID] = info
	}
	info.LeavesQty = 0
	if info.Quantity < info.CumQty+int64(r.OrderQty) {
		info.Quantity = info.CumQty + int64(r.OrderQty)
	}
	info.Status = types.OrdStatusCanceled
	info.TransactTimeMs = twimeMs(r.Timestamp)
	if r.Flags.Has(twime.FlagCOD) {
		info.RejectReasonText = "cancel-on-disconnect"
	} else if r.Flags.Has(twime.FlagDueToCrossCancel) {
		info.RejectReasonText = "cross-trade cancel"
	}
	snapshot := *info
	l.c.openOrdersMu.Unlock()

	if key != "" {
		if !l.c.cancelCorrelator.Resolve(key, &snapshot, nil) {
			// Replace with Mode CheckOrderQtyAndCancelOrder / FixStyleReplace
			// may end in a cancel; answer the modify call too.
			l.c.orderCorrelator.Resolve(key, &snapshot, nil)
		}
	}
	l.c.fanOutOrder(&snapshot)
}

func (l *twimeLeg) onReplace(r twime.OrderReplaceResponse) {
	key := clOrdKey(r.ClOrdID)
	p, _ := l.takePending(r.ClOrdID)
	l.c.openOrdersMu.Lock()
	old := l.c.openOrders[r.PrevOrderID]
	var oldSnap *types.OrderInfo
	newInfo := &types.OrderInfo{
		OrderID:        r.OrderID,
		ClientOrderID:  key,
		Symbol:         p.symbol,
		Side:           p.side,
		Price:          decimalFromSIMBAMantissa(r.Price),
		LeavesQty:      int64(r.OrderQty),
		Account:        p.account,
		TransactTimeMs: twimeMs(r.Timestamp),
		Status:         types.OrdStatusNew,
	}
	if old != nil {
		if newInfo.Symbol == "" {
			newInfo.Symbol = old.Symbol
		}
		if newInfo.Side == "" {
			newInfo.Side = old.Side
		}
		if newInfo.Account == "" {
			newInfo.Account = old.Account
		}
		newInfo.CumQty, newInfo.AvgPx = old.CumQty, old.AvgPx
		old.LeavesQty = 0
		old.Status = types.OrdStatusCanceled
		old.RejectReasonText = "replaced by " + strconv.FormatInt(r.OrderID, 10)
		s := *old
		oldSnap = &s
	}
	newInfo.Quantity = newInfo.CumQty + newInfo.LeavesQty
	if newInfo.LeavesQty == 0 {
		newInfo.Status = types.OrdStatusFilled
	}
	l.c.openOrders[r.OrderID] = newInfo
	l.c.clOrdToOrder[key] = r.OrderID
	snapshot := *newInfo
	l.c.openOrdersMu.Unlock()

	l.c.orderCorrelator.Resolve(key, &snapshot, nil)
	if oldSnap != nil {
		l.c.fanOutOrder(oldSnap)
	}
	l.c.fanOutOrder(&snapshot)
}

// onReject answers whichever call is waiting on clOrdID with err. For a
// rejected new order the watchers also get a Rejected OrderInfo, matching
// what the FIX leg emits.
func (l *twimeLeg) onReject(clOrdID uint64, err error) {
	key := clOrdKey(clOrdID)
	p, ok := l.takePending(clOrdID)
	if !l.c.orderCorrelator.Resolve(key, nil, err) {
		if !l.c.cancelCorrelator.Resolve(key, nil, err) {
			l.c.massCancelCorrelator.Resolve(key, massCancelResult{}, err)
		}
	}
	if ok && p.kind == twimeReqNew {
		l.c.fanOutOrder(&types.OrderInfo{
			ClientOrderID:    key,
			Symbol:           p.symbol,
			Side:             p.side,
			Price:            p.price,
			Quantity:         p.qty,
			Account:          p.account,
			Status:           types.OrdStatusRejected,
			RejectReasonText: err.Error(),
		})
	}
}

// onEmptyBook — main clearing started: the exchange has removed every
// order of the trading session; mirror that locally (the FIX leg would
// see individual Execution Reports, TWIME sends one EmptyBook).
func (l *twimeLeg) onEmptyBook(r twime.EmptyBook) {
	var gone []*types.OrderInfo
	l.c.openOrdersMu.Lock()
	for _, o := range l.c.openOrders {
		if o.Status == types.OrdStatusFilled || o.Status == types.OrdStatusCanceled || o.Status == types.OrdStatusRejected {
			continue
		}
		o.LeavesQty = 0
		o.Status = types.OrdStatusCanceled
		o.RejectReasonText = "clearing (EmptyBook)"
		o.TransactTimeMs = twimeMs(r.Timestamp)
		s := *o
		gone = append(gone, &s)
	}
	l.c.openOrdersMu.Unlock()
	l.c.logger.Warn("forts: TWIME EmptyBook — clearing started, open orders dropped",
		moex.Int("trading_session_id", int64(r.TradingSessionID)), moex.Int("orders", int64(len(gone))))
	for _, o := range gone {
		l.c.fanOutOrder(o)
	}
}
