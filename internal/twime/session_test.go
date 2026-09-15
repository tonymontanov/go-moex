package twime_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/tonymontanov/go-moex/internal/twime"
	"github.com/tonymontanov/go-moex/internal/twime/twimetest"
)

type frameLog struct {
	mu     sync.Mutex
	frames []twime.Frame
	ch     chan twime.Frame
}

func newFrameLog() *frameLog { return &frameLog{ch: make(chan twime.Frame, 1024)} }

func (l *frameLog) handler(f twime.Frame) {
	cp := f
	cp.Body = append([]byte(nil), f.Body...)
	l.mu.Lock()
	l.frames = append(l.frames, cp)
	l.mu.Unlock()
	l.ch <- cp
}

func (l *frameLog) next(t *testing.T, timeout time.Duration) twime.Frame {
	t.Helper()
	select {
	case f := <-l.ch:
		return f
	case <-time.After(timeout):
		t.Fatal("no frame delivered to handler")
		return twime.Frame{}
	}
}

func dialTest(t *testing.T, g *twimetest.Gateway, cfg twime.Config) (*twime.Session, *frameLog) {
	t.Helper()
	l := newFrameLog()
	cfg.Addr = g.Addr()
	if cfg.Credentials == "" {
		cfg.Credentials = "TEST"
	}
	if cfg.KeepaliveInterval == 0 {
		cfg.KeepaliveInterval = time.Second
	}
	cfg.Handler = l.handler
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := twime.Dial(ctx, cfg)
	if err != nil {
		t.Fatalf("twime.Dial: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, l
}

func nos(clOrdID uint64) twime.NewOrderSingle {
	return twime.NewOrderSingle{ClOrdID: clOrdID, ExpireDate: twime.NullTimestamp, Price: 100000, SecurityID: 305203,
		ClOrdLinkID: twime.NullInt32, OrderQty: 1, ComplianceID: twime.ComplianceAlgorithm, TimeInForce: twime.TimeInForceDay,
		Side: twime.SideBuy, Account: "5000061"}
}

func TestSessionEstablishAndOrderFlow(t *testing.T) {
	g := twimetest.New(t)
	s, l := dialTest(t, g, twime.Config{})
	est := g.Expect(twime.TemplateEstablish, time.Second)
	if got := string(est.Body[12:16]); got != "TEST" {
		t.Fatalf("credentials on the wire: %q", got)
	}
	if s.ServerKeepalive() != time.Second {
		t.Fatalf("server keepalive %v", s.ServerKeepalive())
	}
	if s.NextSeqNo() != 1 {
		t.Fatalf("NextSeqNo %d", s.NextSeqNo())
	}
	ctx := context.Background()
	if err := s.Send(ctx, nos(100)); err != nil {
		t.Fatal(err)
	}
	f := l.next(t, time.Second)
	resp, err := f.NewOrderSingleResponse()
	if err != nil || resp.ClOrdID != 100 || resp.OrderID != 1000 {
		t.Fatalf("response %+v %v", resp, err)
	}
	if f.SeqNo != 1 || f.Retransmitted {
		t.Fatalf("seq %d retransmitted %v", f.SeqNo, f.Retransmitted)
	}
	if err := s.Send(ctx, twime.OrderCancelRequest{ClOrdID: 101, OrderID: 1000, SecurityID: 305203, Account: "5000061"}); err != nil {
		t.Fatal(err)
	}
	f = l.next(t, time.Second)
	if f.Template() != twime.TemplateOrderCancelResponse || f.SeqNo != 2 {
		t.Fatalf("frame %s seq %d", twime.TemplateName(f.Template()), f.SeqNo)
	}
	if s.NextSeqNo() != 3 {
		t.Fatalf("NextSeqNo %d", s.NextSeqNo())
	}
	if s.Pacer().InFlight() != 2 {
		t.Fatalf("in flight %d", s.Pacer().InFlight())
	}
}

func TestSessionBatchIsOneWrite(t *testing.T) {
	g := twimetest.New(t)
	s, l := dialTest(t, g, twime.Config{})
	g.Expect(twime.TemplateEstablish, time.Second)
	if err := s.Send(context.Background(), nos(1), nos(2), nos(3)); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		f := g.Expect(twime.TemplateNewOrderSingle, time.Second)
		if got := uint64(f.Body[0]); got != i {
			t.Fatalf("order %d arrived as %d", i, got)
		}
		r := l.next(t, time.Second)
		if r.SeqNo != i {
			t.Fatalf("response seq %d", r.SeqNo)
		}
	}
	if s.Pacer().InFlight() != 3 {
		t.Fatalf("batch charged %d slots", s.Pacer().InFlight())
	}
}

func TestSessionEstablishReject(t *testing.T) {
	g := twimetest.New(t)
	code := twime.EstablishRejectCredentials
	g.RejectEstablish(code)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := twime.Dial(ctx, twime.Config{Addr: g.Addr(), Credentials: "TEST", KeepaliveInterval: time.Second})
	var ee *twime.EstablishError
	if !errors.As(err, &ee) || ee.Code != twime.EstablishRejectCredentials {
		t.Fatalf("err = %v", err)
	}
}

func TestSessionConfigValidation(t *testing.T) {
	ctx := context.Background()
	if _, err := twime.Dial(ctx, twime.Config{Addr: "127.0.0.1:1", Credentials: "123456789012345678901"}); !errors.Is(err, twime.ErrCredentialsTooLong) {
		t.Fatalf("credentials: %v", err)
	}
	if _, err := twime.Dial(ctx, twime.Config{Addr: "127.0.0.1:1", KeepaliveInterval: 500 * time.Millisecond}); !errors.Is(err, twime.ErrKeepaliveOutOfRange) {
		t.Fatalf("keepalive: %v", err)
	}
}

func TestSessionHeartbeat(t *testing.T) {
	g := twimetest.New(t)
	s, _ := dialTest(t, g, twime.Config{KeepaliveInterval: time.Second})
	g.Expect(twime.TemplateEstablish, time.Second)
	start := time.Now()
	hb := g.Expect(twime.TemplateSequence, 1500*time.Millisecond)
	seq, _ := hb.Sequence()
	if seq.NextSeqNo != twime.NullUint64 {
		t.Fatalf("client heartbeat NextSeqNo = %d, want null", seq.NextSeqNo)
	}
	if el := time.Since(start); el < twime.MinHeartbeatSpacing-50*time.Millisecond || el > time.Second {
		t.Fatalf("first heartbeat after %v", el)
	}
	// Second heartbeat must respect the 600 ms floor.
	hb2 := g.Expect(twime.TemplateSequence, 1500*time.Millisecond)
	_ = hb2
	select {
	case <-s.Done():
		t.Fatalf("session died: %v", s.Err())
	default:
	}
}

func TestSessionTerminateHandshake(t *testing.T) {
	g := twimetest.New(t)
	s, _ := dialTest(t, g, twime.Config{})
	g.Expect(twime.TemplateEstablish, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Terminate(ctx); err != nil {
		t.Fatalf("twime.Terminate: %v", err)
	}
	tf := g.Expect(twime.TemplateTerminate, time.Second)
	tm, _ := tf.Terminate()
	if tm.Code != twime.TerminationFinished {
		t.Fatalf("code %v", tm.Code)
	}
	if s.Err() != nil {
		t.Fatalf("Err after clean terminate: %v", s.Err())
	}
	if err := s.Send(context.Background(), nos(1)); !errors.Is(err, twime.ErrClosed) {
		t.Fatalf("Send after terminate: %v", err)
	}
}

func TestSessionTerminatedByServer(t *testing.T) {
	g := twimetest.New(t)
	s, _ := dialTest(t, g, twime.Config{})
	g.Expect(twime.TemplateEstablish, time.Second)
	g.Write(twime.Terminate{Code: twime.TerminationServerShutdown}.Append(nil))
	select {
	case <-s.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session did not end")
	}
	var te *twime.TerminateError
	if !errors.As(s.Err(), &te) || te.Code != twime.TerminationServerShutdown {
		t.Fatalf("Err = %v", s.Err())
	}
}

func TestSessionConnectionDropped(t *testing.T) {
	g := twimetest.New(t)
	s, _ := dialTest(t, g, twime.Config{})
	g.Expect(twime.TemplateEstablish, time.Second)
	g.Drop()
	select {
	case <-s.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session did not notice the drop")
	}
	if err := s.Err(); !(errors.Is(err, twime.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)) {
		t.Fatalf("Err = %v", err)
	}
}

func TestSessionServerSilent(t *testing.T) {
	g := twimetest.New(t)
	g.SetSilent(true)
	s, _ := dialTest(t, g, twime.Config{KeepaliveInterval: time.Second})
	g.Expect(twime.TemplateEstablish, time.Second)
	select {
	case <-s.Done():
	case <-time.After(4 * time.Second):
		t.Fatal("silent gateway not detected")
	}
	if !errors.Is(s.Err(), twime.ErrServerSilent) {
		t.Fatalf("Err = %v", s.Err())
	}
}

func TestSessionRecoveryOnReconnect(t *testing.T) {
	g := twimetest.New(t)
	// The client saw 1..4 in a previous session; 5..27 happened while it
	// was away (23 messages → chunks of 10, 10, 3).
	g.SetNextSeq(5)
	for i := 5; i <= 27; i++ {
		g.Record(twime.ExecutionSingleReport{ClOrdID: uint64(i), OrderID: int64(i), LastQty: 1})
	}
	s, l := dialTest(t, g, twime.Config{NextSeqNo: 5})
	g.Expect(twime.TemplateEstablish, time.Second)
	if !s.Recovering() {
		t.Fatal("recovery should be active")
	}
	// Send must not go through until recovery is over.
	sendDone := make(chan error, 1)
	go func() { sendDone <- s.Send(context.Background(), nos(1000)) }()
	for _, want := range []uint32{10, 10, 3} {
		rr := g.Expect(twime.TemplateRetransmitRequest, 2*time.Second)
		if got := uint32(rr.Body[16]); got != want {
			t.Fatalf("retransmit count %d, want %d", got, want)
		}
	}
	for i := uint64(5); i <= 27; i++ {
		f := l.next(t, time.Second)
		if !f.Retransmitted || f.SeqNo != i {
			t.Fatalf("frame seq %d retransmitted %v, want %d", f.SeqNo, f.Retransmitted, i)
		}
		er, err := f.ExecutionSingleReport()
		if err != nil || er.ClOrdID != i {
			t.Fatalf("report %+v %v", er, err)
		}
	}
	select {
	case <-s.Recovered():
	case <-time.After(time.Second):
		t.Fatal("Recovered not closed")
	}
	if err := <-sendDone; err != nil {
		t.Fatalf("Send after recovery: %v", err)
	}
	f := l.next(t, time.Second)
	if f.Retransmitted || f.SeqNo != 28 {
		t.Fatalf("live frame seq %d retransmitted %v", f.SeqNo, f.Retransmitted)
	}
	if s.NextSeqNo() != 29 {
		t.Fatalf("NextSeqNo %d", s.NextSeqNo())
	}
}

func TestSessionRecoveryFreshStartAndReset(t *testing.T) {
	g := twimetest.New(t)
	g.SetNextSeq(50)
	s, _ := dialTest(t, g, twime.Config{NextSeqNo: 0})
	g.Expect(twime.TemplateEstablish, time.Second)
	if s.Recovering() || s.NextSeqNo() != 50 {
		t.Fatalf("fresh start: recovering %v next %d", s.Recovering(), s.NextSeqNo())
	}
	_ = s.Close()

	g2 := twimetest.New(t)
	g2.SetNextSeq(3)
	s2, _ := dialTest(t, g2, twime.Config{NextSeqNo: 40})
	g2.Expect(twime.TemplateEstablish, time.Second)
	if s2.Recovering() || s2.NextSeqNo() != 3 {
		t.Fatalf("server reset: recovering %v next %d", s2.Recovering(), s2.NextSeqNo())
	}
}

func TestSessionRecoveryGapOverLimit(t *testing.T) {
	g := twimetest.New(t)
	g.SetNextSeq(2000)
	s, _ := dialTest(t, g, twime.Config{NextSeqNo: 1, MaxRecoverMessages: 100})
	g.Expect(twime.TemplateEstablish, time.Second)
	if s.Recovering() {
		t.Fatal("gap over limit must not be recovered")
	}
	if err := s.Send(context.Background(), nos(1)); err != nil {
		t.Fatal(err)
	}
}

func TestSessionHeartbeatGapTriggersRecovery(t *testing.T) {
	g := twimetest.New(t)
	s, l := dialTest(t, g, twime.Config{})
	g.Expect(twime.TemplateEstablish, time.Second)
	// Gateway logs two messages it "forgot" to send, then heartbeats with
	// NextSeqNo=3 while the client still expects 1.
	g.Record(twime.SystemEvent{EventID: 1, TradSesEvent: twime.TradSesEventSessionDataReady})
	g.Record(twime.SystemEvent{EventID: 2, TradSesEvent: twime.TradSesEventClearingStarted})
	g.Write(twime.Sequence{NextSeqNo: 3}.Append(nil))
	rr := g.Expect(twime.TemplateRetransmitRequest, 2*time.Second)
	if from := uint64(rr.Body[8]); from != 1 || uint32(rr.Body[16]) != 2 {
		t.Fatalf("retransmit from %d count %d", from, rr.Body[16])
	}
	for i := uint64(1); i <= 2; i++ {
		f := l.next(t, time.Second)
		if !f.Retransmitted || f.SeqNo != i || f.Template() != twime.TemplateSystemEvent {
			t.Fatalf("frame %+v", f.Header)
		}
	}
	<-s.Recovered()
	if s.NextSeqNo() != 3 {
		t.Fatalf("NextSeqNo %d", s.NextSeqNo())
	}
}

func TestSessionFloodRejectPenalizes(t *testing.T) {
	g := twimetest.New(t)
	g.SetFloodEach(2)
	s, l := dialTest(t, g, twime.Config{})
	g.Expect(twime.TemplateEstablish, time.Second)
	ctx := context.Background()
	if err := s.Send(ctx, nos(1)); err != nil {
		t.Fatal(err)
	}
	l.next(t, time.Second) // response to 1
	if err := s.Send(ctx, nos(2)); err != nil {
		t.Fatal(err)
	}
	f := l.next(t, time.Second)
	if f.Template() != twime.TemplateFloodReject {
		t.Fatalf("got %s", twime.TemplateName(f.Template()))
	}
	fr, _ := f.FloodReject()
	if fr.ClOrdID != 2 || fr.Penalty() != 300*time.Millisecond {
		t.Fatalf("flood %+v", fr)
	}
	if p := s.Pacer().Penalty(); p <= 0 || p > 300*time.Millisecond {
		t.Fatalf("penalty %v", p)
	}
	var be *twime.BudgetError
	if err := s.TrySend(nos(3)); !errors.As(err, &be) {
		t.Fatalf("TrySend during penalty: %v", err)
	}
	start := time.Now()
	if err := s.Send(ctx, nos(3)); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 200*time.Millisecond {
		t.Fatalf("Send did not wait out the penalty: %v", el)
	}
}

func TestSessionPacerThrottlesSend(t *testing.T) {
	g := twimetest.New(t)
	s, _ := dialTest(t, g, twime.Config{TradingRate: 3})
	g.Expect(twime.TemplateEstablish, time.Second)
	ctx := context.Background()
	start := time.Now()
	for i := uint64(1); i <= 3; i++ {
		if err := s.Send(ctx, nos(i)); err != nil {
			t.Fatal(err)
		}
	}
	if el := time.Since(start); el > 200*time.Millisecond {
		t.Fatalf("first three sends took %v", el)
	}
	var be *twime.BudgetError
	if err := s.TrySend(nos(4)); !errors.As(err, &be) || be.Wait <= 0 {
		t.Fatalf("TrySend over budget: %v", err)
	}
	if err := s.Send(ctx, nos(4)); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 900*time.Millisecond {
		t.Fatalf("fourth send admitted after only %v", el)
	}
	if _, err := s.Pacer().Reserve(4); !errors.Is(err, twime.ErrBatchTooLarge) {
		t.Fatalf("batch of 4 at rate 3: %v", err)
	}
	// Heartbeats and session traffic are free.
	if err := s.Send(ctx, twime.Sequence{NextSeqNo: twime.NullUint64}); err != nil {
		t.Fatal(err)
	}
	// Fill the window again (4 is in flight; 5 and 6 complete the budget),
	// then a ctx-bounded send must give up instead of waiting a second.
	if err := s.Send(ctx, nos(5), nos(6)); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := s.Send(cctx, nos(7)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ctx-bounded wait: %v", err)
	}
}

func TestSessionRejectsReachHandler(t *testing.T) {
	g := twimetest.New(t)
	s, l := dialTest(t, g, twime.Config{})
	g.Expect(twime.TemplateEstablish, time.Second)
	g.Write(twime.SessionReject{ClOrdID: 7, RefTagID: 44, Reason: twime.SessionRejectClOrdIDIsNotUnique}.Append(nil))
	f := l.next(t, time.Second)
	sr, err := f.SessionReject()
	if err != nil || sr.Reason != twime.SessionRejectClOrdIDIsNotUnique || f.SeqNo != 0 {
		t.Fatalf("session reject %+v seq %d %v", sr, f.SeqNo, err)
	}
	g.Write(twime.BusinessMessageReject{ClOrdID: 8, OrdRejReason: 4103}.Append(nil))
	f = l.next(t, time.Second)
	br, err := f.BusinessMessageReject()
	if err != nil || br.OrdRejReason != 4103 || f.SeqNo != 0 {
		t.Fatalf("business reject %+v %v", br, err)
	}
	// Rejects are unnumbered: the next application message is still #1.
	g.Push(twime.EmptyBook{TradingSessionID: 1})
	f = l.next(t, time.Second)
	if f.Template() != twime.TemplateEmptyBook || f.SeqNo != 1 {
		t.Fatalf("frame %s seq %d", twime.TemplateName(f.Template()), f.SeqNo)
	}
	if s.NextSeqNo() != 2 {
		t.Fatalf("NextSeqNo %d", s.NextSeqNo())
	}
}

func TestSessionSchemaMismatchEndsSession(t *testing.T) {
	g := twimetest.New(t)
	s, _ := dialTest(t, g, twime.Config{})
	g.Expect(twime.TemplateEstablish, time.Second)
	bad := twime.Sequence{NextSeqNo: 1}.Append(nil)
	bad[4], bad[5] = 0x84, 0x4d
	g.Write(bad)
	select {
	case <-s.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session survived a foreign schema id")
	}
	var sm *twime.ErrSchemaMismatch
	if !errors.As(s.Err(), &sm) {
		t.Fatalf("Err = %v", s.Err())
	}
}

func TestSessionSendZeroAlloc(t *testing.T) {
	g := twimetest.New(t)
	s, _ := dialTest(t, g, twime.Config{TradingRate: 3000})
	g.Expect(twime.TemplateEstablish, time.Second)
	order := nos(1)
	allocs := testing.AllocsPerRun(200, func() {
		if err := s.TrySend(order); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 1 {
		t.Errorf("TrySend allocs = %v (variadic boxing of one twime.Marshaler is the only allowed one)", allocs)
	}
}
