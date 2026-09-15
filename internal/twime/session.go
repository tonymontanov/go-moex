/*
FILE: internal/twime/session.go

DESCRIPTION:
TWIME session layer (spec §3, FAQ §4): one TCP connection per login,
Establish/EstablishmentAck handshake, bidirectional Sequence heartbeats,
Terminate handshake, server-side sequence numbering of application
messages with RetransmitRequest/Retransmission recovery, FloodReject-
driven pacing. Everything above the session layer — what an order
response means for the consumer's order book — is the caller's
(forts/) business: application frames are handed to Handler as they
arrive, already decoded to the byte slice, with their sequence number.

PROTOCOL FACTS THE CODE RELIES ON (spectra_twime_en.pdf, doc 9.9.0):
  - Frame = 8-byte SBE header + BlockLength bytes; no length prefix.
  - Establish must reach the gateway within 10 s of the TCP connect;
    Establish retries and TCP connects no more often than once a second
    (EstablishmentReject.TooFastReconnect otherwise).
  - KeepaliveInterval is negotiated per direction: the client promises
    to send SOMETHING (any message) within its interval or gets
    Terminate(MissedHeartbeat) after 1–2 intervals; the server promises
    to send something within the interval returned in EstablishmentAck.
    At most 3 heartbeats per second, recommended spacing >= 600 ms.
  - Client heartbeats are Sequence with NextSeqNo = NullUint64; server
    heartbeats carry the next application sequence number.
  - Only server-to-client APPLICATION messages (template >= 6000) are
    numbered, starting from EstablishmentAck.NextSeqNo; session-layer
    frames (FloodReject, SessionReject, BusinessMessageReject, ...) are
    not numbered and can never be recovered.
  - RetransmitRequest fetches at most 10 messages on the transactional
    gateway; the Retransmission header arrives about one second later,
    followed by exactly Count application frames; the server sends no
    live messages while a retransmission is in progress.
  - Terminate is a handshake: the initiator waits for the peer's
    Terminate before closing TCP (an early close loses in-flight
    execution reports). The server closes TCP after its own Terminate.
  - Trading messages are budgeted per fixed second (30 x performance
    units); FloodReject.PenaltyRemain is microseconds.

CONCURRENCY:
  - readLoop is the only reader of the socket and the only writer of the
    inbound sequence state; Handler runs on it and must not block.
  - keepaliveLoop owns the heartbeat timer and the server-silence check.
  - Every socket write goes through send() under writeMu; Send/TrySend
    take the pacer before writeMu, so a paced caller never holds the
    write lock while waiting.
  - done is closed exactly once (closeOnce); Err() is stable after that.
*/
package twime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tonymontanov/go-moex/internal/moexlog"
	"github.com/tonymontanov/go-moex/internal/moexmet"
)

// Protocol constants from the spec.
const (
	// DefaultKeepaliveInterval — client-side promise sent in Establish.
	DefaultKeepaliveInterval = 10 * time.Second
	// DefaultDialTimeout — TCP connect timeout.
	DefaultDialTimeout = 5 * time.Second
	// DefaultEstablishTimeout — wait for EstablishmentAck/Reject. Keep it
	// below the gateway's own 10 s Establish deadline.
	DefaultEstablishTimeout = 5 * time.Second
	// MinReconnectInterval — the gateway rejects Establish and TCP
	// connects that come sooner than one second after the previous one.
	MinReconnectInterval = time.Second
	// MaxRetransmitCount — messages per RetransmitRequest on the
	// transactional gateway (the recovery gateway allows 1000).
	MaxRetransmitCount = 10
	// MinHeartbeatSpacing — recommended minimum between client heartbeats
	// (hard limit: 3 per second → Terminate(TooFastClient)).
	MinHeartbeatSpacing = 600 * time.Millisecond
	// DefaultMaxRecoverMessages — largest gap Dial recovers through the
	// transactional gateway (10 messages per ~1 s round trip); larger gaps
	// are logged and skipped — use the recovery gateway for those.
	DefaultMaxRecoverMessages = 1000
)

var (
	// ErrClosed — session is closed (Close called or peer went away).
	ErrClosed = errors.New("twime: session closed")
	// ErrServerSilent — nothing from the gateway for two server keepalive
	// intervals; the TCP connection is considered dead.
	ErrServerSilent = errors.New("twime: server silent for two keepalive intervals")
	// ErrRecovering — TrySend refused because a retransmission is in
	// progress (the spec forbids sending during recovery).
	ErrRecovering = errors.New("twime: sequence recovery in progress")
	// ErrCredentialsTooLong — Credentials exceeds String20.
	ErrCredentialsTooLong = errors.New("twime: credentials longer than 20 bytes")
	// ErrKeepaliveOutOfRange — KeepaliveInterval outside 1..60 s.
	ErrKeepaliveOutOfRange = errors.New("twime: keepalive interval must be within 1s..60s")
)

// EstablishError — the gateway answered Establish with EstablishmentReject.
type EstablishError struct {
	Code EstablishmentRejectCode
}

func (e *EstablishError) Error() string {
	return fmt.Sprintf("twime: establish rejected: %s (%d)", e.Code, uint8(e.Code))
}

// TerminateError — the gateway ended the session with Terminate.
type TerminateError struct {
	Code TerminationCode
}

func (e *TerminateError) Error() string {
	return fmt.Sprintf("twime: terminated by server: %s (%d)", e.Code, uint8(e.Code))
}

// ProtocolError — an unexpected frame where the spec allows only
// specific ones (e.g. during Establish).
type ProtocolError struct {
	Template uint16
	Phase    string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("twime: unexpected %s(%d) during %s", TemplateName(e.Template), e.Template, e.Phase)
}

// BudgetError — TrySend refused: the pacer would have to wait.
type BudgetError struct {
	Wait time.Duration
}

func (e *BudgetError) Error() string {
	return fmt.Sprintf("twime: trading budget exhausted, retry in %v", e.Wait)
}

// Handler receives every application-layer frame and the three
// unnumbered rejects (FloodReject, SessionReject, BusinessMessageReject)
// in arrival order, on the session's read goroutine. Frame.Body aliases
// the read buffer: decode or copy before returning, never retain.
type Handler func(Frame)

// Config — session parameters.
type Config struct {
	// Addr — host:port of the transactional gateway.
	Addr string
	// Credentials — TWIME login (Establish.Credentials, String20). There
	// is no password: the gateway authenticates login + source IP.
	Credentials string
	// KeepaliveInterval — the client's promise; 1s..60s. Default 10s.
	KeepaliveInterval time.Duration
	// DialTimeout / EstablishTimeout — see Default*.
	DialTimeout      time.Duration
	EstablishTimeout time.Duration
	// TradingRate — trading messages per second the login is allowed
	// (30 per performance unit). Default DefaultTradingRate.
	TradingRate int
	// NextSeqNo — next server sequence number expected from a previous
	// session of the same login (Session.NextSeqNo of the old session).
	// Zero = fresh start, no recovery. If the gateway's NextSeqNo is
	// larger, Dial requests the missed application messages before
	// admitting any Send.
	NextSeqNo uint64
	// MaxRecoverMessages — largest gap recovered automatically. Default
	// DefaultMaxRecoverMessages; negative disables recovery.
	MaxRecoverMessages int
	// Handler — required for anything useful; nil drops every frame.
	Handler Handler
	Logger  moexlog.Logger
	Metrics moexmet.CounterFactory
	// Dialer — optional replacement for net.Dialer (tests, proxies).
	Dialer func(ctx context.Context, addr string) (net.Conn, error)
}

func (c Config) withDefaults() Config {
	if c.KeepaliveInterval == 0 {
		c.KeepaliveInterval = DefaultKeepaliveInterval
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}
	if c.EstablishTimeout == 0 {
		c.EstablishTimeout = DefaultEstablishTimeout
	}
	if c.TradingRate <= 0 {
		c.TradingRate = DefaultTradingRate
	}
	if c.MaxRecoverMessages == 0 {
		c.MaxRecoverMessages = DefaultMaxRecoverMessages
	}
	if c.Handler == nil {
		c.Handler = func(Frame) {}
	}
	if c.Logger == nil {
		c.Logger = moexlog.Noop()
	}
	if c.Dialer == nil {
		c.Dialer = func(ctx context.Context, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: c.DialTimeout}
			return d.DialContext(ctx, "tcp", addr)
		}
	}
	return c
}

func (c Config) validate() error {
	if len(c.Credentials) > CredentialsLen {
		return ErrCredentialsTooLong
	}
	ms := c.KeepaliveInterval / time.Millisecond
	if ms < time.Duration(MinKeepaliveMillis) || ms > time.Duration(MaxKeepaliveMillis) {
		return ErrKeepaliveOutOfRange
	}
	return nil
}

// Session — one established TWIME session.
type Session struct {
	cfg   Config
	log   moexlog.Logger
	met   *metrics
	conn  net.Conn
	rd    *Reader
	pacer *Pacer

	writeMu  sync.Mutex
	wbuf     []byte
	lastSent atomic.Int64 // unix nanos
	lastRecv atomic.Int64

	serverKeepalive time.Duration
	nextSeq         atomic.Uint64 // next expected LIVE application seq

	recMu        sync.Mutex
	recActive    bool
	recFrom      uint64 // next seq to request
	recTo        uint64 // exclusive end of the gap
	recRemaining uint32 // frames left in the current Retransmission block
	recSeq       uint64 // seq of the next retransmitted frame
	recovered    chan struct{}

	terminating atomic.Bool
	done        chan struct{}
	closeOnce   sync.Once
	errMu       sync.Mutex
	err         error
}

var closedCh = func() chan struct{} { c := make(chan struct{}); close(c); return c }()

// Dial connects, performs Establish and starts the session goroutines.
// On EstablishmentReject the error is *EstablishError; the caller must
// not reconnect sooner than MinReconnectInterval.
func Dial(ctx context.Context, cfg Config) (*Session, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	conn, err := cfg.Dialer(ctx, cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("twime: dial %s: %w", cfg.Addr, err)
	}
	s := &Session{
		cfg:       cfg,
		log:       cfg.Logger,
		met:       newMetrics(cfg.Metrics),
		conn:      conn,
		rd:        NewReader(conn),
		pacer:     NewPacer(cfg.TradingRate, nil),
		wbuf:      make([]byte, 0, 4096),
		recovered: closedCh,
		done:      make(chan struct{}),
	}
	now := time.Now().UnixNano()
	s.lastSent.Store(now)
	s.lastRecv.Store(now)
	if err := s.establish(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	go s.readLoop()
	go s.keepaliveLoop()
	return s, nil
}

func (s *Session) establish(ctx context.Context) error {
	deadline := time.Now().Add(s.cfg.EstablishTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = s.conn.SetDeadline(deadline)
	defer func() { _ = s.conn.SetDeadline(time.Time{}) }()

	req := Establish{
		Timestamp:         TimestampOf(time.Now()),
		KeepaliveInterval: uint32(s.cfg.KeepaliveInterval / time.Millisecond),
		Credentials:       s.cfg.Credentials,
	}
	if err := s.send(req); err != nil {
		return err
	}
	for {
		f, err := s.rd.ReadFrame()
		if err != nil {
			return fmt.Errorf("twime: waiting for EstablishmentAck: %w", err)
		}
		s.lastRecv.Store(time.Now().UnixNano())
		s.met.onReceived(f.Template())
		switch f.Template() {
		case TemplateEstablishmentAck:
			ack, err := f.EstablishmentAck()
			if err != nil {
				return err
			}
			s.serverKeepalive = time.Duration(ack.KeepaliveInterval) * time.Millisecond
			if s.serverKeepalive <= 0 {
				s.serverKeepalive = s.cfg.KeepaliveInterval
			}
			s.nextSeq.Store(ack.NextSeqNo)
			s.log.Info("twime: session established",
				moexlog.Str("addr", s.cfg.Addr),
				moexlog.Int("server_keepalive_ms", int64(ack.KeepaliveInterval)),
				moexlog.Int("next_seq_no", int64(ack.NextSeqNo)),
				moexlog.Int("expected_seq_no", int64(s.cfg.NextSeqNo)))
			return s.planRecovery(ack.NextSeqNo)
		case TemplateEstablishmentReject:
			rej, err := f.EstablishmentReject()
			if err != nil {
				return err
			}
			s.met.onEstablishReject(rej.Code)
			return &EstablishError{Code: rej.Code}
		case TemplateTerminate:
			t, err := f.Terminate()
			if err != nil {
				return err
			}
			s.met.onTerminate(t.Code)
			return &TerminateError{Code: t.Code}
		case TemplateSequence:
			// Tolerated: some gateways heartbeat before the ack lands.
			continue
		default:
			return &ProtocolError{Template: f.Template(), Phase: "establish"}
		}
	}
}

// planRecovery compares the gateway's NextSeqNo with the one carried over
// from the previous session and, if messages were missed, starts the
// first RetransmitRequest. Sends are held until recovery completes.
func (s *Session) planRecovery(serverNext uint64) error {
	want := s.cfg.NextSeqNo
	switch {
	case want == 0 || want == serverNext:
		return nil
	case want > serverNext:
		s.log.Warn("twime: gateway sequence reset (nightly cleanup or new login state)",
			moexlog.Int("expected", int64(want)), moexlog.Int("server", int64(serverNext)))
		s.met.seqMismatch.Inc()
		return nil
	}
	gap := serverNext - want
	if s.cfg.MaxRecoverMessages < 0 || gap > uint64(s.cfg.MaxRecoverMessages) {
		s.log.Warn("twime: missed messages not recovered (gap over limit)",
			moexlog.Int("gap", int64(gap)), moexlog.Int("limit", int64(s.cfg.MaxRecoverMessages)))
		s.met.seqMismatch.Inc()
		return nil
	}
	s.recMu.Lock()
	s.startRecoveryLocked(want, serverNext)
	err := s.requestNextChunkLocked()
	s.recMu.Unlock()
	return err
}

// startRecoveryLocked — recMu held.
func (s *Session) startRecoveryLocked(from, to uint64) {
	s.recActive = true
	s.recFrom = from
	s.recTo = to
	s.recRemaining = 0
	s.recovered = make(chan struct{})
	s.log.Warn("twime: recovering missed application messages",
		moexlog.Int("from", int64(from)), moexlog.Int("to", int64(to)))
}

// requestNextChunkLocked — recMu held. Sends the next RetransmitRequest
// or finishes recovery when the gap is closed.
func (s *Session) requestNextChunkLocked() error {
	if s.recFrom >= s.recTo {
		s.finishRecoveryLocked()
		return nil
	}
	count := s.recTo - s.recFrom
	if count > MaxRetransmitCount {
		count = MaxRetransmitCount
	}
	s.met.retransmitRq.Inc()
	return s.send(RetransmitRequest{
		Timestamp: TimestampOf(time.Now()),
		FromSeqNo: s.recFrom,
		Count:     uint32(count),
	})
}

func (s *Session) finishRecoveryLocked() {
	if !s.recActive {
		return
	}
	s.recActive = false
	s.recRemaining = 0
	close(s.recovered)
	s.log.Info("twime: recovery complete", moexlog.Int("next_seq_no", int64(s.nextSeq.Load())))
}

func (s *Session) recoveredCh() <-chan struct{} {
	s.recMu.Lock()
	defer s.recMu.Unlock()
	return s.recovered
}

// Recovered — closed when no sequence recovery is pending. Dial returns
// before recovery completes; Send waits on this channel automatically.
func (s *Session) Recovered() <-chan struct{} { return s.recoveredCh() }

// Recovering — a retransmission is in progress.
func (s *Session) Recovering() bool {
	s.recMu.Lock()
	defer s.recMu.Unlock()
	return s.recActive
}

// NextSeqNo — next server application sequence number expected on the
// live stream. Persist it and pass as Config.NextSeqNo to the next
// session of this login to recover what was missed while disconnected.
func (s *Session) NextSeqNo() uint64 { return s.nextSeq.Load() }

// ServerKeepalive — interval promised by the gateway in EstablishmentAck.
func (s *Session) ServerKeepalive() time.Duration { return s.serverKeepalive }

// Pacer — the session's trading-budget limiter (read-only use: InFlight,
// Penalty, Waits).
func (s *Session) Pacer() *Pacer { return s.pacer }

// Done — closed when the session is over (any reason).
func (s *Session) Done() <-chan struct{} { return s.done }

// Err — why the session ended; nil for a clean Terminate handshake, nil
// before the session ends.
func (s *Session) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// send writes one message immediately (no pacing, no recovery barrier):
// session-layer traffic and the establish handshake.
func (s *Session) send(m Marshaler) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeLocked(m)
}

func (s *Session) writeLocked(msgs ...Marshaler) error {
	s.wbuf = s.wbuf[:0]
	for _, m := range msgs {
		s.wbuf = m.Append(s.wbuf)
	}
	if _, err := s.conn.Write(s.wbuf); err != nil {
		s.fail(err)
		return err
	}
	s.lastSent.Store(time.Now().UnixNano())
	for _, m := range msgs {
		s.met.onSent(m.Template())
	}
	return nil
}

func tradingCount(msgs []Marshaler) int {
	n := 0
	for _, m := range msgs {
		if IsTradingTemplate(m.Template()) {
			n++
		}
	}
	return n
}

// Send writes msgs as one TCP write (pipelining: N frames, one syscall,
// still N transactions for the budget). It blocks while sequence
// recovery is pending and while the pacer or a FloodReject penalty
// requires; ctx bounds the wait. A batch larger than the whole
// per-second budget fails with ErrBatchTooLarge.
func (s *Session) Send(ctx context.Context, msgs ...Marshaler) error {
	if len(msgs) == 0 {
		return nil
	}
	select {
	case <-s.done:
		return s.closedErr()
	default:
	}
	select {
	case <-s.recoveredCh():
	case <-s.done:
		return s.closedErr()
	case <-ctx.Done():
		return ctx.Err()
	}
	n := tradingCount(msgs)
	for {
		w, err := s.pacer.Reserve(n)
		if err != nil {
			return err
		}
		if w == 0 {
			break
		}
		s.met.pacerWaits.Inc()
		t := time.NewTimer(w)
		select {
		case <-t.C:
		case <-s.done:
			t.Stop()
			return s.closedErr()
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeLocked(msgs...)
}

// TrySend is Send without waiting: *BudgetError when the pacer would
// block, ErrRecovering during recovery, ErrClosed after the end.
func (s *Session) TrySend(msgs ...Marshaler) error {
	if len(msgs) == 0 {
		return nil
	}
	select {
	case <-s.done:
		return s.closedErr()
	default:
	}
	if s.Recovering() {
		return ErrRecovering
	}
	w, err := s.pacer.Reserve(tradingCount(msgs))
	if err != nil {
		return err
	}
	if w > 0 {
		s.met.pacerWaits.Inc()
		return &BudgetError{Wait: w}
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeLocked(msgs...)
}

func (s *Session) closedErr() error {
	if err := s.Err(); err != nil {
		return err
	}
	return ErrClosed
}

// Terminate performs the graceful handshake: sends Terminate(Finished),
// keeps reading until the gateway's Terminate arrives (in-flight
// execution reports are still delivered to Handler), then closes. If
// ctx expires first the socket is closed anyway. Returns nil on a clean
// handshake.
func (s *Session) Terminate(ctx context.Context) error {
	if s.terminating.CompareAndSwap(false, true) {
		select {
		case <-s.done:
			return s.Err()
		default:
		}
		if err := s.send(Terminate{Code: TerminationFinished}); err != nil {
			return err
		}
	}
	select {
	case <-s.done:
		return s.Err()
	case <-ctx.Done():
		s.fail(ctx.Err())
		return ctx.Err()
	}
}

// Close drops the connection immediately without the Terminate
// handshake. Orders under Cancel-on-Disconnect are cancelled by the
// gateway.
func (s *Session) Close() error {
	s.fail(ErrClosed)
	return nil
}

// fail ends the session once; the first error wins, nil means a clean
// termination.
func (s *Session) fail(err error) {
	s.closeOnce.Do(func() {
		s.errMu.Lock()
		s.err = err
		s.errMu.Unlock()
		_ = s.conn.Close()
		s.recMu.Lock()
		if s.recActive {
			s.recActive = false
			close(s.recovered)
		}
		s.recMu.Unlock()
		close(s.done)
	})
}

func (s *Session) readLoop() {
	for {
		f, err := s.rd.ReadFrame()
		if err != nil {
			if s.terminating.Load() && (err == io.EOF || errors.Is(err, net.ErrClosed)) {
				// Gateway closed after our Terminate without answering:
				// treat as a clean end.
				s.fail(nil)
				return
			}
			select {
			case <-s.done:
			default:
				if err == io.EOF {
					err = fmt.Errorf("twime: gateway closed the connection: %w", ErrClosed)
				}
				s.log.Warn("twime: read failed", moexlog.Err(err))
			}
			s.fail(err)
			return
		}
		s.lastRecv.Store(time.Now().UnixNano())
		s.met.onReceived(f.Template())
		if !s.dispatch(f) {
			return
		}
	}
}

// dispatch handles one frame; false = the session is over.
func (s *Session) dispatch(f Frame) bool {
	switch f.Template() {
	case TemplateSequence:
		seq, err := f.Sequence()
		if err != nil {
			s.fail(err)
			return false
		}
		s.onServerSequence(seq.NextSeqNo)
	case TemplateTerminate:
		t, err := f.Terminate()
		if err != nil {
			s.fail(err)
			return false
		}
		if s.terminating.Load() {
			s.log.Info("twime: terminate acknowledged", moexlog.Str("code", t.Code.String()))
			s.fail(nil)
			return false
		}
		s.met.onTerminate(t.Code)
		s.log.Warn("twime: terminated by gateway", moexlog.Str("code", t.Code.String()),
			moexlog.Str("code_num", codeLabel(uint8(t.Code))))
		s.fail(&TerminateError{Code: t.Code})
		return false
	case TemplateRetransmission:
		r, err := f.Retransmission()
		if err != nil {
			s.fail(err)
			return false
		}
		s.onRetransmission(r)
	case TemplateFloodReject:
		fr, err := f.FloodReject()
		if err != nil {
			s.fail(err)
			return false
		}
		s.met.floodRejects.Inc()
		s.pacer.Penalize(fr.Penalty())
		s.log.Warn("twime: flood reject", moexlog.Int("clordid", int64(fr.ClOrdID)),
			moexlog.Int("queue_size", int64(fr.QueueSize)), moexlog.Int("penalty_us", int64(fr.PenaltyRemain)))
		s.cfg.Handler(f)
	case TemplateSessionReject:
		sr, err := f.SessionReject()
		if err != nil {
			s.fail(err)
			return false
		}
		s.met.onSessionReject(sr.Reason)
		s.cfg.Handler(f)
	case TemplateBusinessMessageReject:
		s.met.businessRej.Inc()
		s.cfg.Handler(f)
	case TemplateEstablishmentAck, TemplateEstablishmentReject:
		s.log.Warn("twime: unexpected establish frame after handshake", moexlog.Int("template", int64(f.Template())))
	default:
		if IsSessionTemplate(f.Template()) {
			s.log.Warn("twime: unknown session-layer frame", moexlog.Int("template", int64(f.Template())))
			return true
		}
		f.SeqNo, f.Retransmitted = s.assignSeq()
		s.cfg.Handler(f)
	}
	return true
}

// assignSeq numbers an application frame: retransmitted frames take
// the numbers announced by Retransmission, live frames advance nextSeq.
func (s *Session) assignSeq() (uint64, bool) {
	s.recMu.Lock()
	defer s.recMu.Unlock()
	if s.recActive && s.recRemaining > 0 {
		seq := s.recSeq
		s.recSeq++
		s.recRemaining--
		s.met.retransmitted.Inc()
		if s.recRemaining == 0 {
			s.recFrom = s.recSeq
			if err := s.requestNextChunkLocked(); err != nil {
				s.log.Warn("twime: retransmit request failed", moexlog.Err(err))
			}
		}
		return seq, true
	}
	return s.nextSeq.Add(1) - 1, false
}

func (s *Session) onRetransmission(r Retransmission) {
	s.recMu.Lock()
	defer s.recMu.Unlock()
	if !s.recActive {
		s.log.Warn("twime: unsolicited Retransmission", moexlog.Int("next_seq_no", int64(r.NextSeqNo)),
			moexlog.Int("count", int64(r.Count)))
		return
	}
	if r.NextSeqNo != s.recFrom {
		s.log.Warn("twime: Retransmission starts off the requested seq",
			moexlog.Int("requested", int64(s.recFrom)), moexlog.Int("got", int64(r.NextSeqNo)))
		s.met.seqMismatch.Inc()
	}
	if r.Count == 0 {
		// Nothing available for the range: stop asking, keep the live
		// counter as announced in EstablishmentAck.
		s.log.Warn("twime: empty Retransmission, giving up on the gap",
			moexlog.Int("from", int64(s.recFrom)), moexlog.Int("to", int64(s.recTo)))
		s.finishRecoveryLocked()
		return
	}
	s.recSeq = r.NextSeqNo
	s.recRemaining = r.Count
}

// onServerSequence checks the gateway's heartbeat counter against ours.
// Over TCP a mismatch means messages were lost between sessions that
// Dial did not recover, or a bug; the gateway is authoritative.
func (s *Session) onServerSequence(serverNext uint64) {
	cur := s.nextSeq.Load()
	if serverNext == cur {
		return
	}
	s.met.seqMismatch.Inc()
	s.recMu.Lock()
	defer s.recMu.Unlock()
	if serverNext < cur {
		s.log.Warn("twime: gateway heartbeat behind local counter, adopting",
			moexlog.Int("local", int64(cur)), moexlog.Int("server", int64(serverNext)))
		s.nextSeq.Store(serverNext)
		return
	}
	if s.recActive {
		return
	}
	gap := serverNext - cur
	s.nextSeq.Store(serverNext)
	if s.cfg.MaxRecoverMessages < 0 || gap > uint64(s.cfg.MaxRecoverMessages) {
		s.log.Warn("twime: gateway heartbeat ahead of local counter, gap not recovered",
			moexlog.Int("local", int64(cur)), moexlog.Int("server", int64(serverNext)))
		return
	}
	s.startRecoveryLocked(cur, serverNext)
	if err := s.requestNextChunkLocked(); err != nil {
		s.log.Warn("twime: retransmit request failed", moexlog.Err(err))
	}
}

// keepaliveLoop sends a client heartbeat when nothing else has been sent
// for half the promised interval, and declares the gateway dead after
// two of its own intervals of silence.
func (s *Session) keepaliveLoop() {
	period := s.cfg.KeepaliveInterval / 2
	if period < MinHeartbeatSpacing {
		period = MinHeartbeatSpacing
	}
	tick := time.NewTicker(period)
	defer tick.Stop()
	silence := 2 * s.serverKeepalive
	for {
		select {
		case <-s.done:
			return
		case now := <-tick.C:
			if now.Sub(time.Unix(0, s.lastRecv.Load())) > silence {
				s.log.Warn("twime: gateway silent", moexlog.Int("silence_ms", int64(silence/time.Millisecond)))
				s.fail(ErrServerSilent)
				return
			}
			if now.Sub(time.Unix(0, s.lastSent.Load())) >= period {
				if err := s.send(Sequence{NextSeqNo: NullUint64}); err != nil {
					return
				}
				s.met.heartbeats.Inc()
			}
		}
	}
}
