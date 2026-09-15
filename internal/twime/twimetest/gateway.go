/*
FILE: internal/twime/twimetest/gateway.go

DESCRIPTION:
A scriptable in-process TWIME gateway for tests (internal/twime and
forts/). It speaks the session layer the way the spec describes: answers
Establish with EstablishmentAck (or a configured EstablishmentReject),
numbers application messages from NextSeq, keeps a log for
RetransmitRequest (chunks of at most twime.MaxRetransmitCount), answers
client heartbeats with its own Sequence, answers Terminate with
Terminate(Finished) and closes. Trading requests get the obvious
response (NewOrderSingle → NewOrderSingleResponse with OrderID =
ClOrdID*10, cancel → OrderCancelResponse, replace →
OrderReplaceResponse, mass cancel → OrderMassCancelResponse) unless a
Script hook overrides it.

Not a matching engine: no book, no fills unless the test pushes them
with Push.
*/
package twimetest

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/tonymontanov/go-moex/internal/twime"
)

// Script — optional per-request override. Return true to suppress the
// default response; use g.Push / g.Write to answer yourself.
type Script func(g *Gateway, f twime.Frame) (handled bool)

// Gateway — one listening fake gateway.
type Gateway struct {
	t  testing.TB
	ln net.Listener

	mu        sync.Mutex
	conn      net.Conn
	nextSeq   uint64
	log       map[uint64][]byte
	keepalive uint32
	reject    *twime.EstablishmentRejectCode
	silent    bool
	floodEach int
	tradingN  int
	script    Script

	inbound     chan twime.Frame
	established chan struct{}
	closed      chan struct{}
}

// New starts a gateway on 127.0.0.1:0 (closed on test cleanup).
func New(t testing.TB) *Gateway {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &Gateway{
		t: t, ln: ln, nextSeq: 1, log: map[uint64][]byte{}, keepalive: 1000,
		inbound: make(chan twime.Frame, 4096), established: make(chan struct{}, 1), closed: make(chan struct{}, 16),
	}
	go g.acceptLoop()
	t.Cleanup(func() { _ = ln.Close(); g.Drop() })
	return g
}

// Addr — host:port to dial.
func (g *Gateway) Addr() string { return g.ln.Addr().String() }

// SetNextSeq — the sequence number the next application message gets
// (and what EstablishmentAck announces).
func (g *Gateway) SetNextSeq(n uint64) { g.mu.Lock(); g.nextSeq = n; g.mu.Unlock() }

// NextSeq — current value.
func (g *Gateway) NextSeq() uint64 { g.mu.Lock(); defer g.mu.Unlock(); return g.nextSeq }

// SetKeepalive — KeepaliveInterval (ms) announced in EstablishmentAck.
func (g *Gateway) SetKeepalive(ms uint32) { g.mu.Lock(); g.keepalive = ms; g.mu.Unlock() }

// RejectEstablish — answer Establish with the given code.
func (g *Gateway) RejectEstablish(code twime.EstablishmentRejectCode) {
	g.mu.Lock()
	g.reject = &code
	g.mu.Unlock()
}

// SetSilent — stop answering heartbeats (simulates a dead gateway).
func (g *Gateway) SetSilent(v bool) { g.mu.Lock(); g.silent = v; g.mu.Unlock() }

// SetFloodEach — every n-th trading message is answered with FloodReject
// (PenaltyRemain 300 ms); 0 disables.
func (g *Gateway) SetFloodEach(n int) { g.mu.Lock(); g.floodEach = n; g.mu.Unlock() }

// SetScript — install a per-request hook.
func (g *Gateway) SetScript(s Script) { g.mu.Lock(); g.script = s; g.mu.Unlock() }

// Inbound — every client frame (Body copied), including heartbeats.
func (g *Gateway) Inbound() <-chan twime.Frame { return g.inbound }

// Closed — signalled when a client connection ends.
func (g *Gateway) Closed() <-chan struct{} { return g.closed }

func (g *Gateway) acceptLoop() {
	for {
		c, err := g.ln.Accept()
		if err != nil {
			return
		}
		g.mu.Lock()
		g.conn = c
		g.mu.Unlock()
		g.serve(c)
		select {
		case g.closed <- struct{}{}:
		default:
		}
	}
}

// Write sends raw bytes to the connected client.
func (g *Gateway) Write(b []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conn != nil {
		_, _ = g.conn.Write(b)
	}
}

// Push sends an application message with the next sequence number and
// records it for retransmission. Returns the number assigned.
func (g *Gateway) Push(m twime.Marshaler) uint64 {
	frame := m.Append(nil)
	g.mu.Lock()
	seq := g.nextSeq
	g.log[seq] = frame
	g.nextSeq++
	c := g.conn
	g.mu.Unlock()
	if c != nil {
		_, _ = c.Write(frame)
	}
	return seq
}

// Record stores an application message in the retransmit log WITHOUT
// sending it — the client "missed" it while disconnected.
func (g *Gateway) Record(m twime.Marshaler) uint64 {
	frame := m.Append(nil)
	g.mu.Lock()
	seq := g.nextSeq
	g.log[seq] = frame
	g.nextSeq++
	g.mu.Unlock()
	return seq
}

// Drop closes the client connection without a Terminate.
func (g *Gateway) Drop() {
	g.mu.Lock()
	c := g.conn
	g.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}

// Expect waits for the next client frame of the given template, skipping
// heartbeats and failing on anything else.
func (g *Gateway) Expect(template uint16, timeout time.Duration) twime.Frame {
	g.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case f := <-g.inbound:
			if f.Template() == template {
				return f
			}
			if f.Template() == twime.TemplateSequence {
				continue
			}
			g.t.Fatalf("gateway got %s, want %s", twime.TemplateName(f.Template()), twime.TemplateName(template))
		case <-deadline:
			g.t.Fatalf("gateway timed out waiting for %s", twime.TemplateName(template))
		}
	}
}

// ClOrdID — the ClOrdID field of any client application frame (always
// at offset 0 in the schema).
func ClOrdID(f twime.Frame) uint64 { return binary.LittleEndian.Uint64(f.Body[0:8]) }

func (g *Gateway) serve(c net.Conn) {
	defer c.Close()
	rd := twime.NewReader(c)
	for {
		f, err := rd.ReadFrame()
		if err != nil {
			return
		}
		cp := f
		cp.Body = append([]byte(nil), f.Body...)
		select {
		case g.inbound <- cp:
		default:
		}
		g.mu.Lock()
		script := g.script
		g.mu.Unlock()
		if script != nil && script(g, cp) {
			continue
		}
		switch f.Template() {
		case twime.TemplateEstablish:
			ts := binary.LittleEndian.Uint64(f.Body[0:8])
			g.mu.Lock()
			rej := g.reject
			next := g.nextSeq
			ka := g.keepalive
			g.mu.Unlock()
			if rej != nil {
				g.Write(twime.EstablishmentReject{RequestTimestamp: ts, Code: *rej}.Append(nil))
				return
			}
			g.Write(twime.EstablishmentAck{RequestTimestamp: ts, KeepaliveInterval: ka, NextSeqNo: next}.Append(nil))
			select {
			case g.established <- struct{}{}:
			default:
			}
		case twime.TemplateSequence:
			g.mu.Lock()
			silent := g.silent
			next := g.nextSeq
			g.mu.Unlock()
			if silent {
				continue
			}
			g.Write(twime.Sequence{NextSeqNo: next}.Append(nil))
		case twime.TemplateRetransmitRequest:
			from := binary.LittleEndian.Uint64(f.Body[8:16])
			count := binary.LittleEndian.Uint32(f.Body[16:20])
			if count > twime.MaxRetransmitCount {
				g.Write(twime.Terminate{Code: twime.TerminationReRequestOutOfBounds}.Append(nil))
				return
			}
			g.mu.Lock()
			out := twime.Retransmission{NextSeqNo: from, Count: count}.Append(nil)
			for i := uint64(0); i < uint64(count); i++ {
				out = append(out, g.log[from+i]...)
			}
			g.mu.Unlock()
			g.Write(out)
		case twime.TemplateTerminate:
			g.Write(twime.Terminate{Code: twime.TerminationFinished}.Append(nil))
			return
		default:
			if !twime.IsTradingTemplate(f.Template()) {
				continue
			}
			g.mu.Lock()
			g.tradingN++
			flood := g.floodEach > 0 && g.tradingN%g.floodEach == 0
			g.mu.Unlock()
			clOrdID := ClOrdID(cp)
			if flood {
				g.Write(twime.FloodReject{ClOrdID: clOrdID, QueueSize: 31, PenaltyRemain: 300_000}.Append(nil))
				continue
			}
			g.defaultResponse(cp, clOrdID)
		}
	}
}

func (g *Gateway) defaultResponse(f twime.Frame, clOrdID uint64) {
	now := twime.TimestampOf(time.Now())
	switch f.Template() {
	case twime.TemplateNewOrderSingle:
		b := f.Body
		g.Push(twime.NewOrderSingleResponse{
			ClOrdID:          clOrdID,
			Timestamp:        now,
			ExpireDate:       binary.LittleEndian.Uint64(b[8:16]),
			OrderID:          int64(clOrdID) * 10,
			Flags:            twime.FlagDay,
			Price:            int64(binary.LittleEndian.Uint64(b[16:24])),
			SecurityID:       int32(binary.LittleEndian.Uint32(b[24:28])),
			OrderQty:         binary.LittleEndian.Uint32(b[32:36]),
			TradingSessionID: 6543,
			ClOrdLinkID:      int32(binary.LittleEndian.Uint32(b[28:32])),
			Side:             twime.Side(b[38]),
			ComplianceID:     twime.ComplianceID(b[36]),
		})
	case twime.TemplateOrderCancelRequest, twime.TemplateOrderIcebergCancelRequest:
		g.Push(twime.OrderCancelResponse{ClOrdID: clOrdID, Timestamp: now,
			OrderID: int64(binary.LittleEndian.Uint64(f.Body[8:16])), Flags: twime.FlagCancel, OrderQty: 1,
			TradingSessionID: 6543})
	case twime.TemplateOrderReplaceRequest:
		b := f.Body
		prev := int64(binary.LittleEndian.Uint64(b[8:16]))
		g.Push(twime.OrderReplaceResponse{ClOrdID: clOrdID, Timestamp: now, OrderID: prev + 1, PrevOrderID: prev,
			Flags: twime.FlagReplace, Price: int64(binary.LittleEndian.Uint64(b[16:24])),
			OrderQty: binary.LittleEndian.Uint32(b[24:28]), TradingSessionID: 6543,
			ComplianceID: twime.ComplianceID(b[36])})
	case twime.TemplateOrderMassCancelRequest:
		g.Push(twime.OrderMassCancelResponse{ClOrdID: clOrdID, Timestamp: now, TotalAffectedOrders: 0})
	default:
		g.Push(twime.BusinessMessageReject{ClOrdID: clOrdID, Timestamp: now, OrdRejReason: 1})
	}
}
