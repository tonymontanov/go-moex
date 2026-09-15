package twime

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// fakeGateway — a scriptable TWIME gateway for session tests. It speaks
// the session layer the way the spec describes: answers Establish with
// Ack (or Reject), numbers application messages from nextSeq, keeps a
// log for RetransmitRequest, answers Terminate with Terminate(Finished)
// and closes, and answers client heartbeats with its own Sequence.
type fakeGateway struct {
	t  *testing.T
	ln net.Listener

	mu        sync.Mutex
	conn      net.Conn
	nextSeq   uint64
	log       map[uint64][]byte // seq -> full frame
	keepalive uint32
	reject    *EstablishmentRejectCode
	silent    bool // do not heartbeat, do not answer anything after Ack
	floodEach int  // every N-th trading message gets FloodReject
	tradingN  int

	inbound     chan Frame // copies of client frames (Body copied)
	established chan struct{}
	closed      chan struct{}
}

func newFakeGateway(t *testing.T) *fakeGateway {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &fakeGateway{
		t: t, ln: ln, nextSeq: 1, log: map[uint64][]byte{}, keepalive: 1000,
		inbound: make(chan Frame, 1024), established: make(chan struct{}, 1), closed: make(chan struct{}, 1),
	}
	go g.acceptLoop()
	t.Cleanup(func() { _ = ln.Close() })
	return g
}

func (g *fakeGateway) addr() string { return g.ln.Addr().String() }

func (g *fakeGateway) acceptLoop() {
	for {
		c, err := g.ln.Accept()
		if err != nil {
			return
		}
		g.mu.Lock()
		g.conn = c
		g.mu.Unlock()
		g.serve(c)
		g.closed <- struct{}{}
	}
}

func (g *fakeGateway) write(b []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conn != nil {
		_, _ = g.conn.Write(b)
	}
}

// push sends an application message to the client with the next
// sequence number and records it for retransmission.
func (g *fakeGateway) push(m Marshaler) uint64 {
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

// record stores a message in the log WITHOUT sending it — the client
// "missed" it while disconnected.
func (g *fakeGateway) record(m Marshaler) uint64 {
	frame := m.Append(nil)
	g.mu.Lock()
	seq := g.nextSeq
	g.log[seq] = frame
	g.nextSeq++
	g.mu.Unlock()
	return seq
}

func (g *fakeGateway) serve(c net.Conn) {
	defer c.Close()
	rd := NewReader(c)
	for {
		f, err := rd.ReadFrame()
		if err != nil {
			return
		}
		cp := f
		cp.Body = append([]byte(nil), f.Body...)
		g.inbound <- cp
		switch f.Template() {
		case TemplateEstablish:
			e := f.Body
			ts := uint64(e[0]) | uint64(e[1])<<8 | uint64(e[2])<<16 | uint64(e[3])<<24 |
				uint64(e[4])<<32 | uint64(e[5])<<40 | uint64(e[6])<<48 | uint64(e[7])<<56
			g.mu.Lock()
			rej := g.reject
			next := g.nextSeq
			ka := g.keepalive
			g.mu.Unlock()
			if rej != nil {
				g.write(EstablishmentReject{RequestTimestamp: ts, Code: *rej}.Append(nil))
				return
			}
			g.write(EstablishmentAck{RequestTimestamp: ts, KeepaliveInterval: ka, NextSeqNo: next}.Append(nil))
			select {
			case g.established <- struct{}{}:
			default:
			}
		case TemplateSequence:
			if g.silent {
				continue
			}
			g.mu.Lock()
			next := g.nextSeq
			g.mu.Unlock()
			g.write(Sequence{NextSeqNo: next}.Append(nil))
		case TemplateRetransmitRequest:
			rr := RetransmitRequest{}
			b := f.Body
			rr.FromSeqNo = uint64(b[8]) | uint64(b[9])<<8 | uint64(b[10])<<16 | uint64(b[11])<<24 |
				uint64(b[12])<<32 | uint64(b[13])<<40 | uint64(b[14])<<48 | uint64(b[15])<<56
			rr.Count = uint32(b[16]) | uint32(b[17])<<8 | uint32(b[18])<<16 | uint32(b[19])<<24
			if rr.Count > MaxRetransmitCount {
				g.write(Terminate{Code: TerminationReRequestOutOfBounds}.Append(nil))
				return
			}
			g.mu.Lock()
			var out []byte
			out = Retransmission{NextSeqNo: rr.FromSeqNo, Count: rr.Count}.Append(out)
			for i := uint64(0); i < uint64(rr.Count); i++ {
				out = append(out, g.log[rr.FromSeqNo+i]...)
			}
			g.mu.Unlock()
			g.write(out)
		case TemplateTerminate:
			g.write(Terminate{Code: TerminationFinished}.Append(nil))
			return
		default:
			if !IsTradingTemplate(f.Template()) {
				continue
			}
			g.mu.Lock()
			g.tradingN++
			flood := g.floodEach > 0 && g.tradingN%g.floodEach == 0
			g.mu.Unlock()
			clOrdID := uint64(f.Body[0]) | uint64(f.Body[1])<<8 | uint64(f.Body[2])<<16 | uint64(f.Body[3])<<24 |
				uint64(f.Body[4])<<32 | uint64(f.Body[5])<<40 | uint64(f.Body[6])<<48 | uint64(f.Body[7])<<56
			if flood {
				g.write(FloodReject{ClOrdID: clOrdID, QueueSize: 31, PenaltyRemain: 300_000}.Append(nil))
				continue
			}
			switch f.Template() {
			case TemplateNewOrderSingle:
				g.push(NewOrderSingleResponse{ClOrdID: clOrdID, OrderID: int64(clOrdID) * 10, Flags: FlagDay, OrderQty: 1})
			case TemplateOrderCancelRequest:
				g.push(OrderCancelResponse{ClOrdID: clOrdID, Flags: FlagCancel})
			case TemplateOrderMassCancelRequest:
				g.push(OrderMassCancelResponse{ClOrdID: clOrdID, TotalAffectedOrders: 0})
			default:
				g.push(BusinessMessageReject{ClOrdID: clOrdID, OrdRejReason: 1})
			}
		}
	}
}

// expect waits for the next client frame of the given template, failing
// on anything else that is not a heartbeat.
func (g *fakeGateway) expect(template uint16, timeout time.Duration) Frame {
	g.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case f := <-g.inbound:
			if f.Template() == template {
				return f
			}
			if f.Template() == TemplateSequence {
				continue
			}
			g.t.Fatalf("gateway got %s, want %s", TemplateName(f.Template()), TemplateName(template))
		case <-deadline:
			g.t.Fatalf("gateway timed out waiting for %s", TemplateName(template))
		}
	}
}

func (g *fakeGateway) drop() {
	g.mu.Lock()
	c := g.conn
	g.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}

func isClosedErr(err error) bool {
	return errors.Is(err, ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)
}
