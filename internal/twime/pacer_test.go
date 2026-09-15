package twime

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
func newFakeClock() *fakeClock               { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }
func mustReserve(t *testing.T, p *Pacer, n int) time.Duration {
	t.Helper()
	w, err := p.Reserve(n)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestPacerSlidingWindow(t *testing.T) {
	clk := newFakeClock()
	p := NewPacer(3, clk.now)
	for i := 0; i < 3; i++ {
		if w := mustReserve(t, p, 1); w != 0 {
			t.Fatalf("send %d: wait %v", i, w)
		}
		clk.advance(100 * time.Millisecond)
	}
	// 3 in flight at t=300ms; the oldest (t=0) expires at t=1s.
	if w := mustReserve(t, p, 1); w != 700*time.Millisecond {
		t.Fatalf("wait = %v, want 700ms", w)
	}
	clk.advance(700 * time.Millisecond)
	if w := mustReserve(t, p, 1); w != 0 {
		t.Fatalf("after expiry wait = %v", w)
	}
	// Now stamps at 100, 200, 1000. Two slots need the two oldest gone:
	// t=200ms+1s = 1200ms, now is 1000ms → 200ms.
	if w := mustReserve(t, p, 2); w != 200*time.Millisecond {
		t.Fatalf("batch wait = %v, want 200ms", w)
	}
	if p.InFlight() != 3 {
		t.Fatalf("in flight %d", p.InFlight())
	}
	if p.Waits() != 2 {
		t.Fatalf("waits %d", p.Waits())
	}
}

func TestPacerNeverExceedsLimitInAnyWindow(t *testing.T) {
	clk := newFakeClock()
	const limit = 30
	p := NewPacer(limit, clk.now)
	var sent []time.Time
	for i := 0; i < 500; i++ {
		w, err := p.Reserve(1 + i%3)
		if err != nil {
			t.Fatal(err)
		}
		if w > 0 {
			clk.advance(w)
			continue
		}
		for j := 0; j < 1+i%3; j++ {
			sent = append(sent, clk.now())
		}
		clk.advance(7 * time.Millisecond)
	}
	for i := range sent {
		cnt := 0
		for j := i; j < len(sent) && sent[j].Sub(sent[i]) < time.Second; j++ {
			cnt++
		}
		if cnt > limit {
			t.Fatalf("window starting at %v holds %d sends", sent[i], cnt)
		}
	}
	if len(sent) < 400 {
		t.Fatalf("throughput too low: %d sends", len(sent))
	}
}

func TestPacerPenalty(t *testing.T) {
	clk := newFakeClock()
	p := NewPacer(30, clk.now)
	p.Penalize(250 * time.Millisecond)
	if w := mustReserve(t, p, 1); w != 250*time.Millisecond {
		t.Fatalf("wait = %v", w)
	}
	if p.Penalty() != 250*time.Millisecond {
		t.Fatalf("penalty = %v", p.Penalty())
	}
	p.Penalize(100 * time.Millisecond) // shorter penalty must not shrink the pause
	if p.Penalty() != 250*time.Millisecond {
		t.Fatalf("penalty shrank to %v", p.Penalty())
	}
	clk.advance(250 * time.Millisecond)
	if w := mustReserve(t, p, 1); w != 0 {
		t.Fatalf("after penalty wait = %v", w)
	}
	if p.Penalty() != 0 {
		t.Fatal("penalty should be over")
	}
}

func TestPacerBatchTooLarge(t *testing.T) {
	p := NewPacer(30, nil)
	if _, err := p.Reserve(31); err != ErrBatchTooLarge {
		t.Fatalf("err = %v", err)
	}
	if w := mustReserve(t, p, 30); w != 0 {
		t.Fatalf("full batch wait = %v", w)
	}
}

func TestIsTradingTemplate(t *testing.T) {
	for _, id := range []uint16{TemplateNewOrderSingle, TemplateOrderCancelRequest, TemplateOrderReplaceRequest,
		TemplateOrderMassCancelRequest, TemplateNewOrderIcebergX, TemplateOrderMassCancelByBFLimitRequest} {
		if !IsTradingTemplate(id) {
			t.Errorf("%d should count", id)
		}
	}
	for _, id := range []uint16{TemplateSequence, TemplateRetransmitRequest, TemplateTerminate, TemplateNewOrderSingleResponse} {
		if IsTradingTemplate(id) {
			t.Errorf("%d should not count", id)
		}
	}
}
