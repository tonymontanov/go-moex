/*
FILE: internal/twime/pacer.go

DESCRIPTION:
Client-side limiter for the TWIME trading-message budget (spec §3.7):
a login is allowed N trading messages (NewOrderSingle, OrderCancelRequest,
OrderReplaceRequest, OrderMassCancelRequest and their iceberg variants)
per one-second interval, N = 30 x performance units of the login. The
gateway counts per fixed second; a message over budget is answered with
FloodReject(5007) carrying PenaltyRemain (microseconds during which
everything else is rejected too), and a sustained 2x overrun ends the
session with Terminate(TooFastClient). Heartbeats and retransmit requests
have their own limits and are not counted here.

The pacer keeps a sliding one-second window of send timestamps. A
sliding window never admits more than N sends in ANY one-second span, so
it satisfies every fixed-second alignment the gateway might use — the
client cannot know the server's second boundaries. A FloodReject from the
server pauses the pacer for PenaltyRemain regardless of local state.

Batching (pipelining several messages into one TCP write) does not
discount the count: Reserve(n) charges n slots.
*/
package twime

import (
	"errors"
	"sync"
	"time"
)

// DefaultTradingRate — messages per second of the smallest login (one
// performance unit).
const DefaultTradingRate = 30

// ErrBatchTooLarge — a single batch asked for more slots than the whole
// per-second budget; it can never be admitted.
var ErrBatchTooLarge = errors.New("twime: batch exceeds the per-second trading budget")

// Pacer — sliding-window limiter. Safe for concurrent use.
type Pacer struct {
	mu           sync.Mutex
	limit        int
	window       time.Duration
	stamps       []time.Time // ring buffer, len == limit
	head         int         // index of the oldest live stamp
	n            int         // live stamps
	penaltyUntil time.Time
	now          func() time.Time
	waits        uint64 // Reserve calls that had to wait (for metrics)
}

// NewPacer — limit trading messages per second. now may be nil
// (time.Now).
func NewPacer(limit int, now func() time.Time) *Pacer {
	if limit <= 0 {
		limit = DefaultTradingRate
	}
	if now == nil {
		now = time.Now
	}
	return &Pacer{limit: limit, window: time.Second, stamps: make([]time.Time, limit), now: now}
}

// Limit — configured budget per second.
func (p *Pacer) Limit() int { return p.limit }

// evict drops stamps older than the window at time t.
func (p *Pacer) evict(t time.Time) {
	cutoff := t.Add(-p.window)
	for p.n > 0 && !p.stamps[p.head].After(cutoff) {
		p.head = (p.head + 1) % p.limit
		p.n--
	}
}

// Reserve charges n slots at the current time and returns 0, or charges
// nothing and returns how long the caller must wait before retrying. The
// caller loops: for w := p.Reserve(n); w > 0; w = p.Reserve(n) { sleep(w) }.
func (p *Pacer) Reserve(n int) (time.Duration, error) {
	if n <= 0 {
		return 0, nil
	}
	if n > p.limit {
		return 0, ErrBatchTooLarge
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	t := p.now()
	if t.Before(p.penaltyUntil) {
		p.waits++
		return p.penaltyUntil.Sub(t), nil
	}
	p.evict(t)
	if p.n+n > p.limit {
		// The k-th oldest live stamp must expire first, k = n_live + n - limit.
		k := p.n + n - p.limit
		idx := (p.head + k - 1) % p.limit
		p.waits++
		w := p.stamps[idx].Add(p.window).Sub(t)
		if w <= 0 {
			w = time.Microsecond
		}
		return w, nil
	}
	for i := 0; i < n; i++ {
		p.stamps[(p.head+p.n)%p.limit] = t
		p.n++
	}
	return 0, nil
}

// Penalize pauses admission for d from now (FloodReject.PenaltyRemain).
func (p *Pacer) Penalize(d time.Duration) {
	if d <= 0 {
		return
	}
	p.mu.Lock()
	until := p.now().Add(d)
	if until.After(p.penaltyUntil) {
		p.penaltyUntil = until
	}
	p.mu.Unlock()
}

// Penalty — remaining server-imposed pause, 0 if none.
func (p *Pacer) Penalty() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if d := p.penaltyUntil.Sub(p.now()); d > 0 {
		return d
	}
	return 0
}

// InFlight — sends charged within the last second.
func (p *Pacer) InFlight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.evict(p.now())
	return p.n
}

// Waits — number of Reserve calls that could not be admitted immediately.
func (p *Pacer) Waits() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waits
}

// IsTradingTemplate — message counted against the trading budget: every
// client-to-server application message (6000..6999).
func IsTradingTemplate(id uint16) bool { return id >= 6000 && id < 7000 }
