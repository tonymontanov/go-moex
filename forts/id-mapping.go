/*
FILE: forts/id-mapping.go

DESCRIPTION:
ClOrdID generation and request/response correlation for the FIX order-entry
flow. FIX Gate's Execution Report always echoes back the ClOrdID that
originated it (§4.1.6), so this is the only correlation key available —
there is no "request id" wrapper the way OKX/Bybit REST responses have one.

correlator is intentionally generic over the outcome type (*types.OrderInfo
for New/Cancel/Replace, *massCancelResult for Order Mass Cancel) via Go
generics, so trading.go doesn't need three near-identical pending-map
implementations (see the "no parallel copy-paste" architecture principle).
*/
package forts

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// clOrdIDGenerator produces unique ClOrdID values (max 20 chars per FIX
// Gate's String20 — see fields.go doc). Format: "GM" + base36 counter,
// comfortably under the limit for the lifetime of one process.
type clOrdIDGenerator struct {
	counter atomic.Int64
	prefix  string
}

func newClOrdIDGenerator(prefix string) *clOrdIDGenerator {
	if prefix == "" {
		prefix = "GM"
	}
	return &clOrdIDGenerator{prefix: prefix}
}

func (g *clOrdIDGenerator) Next() string {
	var n int64 = g.counter.Add(1)
	return fmt.Sprintf("%s%d", g.prefix, n)
}

// correlator[T] tracks in-flight FIX requests keyed by ClOrdID, resolved by
// the session's AppHandler when the matching Execution Report /
// OrderCancelReject / OrderMassCancelReport arrives.
type correlator[T any] struct {
	mu      sync.Mutex
	pending map[string]chan result[T]
}

type result[T any] struct {
	value T
	err   error
}

func newCorrelator[T any]() *correlator[T] {
	return &correlator[T]{pending: make(map[string]chan result[T])}
}

// Register creates a wait slot for clOrdID. Must be called BEFORE the FIX
// message is sent, to close the race between send and reply.
func (c *correlator[T]) Register(clOrdID string) chan result[T] {
	var ch chan result[T] = make(chan result[T], 1)
	c.mu.Lock()
	c.pending[clOrdID] = ch
	c.mu.Unlock()
	return ch
}

// Resolve delivers value/err to the waiter for clOrdID, if any is
// registered. Returns false if no one was waiting (e.g. an unsolicited
// Execution Report, or a duplicate late reply for an already-resolved
// request — both are normal, not errors).
func (c *correlator[T]) Resolve(clOrdID string, value T, err error) bool {
	c.mu.Lock()
	ch, ok := c.pending[clOrdID]
	if ok {
		delete(c.pending, clOrdID)
	}
	c.mu.Unlock()
	if !ok {
		return false
	}
	ch <- result[T]{value: value, err: err}
	return true
}

// failAll delivers err to every waiter and clears the table — used when
// the transport session ends so in-flight calls do not hang until their
// ctx expires.
func (c *correlator[T]) failAll(err error) {
	c.mu.Lock()
	pending := c.pending
	c.pending = make(map[string]chan result[T])
	c.mu.Unlock()
	var zero T
	for _, ch := range pending {
		ch <- result[T]{value: zero, err: err}
	}
}

// Wait blocks on ch until a value arrives or ctx is done. On ctx
// cancellation the registration is cleaned up so it can't leak.
func (c *correlator[T]) Wait(ctx context.Context, clOrdID string, ch chan result[T]) (T, error) {
	select {
	case r := <-ch:
		return r.value, r.err
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, clOrdID)
		c.mu.Unlock()
		var zero T
		return zero, ctx.Err()
	}
}
