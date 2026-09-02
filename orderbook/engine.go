/*
FILE: orderbook/engine.go

DESCRIPTION:
Order-level ("L3") local order book with price-level ("L2") aggregation.
One Engine instance = one instrument's book (bid and ask sides together,
since best-bid/best-ask queries need both).

DESIGN NOTES:
  - Per-order state (orders map) is the source of truth; price levels are
    kept incrementally in two sorted slices (best first): bids descending,
    asks ascending. A level update is a binary search + in-place add; a
    new/emptied level is a memmove within the slice. FORTS books have up
    to a few thousand orders and a few hundred levels, so the memmove is
    cheap and — unlike the previous map+sort — reading the top of book is
    O(1) and TopN is a copy of N entries into the caller's buffer.
  - No heap allocation in steady state: slices are reused (capacity kept
    across Clear/LoadSnapshot), the orders map is reused, and TopN writes
    into a caller-provided slice. Enforced by TestEngineZeroAlloc.
  - Sequence policy: seq 0 is "unsequenced" (applied, not tracked — SIMBA
    re-broadcasts after EmptyBook carry RptSeq=0); seq <= last is
    ErrDuplicate; seq > last+1 is ErrSequenceGap; both leave the book
    untouched. ErrUnknownOrder consumes seq (the update was valid on the
    feed) — callers count it and consider a resync, never Clear() on it.
  - NOT safe for unsynchronized concurrent calls; callers serialise per
    instrument (forts.BookSession holds one mutex for all its books).
*/
package orderbook

import "sort"

type orderState struct {
	side          Side
	priceMantissa int64
	size          int64
}

// Engine — local order book for one instrument.
type Engine struct {
	orders map[int64]orderState

	bids []Level // descending by price.
	asks []Level // ascending by price.

	lastSeq     uint64
	haveLastSeq bool
}

// NewEngine creates an empty book.
func NewEngine() *Engine {
	return &Engine{
		orders: make(map[int64]orderState, 64),
		bids:   make([]Level, 0, 64),
		asks:   make([]Level, 0, 64),
	}
}

// LoadSnapshot replaces the entire book with entries and resets the
// sequence tracker to seq (the snapshot's own sequence marker — for SIMBA
// SPECTRA, OrderBookSnapshot.RptSeq). Subsequent ApplyDelta calls are
// expected to carry seq+1, seq+2, ...
func (e *Engine) LoadSnapshot(entries []SnapshotEntry, seq uint64) {
	e.Clear()
	for _, entry := range entries {
		if old, ok := e.orders[entry.OrderID]; ok {
			e.addToLevel(old.side, old.priceMantissa, -old.size)
		}
		e.orders[entry.OrderID] = orderState{side: entry.Side, priceMantissa: entry.PriceMantissa, size: entry.Size}
		e.addToLevel(entry.Side, entry.PriceMantissa, entry.Size)
	}
	e.lastSeq = seq
	e.haveLastSeq = seq != 0
}

// Clear empties the book AND resets the sequence tracker. Used when the
// feed announces EmptyBook (spec §4.2.7/§4.2.8): the books are then
// re-broadcast as unsequenced updates (seq 0, PossDupFlag) and the next
// real update restarts the per-instrument numbering, so any remembered
// sequence would only produce false gaps. Capacity is kept.
func (e *Engine) Clear() {
	for id := range e.orders {
		delete(e.orders, id)
	}
	e.bids = e.bids[:0]
	e.asks = e.asks[:0]
	e.haveLastSeq = false
	e.lastSeq = 0
}

// ResetSeq forgets the sequence tracker without touching the book (feed
// SequenceReset: numbering restarts, books stay until EmptyBook).
func (e *Engine) ResetSeq() {
	e.haveLastSeq = false
	e.lastSeq = 0
}

// checkSeq validates seq against the tracker without consuming it.
func (e *Engine) checkSeq(seq uint64) error {
	if seq == 0 || !e.haveLastSeq {
		return nil
	}
	if seq <= e.lastSeq {
		return ErrDuplicate
	}
	if seq != e.lastSeq+1 {
		return ErrSequenceGap
	}
	return nil
}

func (e *Engine) commitSeq(seq uint64) {
	if seq == 0 {
		return
	}
	e.lastSeq = seq
	e.haveLastSeq = true
}

// AdvanceSeq consumes seq without touching the book — for feed messages
// that carry a sequence number but must not change the book (technical
// trades with a null price, NonQuote/OTC records per spec §4.2.9).
// Production feeds number those messages too; skipping them would make
// the next real update look like a gap.
func (e *Engine) AdvanceSeq(seq uint64) error {
	if err := e.checkSeq(seq); err != nil {
		return err
	}
	e.commitSeq(seq)
	return nil
}

// ApplyDelta applies one incremental update. seq must be lastSeq+1 (or 0
// for unsequenced updates): a duplicate returns ErrDuplicate, a gap
// ErrSequenceGap, both leaving the book untouched — on a gap the caller
// must resync (LoadSnapshot) before applying further deltas.
// ErrUnknownOrder consumes seq (the update was valid on the feed).
func (e *Engine) ApplyDelta(orderID int64, side Side, priceMantissa int64, size int64, action UpdateAction, seq uint64) error {
	if err := e.checkSeq(seq); err != nil {
		return err
	}

	switch action {
	case ActionUpsert:
		if old, ok := e.orders[orderID]; ok {
			e.addToLevel(old.side, old.priceMantissa, -old.size)
		}
		e.orders[orderID] = orderState{side: side, priceMantissa: priceMantissa, size: size}
		e.addToLevel(side, priceMantissa, size)
	case ActionDelete:
		old, ok := e.orders[orderID]
		if !ok {
			e.commitSeq(seq)
			return ErrUnknownOrder
		}
		delete(e.orders, orderID)
		e.addToLevel(old.side, old.priceMantissa, -old.size)
	}

	e.commitSeq(seq)
	return nil
}

// levelIndex returns the position of price in the side's sorted slice and
// whether it is present (binary search; best price at index 0).
func levelIndex(levels []Level, side Side, price int64) (int, bool) {
	var i int
	if side == SideBid {
		i = sort.Search(len(levels), func(k int) bool { return levels[k].PriceMantissa <= price })
	} else {
		i = sort.Search(len(levels), func(k int) bool { return levels[k].PriceMantissa >= price })
	}
	return i, i < len(levels) && levels[i].PriceMantissa == price
}

func (e *Engine) addToLevel(side Side, priceMantissa int64, delta int64) {
	var levels *[]Level = &e.asks
	if side == SideBid {
		levels = &e.bids
	}
	var i int
	var found bool
	i, found = levelIndex(*levels, side, priceMantissa)
	if found {
		var newSize int64 = (*levels)[i].Size + delta
		if newSize > 0 {
			(*levels)[i].Size = newSize
			return
		}
		// Level emptied: close the gap.
		copy((*levels)[i:], (*levels)[i+1:])
		*levels = (*levels)[:len(*levels)-1]
		return
	}
	if delta <= 0 {
		return // removing from a level we do not have: nothing to do.
	}
	// New level: open a slot at i.
	*levels = append(*levels, Level{})
	copy((*levels)[i+1:], (*levels)[i:])
	(*levels)[i] = Level{PriceMantissa: priceMantissa, Size: delta}
}

// BestBid returns the best (highest) bid price mantissa and its aggregate
// size. ok is false if the bid side is empty.
func (e *Engine) BestBid() (priceMantissa int64, size int64, ok bool) {
	if len(e.bids) == 0 {
		return 0, 0, false
	}
	return e.bids[0].PriceMantissa, e.bids[0].Size, true
}

// BestAsk returns the best (lowest) ask price mantissa and its aggregate
// size. ok is false if the ask side is empty.
func (e *Engine) BestAsk() (priceMantissa int64, size int64, ok bool) {
	if len(e.asks) == 0 {
		return 0, 0, false
	}
	return e.asks[0].PriceMantissa, e.asks[0].Size, true
}

// TopN copies up to n best levels of side into dst (reusing its backing
// array; dst may be nil) and returns the filled slice. n <= 0 copies every
// level. No allocation when cap(dst) suffices — the hot-path read API.
func (e *Engine) TopN(side Side, n int, dst []Level) []Level {
	var levels []Level = e.asks
	if side == SideBid {
		levels = e.bids
	}
	if n <= 0 || n > len(levels) {
		n = len(levels)
	}
	dst = dst[:0]
	if cap(dst) < n {
		dst = make([]Level, 0, n)
	}
	return append(dst, levels[:n]...)
}

// Levels returns up to n aggregated price levels for side, best-first
// (descending for bids, ascending for asks), in a fresh slice. n <= 0
// returns every level. Convenience wrapper over TopN for non-hot callers.
func (e *Engine) Levels(side Side, n int) []Level {
	return e.TopN(side, n, nil)
}

// LevelCount returns the number of price levels on side.
func (e *Engine) LevelCount(side Side) int {
	if side == SideBid {
		return len(e.bids)
	}
	return len(e.asks)
}

// LastSeq returns the last applied sequence number and whether one has
// been observed yet (via LoadSnapshot or ApplyDelta).
func (e *Engine) LastSeq() (uint64, bool) { return e.lastSeq, e.haveLastSeq }

// OrderCount returns the number of individually tracked resting orders
// (diagnostics/tests).
func (e *Engine) OrderCount() int { return len(e.orders) }

// ForEachOrder visits every resting order (unspecified order). For
// diagnostics and oracle tests.
func (e *Engine) ForEachOrder(fn func(orderID int64, side Side, priceMantissa int64, size int64)) {
	for id, o := range e.orders {
		fn(id, o.side, o.priceMantissa, o.size)
	}
}
