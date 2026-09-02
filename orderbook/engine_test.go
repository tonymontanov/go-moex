package orderbook

import (
	"errors"
	"testing"
)

func TestEngineUpsertAndBestPrices(t *testing.T) {
	var e *Engine = NewEngine()

	mustApply(t, e, 1, SideBid, 9500000, 10, ActionUpsert, 1)
	mustApply(t, e, 2, SideBid, 9600000, 5, ActionUpsert, 2)
	mustApply(t, e, 3, SideAsk, 9700000, 7, ActionUpsert, 3)
	mustApply(t, e, 4, SideAsk, 9650000, 3, ActionUpsert, 4)

	price, size, ok := e.BestBid()
	if !ok || price != 9600000 || size != 5 {
		t.Fatalf("BestBid = (%d, %d, %v), want (9600000, 5, true)", price, size, ok)
	}
	price, size, ok = e.BestAsk()
	if !ok || price != 9650000 || size != 3 {
		t.Fatalf("BestAsk = (%d, %d, %v), want (9650000, 3, true)", price, size, ok)
	}
}

func TestEngineDeleteRecomputesBest(t *testing.T) {
	var e *Engine = NewEngine()
	mustApply(t, e, 1, SideBid, 9500000, 10, ActionUpsert, 1)
	mustApply(t, e, 2, SideBid, 9600000, 5, ActionUpsert, 2)

	mustApply(t, e, 2, SideBid, 0, 0, ActionDelete, 3)

	price, _, ok := e.BestBid()
	if !ok || price != 9500000 {
		t.Fatalf("BestBid after deleting the top order = (%d, %v), want (9500000, true)", price, ok)
	}
}

func TestEngineUpsertReplacesResting(t *testing.T) {
	var e *Engine = NewEngine()
	mustApply(t, e, 1, SideBid, 9500000, 10, ActionUpsert, 1)
	// Partial fill: same order id, smaller absolute remaining size.
	mustApply(t, e, 1, SideBid, 9500000, 4, ActionUpsert, 2)

	_, size, ok := e.BestBid()
	if !ok || size != 4 {
		t.Fatalf("aggregate size after partial fill = %d, want 4", size)
	}
}

func TestEngineSequenceGapIsDetectedAndBlocksMutation(t *testing.T) {
	var e *Engine = NewEngine()
	mustApply(t, e, 1, SideBid, 9500000, 10, ActionUpsert, 1)

	var err error = e.ApplyDelta(2, SideBid, 9600000, 5, ActionUpsert, 5) // gap: expected 2, got 5.
	if !errors.Is(err, ErrSequenceGap) {
		t.Fatalf("err = %v, want ErrSequenceGap", err)
	}
	if e.OrderCount() != 1 {
		t.Fatalf("OrderCount = %d, want 1 (gap must not mutate the book)", e.OrderCount())
	}
}

func TestEngineSequencePolicy(t *testing.T) {
	var e *Engine = NewEngine()
	// Unsequenced updates (seq 0) are applied and never tracked.
	if err := e.ApplyDelta(1, SideBid, 100, 1, ActionUpsert, 0); err != nil {
		t.Fatal(err)
	}
	if err := e.ApplyDelta(2, SideBid, 100, 1, ActionUpsert, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.LastSeq(); ok {
		t.Fatal("seq 0 must not be tracked")
	}
	// First sequenced update after unsequenced ones is accepted at any number.
	mustApply(t, e, 3, SideAsk, 200, 1, ActionUpsert, 7)
	// Duplicate: rejected, book untouched, not a gap.
	if err := e.ApplyDelta(4, SideAsk, 200, 1, ActionUpsert, 7); !errors.Is(err, ErrDuplicate) || e.OrderCount() != 3 {
		t.Fatalf("duplicate: err=%v orders=%d", err, e.OrderCount())
	}
	if err := e.ApplyDelta(4, SideAsk, 200, 1, ActionUpsert, 3); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("older seq: %v", err)
	}
	// AdvanceSeq consumes a number without touching the book.
	if err := e.AdvanceSeq(8); err != nil || e.OrderCount() != 3 {
		t.Fatalf("AdvanceSeq: %v", err)
	}
	if err := e.AdvanceSeq(8); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("AdvanceSeq duplicate: %v", err)
	}
	if err := e.AdvanceSeq(10); !errors.Is(err, ErrSequenceGap) {
		t.Fatalf("AdvanceSeq gap: %v", err)
	}
	mustApply(t, e, 5, SideBid, 90, 1, ActionUpsert, 9)
	// Unknown order consumes the sequence number.
	if err := e.ApplyDelta(999, SideBid, 0, 0, ActionDelete, 10); !errors.Is(err, ErrUnknownOrder) {
		t.Fatalf("unknown: %v", err)
	}
	if seq, _ := e.LastSeq(); seq != 10 {
		t.Fatalf("unknown order must consume seq, got %d", seq)
	}
	// Clear resets the tracker: next update accepted at any number.
	e.Clear()
	if _, ok := e.LastSeq(); ok || e.OrderCount() != 0 {
		t.Fatal("Clear must reset seq and book")
	}
	mustApply(t, e, 6, SideBid, 90, 1, ActionUpsert, 1)
}

func TestEngineLoadSnapshotResetsSeqAndBook(t *testing.T) {
	var e *Engine = NewEngine()
	mustApply(t, e, 1, SideBid, 9500000, 10, ActionUpsert, 1)

	e.LoadSnapshot([]SnapshotEntry{
		{OrderID: 10, Side: SideBid, PriceMantissa: 9000000, Size: 1},
		{OrderID: 11, Side: SideAsk, PriceMantissa: 9100000, Size: 2},
	}, 100)

	seq, ok := e.LastSeq()
	if !ok || seq != 100 {
		t.Fatalf("LastSeq = (%d, %v), want (100, true)", seq, ok)
	}
	if e.OrderCount() != 2 {
		t.Fatalf("OrderCount = %d, want 2", e.OrderCount())
	}

	// Continuing from the snapshot's seq works without a gap.
	if err := e.ApplyDelta(12, SideBid, 9050000, 3, ActionUpsert, 101); err != nil {
		t.Fatalf("ApplyDelta after LoadSnapshot: %v", err)
	}
}

func TestEngineLevelsSortedBestFirst(t *testing.T) {
	var e *Engine = NewEngine()
	mustApply(t, e, 1, SideBid, 9500000, 10, ActionUpsert, 1)
	mustApply(t, e, 2, SideBid, 9600000, 5, ActionUpsert, 2)
	mustApply(t, e, 3, SideBid, 9400000, 8, ActionUpsert, 3)

	var levels []Level = e.Levels(SideBid, 0)
	if len(levels) != 3 {
		t.Fatalf("len(levels) = %d, want 3", len(levels))
	}
	if levels[0].PriceMantissa != 9600000 || levels[1].PriceMantissa != 9500000 || levels[2].PriceMantissa != 9400000 {
		t.Fatalf("levels not sorted descending: %+v", levels)
	}
}

func mustApply(t *testing.T, e *Engine, orderID int64, side Side, price int64, size int64, action UpdateAction, seq uint64) {
	t.Helper()
	if err := e.ApplyDelta(orderID, side, price, size, action, seq); err != nil {
		t.Fatalf("ApplyDelta(seq=%d): %v", seq, err)
	}
}
