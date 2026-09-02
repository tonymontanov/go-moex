package orderbook

import (
	"math/rand"
	"testing"
)

// TestEngineZeroAlloc — steady-state updates (upserts/deletes across a
// stable set of price levels) and TopN reads must not allocate.
func TestEngineZeroAlloc(t *testing.T) {
	var e *Engine = NewEngine()
	var rng *rand.Rand = rand.New(rand.NewSource(1))
	// Warm up: 2000 orders across 200 levels per side.
	var seq uint64
	for i := int64(1); i <= 2000; i++ {
		seq++
		var side Side = Side(i % 2)
		var px int64 = 100000 + (i%200)*10
		if side == SideAsk {
			px += 5000
		}
		if err := e.ApplyDelta(i, side, px, 1+i%7, ActionUpsert, seq); err != nil {
			t.Fatal(err)
		}
	}
	var buf []Level = make([]Level, 0, 32)
	var sink int64
	var allocs float64 = testing.AllocsPerRun(2000, func() {
		seq++
		var id int64 = 1 + rng.Int63n(2000)
		var side Side = Side(id % 2)
		var px int64 = 100000 + (id%200)*10
		if side == SideAsk {
			px += 5000
		}
		// Replace the order at the same level (delete + upsert as on the wire).
		if err := e.ApplyDelta(id, side, px, 0, ActionDelete, seq); err != nil {
			panic(err)
		}
		seq++
		if err := e.ApplyDelta(id, side, px, 1+id%5, ActionUpsert, seq); err != nil {
			panic(err)
		}
		buf = e.TopN(SideBid, 10, buf)
		sink += buf[0].Size
		var _, size, _ = e.BestAsk()
		sink += size
	})
	if allocs != 0 {
		t.Fatalf("steady-state engine allocates: %.2f allocs/op", allocs)
	}
	if sink == 0 {
		t.Fatal("sink")
	}
}

func TestEngineLevelsStaySorted(t *testing.T) {
	var e *Engine = NewEngine()
	var rng *rand.Rand = rand.New(rand.NewSource(7))
	var seq uint64
	for i := int64(1); i <= 5000; i++ {
		seq++
		var side Side = Side(rng.Intn(2))
		var px int64 = 1000 + rng.Int63n(300)
		if err := e.ApplyDelta(i, side, px, 1+rng.Int63n(9), ActionUpsert, seq); err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			seq++
			var victim int64 = 1 + rng.Int63n(i)
			var err error = e.ApplyDelta(victim, SideBid, 0, 0, ActionDelete, seq)
			if err != nil && err != ErrUnknownOrder {
				t.Fatal(err)
			}
		}
	}
	var bids []Level = e.Levels(SideBid, 0)
	for i := 1; i < len(bids); i++ {
		if bids[i-1].PriceMantissa <= bids[i].PriceMantissa {
			t.Fatalf("bids not descending at %d: %v", i, bids[i-1:i+1])
		}
	}
	var asks []Level = e.Levels(SideAsk, 0)
	for i := 1; i < len(asks); i++ {
		if asks[i-1].PriceMantissa >= asks[i].PriceMantissa {
			t.Fatalf("asks not ascending at %d: %v", i, asks[i-1:i+1])
		}
	}
	// Aggregates must equal the sum over orders.
	var bidSum, askSum map[int64]int64 = make(map[int64]int64), make(map[int64]int64)
	e.ForEachOrder(func(id int64, side Side, px, size int64) {
		if side == SideBid {
			bidSum[px] += size
		} else {
			askSum[px] += size
		}
	})
	if len(bidSum) != len(bids) || len(askSum) != len(asks) {
		t.Fatalf("level counts: bids %d/%d asks %d/%d", len(bidSum), len(bids), len(askSum), len(asks))
	}
	for _, l := range bids {
		if bidSum[l.PriceMantissa] != l.Size {
			t.Fatalf("bid level %d: %d != %d", l.PriceMantissa, l.Size, bidSum[l.PriceMantissa])
		}
	}
	for _, l := range asks {
		if askSum[l.PriceMantissa] != l.Size {
			t.Fatalf("ask level %d: %d != %d", l.PriceMantissa, l.Size, askSum[l.PriceMantissa])
		}
	}
}

func BenchmarkEngineReplaceOrder(b *testing.B) {
	var e *Engine = NewEngine()
	var seq uint64
	for i := int64(1); i <= 2000; i++ {
		seq++
		_ = e.ApplyDelta(i, Side(i%2), 100000+(i%200)*10+int64(i%2)*5000, 3, ActionUpsert, seq)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var id int64 = 1 + int64(i%2000)
		var side Side = Side(id % 2)
		var px int64 = 100000 + (id%200)*10 + int64(id%2)*5000
		seq++
		_ = e.ApplyDelta(id, side, px, 0, ActionDelete, seq)
		seq++
		_ = e.ApplyDelta(id, side, px, 3, ActionUpsert, seq)
	}
}
