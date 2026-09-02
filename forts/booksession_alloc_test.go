package forts

import (
	"runtime"
	"testing"

	"github.com/tonymontanov/go-moex/internal/simba"
	"github.com/tonymontanov/go-moex/orderbook"
)

// TestBookSessionSteadyStateAllocs — once every subscribed instrument is
// live, feeding production packets through the session (decode, sequence
// checks, engine updates, OnBook reading the top of book into a reused
// buffer) must not touch the heap. Measured over the whole main-session
// fixture after the first snapshot cycle: allowance is a handful of
// mallocs for engine level-slice growth, not per packet.
func TestBookSessionSteadyStateAllocs(t *testing.T) {
	var inc, snap = loadFixture(t, "main-session-2instr.pcap.gz")
	var secs []int32 = []int32{7299709, 3416766}
	var top []orderbook.Level = make([]orderbook.Level, 0, 16)
	var reads int
	var s *BookSession = NewBookSession(BookSessionConfig{
		OnBook: func(id int32, e *orderbook.Engine) {
			top = e.TopN(orderbook.SideBid, 5, top)
			top = e.TopN(orderbook.SideAsk, 5, top)
			reads++
		},
	})
	for _, sec := range secs {
		s.Subscribe(sec)
	}
	// Sync from the first cycle.
	var next int
	var deliver = func(upTo uint32) {
		for next < len(inc) && (upTo == 0 || inc[next].seq <= upTo) {
			s.HandleIncrementalPacket(inc[next].payload)
			next++
		}
	}
	for _, sp := range snap {
		var hdr simba.PacketHeader
		var v simba.SnapshotView
		var ok bool
		hdr, v, ok = snapshotOf(t, sp.payload)
		if !ok {
			continue
		}
		deliver(v.LastMsgSeqNumProcessed)
		s.HandleSnapshotPacket(sp.payload)
		if hdr.IsEndOfSnapshot() {
			var allLive bool = true
			for _, sec := range secs {
				if st, _ := s.State(sec); st != BookLive {
					allLive = false
				}
			}
			if allLive {
				break
			}
		}
	}
	for _, sec := range secs {
		if st, _ := s.State(sec); st != BookLive {
			t.Fatalf("instrument %d not live after first cycle", sec)
		}
	}
	// Warm the engines' level slices with a few hundred packets, then measure.
	var warm int = next + 300
	deliver(inc[warm].seq)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	var start int = next
	deliver(0)
	runtime.ReadMemStats(&after)
	var packets int = next - start
	var mallocs uint64 = after.Mallocs - before.Mallocs
	t.Logf("packets=%d onBook=%d mallocs=%d bytes=%d stats=%s", packets, reads, mallocs, after.TotalAlloc-before.TotalAlloc, s.Stats())
	if packets < 1000 {
		t.Fatalf("too few packets measured: %d", packets)
	}
	if mallocs > 16 {
		t.Fatalf("steady-state session allocates: %d mallocs over %d packets", mallocs, packets)
	}
	if st := s.Stats(); st.Resyncs != 0 || st.UnknownOrders != 0 || st.IncGaps != 0 {
		t.Fatalf("unexpected stats: %s", st)
	}
}
