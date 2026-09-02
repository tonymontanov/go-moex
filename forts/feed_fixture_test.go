package forts

import (
	"context"
	"testing"
	"time"

	"github.com/tonymontanov/go-moex/internal/simba"
	"github.com/tonymontanov/go-moex/orderbook"
)

// fixtureReplayer serves replay requests from the fixture's own packets.
type fixtureReplayer struct {
	packets map[uint32][]byte
	calls   int
}

func (r *fixtureReplayer) Replay(ctx context.Context, beg, end uint32, h func([]byte)) (simba.ReplayResult, error) {
	r.calls++
	var res simba.ReplayResult
	for seq := beg; seq <= end; seq++ {
		if p, ok := r.packets[seq]; ok {
			h(p)
			res.Packets++
		}
	}
	return res, nil
}

// TestFeedReplayedPacketRestoresExactBook drops every 50th incremental
// packet of the production fixture on a session with replay and checks,
// after each transaction, that its books equal those of a session that
// saw everything. Guards the replay path against ordering/duplication
// bugs the snapshot oracle (which only looks at cycle boundaries) could
// miss.
func TestFeedReplayedPacketRestoresExactBook(t *testing.T) {
	var inc, snap = loadFixture(t, "main-session-2instr.pcap.gz")
	var secs []int32 = []int32{7299709, 3416766}

	var full *BookSession = NewBookSession(BookSessionConfig{})
	var fr *fixtureReplayer = &fixtureReplayer{packets: make(map[uint32][]byte)}
	for _, p := range inc {
		fr.packets[p.seq] = p.payload
	}
	var lossy *BookSession = NewBookSession(BookSessionConfig{Feed: FeedConfig{GapTimeout: 2 * time.Millisecond, Replayer: fr}})
	var fullE, lossyE map[int32]*orderbook.Engine = make(map[int32]*orderbook.Engine), make(map[int32]*orderbook.Engine)
	for _, sec := range secs {
		fullE[sec] = full.Subscribe(sec)
		lossyE[sec] = lossy.Subscribe(sec)
	}
	// Sync both from the first complete snapshot cycle: deliver snapshots
	// first (their L predates every incremental in the fixture? no — the
	// fixture starts at cycle boundary 3s, so feed incrementals up to L).
	var next int
	var deliverBoth = func(upTo uint32) {
		for next < len(inc) && (upTo == 0 || inc[next].seq <= upTo) {
			full.HandleIncrementalPacket(inc[next].payload)
			if next%50 != 25 { // drop every 50th (offset 25) on the lossy session
				lossy.HandleIncrementalPacket(inc[next].payload)
			}
			next++
		}
	}
	var firstCycleDone bool
	for _, sp := range snap {
		var hdr simba.PacketHeader
		var v simba.SnapshotView
		var ok bool
		hdr, v, ok = snapshotOf(t, sp.payload)
		if !ok {
			continue
		}
		deliverBoth(v.LastMsgSeqNumProcessed)
		full.HandleSnapshotPacket(sp.payload)
		lossy.HandleSnapshotPacket(sp.payload)
		if hdr.IsEndOfSnapshot() {
			var allLive bool = true
			for _, sec := range secs {
				if st, _ := full.State(sec); st != BookLive {
					allLive = false
				}
			}
			if allLive {
				firstCycleDone = true
				break
			}
		}
	}
	if !firstCycleDone {
		t.Fatal("could not sync from the first cycle")
	}
	// Now stream the rest, comparing after every packet that ends a
	// transaction (once the lossy session has caught up).
	var compares, dropped int
	for next < len(inc) {
		var p fixturePacket = inc[next]
		var _, eot = bestPricesAndEOT(t, p.payload)
		full.HandleIncrementalPacket(p.payload)
		if next%50 == 25 {
			dropped++
		} else {
			lossy.HandleIncrementalPacket(p.payload)
		}
		next++
		if !eot {
			continue
		}
		waitFor(t, func() bool { return lossy.FeedPending() == 0 })
		var exp uint32
		exp, _ = lossy.FeedExpected()
		if exp != p.seq+1 {
			continue // the lost packet is not yet detectable
		}
		compares++
		for _, sec := range secs {
			var a, b = bookOrders(fullE[sec]), bookOrders(lossyE[sec])
			if len(a) != len(b) {
				t.Fatalf("after seq %d sec %d: full=%d orders, lossy=%d orders (stats %s)", p.seq, sec, len(a), len(b), lossy.Stats())
			}
			for id, o := range a {
				if b[id] != o {
					t.Fatalf("after seq %d sec %d order %d: full=%v lossy=%v", p.seq, sec, id, o, b[id])
				}
			}
		}
	}
	var st BookSessionStats = lossy.Stats()
	t.Logf("compares=%d dropped=%d replays=%d stats=%s", compares, dropped, fr.calls, st)
	if compares == 0 || dropped == 0 || st.Replays == 0 || st.IncGaps != 0 || st.Resyncs != 0 {
		t.Fatalf("test did not exercise replay: compares=%d dropped=%d stats=%s", compares, dropped, st)
	}
}
