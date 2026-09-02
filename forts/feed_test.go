package forts

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tonymontanov/go-moex/internal/simba"
	"github.com/tonymontanov/go-moex/orderbook"
)

type fakeReplayer struct {
	packets  map[uint32][]byte
	calls    [][2]uint32
	fail     error
	delay    time.Duration
	deliverN int // deliver at most N packets (0 = all).
}

func (f *fakeReplayer) Replay(ctx context.Context, beg, end uint32, h func([]byte)) (simba.ReplayResult, error) {
	f.calls = append(f.calls, [2]uint32{beg, end})
	if f.fail != nil {
		return simba.ReplayResult{}, f.fail
	}
	time.Sleep(f.delay)
	var res simba.ReplayResult
	for seq := beg; seq <= end; seq++ {
		if p, ok := f.packets[seq]; ok {
			if f.deliverN > 0 && res.Packets >= f.deliverN {
				break
			}
			h(p)
			res.Packets++
		}
	}
	return res, nil
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	var deadline time.Time = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func liveSession(t *testing.T, feed FeedConfig) (*BookSession, *orderbook.Engine) {
	t.Helper()
	var s *BookSession = NewBookSession(BookSessionConfig{Feed: feed})
	var e *orderbook.Engine = s.Subscribe(42)
	s.HandleIncrementalPacket(incPacket(1, simba.FlagLastFragment, emptyBookMsg())) // live-empty
	return s, e
}

func TestFeedABDuplicatesAndReorder(t *testing.T) {
	var s, e = liveSession(t, FeedConfig{GapTimeout: 500 * time.Millisecond})
	var p2 = incPacket(2, simba.FlagLastFragment, upd(42, 1, 1, 100, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid))
	var p3 = incPacket(3, simba.FlagLastFragment, upd(42, 2, 2, 101, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid))
	var p4 = incPacket(4, simba.FlagLastFragment, upd(42, 3, 3, 102, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid))
	// Leg B ahead: 3 arrives before 2, then A delivers 2, 3 (dup), then 4 twice.
	s.HandleIncrementalPacket(p3)
	if e.OrderCount() != 0 || s.FeedPending() != 1 {
		t.Fatalf("out-of-order packet must be held: orders=%d pending=%d", e.OrderCount(), s.FeedPending())
	}
	s.HandleIncrementalPacket(p2)
	if e.OrderCount() != 2 || s.FeedPending() != 0 {
		t.Fatalf("drain after the missing packet: orders=%d pending=%d", e.OrderCount(), s.FeedPending())
	}
	s.HandleIncrementalPacket(p3)
	s.HandleIncrementalPacket(p4)
	s.HandleIncrementalPacket(p4)
	var st BookSessionStats = s.Stats()
	if e.OrderCount() != 3 || st.IncDuplicates != 2 || st.IncGaps != 0 || st.Resyncs != 0 || st.IncPackets != 4 {
		t.Fatalf("orders=%d stats=%s", e.OrderCount(), st)
	}
}

func TestFeedGapWithoutReplayIsSkippedAfterTimeout(t *testing.T) {
	var s, e = liveSession(t, FeedConfig{GapTimeout: 5 * time.Millisecond})
	s.HandleIncrementalPacket(incPacket(2, simba.FlagLastFragment, upd(42, 1, 1, 100, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	// 3 lost on both legs.
	s.HandleIncrementalPacket(incPacket(4, simba.FlagLastFragment, upd(42, 3, 3, 102, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	if st, _ := s.State(42); st != BookLive {
		t.Fatal("held packet must not be applied before the timeout")
	}
	waitFor(t, func() bool { st, _ := s.State(42); return st == BookCollecting })
	var st BookSessionStats = s.Stats()
	if st.IncGaps != 1 || st.IncMissing != 1 || st.Resyncs != 1 || st.Replays != 0 || s.FeedPending() != 0 {
		t.Fatalf("stats=%s pending=%d", st, s.FeedPending())
	}
	_ = e
}

func TestFeedGapRecoveredByReplay(t *testing.T) {
	var p3 = incPacket(3, simba.FlagLastFragment, upd(42, 2, 2, 101, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid))
	var fr *fakeReplayer = &fakeReplayer{packets: map[uint32][]byte{3: p3}}
	var s, e = liveSession(t, FeedConfig{GapTimeout: 5 * time.Millisecond, Replayer: fr})
	s.HandleIncrementalPacket(incPacket(2, simba.FlagLastFragment, upd(42, 1, 1, 100, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	s.HandleIncrementalPacket(incPacket(4, simba.FlagLastFragment, upd(42, 3, 3, 102, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	s.HandleIncrementalPacket(incPacket(5, simba.FlagLastFragment, upd(42, 4, 4, 103, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	// Engines are only safe to read under the session lock: wait on the
	// locked counters, then read the book.
	waitFor(t, func() bool { st := s.Stats(); return st.ReplayedPackets == 1 && s.FeedPending() == 0 })
	if e.OrderCount() != 4 {
		t.Fatalf("orders=%d want 4", e.OrderCount())
	}
	var st BookSessionStats = s.Stats()
	if st.Replays != 1 || st.ReplayedPackets != 1 || st.IncGaps != 0 || st.Resyncs != 0 || s.FeedPending() != 0 {
		t.Fatalf("stats=%s pending=%d", st, s.FeedPending())
	}
	if len(fr.calls) != 1 || fr.calls[0] != [2]uint32{3, 3} {
		t.Fatalf("replay calls %v", fr.calls)
	}
	if stt, _ := s.State(42); stt != BookLive {
		t.Fatal("book must stay live through a replayed gap")
	}
}

func TestFeedReplayFailureFallsBackToResync(t *testing.T) {
	var fr *fakeReplayer = &fakeReplayer{fail: errors.New("boom")}
	var s, _ = liveSession(t, FeedConfig{GapTimeout: 5 * time.Millisecond, Replayer: fr})
	s.HandleIncrementalPacket(incPacket(2, simba.FlagLastFragment, upd(42, 1, 1, 100, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	s.HandleIncrementalPacket(incPacket(4, simba.FlagLastFragment, upd(42, 3, 3, 102, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	waitFor(t, func() bool { st, _ := s.State(42); return st == BookCollecting })
	var st BookSessionStats = s.Stats()
	if st.Replays != 1 || st.ReplayFailures != 1 || st.IncGaps != 1 || st.Resyncs != 1 {
		t.Fatalf("stats=%s", st)
	}
}

func TestFeedGapTooLargeForReplay(t *testing.T) {
	var fr *fakeReplayer = &fakeReplayer{}
	var s, _ = liveSession(t, FeedConfig{GapTimeout: 5 * time.Millisecond, Replayer: fr})
	s.HandleIncrementalPacket(incPacket(2, simba.FlagLastFragment, upd(42, 1, 1, 100, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	s.HandleIncrementalPacket(incPacket(2000, simba.FlagLastFragment, upd(42, 3, 3, 102, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	waitFor(t, func() bool { st, _ := s.State(42); return st == BookCollecting })
	if len(fr.calls) != 0 || s.Stats().IncGaps != 1 || s.Stats().IncMissing != 1997 {
		t.Fatalf("calls=%v stats=%s", fr.calls, s.Stats())
	}
}

func TestFeedPendingOverflowSkipsGap(t *testing.T) {
	var s, _ = liveSession(t, FeedConfig{GapTimeout: time.Hour, MaxPending: 3})
	s.HandleIncrementalPacket(incPacket(2, simba.FlagLastFragment, upd(42, 1, 1, 100, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	for seq := uint32(4); seq <= 7; seq++ {
		s.HandleIncrementalPacket(incPacket(seq, simba.FlagLastFragment, upd(42, seq-1, int64(seq), 100+int64(seq), 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	}
	var st BookSessionStats = s.Stats()
	if st.IncGaps != 1 || st.Resyncs != 1 || s.FeedPending() != 0 {
		t.Fatalf("stats=%s pending=%d", st, s.FeedPending())
	}
}

func TestFeedSequenceResetDropsPending(t *testing.T) {
	var s, e = liveSession(t, FeedConfig{GapTimeout: time.Hour})
	s.HandleIncrementalPacket(incPacket(2, simba.FlagLastFragment, upd(42, 1, 1, 100, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	s.HandleIncrementalPacket(incPacket(9, simba.FlagLastFragment, upd(42, 5, 5, 100, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	s.HandleIncrementalPacket(incPacket(1, simba.FlagLastFragment, seqResetMsg(1)))
	s.HandleIncrementalPacket(incPacket(1, simba.FlagLastFragment, emptyBookMsg()))
	s.HandleIncrementalPacket(incPacket(2, simba.FlagPossDupFlag|simba.FlagLastFragment, upd(42, 0, 7, 100, 1, simba.MDUpdateActionNew, simba.MDEntryTypeBid)))
	if s.FeedPending() != 0 || e.OrderCount() != 1 {
		t.Fatalf("pending=%d orders=%d stats=%s", s.FeedPending(), e.OrderCount(), s.Stats())
	}
	if st := s.Stats(); st.IncGaps != 0 || st.SequenceResets != 1 {
		t.Fatalf("stats=%s", st)
	}
}
