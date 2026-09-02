/*
FILE: forts/feed.go

DESCRIPTION:
Feed layer of BookSession: merges the redundant A/B Incremental feeds,
reorders by MsgSeqNum and recovers small gaps through the TCP Replay
service before declaring them lost.

	packet from A or B ──► seq == expected?  ──yes──► process, drain pending
	                        │ seq < expected ──────► duplicate, drop
	                        └ seq > expected ──────► hold a copy in pending,
	                                                 arm the gap timer

When the timer fires the packets [expected, first pending) are missing on
both feeds. If a Replay endpoint is configured and the gap is within the
service limit (1000 packets), a replay is requested in the background;
replayed packets enter through the same path. Otherwise — or if the
replay fails — the gap is skipped: the pending packets are processed and
the instruments whose RptSeq jumped resync from the Snapshot feed.

SequenceReset is handled before any of this (its packet is numbered in
the new sequence): pending packets of the old numbering are dropped.
*/
package forts

import (
	"context"
	"time"

	moex "github.com/tonymontanov/go-moex"
	"github.com/tonymontanov/go-moex/internal/simba"
)

// Replayer — TCP Replay service abstraction (simba.ReplayClient satisfies
// it; tests substitute a fake).
type Replayer interface {
	Replay(ctx context.Context, begSeq, endSeq uint32, handler func(buf []byte)) (simba.ReplayResult, error)
}

// FeedConfig — reordering/recovery knobs of BookSession.
type FeedConfig struct {
	// GapTimeout — how long a missing packet may stay missing on both
	// feeds before recovery starts. A/B skew on production is well below a
	// millisecond; default 20ms.
	GapTimeout time.Duration
	// MaxPending — cap on held-back packets while a gap is open (copies of
	// datagrams). Default 65536 (~50 MB worst case, ~20 s of peak traffic).
	// On overflow the gap is skipped immediately.
	MaxPending int
	// Replayer — TCP Replay client; nil disables replay recovery.
	Replayer Replayer
	// ReplayTimeout bounds one replay attempt; default 3s.
	ReplayTimeout time.Duration
}

type feedState struct {
	have           bool
	expected       uint32
	pending        map[uint32][]byte
	minPend        uint32 // smallest key in pending (0 when empty).
	timer          *time.Timer
	replayInFlight bool
	replayCancel   context.CancelFunc
}

func (s *BookSession) feedInit() {
	if s.cfg.Feed.GapTimeout <= 0 {
		s.cfg.Feed.GapTimeout = 20 * time.Millisecond
	}
	if s.cfg.Feed.MaxPending <= 0 {
		s.cfg.Feed.MaxPending = 65536
	}
	if s.cfg.Feed.ReplayTimeout <= 0 {
		s.cfg.Feed.ReplayTimeout = 3 * time.Second
	}
	s.feed.pending = make(map[uint32][]byte)
}

// HandleIncrementalPacket is the entry point for datagrams of the
// Incremental feed (either leg). Exposed for replay/testing.
func (s *BookSession) HandleIncrementalPacket(buf []byte) {
	if newSeq, isReset := simba.SequenceResetIn(buf); isReset {
		s.mu.Lock()
		s.feedResetLocked()
		s.onSequenceReset(newSeq)
		s.feed.have, s.feed.expected = true, newSeq
		s.notifyChangedLocked()
		s.mu.Unlock()
		return
	}
	var seq uint32 = simba.PacketHeaderSeq(buf)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.feedAcceptLocked(seq, buf, false)
}

// feedAcceptLocked routes one packet through the reorder buffer. copied
// tells whether buf is already owned by the session (replay/pending).
func (s *BookSession) feedAcceptLocked(seq uint32, buf []byte, copied bool) {
	var f *feedState = &s.feed
	if !f.have {
		f.have = true
		f.expected = seq
	}
	switch {
	case seq == f.expected:
		s.processIncrementalLocked(buf)
		f.expected = seq + 1
		s.feedDrainLocked()
	case seq < f.expected:
		s.stats.IncDuplicates++
		s.metricInc(s.metrics.dups)
	default:
		if _, held := f.pending[seq]; held {
			s.stats.IncDuplicates++
			return
		}
		if len(f.pending) >= s.cfg.Feed.MaxPending {
			s.feedSkipGapLocked("pending buffer overflow")
			if seq == f.expected {
				s.processIncrementalLocked(buf)
				f.expected = seq + 1
				s.feedDrainLocked()
				return
			}
		}
		if copied {
			f.pending[seq] = buf
		} else {
			f.pending[seq] = append([]byte(nil), buf...)
		}
		if f.minPend == 0 || seq < f.minPend {
			f.minPend = seq
		}
		if f.timer == nil && !f.replayInFlight {
			f.timer = time.AfterFunc(s.cfg.Feed.GapTimeout, s.onGapTimeout)
		}
	}
}

// feedDrainLocked processes consecutive pending packets from expected on.
func (s *BookSession) feedDrainLocked() {
	var f *feedState = &s.feed
	for len(f.pending) > 0 {
		var buf []byte
		var ok bool
		buf, ok = f.pending[f.expected]
		if !ok {
			break
		}
		delete(f.pending, f.expected)
		s.processIncrementalLocked(buf)
		f.expected++
	}
	f.minPend = 0
	for k := range f.pending {
		if f.minPend == 0 || k < f.minPend {
			f.minPend = k
		}
	}
	if len(f.pending) == 0 && f.timer != nil {
		f.timer.Stop()
		f.timer = nil
	}
}

// onGapTimeout — the gap [expected, minPend) is missing on both feeds.
func (s *BookSession) onGapTimeout() {
	s.mu.Lock()
	defer s.mu.Unlock()
	var f *feedState = &s.feed
	f.timer = nil
	if len(f.pending) == 0 || f.minPend <= f.expected {
		s.feedDrainLocked()
		return
	}
	var beg, end uint32 = f.expected, f.minPend - 1
	if s.cfg.Feed.Replayer != nil && !f.replayInFlight && end-beg+1 <= simba.ReplayMaxPackets {
		s.startReplayLocked(beg, end)
		return
	}
	s.feedSkipGapLocked("no replay available")
}

func (s *BookSession) startReplayLocked(beg, end uint32) {
	var f *feedState = &s.feed
	f.replayInFlight = true
	s.stats.Replays++
	s.metricInc(s.metrics.replays)
	var ctx context.Context
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(context.Background(), s.cfg.Feed.ReplayTimeout)
	f.replayCancel = cancel
	s.logger.Warn("forts: Incremental feed gap, requesting TCP replay", moex.Int("from", int64(beg)), moex.Int("to", int64(end)))
	go func() {
		defer cancel()
		var res simba.ReplayResult
		var err error
		res, err = s.cfg.Feed.Replayer.Replay(ctx, beg, end, func(buf []byte) {
			var seq uint32 = simba.PacketHeaderSeq(buf)
			var copy []byte = append([]byte(nil), buf...)
			s.mu.Lock()
			s.feedAcceptLocked(seq, copy, true)
			s.mu.Unlock()
		})
		s.mu.Lock()
		defer s.mu.Unlock()
		f.replayInFlight = false
		f.replayCancel = nil
		if err != nil {
			s.stats.ReplayFailures++
			s.metricInc(s.metrics.replayFailures)
			s.logger.Warn("forts: TCP replay failed", moex.Err(err))
		} else {
			s.stats.ReplayedPackets += uint64(res.Packets)
		}
		if len(f.pending) > 0 && f.minPend > f.expected {
			// Still missing something: give up on the remainder.
			s.feedSkipGapLocked("after replay")
		} else {
			s.feedDrainLocked()
		}
	}()
}

// feedSkipGapLocked declares [expected, minPend) lost and moves on.
func (s *BookSession) feedSkipGapLocked(reason string) {
	var f *feedState = &s.feed
	if len(f.pending) == 0 || f.minPend <= f.expected {
		s.feedDrainLocked()
		return
	}
	var missing uint64 = uint64(f.minPend - f.expected)
	s.stats.IncGaps++
	s.stats.IncMissing += missing
	s.metricInc(s.metrics.gaps)
	_ = reason
	s.logger.Warn("forts: Incremental feed gap declared lost", moex.Int("from", int64(f.expected)), moex.Int("to", int64(f.minPend-1)), moex.Int("pending", int64(len(f.pending))))
	f.expected = f.minPend
	s.lossWindow = true
	s.lossWindowCycles = 0
	s.ensureSnapshotListenerLocked()
	s.feedDrainLocked()
}

// feedResetLocked drops everything held (numbering restarted).
func (s *BookSession) feedResetLocked() {
	var f *feedState = &s.feed
	for k := range f.pending {
		delete(f.pending, k)
	}
	f.minPend = 0
	if f.timer != nil {
		f.timer.Stop()
		f.timer = nil
	}
	if f.replayCancel != nil {
		f.replayCancel()
	}
}

// FeedExpected returns the next Incremental MsgSeqNum the session expects
// (ok=false before the first packet). Everything below it has been
// processed or declared lost.
func (s *BookSession) FeedExpected() (uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.feed.expected, s.feed.have
}

// FeedPending returns how many packets are currently held back.
func (s *BookSession) FeedPending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.feed.pending)
}
