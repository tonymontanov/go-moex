package forts

import (
	"io"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/tonymontanov/go-moex/internal/pcap"
	"github.com/tonymontanov/go-moex/internal/simba"
)

// fut-info-4instr.pcap.gz — 22 s of the FUT-INFO Replay (.83) and
// Incremental (.84) groups from the 2026-05-15 main-session capture,
// schema 8, cut to four instruments (one multileg) across two full
// SequenceReset cycles; SecurityGroupStatus capped at 20 messages.
func loadInfoFixture(t *testing.T) [][]byte {
	t.Helper()
	var src *pcap.Source
	var err error
	src, err = pcap.Open("../internal/simba/testdata/fut-info-4instr.pcap.gz", "")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	var out [][]byte
	for {
		var pkt pcap.Packet
		pkt, err = src.Reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, append([]byte{}, pkt.Payload...))
	}
	return out
}

func TestInstrumentSessionFixture(t *testing.T) {
	var packets [][]byte = loadInfoFixture(t)
	if len(packets) != 39 {
		t.Fatalf("fixture packets %d", len(packets))
	}
	var defs, sessionStatuses int
	var s *InstrumentSession = NewInstrumentSession(InstrumentSessionConfig{
		OnDefinition:    func(*Instrument) { defs++ },
		OnSessionStatus: func(simba.TradingSessionStatus) { sessionStatuses++ },
	})
	for _, p := range packets {
		s.HandlePacket(p)
	}
	var st InstrumentSessionStats = s.Stats()
	if st.Packets != 39 || st.ParseErrors != 0 || st.UnknownMessages != 0 {
		t.Fatalf("stats %+v", st)
	}
	if st.Definitions != 12 || defs != 12 || st.Cycles != 2 || st.GroupStatuses != 20 || st.SessionStatuses != 3 || sessionStatuses != 3 {
		t.Fatalf("stats %+v (defs=%d sessions=%d)", st, defs, sessionStatuses)
	}
	if st.TotNumReports != 745 || st.Instruments != 4 {
		t.Fatalf("TotNumReports=%d instruments=%d", st.TotNumReports, st.Instruments)
	}
	// A filtered feed never reaches TotNumReports; two identical cycles
	// still mark the cache complete.
	select {
	case <-s.Ready():
	default:
		t.Fatal("Ready not closed after two identical cycles")
	}

	type want struct {
		symbol, secType, cfi      string
		tick, high, low           string
		maturity                  uint32
		underlyings, legs, events int
	}
	var wants = map[int32]want{
		7824845: {"NGU6NGV6", "MLEG", "FMXXSX", "0.001", "0.524", "0.258", 20260928, 1, 2, 2},
		7362296: {"HDM6", "", "FFXPSX", "1", "3082", "2384", 20260618, 1, 0, 2},
		7727697: {"ANU6", "", "FCXCSX", "0.5", "3877.5", "3441.5", 20260915, 1, 0, 2},
		7727694: {"NCU6", "", "FCXCSX", "5", "20900", "17740", 20260915, 1, 0, 2},
	}
	for id, w := range wants {
		var in Instrument
		var ok bool
		in, ok = s.Get(id)
		if !ok {
			t.Fatalf("instrument %d missing", id)
		}
		if in.Symbol != w.symbol || in.SecurityType != w.secType || in.CFICode != w.cfi || in.SchemaVersion != simba.SchemaVersion8 ||
			in.TradingStatus != simba.TradingStatusReadyToTrade || in.MaturityDate != w.maturity {
			t.Fatalf("%d: %+v", id, in)
		}
		if !in.MinPriceIncrement.Equal(decimal.RequireFromString(w.tick)) || !in.HighLimitPx.Equal(decimal.RequireFromString(w.high)) ||
			!in.LowLimitPx.Equal(decimal.RequireFromString(w.low)) {
			t.Fatalf("%d: tick=%s high=%s low=%s", id, in.MinPriceIncrement, in.HighLimitPx, in.LowLimitPx)
		}
		if len(in.Underlyings) != w.underlyings || len(in.Legs) != w.legs || len(in.Events) != w.events {
			t.Fatalf("%d: underlyings=%d legs=%d events=%d", id, len(in.Underlyings), len(in.Legs), len(in.Events))
		}
		if (in.Currency != "RUB" && in.Currency != "USD") || in.UpdatedAtNs == 0 || in.Description == "" {
			t.Fatalf("%d: currency=%q updated=%d desc=%q", id, in.Currency, in.UpdatedAtNs, in.Description)
		}
		var bySym Instrument
		if bySym, ok = s.BySymbol(w.symbol); !ok || bySym.SecurityID != id {
			t.Fatalf("BySymbol(%q) = %d %v", w.symbol, bySym.SecurityID, ok)
		}
	}
	var spread Instrument
	spread, _ = s.Get(7824845)
	if spread.Legs[0].Symbol == "" || spread.Legs[0].SecurityID == 0 || spread.Legs[1].SecurityID == spread.Legs[0].SecurityID {
		t.Fatalf("multileg legs %+v", spread.Legs)
	}
	if len(s.All()) != 4 {
		t.Fatalf("All() = %d", len(s.All()))
	}
}

func TestInstrumentSessionStatusUpdate(t *testing.T) {
	var s *InstrumentSession = NewInstrumentSession(InstrumentSessionConfig{})
	for _, p := range loadInfoFixture(t) {
		s.HandlePacket(p)
	}
	var prevSeen simba.SecurityTradingStatus
	var updated *Instrument
	s.cfg.OnStatus = func(in *Instrument, prev simba.SecurityTradingStatus) { updated, prevSeen = in, prev }

	// Synthetic v8 SecurityStatus (template 9, 70 bytes) halting HDM6 with
	// new limits.
	var body []byte = make([]byte, 70)
	putU32 := func(off int, v uint32) {
		body[off], body[off+1], body[off+2], body[off+3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
	}
	putU64 := func(off int, v uint64) {
		for i := 0; i < 8; i++ {
			body[off+i] = byte(v >> (8 * i))
		}
	}
	putU32(0, 7362296)
	copy(body[4:], "HDM6")
	body[29] = uint8(simba.TradingStatusInstrumentHalt)
	putU64(30, 310000000)
	putU64(38, 240000000)
	putU64(46, uint64(simba.NullDecimalMantissa))
	putU64(54, uint64(simba.NullDecimalMantissa))
	putU64(62, uint64(simba.NullDecimalMantissa))
	var msg []byte = []byte{70, 0, 9, 0, 0x44, 0x4d, 8, 0}
	msg = append(msg, body...)
	var pkt []byte = make([]byte, 16)
	pkt[4], pkt[5] = byte(16+len(msg)), byte((16+len(msg))>>8)
	pkt[6] = byte(simba.FlagLastFragment)
	pkt[8] = 1
	pkt = append(pkt, msg...)
	s.HandlePacket(pkt)

	if updated == nil || updated.SecurityID != 7362296 || prevSeen != simba.TradingStatusReadyToTrade ||
		updated.TradingStatus != simba.TradingStatusInstrumentHalt {
		t.Fatalf("status callback: %+v prev=%v", updated, prevSeen)
	}
	var in, _ = s.Get(7362296)
	if !in.HighLimitPx.Equal(decimal.NewFromInt(3100)) || !in.LowLimitPx.Equal(decimal.NewFromInt(2400)) ||
		!in.InitialMarginOnBuy.Equal(in.InitialMarginOnBuy) || in.TradingStatus.Tradable() {
		t.Fatalf("after status: %+v", in)
	}
	if s.Stats().StatusUpdates != 1 {
		t.Fatalf("StatusUpdates %d", s.Stats().StatusUpdates)
	}
}
