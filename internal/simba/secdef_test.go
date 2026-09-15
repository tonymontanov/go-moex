package simba

import (
	"encoding/binary"
	"math"
	"testing"
)

// buildSecDefPacket assembles a SIMBA datagram with one SecurityDefinition
// in the given schema version (root block per secdef.go's table, five
// groups, two var-data fields), non-incremental packet header.
func buildSecDefPacket(version uint16) []byte {
	var root int = 342
	var template uint16 = TemplateSecurityDefinition
	if version == SchemaVersion8 {
		root = 326
		template = 21
	}
	var body []byte = make([]byte, root)
	put32 := func(off int, v uint32) { binary.LittleEndian.PutUint32(body[off:], v) }
	put64 := func(off int, v uint64) { binary.LittleEndian.PutUint64(body[off:], v) }
	putS := func(off int, s string) { copy(body[off:], s) }
	put32(0, 745)
	putS(4, "Si-12.26")
	put32(29, 305203)
	putS(33, "SiZ6")
	body[58] = '8'
	putS(59, "FUT")
	putS(63, "FFXXSX")
	put64(69, uint64(NullDecimalMantissa))
	put32(77, 1)
	body[81] = uint8(TradingStatusReadyToTrade)
	putS(82, "RUB")
	body[85] = 'F'
	body[86] = 3
	put32(87, 7562)
	put64(91, uint64(NullDecimalMantissa))
	put64(99, 9950000000)  // HighLimitPx 99500.00000
	put64(107, 9050000000) // LowLimitPx 90500
	put64(115, 100000)     // MinPriceIncrement 1.00000
	put64(123, 100000)
	put64(131, 1399300) // InitialMarginOnBuy 13993.00 (Decimal2)
	put64(139, 1448410)
	put64(147, 1399300)
	put64(155, 9500000000)
	put64(163, uint64(NullDecimalMantissa))
	put64(171, 100000000)
	putS(179, "USD")
	var flagsOff, shift int = 182, 0
	if version == SchemaVersion8 {
		put32(182, 20261217) // MaturityDate
		put32(186, 190000000)
		flagsOff, shift = 190, 8
	}
	put64(flagsOff, 1<<4|1<<18)
	put64(190+shift, 100000)
	put64(198+shift, 9400000000)
	putS(206+shift, "MTHD")
	putS(234+shift, "RUB")
	body[237+shift] = 0
	put32(238+shift, 1)
	if version == SchemaVersion8 {
		put64(282+shift, math.Float64bits(94123.5))
	} else {
		put64(282, 9412350000)
	}
	put32(290+shift, 1)
	put64(294+shift, 0x0F)
	put32(302+shift, 7)
	put32(306+shift, 4242)
	put64(310+shift, 15)
	if version != SchemaVersion8 {
		put64(318, 9990000000)
		put64(326, 9010000000)
		put64(334, 9412350000)
	}
	// Groups: MDFeedTypes(0), Underlyings(1), Legs(0), InstrAttrib(1), Events(2).
	var g []byte
	dim := func(blockLen, n int) { g = append(g, byte(blockLen), byte(blockLen>>8), byte(n)) }
	dim(33, 0)
	dim(37, 1)
	var u [37]byte
	copy(u[:], "USDRUB")
	copy(u[25:], "CETS")
	binary.LittleEndian.PutUint32(u[29:], 111)
	binary.LittleEndian.PutUint32(u[33:], uint32(0x80000000))
	g = append(g, u[:]...)
	dim(33, 0)
	dim(35, 1)
	var a [35]byte
	if version == SchemaVersion8 {
		copy(a[:], "tick-value")
		binary.LittleEndian.PutUint32(a[31:], 27)
	} else {
		binary.LittleEndian.PutUint32(a[0:], 27)
		copy(a[4:], "tick-value")
	}
	g = append(g, a[:]...)
	dim(16, 2)
	var e [16]byte
	binary.LittleEndian.PutUint32(e[0:], 5)
	binary.LittleEndian.PutUint32(e[4:], 20261217)
	binary.LittleEndian.PutUint64(e[8:], 1797000000000000000)
	g = append(g, e[:]...)
	binary.LittleEndian.PutUint32(e[0:], 7)
	g = append(g, e[:]...)
	// var data: SecurityDesc (UTF-8), QuotationList (empty)
	var desc string = "Фьючерсный контракт Si-12.26"
	g = append(g, byte(len(desc)), byte(len(desc)>>8))
	g = append(g, desc...)
	g = append(g, 0, 0)

	var msg []byte = make([]byte, 8)
	binary.LittleEndian.PutUint16(msg[0:], uint16(root))
	binary.LittleEndian.PutUint16(msg[2:], template)
	binary.LittleEndian.PutUint16(msg[4:], SchemaID)
	binary.LittleEndian.PutUint16(msg[6:], version)
	msg = append(msg, body...)
	msg = append(msg, g...)

	var pkt []byte = make([]byte, 16)
	binary.LittleEndian.PutUint32(pkt[0:], 42)
	binary.LittleEndian.PutUint16(pkt[4:], uint16(16+len(msg)))
	binary.LittleEndian.PutUint16(pkt[6:], FlagLastFragment)
	binary.LittleEndian.PutUint64(pkt[8:], 1778000000000000000)
	return append(pkt, msg...)
}

func decodeSecDef(t *testing.T, version uint16) SecurityDefinitionView {
	t.Helper()
	var p Packet
	var err error
	p, err = ParsePacket(buildSecDefPacket(version), 0)
	if err != nil {
		t.Fatal(err)
	}
	var m Message
	var ok bool
	m, ok, err = p.Next()
	if err != nil || !ok || m.Kind != KindSecurityDefinition {
		t.Fatalf("message: ok=%v err=%v kind=%v", ok, err, m.Kind)
	}
	var v SecurityDefinitionView
	if v, ok = m.SecurityDefinitionView(); !ok {
		t.Fatal("SecurityDefinitionView failed")
	}
	if _, more, _ := p.Next(); more {
		t.Fatal("trailing message after SecurityDefinition")
	}
	return v
}

func TestSecurityDefinitionViewBothVersions(t *testing.T) {
	for _, version := range []uint16{SchemaVersion8, SchemaVersion9} {
		var v SecurityDefinitionView = decodeSecDef(t, version)
		if v.TotNumReports() != 745 || string(v.Symbol()) != "Si-12.26" || v.SecurityID() != 305203 ||
			string(v.SecurityAltID()) != "SiZ6" || v.SecurityAltIDSource() != '8' || string(v.SecurityType()) != "FUT" ||
			string(v.CFICode()) != "FFXXSX" || v.StrikePrice() != NullDecimalMantissa || v.ContractMultiplier() != 1 ||
			v.TradingStatus() != TradingStatusReadyToTrade || string(v.Currency()) != "RUB" || v.MarketSegmentID() != 'F' ||
			v.TradingSessionID() != 3 || v.ExchangeTradingSessionID() != 7562 {
			t.Fatalf("v%d head mismatch: sym=%q id=%d type=%q status=%v", version, v.Symbol(), v.SecurityID(), v.SecurityType(), v.TradingStatus())
		}
		if v.HighLimitPx() != 9950000000 || v.LowLimitPx() != 9050000000 || v.MinPriceIncrement() != 100000 ||
			v.InitialMarginOnBuy() != 1399300 || v.InitialMarginOnSell() != 1448410 || v.TheorPrice() != 9500000000 ||
			v.UnderlyingQty() != 100000000 || string(v.UnderlyingCurrency()) != "USD" {
			t.Fatalf("v%d limits mismatch", version)
		}
		if v.Flags() != 1<<4|1<<18 || v.MinPriceIncrementAmountCurr() != 100000 || v.SettlPriceOpen() != 9400000000 ||
			string(v.ValuationMethod()) != "MTHD" || string(v.SettlCurrency()) != "RUB" || v.DerivativeContractMultiplier() != 1 ||
			v.TradeModeID() != 1 || v.GroupMask() != 0x0F || v.SectionID() != 7 || v.BaseContractID() != 4242 || v.TradePeriodAccess() != 15 {
			t.Fatalf("v%d tail mismatch: flags=%x trademode=%d group=%d section=%d base=%d tpa=%d", version, v.Flags(), v.TradeModeID(), v.GroupMask(), v.SectionID(), v.BaseContractID(), v.TradePeriodAccess())
		}
		if v.SettlPrice() != 9412350000 {
			t.Fatalf("v%d SettlPrice %d", version, v.SettlPrice())
		}
		if version == SchemaVersion8 {
			if v.MaturityDate() != 20261217 || v.MaturityTime() != 190000000 || v.HighLimitPxWeekend() != NullDecimalMantissa {
				t.Fatalf("v8 maturity %d/%d weekend %d", v.MaturityDate(), v.MaturityTime(), v.HighLimitPxWeekend())
			}
		} else {
			if v.MaturityDate() != NullUint32 || v.HighLimitPxWeekend() != 9990000000 || v.LowLimitPxWeekend() != 9010000000 || v.ClearingSettlPrice() != 9412350000 {
				t.Fatalf("v9 weekend/clearing mismatch")
			}
		}
		if v.MDFeedTypesLen() != 0 || v.UnderlyingsLen() != 1 || v.LegsLen() != 0 || v.InstrAttribsLen() != 1 || v.EventsLen() != 2 {
			t.Fatalf("v%d group sizes %d/%d/%d/%d/%d", version, v.MDFeedTypesLen(), v.UnderlyingsLen(), v.LegsLen(), v.InstrAttribsLen(), v.EventsLen())
		}
		var u UnderlyingEntry = v.Underlying(0)
		if string(u.UnderlyingSymbol) != "USDRUB" || string(u.UnderlyingBoard) != "CETS" || u.UnderlyingSecurityID != 111 || u.UnderlyingFutureID != NullInt32 {
			t.Fatalf("v%d underlying %+v", version, u)
		}
		var a InstrAttribEntry = v.InstrAttrib(0)
		if a.InstrAttribType != 27 || string(a.InstrAttribValue) != "tick-value" {
			t.Fatalf("v%d attrib %+v", version, a)
		}
		var e EventEntry = v.Event(1)
		if e.EventType != 7 || e.EventDate != 20261217 || e.EventTime != 1797000000000000000 {
			t.Fatalf("v%d event %+v", version, e)
		}
		if string(v.SecurityDesc()) != "Фьючерсный контракт Si-12.26" || len(v.QuotationList()) != 0 {
			t.Fatalf("v%d var data %q", version, v.SecurityDesc())
		}
	}
}

func TestSecurityDefinitionViewZeroAlloc(t *testing.T) {
	var pkt []byte = buildSecDefPacket(SchemaVersion8)
	var allocs float64 = testing.AllocsPerRun(500, func() {
		var p, _ = ParsePacket(pkt, 0)
		var m, _, _ = p.Next()
		var v, ok = m.SecurityDefinitionView()
		if !ok || v.SecurityID() != 305203 || v.Event(0).EventType != 5 {
			t.Fatal("decode failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("SecurityDefinitionView allocs = %v", allocs)
	}
}

func TestSecurityStatusViewPrefix(t *testing.T) {
	for _, bl := range []uint16{70, 86} {
		var body []byte = make([]byte, bl)
		binary.LittleEndian.PutUint32(body[0:], 305203)
		copy(body[4:], "Si-12.26")
		body[29] = uint8(TradingStatusInstrumentHalt)
		binary.LittleEndian.PutUint64(body[30:], 9950000000)
		binary.LittleEndian.PutUint64(body[38:], 9050000000)
		binary.LittleEndian.PutUint64(body[46:], 1399300)
		binary.LittleEndian.PutUint64(body[54:], 1448410)
		binary.LittleEndian.PutUint64(body[62:], 1399300)
		if bl == 86 {
			binary.LittleEndian.PutUint64(body[70:], 9990000000)
			binary.LittleEndian.PutUint64(body[78:], 9010000000)
		}
		var m Message = Message{Kind: KindSecurityStatus, body: body}
		m.Header.BlockLength = bl
		var v, ok = m.SecurityStatusView()
		if !ok || v.SecurityID() != 305203 || string(v.Symbol()) != "Si-12.26" || v.TradingStatus() != TradingStatusInstrumentHalt ||
			v.HighLimitPx() != 9950000000 || v.InitialMarginSyntetic() != 1399300 {
			t.Fatalf("bl=%d: %v %d %q", bl, ok, v.SecurityID(), v.Symbol())
		}
		if bl == 70 && v.HighLimitPxWeekend() != NullDecimalMantissa {
			t.Fatalf("v8 status must not expose weekend limits")
		}
		if bl == 86 && v.LowLimitPxWeekend() != 9010000000 {
			t.Fatalf("v9 weekend limit")
		}
	}
}

func TestTradingSessionStatusDecode(t *testing.T) {
	var body []byte = make([]byte, 48)
	binary.LittleEndian.PutUint64(body[0:], 20260515060000000)
	binary.LittleEndian.PutUint64(body[8:], 20260515235000000)
	body[16] = 3
	binary.LittleEndian.PutUint32(body[17:], 7562)
	body[21] = uint8(TradSesStatusOpen)
	body[22] = 'F'
	body[23] = 3
	binary.LittleEndian.PutUint64(body[24:], 743)
	var m Message = Message{Kind: KindTradingSessionStatus, body: body}
	var ts, ok = m.TradingSessionStatus()
	if !ok || ts.TradingSessionID != 3 || ts.ExchangeTradingSessionID != 7562 || ts.TradSesStatus != TradSesStatusOpen ||
		ts.MarketSegmentID != 'F' || ts.TradSesEvent != 3 || ts.TradePeriodID != 743 {
		t.Fatalf("%+v %v", ts, ok)
	}
}
