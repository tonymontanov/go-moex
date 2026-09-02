package main

import (
	"testing"

	"github.com/tonymontanov/go-moex/internal/pcap"
)

// The fixtures under internal/simba/testdata/ are cut from MOEX's public production
// captures (ftp.moex.com/pub/SIMBA/Spectra/prod/pcap/, 2026-05-15) with
// `simba-replay extract`. The reference model must reproduce every
// snapshot and every BestPrices message on them exactly — this is the
// regression gate for the real client's decoder and order-book logic.

func openFixture(t *testing.T, name string) *pcap.Source {
	t.Helper()
	var src *pcap.Source
	var err error
	src, err = pcap.Open("../../internal/simba/testdata/"+name, "")
	if err != nil {
		t.Fatalf("open fixture %s: %v", name, err)
	}
	return src
}

func defaultOpts() runOptions {
	return runOptions{inc: defaultIncA, incB: defaultIncB, snap: defaultSnapA, examples: 3}
}

func TestVerifyMainSessionFixture(t *testing.T) {
	var src *pcap.Source = openFixture(t, "main-session-2instr.pcap.gz")
	defer src.Close()
	var vs *verifyState
	var err error
	vs, err = verifyCapture(src.Reader, defaultOpts())
	if err != nil {
		t.Fatal(err)
	}
	var compares, mismatches, bpChecks, bpMism, rptMism, syncRptGap int
	var lazy, live applyStats
	compares, mismatches, bpChecks, bpMism, rptMism, syncRptGap, lazy, live = vs.totals()
	if len(vs.instr) != 2 {
		t.Fatalf("instruments=%d, want 2", len(vs.instr))
	}
	if compares < 4 || mismatches != 0 {
		t.Fatalf("snapshot oracle: compares=%d mismatches=%d", compares, mismatches)
	}
	if bpChecks < 10 || bpMism != 0 {
		t.Fatalf("bestprices oracle: checks=%d mismatches=%d", bpChecks, bpMism)
	}
	if rptMism != 0 || syncRptGap != 0 {
		t.Fatalf("RptSeq semantics: snapshot/book mismatch=%d sync gaps=%d", rptMism, syncRptGap)
	}
	for _, s := range []applyStats{lazy, live} {
		if s.unknownDelete != 0 || s.unknownChange != 0 || s.rptAnomalies != 0 || s.unsequenced != 0 || s.changeSizeGrew != 0 {
			t.Fatalf("apply anomalies: %s", formatApply(&s))
		}
	}
	if vs.inc.gaps != 0 || vs.snap.gaps != 0 {
		t.Fatalf("fixture has MsgSeqNum gaps: inc=%d snap=%d", vs.inc.gaps, vs.snap.gaps)
	}
}

func TestVerifyTechBreakFixture(t *testing.T) {
	var src *pcap.Source = openFixture(t, "tech-break-2instr.pcap.gz")
	defer src.Close()
	var vs *verifyState
	var err error
	vs, err = verifyCapture(src.Reader, defaultOpts())
	if err != nil {
		t.Fatal(err)
	}
	if vs.seqResets != 1 || vs.emptyBooks != 1 {
		t.Fatalf("expected one SequenceReset and one EmptyBook, got %d/%d", vs.seqResets, vs.emptyBooks)
	}
	var compares, mismatches, _, _, rptMism, _, lazy, _ = vs.totals()
	if compares < 4 || mismatches != 0 || rptMism != 0 {
		t.Fatalf("snapshot oracle after EmptyBook rebroadcast: compares=%d mismatches=%d rptMism=%d", compares, mismatches, rptMism)
	}
	// The books after EmptyBook are rebuilt from the PossDup rebroadcast,
	// where every OrderUpdate carries RptSeq=0 (observed on production).
	if lazy.applied == 0 || lazy.unsequenced != lazy.applied {
		t.Fatalf("expected the whole rebroadcast to be RptSeq=0: %s", formatApply(&lazy))
	}
}
