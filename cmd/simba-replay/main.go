/*
FILE: cmd/simba-replay/main.go

DESCRIPTION:
Offline test harness for the SIMBA SPECTRA client: replays MOEX's public
production captures (https://ftp.moex.com/pub/SIMBA/Spectra/prod/pcap/)
through the decoder and a deliberately simple reference order-book model,
and reports what the protocol actually does on the wire — the things the
spec leaves ambiguous and synthetic unit tests cannot see.

Subcommands:

	flows  -pcap FILE            list UDP flows (multicast group:port) with packet counts
	stats  -pcap FILE [-inc A] [-snap A]
	                             per-channel MsgSeqNum continuity, template histogram,
	                             multi-message packets, RptSeq continuity per instrument,
	                             EmptyBook / SequenceReset contexts, snapshot cycles
	verify -pcap FILE [-inc A] [-snap A] [-max-instruments N]
	                             build books from snapshot + incrementals per spec §4.2.5
	                             and compare against every later snapshot of the same
	                             instrument (the correctness oracle for the real client)
	session -pcap FILE [-max-instruments N] [-ab] [-loss P] [-replay] [-seed S]
	                             drive the production forts.BookSession over the capture
	                             and check it against every snapshot and BestPrices;
	                             -ab/-loss/-replay simulate redundant legs, packet loss
	                             and TCP replay served from the capture
	extract -pcap FILE -sec ID[,ID] [-from 3s] [-to 20s] -out small.pcap
	                             cut a self-consistent fixture for the chosen instruments

FILE may be a .pcap or a .zip containing one (streamed, never extracted).
Group addresses default to the production "primary" (A) feeds from
configuration.xml; pass -inc/-snap explicitly for B or test circuits.
*/
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tonymontanov/go-moex/internal/pcap"
)

const (
	defaultIncA  = "239.195.20.81:20081"
	defaultIncB  = "239.195.148.81:21081"
	defaultSnapA = "239.195.20.82:20082"
	defaultSnapB = "239.195.148.82:21082"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var sub string = os.Args[1]
	var fs *flag.FlagSet = flag.NewFlagSet(sub, flag.ExitOnError)
	var pcapPath *string = fs.String("pcap", "", "capture file (.pcap or .zip)")
	var entry *string = fs.String("entry", "", "zip entry name substring (if the archive holds several captures)")
	var incAddr *string = fs.String("inc", defaultIncA, "Incremental feed group:port")
	var snapAddr *string = fs.String("snap", defaultSnapA, "Snapshot feed group:port")
	var incB *string = fs.String("inc-b", defaultIncB, "Incremental feed B group:port (A/B comparison in stats)")
	var maxInstr *int = fs.Int("max-instruments", 0, "verify: limit tracked instruments (0 = all)")
	var maxPackets *uint64 = fs.Uint64("max-packets", 0, "stop after this many UDP packets (0 = all)")
	var verbose *bool = fs.Bool("v", false, "verbose: print every EmptyBook/SequenceReset/mismatch context")
	var examples *int = fs.Int("examples", 5, "how many example contexts/mismatches to print per category")
	var secList *string = fs.String("sec", "", "extract: comma-separated SecurityIDs to keep")
	var from *string = fs.String("from", "", "extract: window start as offset from the first packet (e.g. 3s)")
	var to *string = fs.String("to", "", "extract: window end as offset from the first packet (e.g. 20s)")
	var out *string = fs.String("out", "", "extract: output .pcap path")
	var ab *bool = fs.Bool("ab", false, "session: feed every incremental packet through two legs (A/B) with random ordering")
	var loss *float64 = fs.Float64("loss", 0, "session: independent per-leg packet loss probability (0..1); with -ab correlated loss is loss^2")
	var replay *bool = fs.Bool("replay", false, "session: serve TCP-replay requests from the capture itself")
	var seed *int64 = fs.Int64("seed", 1, "session: RNG seed for -ab/-loss")
	_ = fs.Parse(os.Args[2:])
	if *pcapPath == "" {
		usage()
		os.Exit(2)
	}

	var src *pcap.Source
	var err error
	src, err = pcap.Open(*pcapPath, *entry)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer src.Close()
	fmt.Printf("# %s (link type %d)\n", src.Name, src.Reader.LinkType())

	var opts runOptions = runOptions{
		inc:        *incAddr,
		incB:       *incB,
		snap:       *snapAddr,
		maxInstr:   *maxInstr,
		maxPackets: *maxPackets,
		verbose:    *verbose,
		examples:   *examples,
		ab:         *ab,
		loss:       *loss,
		replay:     *replay,
		seed:       *seed,
	}
	var started time.Time = time.Now()
	switch sub {
	case "flows":
		err = runFlows(src.Reader, opts)
	case "stats":
		err = runStats(src.Reader, opts)
	case "verify":
		err = runVerify(src.Reader, opts)
	case "session":
		err = runSession(src.Reader, opts)
	case "extract":
		var eo extractOptions
		eo, err = parseExtractFlags(*secList, *from, *to, *out)
		if err == nil {
			err = runExtract(src.Reader, opts, eo)
		}
	default:
		usage()
		os.Exit(2)
	}
	var st pcap.Stats = src.Reader.Stats()
	fmt.Printf("\n# pcap: records=%d udp=%d skipped_link=%d skipped_proto=%d fragments=%d truncated=%d elapsed=%s\n",
		st.Records, st.UDP, st.SkippedLink, st.SkippedProto, st.Fragments, st.Truncated, time.Since(started).Round(time.Millisecond))
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type runOptions struct {
	inc, incB, snap string
	maxInstr        int
	maxPackets      uint64
	verbose         bool
	examples        int
	ab              bool
	loss            float64
	replay          bool
	seed            int64
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: simba-replay {flows|stats|verify|session|extract} -pcap FILE [-inc GROUP:PORT] [-snap GROUP:PORT] [flags]")
}
