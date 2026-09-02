package main

import (
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tonymontanov/go-moex/internal/pcap"
	"github.com/tonymontanov/go-moex/internal/simba"
)

/*
extract — cut a small, self-consistent fixture out of a production capture.

Keeps only the packets/messages that concern the chosen instruments inside
a time window, rewriting what must stay consistent for a client to
process the fixture exactly like live traffic:

  - Incremental packets are rebuilt with only the matching OrderUpdate /
    OrderExecution messages (BestPrices groups are filtered to matching
    entries); session messages (Heartbeat, SequenceReset, EmptyBook) are
    kept. Packets left without messages are dropped and MsgSeqNum is
    renumbered contiguously.
  - Snapshot packets of the chosen instruments are kept; their
    LastMsgSeqNumProcessed is remapped to the renumbered Incremental
    sequence; the snapshot channel's MsgSeqNum is renumbered per cycle.
  - Output is classic pcap (nanosecond magic, Ethernet/IPv4/UDP frames
    synthesised with the original addresses and timestamps).

The result is a few hundred KB and goes into testdata/ as a golden
fixture — the verify oracle must pass on it, forever.
*/

type extractOptions struct {
	secs     map[int32]bool
	from, to time.Duration // offsets from the first packet; to==0 means no limit.
	out      string
}

func parseExtractFlags(secList, from, to, out string) (extractOptions, error) {
	var eo extractOptions = extractOptions{secs: make(map[int32]bool), out: out}
	for _, s := range strings.Split(secList, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		var v int64
		var err error
		v, err = strconv.ParseInt(s, 10, 32)
		if err != nil {
			return eo, fmt.Errorf("bad -sec %q: %w", s, err)
		}
		eo.secs[int32(v)] = true
	}
	if len(eo.secs) == 0 {
		return eo, fmt.Errorf("-sec is required (comma-separated SecurityIDs)")
	}
	var err error
	if from != "" {
		if eo.from, err = time.ParseDuration(from); err != nil {
			return eo, fmt.Errorf("bad -from: %w", err)
		}
	}
	if to != "" {
		if eo.to, err = time.ParseDuration(to); err != nil {
			return eo, fmt.Errorf("bad -to: %w", err)
		}
	}
	if out == "" {
		return eo, fmt.Errorf("-out is required")
	}
	return eo, nil
}

func runExtract(rd *pcap.Reader, opts runOptions, eo extractOptions) error {
	var f *os.File
	var err error
	f, err = os.Create(eo.out)
	if err != nil {
		return err
	}
	defer f.Close()
	var gz *gzip.Writer
	var sink io.Writer = f
	if strings.HasSuffix(strings.ToLower(eo.out), ".gz") {
		gz = gzip.NewWriter(f)
		sink = gz
	}
	var pw *pcap.Writer
	pw, err = pcap.NewWriter(sink)
	if err != nil {
		return err
	}
	var write = func(pkt pcap.Packet, payload []byte) error {
		return pw.WriteUDP(pkt.TS, pkt.SrcIP, pkt.DstIP, pkt.SrcPort, pkt.DstPort, payload)
	}
	var first time.Time
	var keptIncOldSeq []uint32 // old MsgSeqNum of every kept Incremental packet, ascending.
	var incNewSeq uint32
	var snapNewSeq uint32
	var dropped, msgsKept, msgsDropped uint64

	err = forEachPacket(rd, opts.maxPackets, func(pkt pcap.Packet) error {
		if first.IsZero() {
			first = pkt.TS
		}
		var off time.Duration = pkt.TS.Sub(first)
		if off < eo.from {
			return nil
		}
		if eo.to > 0 && off > eo.to {
			return nil
		}
		switch addrOf(pkt) {
		case opts.inc:
			var payload []byte
			var oldSeq uint32
			var kept, total int
			payload, oldSeq, kept, total = filterIncremental(pkt.Payload, eo.secs)
			msgsKept += uint64(kept)
			msgsDropped += uint64(total - kept)
			if payload == nil {
				dropped++
				return nil
			}
			if newSeq, isReset := simba.SequenceResetIn(payload); isReset {
				// Mirror the wire: the SequenceReset packet is numbered in the
				// new sequence and the next packet carries NewSeqNo again; L
				// values after it refer to the new numbering (new epoch).
				incNewSeq = newSeq - 1
				keptIncOldSeq = keptIncOldSeq[:0]
				binary.LittleEndian.PutUint32(payload[0:4], newSeq)
				return write(pkt, payload)
			}
			incNewSeq++
			keptIncOldSeq = append(keptIncOldSeq, oldSeq)
			binary.LittleEndian.PutUint32(payload[0:4], incNewSeq)
			return write(pkt, payload)
		case opts.snap:
			var payload []byte
			var restart bool
			payload, restart = filterSnapshot(pkt.Payload, eo.secs, func(L uint32) uint32 {
				// New L = number of kept incremental packets with old seq <= L.
				return uint32(sort.Search(len(keptIncOldSeq), func(i int) bool { return keptIncOldSeq[i] > L }))
			})
			if payload == nil {
				dropped++
				return nil
			}
			if restart {
				snapNewSeq = 0
			}
			snapNewSeq++
			binary.LittleEndian.PutUint32(payload[0:4], snapNewSeq)
			return write(pkt, payload)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err = pw.Flush(); err != nil {
		return err
	}
	if gz != nil {
		if err = gz.Close(); err != nil {
			return err
		}
	}
	fmt.Printf("wrote %s: packets=%d bytes=%d (dropped packets=%d, messages kept=%d dropped=%d)\n", eo.out, pw.Packets, pw.Bytes, dropped, msgsKept, msgsDropped)
	return nil
}

// filterIncremental rebuilds an Incremental packet with only the messages
// concerning secs. Returns nil if nothing is left.
func filterIncremental(buf []byte, secs map[int32]bool) (out []byte, oldSeq uint32, kept int, total int) {
	var hdrLen int = 16 + 12
	if len(buf) < hdrLen {
		return nil, 0, 0, 0
	}
	out = append(out, buf[:hdrLen]...)
	var info simba.PacketInfo
	var err error
	info, err = simba.Walk(buf, func(hdr simba.SBEHeader, body []byte) bool {
		total++
		var keep bool
		var newBody []byte = body
		switch hdr.TemplateID {
		case simba.TemplateOrderUpdate:
			keep = len(body) >= 44 && secs[int32(binary.LittleEndian.Uint32(body[40:44]))]
		case simba.TemplateOrderExecution:
			keep = len(body) >= 68 && secs[int32(binary.LittleEndian.Uint32(body[64:68]))]
		case simba.TemplateBestPrices:
			newBody = filterBestPrices(body, hdr.BlockLength, secs)
			keep = newBody != nil
		case simba.TemplateHeartbeat, simba.TemplateSequenceReset, simba.TemplateEmptyBook:
			keep = true
		}
		if !keep {
			return true
		}
		kept++
		var sh [8]byte
		binary.LittleEndian.PutUint16(sh[0:2], hdr.BlockLength)
		binary.LittleEndian.PutUint16(sh[2:4], hdr.TemplateID)
		binary.LittleEndian.PutUint16(sh[4:6], hdr.SchemaID)
		binary.LittleEndian.PutUint16(sh[6:8], hdr.Version)
		out = append(out, sh[:]...)
		out = append(out, newBody...)
		return true
	})
	if err != nil || !info.Header.IsIncremental() || kept == 0 {
		return nil, 0, kept, total
	}
	binary.LittleEndian.PutUint16(out[4:6], uint16(len(out)))
	return out, info.Header.MsgSeqNum, kept, total
}

func filterBestPrices(body []byte, blockLen uint16, secs map[int32]bool) []byte {
	var off int = int(blockLen)
	if len(body) < off+3 {
		return nil
	}
	var entryLen int = int(binary.LittleEndian.Uint16(body[off : off+2]))
	var num int = int(body[off+2])
	var out []byte = append([]byte{}, body[:off+3]...)
	var kept int
	var p int = off + 3
	for i := 0; i < num && p+entryLen <= len(body); i++ {
		var e []byte = body[p : p+entryLen]
		if entryLen >= 36 && secs[int32(binary.LittleEndian.Uint32(e[32:36]))] {
			out = append(out, e...)
			kept++
		}
		p += entryLen
	}
	if kept == 0 {
		return nil
	}
	out[off+2] = byte(kept)
	return out
}

// filterSnapshot keeps Snapshot-channel packets that are session messages
// or OrderBookSnapshot of a chosen instrument, remapping
// LastMsgSeqNumProcessed via remapL. restart reports old MsgSeqNum == 1.
func filterSnapshot(buf []byte, secs map[int32]bool, remapL func(uint32) uint32) (out []byte, restart bool) {
	if len(buf) < 16+8 {
		return nil, false
	}
	var oldSeq uint32 = binary.LittleEndian.Uint32(buf[0:4])
	restart = oldSeq == 1
	var hdr simba.SBEHeader
	hdr.TemplateID = binary.LittleEndian.Uint16(buf[18:20])
	switch hdr.TemplateID {
	case simba.TemplateHeartbeat, simba.TemplateSequenceReset:
		return append([]byte{}, buf...), restart
	case simba.TemplateOrderBookSnapshot:
		var body []byte = buf[24:]
		if len(body) < 16 {
			return nil, restart
		}
		if !secs[int32(binary.LittleEndian.Uint32(body[0:4]))] {
			return nil, restart
		}
		out = append([]byte{}, buf...)
		var L uint32 = binary.LittleEndian.Uint32(out[24+4 : 24+8])
		binary.LittleEndian.PutUint32(out[24+4:24+8], remapL(L))
		return out, restart
	}
	return nil, restart
}
