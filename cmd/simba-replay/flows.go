package main

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/tonymontanov/go-moex/internal/pcap"
	"github.com/tonymontanov/go-moex/internal/simba"
)

type flowKey struct {
	dst  [4]byte
	port uint16
}

type flowStat struct {
	src              [4]byte
	packets, bytes   uint64
	first, last      time.Time
	minSeq, maxSeq   uint32
	incremental      uint64
	notSimba         uint64
	templates        map[uint16]uint64
	multiMsgPackets  uint64
	maxMsgsPerPacket int
}

// runFlows enumerates every UDP destination (group:port) in the capture and
// characterises it as a SIMBA channel where possible.
func runFlows(rd *pcap.Reader, opts runOptions) error {
	var flows map[flowKey]*flowStat = make(map[flowKey]*flowStat)
	var n uint64
	for {
		var pkt pcap.Packet
		var err error
		pkt, err = rd.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		n++
		if opts.maxPackets > 0 && n > opts.maxPackets {
			break
		}
		var key flowKey = flowKey{dst: pkt.DstIP, port: pkt.DstPort}
		var fs *flowStat = flows[key]
		if fs == nil {
			fs = &flowStat{src: pkt.SrcIP, first: pkt.TS, templates: make(map[uint16]uint64)}
			flows[key] = fs
		}
		fs.packets++
		fs.bytes += uint64(len(pkt.Payload))
		fs.last = pkt.TS

		var msgs int
		var info simba.PacketInfo
		info, err = simba.Walk(pkt.Payload, func(hdr simba.SBEHeader, body []byte) bool {
			fs.templates[hdr.TemplateID]++
			msgs++
			return true
		})
		if err != nil || len(pkt.Payload) < 16 || int(info.Header.MsgSize) != len(pkt.Payload) {
			fs.notSimba++
			continue
		}
		if fs.packets == 1 || info.Header.MsgSeqNum < fs.minSeq {
			fs.minSeq = info.Header.MsgSeqNum
		}
		if info.Header.MsgSeqNum > fs.maxSeq {
			fs.maxSeq = info.Header.MsgSeqNum
		}
		if info.Header.IsIncremental() {
			fs.incremental++
		}
		if msgs > 1 {
			fs.multiMsgPackets++
		}
		if msgs > fs.maxMsgsPerPacket {
			fs.maxMsgsPerPacket = msgs
		}
	}

	var keys []flowKey
	for k := range flows {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return flows[keys[i]].packets > flows[keys[j]].packets })
	fmt.Printf("%-24s %-16s %10s %12s %8s %10s %10s %9s %s\n", "dst", "src", "packets", "bytes", "inc%", "seq_min", "seq_max", "multi", "templates")
	for _, k := range keys {
		var fs *flowStat = flows[k]
		var incPct float64
		if fs.packets > 0 {
			incPct = 100 * float64(fs.incremental) / float64(fs.packets)
		}
		fmt.Printf("%-24s %-16s %10d %12d %7.1f%% %10d %10d %9d %s\n",
			pcap.FormatAddr(k.dst, k.port), pcap.FormatAddr(fs.src, 0)[:len(pcap.FormatAddr(fs.src, 0))-2],
			fs.packets, fs.bytes, incPct, fs.minSeq, fs.maxSeq, fs.multiMsgPackets, formatTemplates(fs.templates))
		if fs.notSimba > 0 {
			fmt.Printf("%-24s   not-simba packets: %d\n", "", fs.notSimba)
		}
	}
	if len(keys) > 0 {
		var first, last time.Time = flows[keys[0]].first, flows[keys[0]].last
		for _, k := range keys {
			if flows[k].first.Before(first) {
				first = flows[k].first
			}
			if flows[k].last.After(last) {
				last = flows[k].last
			}
		}
		fmt.Printf("\ncapture window: %s .. %s (%s)\n", first.Format(time.RFC3339Nano), last.Format(time.RFC3339Nano), last.Sub(first).Round(time.Millisecond))
	}
	return nil
}

func formatTemplates(m map[uint16]uint64) string {
	var ids []int
	for id := range m {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	var s string
	for i, id := range ids {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("%s=%d", templateName(uint16(id)), m[uint16(id)])
	}
	return s
}

func templateName(id uint16) string {
	switch id {
	case simba.TemplateHeartbeat:
		return "Heartbeat"
	case simba.TemplateSequenceReset:
		return "SeqReset"
	case simba.TemplateEmptyBook:
		return "EmptyBook"
	case simba.TemplateBestPrices:
		return "BestPrices"
	case simba.TemplateOrderUpdate:
		return "OrderUpdate"
	case simba.TemplateOrderExecution:
		return "OrderExec"
	case simba.TemplateOrderBookSnapshot:
		return "Snapshot"
	case simba.TemplateSecurityDefinition:
		return "SecDef"
	case simba.TemplateSecurityStatus:
		return "SecStatus"
	case simba.TemplateSecurityDefinitionUpdate:
		return "SecDefUpd"
	case simba.TemplateTradingSessionStatus:
		return "TrdSessStatus"
	case simba.TemplateDiscreteAuction:
		return "DiscreteAuction"
	case simba.TemplateSecurityMassStatus:
		return "SecMassStatus"
	case simba.TemplateSecurityGroupStatus:
		return "SecGroupStatus"
	}
	return fmt.Sprintf("T%d", id)
}
