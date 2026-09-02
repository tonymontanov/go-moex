package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/tonymontanov/go-moex/internal/pcap"
	"github.com/tonymontanov/go-moex/internal/simba"
)

// MDFlags bits (MDFlagsSet, simba_spectra-9.0.xml) used by the harness.
const (
	mdFlagDay              uint64 = 1 << 0
	mdFlagIOC              uint64 = 1 << 1
	mdFlagNonQuote         uint64 = 1 << 2
	mdFlagEndOfTransaction uint64 = 1 << 12
	mdFlagActiveSide       uint64 = 1 << 41
	mdFlagPassiveSide      uint64 = 1 << 42
)

// channel — MsgSeqNum continuity tracker for one multicast channel.
type channel struct {
	name       string
	packets    uint64
	have       bool
	last       uint32
	gaps       uint64
	missing    uint64
	dups       uint64
	resets     uint64 // seq went back to 1 (snapshot cycle boundary / daily reset).
	backwards  uint64 // seq decreased but not to 1.
	schemas    map[[2]uint16]uint64
	truncated  uint64
	walkErrors uint64
	firstTS    time.Time
	lastTS     time.Time
}

func newChannel(name string) *channel {
	return &channel{name: name, schemas: make(map[[2]uint16]uint64)}
}

// observe updates the tracker; returns true if seq restarted from 1.
func (c *channel) observe(seq uint32, ts time.Time) (restart bool) {
	c.packets++
	if c.firstTS.IsZero() {
		c.firstTS = ts
	}
	c.lastTS = ts
	if !c.have {
		c.have = true
		c.last = seq
		return false
	}
	switch {
	case seq == c.last+1:
	case seq == 1:
		c.resets++
		restart = true
	case seq > c.last+1:
		c.gaps++
		c.missing += uint64(seq - c.last - 1)
	case seq == c.last:
		c.dups++
	default:
		c.backwards++
	}
	c.last = seq
	return restart
}

func (c *channel) summary() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s: packets=%d gaps=%d missing=%d dups=%d restarts=%d backwards=%d walk_errors=%d truncated=%d schemas=",
		c.name, c.packets, c.gaps, c.missing, c.dups, c.resets, c.backwards, c.walkErrors, c.truncated)
	var keys [][2]uint16
	for k := range c.schemas {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i][1] < keys[j][1] })
	for _, k := range keys {
		fmt.Fprintf(&sb, "[id=%d v=%d]=%d ", k[0], k[1], c.schemas[k])
	}
	return sb.String()
}

// histogram — small integer histogram with overflow bucket.
type histogram struct {
	buckets map[int64]uint64
	total   uint64
}

func newHistogram() *histogram { return &histogram{buckets: make(map[int64]uint64)} }

func (h *histogram) add(v int64) {
	h.buckets[v]++
	h.total++
}

func (h *histogram) String() string {
	var keys []int64
	for k := range h.buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	var parts []string
	for i, k := range keys {
		if i >= 24 {
			parts = append(parts, fmt.Sprintf("...(+%d buckets)", len(keys)-i))
			break
		}
		parts = append(parts, fmt.Sprintf("%d:%d", k, h.buckets[k]))
	}
	return fmt.Sprintf("n=%d {%s}", h.total, strings.Join(parts, " "))
}

// sizeBucket maps a payload length / message count to a coarse bucket.
func sizeBucket(n int) int64 {
	switch {
	case n <= 1:
		return 1
	case n <= 2:
		return 2
	case n <= 5:
		return 5
	case n <= 10:
		return 10
	case n <= 20:
		return 20
	case n <= 50:
		return 50
	default:
		return 100
	}
}

// addrOf renders the destination of a packet as group:port.
func addrOf(pkt pcap.Packet) string { return pcap.FormatAddr(pkt.DstIP, pkt.DstPort) }

// walkDecoded runs Walk and decodes every message; fn receives the index
// of the message inside the packet and a Decoded with only that message's
// payload field set.
func walkDecoded(payload []byte, fn func(i int, hdr simba.SBEHeader, d *simba.Decoded)) (simba.PacketInfo, error) {
	var i int
	var decodeErr error
	var info simba.PacketInfo
	var err error
	info, err = simba.Walk(payload, func(hdr simba.SBEHeader, body []byte) bool {
		var d simba.Decoded
		if e := simba.DecodeMessage(hdr, body, &d); e != nil {
			decodeErr = e
			return false
		}
		fn(i, hdr, &d)
		i++
		return true
	})
	if err != nil {
		return info, err
	}
	return info, decodeErr
}

// forEachPacket streams the capture, dispatching packets by destination.
func forEachPacket(rd *pcap.Reader, maxPackets uint64, fn func(pkt pcap.Packet) error) error {
	var n uint64
	for {
		var pkt pcap.Packet
		var err error
		pkt, err = rd.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		n++
		if maxPackets > 0 && n > maxPackets {
			return nil
		}
		if err = fn(pkt); err != nil {
			return err
		}
	}
}

func flagsString(f uint16) string {
	var parts []string
	if f&simba.FlagLastFragment != 0 {
		parts = append(parts, "LastFragment")
	}
	if f&simba.FlagStartOfSnapshot != 0 {
		parts = append(parts, "StartOfSnapshot")
	}
	if f&simba.FlagEndOfSnapshot != 0 {
		parts = append(parts, "EndOfSnapshot")
	}
	if f&simba.FlagIncrementalPacket != 0 {
		parts = append(parts, "Incremental")
	}
	if f&simba.FlagPossDupFlag != 0 {
		parts = append(parts, "PossDup")
	}
	return strings.Join(parts, "|")
}

func mdFlagsString(f uint64) string {
	var parts []string
	if f&mdFlagDay != 0 {
		parts = append(parts, "Day")
	}
	if f&mdFlagIOC != 0 {
		parts = append(parts, "IOC")
	}
	if f&mdFlagNonQuote != 0 {
		parts = append(parts, "NonQuote")
	}
	if f&mdFlagEndOfTransaction != 0 {
		parts = append(parts, "EOT")
	}
	if f&mdFlagActiveSide != 0 {
		parts = append(parts, "Active")
	}
	if f&mdFlagPassiveSide != 0 {
		parts = append(parts, "Passive")
	}
	var rest uint64 = f &^ (mdFlagDay | mdFlagIOC | mdFlagNonQuote | mdFlagEndOfTransaction | mdFlagActiveSide | mdFlagPassiveSide)
	if rest != 0 {
		parts = append(parts, fmt.Sprintf("0x%x", rest))
	}
	return strings.Join(parts, "|")
}

func describeMessage(hdr simba.SBEHeader, d *simba.Decoded) string {
	switch {
	case d.OrderUpdate != nil:
		var u *simba.OrderUpdate = d.OrderUpdate
		return fmt.Sprintf("OrderUpdate sec=%d rpt=%d act=%d side=%c id=%d px=%d sz=%d flags=%s", u.SecurityID, u.RptSeq, u.MDUpdateAction, u.MDEntryType, u.MDEntryID, u.MDEntryPx, u.MDEntrySize, mdFlagsString(u.MDFlags))
	case d.OrderExecution != nil:
		var e *simba.OrderExecution = d.OrderExecution
		return fmt.Sprintf("OrderExec sec=%d rpt=%d act=%d side=%c id=%d px=%d sz=%d lastpx=%d lastqty=%d trade=%d flags=%s", e.SecurityID, e.RptSeq, e.MDUpdateAction, e.MDEntryType, e.MDEntryID, e.MDEntryPx, e.MDEntrySize, e.LastPx, e.LastQty, e.TradeID, mdFlagsString(e.MDFlags))
	case d.EmptyBook != nil:
		return fmt.Sprintf("EmptyBook lastMsgSeqNumProcessed=%d", d.EmptyBook.LastMsgSeqNumProcessed)
	case d.NewSeqNo != nil:
		return fmt.Sprintf("SequenceReset newSeqNo=%d", *d.NewSeqNo)
	case d.BestPrices != nil:
		return fmt.Sprintf("BestPrices entries=%d", len(d.BestPrices.Entries))
	case d.OrderBookSnapshot != nil:
		var s *simba.OrderBookSnapshot = d.OrderBookSnapshot
		return fmt.Sprintf("Snapshot sec=%d lastMsgSeqNumProcessed=%d rpt=%d session=%d entries=%d", s.SecurityID, s.LastMsgSeqNumProcessed, s.RptSeq, s.ExchangeTradingSessionID, len(s.Entries))
	}
	return templateName(hdr.TemplateID)
}
