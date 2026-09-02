package simba

import (
	"io"
	"testing"

	"github.com/tonymontanov/go-moex/internal/pcap"
)

// The fixtures under testdata/ are cut from MOEX's public production
// captures (2026-05-15, schema version 8) by `simba-replay extract`. Every
// packet must pass the strict ParsePacket guards and yield exactly the
// messages the tolerant Walk sees.
func TestFixturesParseStrict(t *testing.T) {
	for _, name := range []string{"main-session-2instr.pcap.gz", "tech-break-2instr.pcap.gz"} {
		t.Run(name, func(t *testing.T) {
			var src *pcap.Source
			var err error
			src, err = pcap.Open("testdata/"+name, "")
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			var packets, messages int
			var kinds map[Kind]int = make(map[Kind]int)
			for {
				var pkt pcap.Packet
				pkt, err = src.Reader.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				var p Packet
				p, err = ParsePacket(pkt.Payload, SchemaVersion8)
				if err != nil {
					t.Fatalf("record %d: %v", pkt.Index, err)
				}
				var walked int
				var info PacketInfo
				info, err = Walk(pkt.Payload, func(hdr SBEHeader, body []byte) bool { walked++; return true })
				if err != nil || info.Truncated {
					t.Fatalf("record %d: walk %v truncated=%v", pkt.Index, err, info.Truncated)
				}
				for {
					var m Message
					var ok bool
					m, ok, err = p.Next()
					if err != nil {
						t.Fatalf("record %d: %v", pkt.Index, err)
					}
					if !ok {
						break
					}
					kinds[m.Kind]++
					messages++
					switch m.Kind {
					case KindOrderUpdate:
						if _, ok = m.OrderUpdate(); !ok {
							t.Fatalf("record %d: OrderUpdate decode", pkt.Index)
						}
					case KindOrderExecution:
						if _, ok = m.OrderExecution(); !ok {
							t.Fatalf("record %d: OrderExecution decode", pkt.Index)
						}
					case KindOrderBookSnapshot:
						var v SnapshotView
						if v, ok = m.Snapshot(); !ok || v.Len() == 0 {
							t.Fatalf("record %d: snapshot decode", pkt.Index)
						}
					case KindBestPrices:
						var v BestPricesView
						if v, ok = m.BestPrices(); !ok || v.Len() == 0 {
							t.Fatalf("record %d: bestprices decode", pkt.Index)
						}
					case KindUnknown:
						t.Fatalf("record %d: unknown template %d", pkt.Index, m.Header.TemplateID)
					}
				}
				if p.Messages() != walked {
					t.Fatalf("record %d: Packet yielded %d messages, Walk %d", pkt.Index, p.Messages(), walked)
				}
				packets++
			}
			if packets == 0 || kinds[KindOrderUpdate] == 0 || kinds[KindOrderBookSnapshot] == 0 {
				t.Fatalf("fixture too thin: packets=%d kinds=%v", packets, kinds)
			}
			t.Logf("%s: packets=%d messages=%d kinds=%v", name, packets, messages, kinds)
		})
	}
}
