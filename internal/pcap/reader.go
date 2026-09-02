/*
FILE: internal/pcap/reader.go

DESCRIPTION:
Minimal streaming reader for libpcap capture files (the classic "pcap"
format, magic 0xa1b2c3d4 / 0xa1b23c4d, either byte order) that yields the
UDP/IPv4 payload of every datagram in capture order. No cgo, no external
dependencies — the format is 24 bytes of global header plus 16 bytes per
record, and stripping Ethernet/VLAN/IPv4/UDP is a few dozen lines.

Written for the SIMBA SPECTRA test harness (cmd/simba-replay): MOEX
publishes production traffic dumps at
https://ftp.moex.com/pub/SIMBA/Spectra/prod/pcap/ and those are the only
way to validate a SIMBA client without colocation. The reader is
deliberately streaming (one record in memory at a time) — the dumps are
hundreds of MB to GB and the harness must run on a small box.

Supported link types: Ethernet (1, with 802.1Q/802.1ad VLAN tags), Linux
cooked SLL (113), SLL2 (276), raw IPv4 (101/228). Anything else, IPv6,
non-UDP and IP fragments other than the first are skipped and counted in
Stats — never silently dropped without a trace.
*/
package pcap

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	magicMicros   uint32 = 0xa1b2c3d4
	magicNanos    uint32 = 0xa1b23c4d
	magicPcapNG   uint32 = 0x0a0d0d0a
	globalHdrSize        = 24
	recordHdrSize        = 16
	maxSnapLen           = 256 * 1024
	linkEthernet  uint32 = 1
	linkRawIP     uint32 = 101
	linkLinuxSLL  uint32 = 113
	linkRawIP2    uint32 = 228
	linkLinuxSLL2 uint32 = 276
	etherTypeIPv4 uint16 = 0x0800
	etherTypeVLAN uint16 = 0x8100
	etherTypeQinQ uint16 = 0x88a8
	ipProtoUDP    uint8  = 17
)

// ErrPcapNG is returned by NewReader for pcapng captures, which this reader
// does not parse (convert with `tshark -F pcap` or `editcap -F pcap`).
var ErrPcapNG = errors.New("pcap: pcapng format is not supported, convert to classic pcap first")

// Packet — one UDP/IPv4 datagram from the capture. Payload aliases the
// reader's internal buffer and is only valid until the next call to Next.
type Packet struct {
	Index   uint64 // 0-based record index in the file (all records, not just UDP).
	TS      time.Time
	SrcIP   [4]byte
	DstIP   [4]byte
	SrcPort uint16
	DstPort uint16
	Payload []byte
}

// Stats — counters of what the reader saw and skipped.
type Stats struct {
	Records      uint64
	UDP          uint64
	SkippedLink  uint64 // non-IPv4 ethertype / unsupported link frames.
	SkippedProto uint64 // IPv4 but not UDP.
	Fragments    uint64 // IPv4 fragments (non-first or MF set) — skipped.
	Truncated    uint64 // caplen < len or malformed headers.
}

// Reader — streaming pcap reader.
type Reader struct {
	r        *bufio.Reader
	order    binary.ByteOrder
	nanos    bool
	linkType uint32
	buf      []byte
	stats    Stats
}

// NewReader parses the global header from r.
func NewReader(r io.Reader) (*Reader, error) {
	var br *bufio.Reader = bufio.NewReaderSize(r, 1<<20)
	var hdr [globalHdrSize]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return nil, fmt.Errorf("pcap: read global header: %w", err)
	}
	var magicLE uint32 = binary.LittleEndian.Uint32(hdr[0:4])
	var magicBE uint32 = binary.BigEndian.Uint32(hdr[0:4])
	var rd *Reader = &Reader{r: br}
	switch {
	case magicLE == magicMicros:
		rd.order = binary.LittleEndian
	case magicLE == magicNanos:
		rd.order, rd.nanos = binary.LittleEndian, true
	case magicBE == magicMicros:
		rd.order = binary.BigEndian
	case magicBE == magicNanos:
		rd.order, rd.nanos = binary.BigEndian, true
	case magicLE == magicPcapNG || magicBE == magicPcapNG:
		return nil, ErrPcapNG
	default:
		return nil, fmt.Errorf("pcap: unknown magic 0x%08x", magicLE)
	}
	rd.linkType = rd.order.Uint32(hdr[20:24])
	switch rd.linkType {
	case linkEthernet, linkRawIP, linkRawIP2, linkLinuxSLL, linkLinuxSLL2:
	default:
		return nil, fmt.Errorf("pcap: unsupported link type %d", rd.linkType)
	}
	rd.buf = make([]byte, 64*1024)
	return rd, nil
}

// LinkType returns the capture's link-layer type.
func (rd *Reader) LinkType() uint32 { return rd.linkType }

// Stats returns the counters accumulated so far.
func (rd *Reader) Stats() Stats { return rd.stats }

// Next returns the next UDP/IPv4 datagram. io.EOF at end of file.
func (rd *Reader) Next() (Packet, error) {
	for {
		var pkt Packet
		var rh [recordHdrSize]byte
		if _, err := io.ReadFull(rd.r, rh[:]); err != nil {
			if err == io.ErrUnexpectedEOF {
				return pkt, io.EOF
			}
			return pkt, err
		}
		var sec uint32 = rd.order.Uint32(rh[0:4])
		var frac uint32 = rd.order.Uint32(rh[4:8])
		var caplen uint32 = rd.order.Uint32(rh[8:12])
		if caplen > maxSnapLen {
			return pkt, fmt.Errorf("pcap: record %d caplen %d exceeds sane maximum", rd.stats.Records, caplen)
		}
		if int(caplen) > len(rd.buf) {
			rd.buf = make([]byte, caplen)
		}
		var frame []byte = rd.buf[:caplen]
		if _, err := io.ReadFull(rd.r, frame); err != nil {
			return pkt, fmt.Errorf("pcap: record %d body: %w", rd.stats.Records, err)
		}
		pkt.Index = rd.stats.Records
		rd.stats.Records++
		if rd.nanos {
			pkt.TS = time.Unix(int64(sec), int64(frac)).UTC()
		} else {
			pkt.TS = time.Unix(int64(sec), int64(frac)*1000).UTC()
		}

		var ip []byte
		var ok bool
		ip, ok = rd.stripLink(frame)
		if !ok {
			continue
		}
		if !rd.parseIPv4UDP(ip, &pkt) {
			continue
		}
		rd.stats.UDP++
		return pkt, nil
	}
}

// stripLink removes the link-layer header and returns the IPv4 packet.
func (rd *Reader) stripLink(frame []byte) ([]byte, bool) {
	switch rd.linkType {
	case linkRawIP, linkRawIP2:
		return frame, true
	case linkEthernet:
		if len(frame) < 14 {
			rd.stats.Truncated++
			return nil, false
		}
		var et uint16 = binary.BigEndian.Uint16(frame[12:14])
		var off int = 14
		for et == etherTypeVLAN || et == etherTypeQinQ {
			if len(frame) < off+4 {
				rd.stats.Truncated++
				return nil, false
			}
			et = binary.BigEndian.Uint16(frame[off+2 : off+4])
			off += 4
		}
		if et != etherTypeIPv4 {
			rd.stats.SkippedLink++
			return nil, false
		}
		return frame[off:], true
	case linkLinuxSLL:
		if len(frame) < 16 {
			rd.stats.Truncated++
			return nil, false
		}
		if binary.BigEndian.Uint16(frame[14:16]) != etherTypeIPv4 {
			rd.stats.SkippedLink++
			return nil, false
		}
		return frame[16:], true
	case linkLinuxSLL2:
		if len(frame) < 20 {
			rd.stats.Truncated++
			return nil, false
		}
		if binary.BigEndian.Uint16(frame[0:2]) != etherTypeIPv4 {
			rd.stats.SkippedLink++
			return nil, false
		}
		return frame[20:], true
	}
	rd.stats.SkippedLink++
	return nil, false
}

// parseIPv4UDP fills pkt from an IPv4 packet; false if not a whole UDP datagram.
func (rd *Reader) parseIPv4UDP(ip []byte, pkt *Packet) bool {
	if len(ip) < 20 || ip[0]>>4 != 4 {
		rd.stats.SkippedLink++
		return false
	}
	var ihl int = int(ip[0]&0x0f) * 4
	if ihl < 20 || len(ip) < ihl {
		rd.stats.Truncated++
		return false
	}
	var totalLen int = int(binary.BigEndian.Uint16(ip[2:4]))
	if totalLen > len(ip) {
		rd.stats.Truncated++
		return false
	}
	if totalLen >= ihl {
		ip = ip[:totalLen] // drop Ethernet padding.
	}
	var fragField uint16 = binary.BigEndian.Uint16(ip[6:8])
	if fragField&0x2000 != 0 || fragField&0x1fff != 0 { // MF set or non-zero offset.
		rd.stats.Fragments++
		return false
	}
	if ip[9] != ipProtoUDP {
		rd.stats.SkippedProto++
		return false
	}
	copy(pkt.SrcIP[:], ip[12:16])
	copy(pkt.DstIP[:], ip[16:20])
	var udp []byte = ip[ihl:]
	if len(udp) < 8 {
		rd.stats.Truncated++
		return false
	}
	pkt.SrcPort = binary.BigEndian.Uint16(udp[0:2])
	pkt.DstPort = binary.BigEndian.Uint16(udp[2:4])
	var udpLen int = int(binary.BigEndian.Uint16(udp[4:6]))
	if udpLen < 8 || udpLen > len(udp) {
		rd.stats.Truncated++
		return false
	}
	pkt.Payload = udp[8:udpLen]
	return true
}

// FormatAddr renders ip:port the way multicast groups are written in
// configuration.xml ("239.195.20.81:20081").
func FormatAddr(ip [4]byte, port uint16) string {
	return fmt.Sprintf("%d.%d.%d.%d:%d", ip[0], ip[1], ip[2], ip[3], port)
}
