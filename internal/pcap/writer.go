/*
FILE: internal/pcap/writer.go

DESCRIPTION:
Classic pcap writer (nanosecond magic, Ethernet link type) that
synthesises Ethernet/IPv4/UDP framing around a UDP payload. Used by the
harness to cut fixtures out of production captures and by tests to build
captures from scratch.
*/
package pcap

import (
	"bufio"
	"encoding/binary"
	"io"
	"time"
)

// Writer — streaming classic-pcap writer.
type Writer struct {
	w       *bufio.Writer
	Packets uint64
	Bytes   uint64
}

// NewWriter writes the global header to w.
func NewWriter(w io.Writer) (*Writer, error) {
	var pw *Writer = &Writer{w: bufio.NewWriterSize(w, 1<<20)}
	var gh [globalHdrSize]byte
	binary.LittleEndian.PutUint32(gh[0:4], magicNanos)
	binary.LittleEndian.PutUint16(gh[4:6], 2)
	binary.LittleEndian.PutUint16(gh[6:8], 4)
	binary.LittleEndian.PutUint32(gh[16:20], 65535)
	binary.LittleEndian.PutUint32(gh[20:24], linkEthernet)
	if _, err := pw.w.Write(gh[:]); err != nil {
		return nil, err
	}
	return pw, nil
}

// WriteUDP appends one UDP/IPv4 datagram with the given addressing and
// timestamp.
func (pw *Writer) WriteUDP(ts time.Time, src, dst [4]byte, srcPort, dstPort uint16, payload []byte) error {
	var frame []byte = make([]byte, 14+20+8+len(payload))
	// Ethernet: multicast MAC 01:00:5e + low 23 bits of the group.
	frame[0], frame[1], frame[2] = 0x01, 0x00, 0x5e
	frame[3], frame[4], frame[5] = dst[1]&0x7f, dst[2], dst[3]
	binary.BigEndian.PutUint16(frame[12:14], etherTypeIPv4)
	var ip []byte = frame[14:34]
	ip[0] = 0x45
	binary.BigEndian.PutUint16(ip[2:4], uint16(20+8+len(payload)))
	ip[8] = 1
	ip[9] = ipProtoUDP
	copy(ip[12:16], src[:])
	copy(ip[16:20], dst[:])
	binary.BigEndian.PutUint16(ip[10:12], ipChecksum(ip))
	var udp []byte = frame[34:42]
	binary.BigEndian.PutUint16(udp[0:2], srcPort)
	binary.BigEndian.PutUint16(udp[2:4], dstPort)
	binary.BigEndian.PutUint16(udp[4:6], uint16(8+len(payload)))
	copy(frame[42:], payload)

	var rh [recordHdrSize]byte
	binary.LittleEndian.PutUint32(rh[0:4], uint32(ts.Unix()))
	binary.LittleEndian.PutUint32(rh[4:8], uint32(ts.Nanosecond()))
	binary.LittleEndian.PutUint32(rh[8:12], uint32(len(frame)))
	binary.LittleEndian.PutUint32(rh[12:16], uint32(len(frame)))
	if _, err := pw.w.Write(rh[:]); err != nil {
		return err
	}
	if _, err := pw.w.Write(frame); err != nil {
		return err
	}
	pw.Packets++
	pw.Bytes += uint64(len(frame))
	return nil
}

// Flush flushes buffered records; the caller closes the underlying file.
func (pw *Writer) Flush() error { return pw.w.Flush() }

func ipChecksum(hdr []byte) uint16 {
	var sum uint32
	for i := 0; i+1 < len(hdr); i += 2 {
		if i == 10 {
			continue
		}
		sum += uint32(binary.BigEndian.Uint16(hdr[i : i+2]))
	}
	for sum>>16 != 0 {
		sum = (sum & 0xffff) + (sum >> 16)
	}
	return ^uint16(sum)
}
