/*
FILE: internal/simba/replay.go

DESCRIPTION:
TCP Replay service client (spec §4.2.6): re-requests a range of
Incremental-feed packets the client missed. Scenario:

	client: Logon(1000)                 server: Logon(1000)
	client: MarketDataRequest(1002)     server: the requested packets, then Logout(1001)
	client: Logout(1001)                server closes

Session messages use the Snapshot packet format (§2.3.2): a
MarketDataPacketHeader (MsgSize framing) + SBE header + body. Replayed
packets are ordinary Incremental packets (their MsgSeqNum is the original
one), delivered to the caller's handler in order.

Limits (spec §4.2.6, enforced client-side): at most 1000 packets per
request, 2 concurrent connections, 1000 connections per day per IP,
1 second activity timeout (the server drops the session if the client is
silent after connect / Logon / Logout). This is a small-loss recovery
tool, not a bulk backfill — larger gaps go to the Snapshot feed.
*/
package simba

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	// ReplayMaxPackets — spec limit on ApplEndSeqNum-ApplBegSeqNum+1.
	ReplayMaxPackets = 1000
	// ReplayActivityTimeout — the server's client-activity timeout.
	ReplayActivityTimeout = time.Second
)

// Replay errors.
var (
	ErrReplayRange   = errors.New("simba: replay range exceeds 1000 packets or is empty")
	ErrReplayLogon   = errors.New("simba: replay server did not confirm Logon")
	ErrReplayLogout  = errors.New("simba: replay server logged out before serving the range")
	ErrReplayTimeout = errors.New("simba: replay session timed out")
)

// ReplayResult — what a replay call delivered.
type ReplayResult struct {
	Packets    int    // Incremental packets delivered to the handler.
	FirstSeq   uint32 // MsgSeqNum of the first delivered packet (0 if none).
	LastSeq    uint32
	LogoutText string // server's Logout text, if any.
}

// ReplayClient — TCP Replay service endpoint.
type ReplayClient struct {
	Addr        string        // host:port of the Replay service (configuration.xml, e.g. "91.203.255.244:7029").
	DialTimeout time.Duration // default 2s.
	Version     uint16        // schema version to stamp on outgoing SBE headers; default SchemaVersion9.
	// SessionTimeout bounds the whole exchange; default 10s.
	SessionTimeout time.Duration
}

// Replay requests packets [begSeq, endSeq] and calls handler for every
// replayed Incremental packet in order. handler must copy buf if it
// retains it.
func (c *ReplayClient) Replay(ctx context.Context, begSeq, endSeq uint32, handler func(buf []byte)) (ReplayResult, error) {
	var res ReplayResult
	if endSeq < begSeq || endSeq-begSeq+1 > ReplayMaxPackets {
		return res, ErrReplayRange
	}
	var dialTimeout time.Duration = c.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 2 * time.Second
	}
	var sessionTimeout time.Duration = c.SessionTimeout
	if sessionTimeout <= 0 {
		sessionTimeout = 10 * time.Second
	}
	var version uint16 = c.Version
	if version == 0 {
		version = SchemaVersion9
	}
	var deadline time.Time = time.Now().Add(sessionTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}

	var d net.Dialer = net.Dialer{Timeout: dialTimeout}
	var conn net.Conn
	var err error
	conn, err = d.DialContext(ctx, "tcp", c.Addr)
	if err != nil {
		return res, fmt.Errorf("simba: replay dial %s: %w", c.Addr, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(deadline)
	go func() {
		<-ctx.Done()
		_ = conn.SetDeadline(time.Now())
	}()

	var seq uint32 = 1
	var write = func(pkt []byte) error {
		var werr error
		_, werr = conn.Write(pkt)
		return werr
	}

	// Logon
	if err = write(sessionPacket(seq, version, TemplateLogon, 0, nil)); err != nil {
		return res, fmt.Errorf("simba: replay send Logon: %w", err)
	}
	seq++
	var hdr PacketHeader
	var body []byte
	var buf []byte = make([]byte, 65536)
	hdr, body, err = readPacket(conn, buf)
	if err != nil {
		return res, wrapReplayErr("Logon reply", err)
	}
	if k := sessionKind(body, hdr); k != KindLogon {
		if k == KindLogout {
			res.LogoutText = logoutText(body, hdr)
			return res, fmt.Errorf("%w: %s", ErrReplayLogout, res.LogoutText)
		}
		return res, ErrReplayLogon
	}

	// MarketDataRequest
	var req [8]byte
	binary.LittleEndian.PutUint32(req[0:4], begSeq)
	binary.LittleEndian.PutUint32(req[4:8], endSeq)
	if err = write(sessionPacket(seq, version, TemplateMarketDataRequest, 8, req[:])); err != nil {
		return res, fmt.Errorf("simba: replay send MarketDataRequest: %w", err)
	}
	seq++

	// Replayed packets until Logout.
	for {
		hdr, body, err = readPacket(conn, buf)
		if err != nil {
			return res, wrapReplayErr("replay stream", err)
		}
		if hdr.IsIncremental() {
			var pkt []byte = buf[:packetHeaderSize+len(body)]
			handler(pkt)
			res.Packets++
			if res.FirstSeq == 0 {
				res.FirstSeq = hdr.MsgSeqNum
			}
			res.LastSeq = hdr.MsgSeqNum
			continue
		}
		switch sessionKind(body, hdr) {
		case KindLogout:
			res.LogoutText = logoutText(body, hdr)
			_ = write(sessionPacket(seq, version, TemplateLogout, 0, nil))
			if res.Packets == 0 && res.LogoutText != "" {
				return res, fmt.Errorf("%w: %s", ErrReplayLogout, res.LogoutText)
			}
			return res, nil
		case KindHeartbeat:
			continue
		default:
			// Unknown session message: ignore.
		}
	}
}

func wrapReplayErr(stage string, err error) error {
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return fmt.Errorf("%w (%s)", ErrReplayTimeout, stage)
	}
	return fmt.Errorf("simba: replay %s: %w", stage, err)
}

// sessionPacket builds a Snapshot-format packet: MarketDataPacketHeader +
// SBE header + body.
func sessionPacket(seq uint32, version uint16, templateID uint16, blockLength uint16, body []byte) []byte {
	var out []byte = make([]byte, packetHeaderSize+sbeHeaderSize+len(body))
	binary.LittleEndian.PutUint32(out[0:4], seq)
	binary.LittleEndian.PutUint16(out[4:6], uint16(len(out)))
	binary.LittleEndian.PutUint16(out[6:8], FlagLastFragment)
	binary.LittleEndian.PutUint64(out[8:16], uint64(time.Now().UnixNano()))
	binary.LittleEndian.PutUint16(out[16:18], blockLength)
	binary.LittleEndian.PutUint16(out[18:20], templateID)
	binary.LittleEndian.PutUint16(out[20:22], SchemaID)
	binary.LittleEndian.PutUint16(out[22:24], version)
	copy(out[24:], body)
	return out
}

// readPacket reads one MsgSize-framed packet into buf; returns the header
// and the bytes after it (still inside buf).
func readPacket(r io.Reader, buf []byte) (PacketHeader, []byte, error) {
	var hdr PacketHeader
	if _, err := io.ReadFull(r, buf[:packetHeaderSize]); err != nil {
		return hdr, nil, err
	}
	hdr.MsgSeqNum = binary.LittleEndian.Uint32(buf[0:4])
	hdr.MsgSize = binary.LittleEndian.Uint16(buf[4:6])
	hdr.MsgFlags = binary.LittleEndian.Uint16(buf[6:8])
	hdr.SendingTime = binary.LittleEndian.Uint64(buf[8:16])
	if int(hdr.MsgSize) < packetHeaderSize || int(hdr.MsgSize) > len(buf) {
		return hdr, nil, fmt.Errorf("simba: replay frame MsgSize %d out of range", hdr.MsgSize)
	}
	var rest []byte = buf[packetHeaderSize:hdr.MsgSize]
	if _, err := io.ReadFull(r, rest); err != nil {
		return hdr, nil, err
	}
	return hdr, rest, nil
}

// sessionKind decodes the SBE header of a Snapshot-format session packet.
func sessionKind(body []byte, hdr PacketHeader) Kind {
	if hdr.IsIncremental() || len(body) < sbeHeaderSize {
		return KindUnknown
	}
	return kindOf(binary.LittleEndian.Uint16(body[6:8]), binary.LittleEndian.Uint16(body[2:4]))
}

func logoutText(body []byte, hdr PacketHeader) string {
	if len(body) < sbeHeaderSize {
		return ""
	}
	var text []byte = body[sbeHeaderSize:]
	var end int = len(text)
	for end > 0 && (text[end-1] == 0 || text[end-1] == ' ') {
		end--
	}
	return string(text[:end])
}
