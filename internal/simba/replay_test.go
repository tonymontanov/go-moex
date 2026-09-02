package simba

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"
)

// fakeReplayServer speaks the §4.2.6 session: expects Logon, answers
// Logon, expects MarketDataRequest, streams `packets`, sends Logout and
// waits for the client's Logout.
func fakeReplayServer(t *testing.T, packets [][]byte, logoutText string, rejectLogon bool) (addr string, done chan error) {
	t.Helper()
	var ln net.Listener
	var err error
	ln, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done = make(chan error, 1)
	go func() {
		defer ln.Close()
		var conn net.Conn
		var aerr error
		conn, aerr = ln.Accept()
		if aerr != nil {
			done <- aerr
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		var buf []byte = make([]byte, 65536)
		var hdr PacketHeader
		var body []byte
		hdr, body, aerr = readPacket(conn, buf)
		if aerr != nil || sessionKind(body, hdr) != KindLogon {
			done <- errors.New("expected Logon")
			return
		}
		if rejectLogon {
			var text []byte = make([]byte, 256)
			copy(text, logoutText)
			_, _ = conn.Write(sessionPacket(1, SchemaVersion9, TemplateLogout, 256, text))
			done <- nil
			return
		}
		_, _ = conn.Write(sessionPacket(1, SchemaVersion9, TemplateLogon, 0, nil))
		hdr, body, aerr = readPacket(conn, buf)
		if aerr != nil || sessionKind(body, hdr) != KindMarketDataRequest {
			done <- errors.New("expected MarketDataRequest")
			return
		}
		var beg uint32 = binary.LittleEndian.Uint32(body[sbeHeaderSize : sbeHeaderSize+4])
		var end uint32 = binary.LittleEndian.Uint32(body[sbeHeaderSize+4 : sbeHeaderSize+8])
		for _, p := range packets {
			var s uint32 = PacketHeaderSeq(p)
			if s >= beg && s <= end {
				_, _ = conn.Write(p)
			}
		}
		var text []byte = make([]byte, 256)
		copy(text, logoutText)
		_, _ = conn.Write(sessionPacket(2, SchemaVersion9, TemplateLogout, 256, text))
		hdr, body, aerr = readPacket(conn, buf)
		if aerr != nil || sessionKind(body, hdr) != KindLogout {
			done <- errors.New("expected client Logout")
			return
		}
		done <- nil
	}()
	return ln.Addr().String(), done
}

func TestReplayClientHappyPath(t *testing.T) {
	var packets [][]byte
	for seq := uint32(10); seq <= 15; seq++ {
		packets = append(packets, incrementalPacket(seq, FlagLastFragment, sbeMessage(TemplateOrderUpdate, orderUpdateSize, orderUpdateBody(1, seq, int64(seq), 1, 1, MDUpdateActionNew, MDEntryTypeBid))))
	}
	var addr string
	var done chan error
	addr, done = fakeReplayServer(t, packets, "range served", false)

	var c ReplayClient = ReplayClient{Addr: addr}
	var got []uint32
	var res ReplayResult
	var err error
	res, err = c.Replay(context.Background(), 12, 14, func(buf []byte) { got = append(got, PacketHeaderSeq(buf)) })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != 12 || got[2] != 14 || res.Packets != 3 || res.FirstSeq != 12 || res.LastSeq != 14 {
		t.Fatalf("got %v res %+v", got, res)
	}
	if res.LogoutText != "range served" {
		t.Fatalf("logout text %q", res.LogoutText)
	}
	if serr := <-done; serr != nil {
		t.Fatal(serr)
	}
}

func TestReplayClientRejectedLogon(t *testing.T) {
	var addr string
	addr, _ = fakeReplayServer(t, nil, "too many connections", true)
	var c ReplayClient = ReplayClient{Addr: addr}
	var _, err = c.Replay(context.Background(), 1, 2, func([]byte) {})
	if !errors.Is(err, ErrReplayLogout) {
		t.Fatalf("expected ErrReplayLogout, got %v", err)
	}
}

func TestReplayClientRangeGuard(t *testing.T) {
	var c ReplayClient = ReplayClient{Addr: "127.0.0.1:1"}
	if _, err := c.Replay(context.Background(), 1, 1001, func([]byte) {}); !errors.Is(err, ErrReplayRange) {
		t.Fatalf("range guard: %v", err)
	}
	if _, err := c.Replay(context.Background(), 5, 4, func([]byte) {}); !errors.Is(err, ErrReplayRange) {
		t.Fatalf("empty range guard: %v", err)
	}
}

// TestListenSourceSpecificLoopback checks the SSM join path compiles and
// either joins or fails with a clear OS error (loopback interfaces on
// many kernels reject IGMPv3 source membership); it must never fall back
// silently.
func TestListenSourceSpecificLoopback(t *testing.T) {
	var l *Listener
	var err error
	l, err = Listen(ListenerConfig{GroupAddr: "239.255.77.1:47001", SourceIP: "127.0.0.1", Interface: "lo"})
	if err != nil {
		t.Skipf("source-specific join not possible here: %v", err)
	}
	defer l.Close()
	if !l.SourceSpecific() {
		t.Fatal("join reported as any-source although SourceIP was set")
	}
}
