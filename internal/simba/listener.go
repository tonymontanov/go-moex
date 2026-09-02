/*
FILE: internal/simba/listener.go

DESCRIPTION:
UDP multicast listener for one SIMBA SPECTRA stream (one of: Incremental
A/B, Snapshot A/B, Instruments A/B). MOEX broadcasts every stream on two
independent multicast groups (A/B) carrying the SAME data for redundancy
— per spec §1.4.2 a correct client joins BOTH and deduplicates by
MsgSeqNum, tolerating loss on either. Deduplication/reordering is the
caller's responsibility (forts.BookSession); this type only delivers raw
datagrams as they arrive.

Source-specific multicast: configuration.xml lists a source IP for every
group and MOEX's reference client joins with IP_ADD_SOURCE_MEMBERSHIP.
When SourceIP is set the listener does the same (Linux; other platforms
fall back to an any-source join with a warning) — an ASM join on an
SSM-only network segment receives nothing.

REQUIRES EXCHANGE COLOCATION: multicast groups are only reachable from
MOEX's own network segment (colocation) or a broker-provided low-latency
circuit — see docs/handoff.md "Open questions". Outside of that
environment Listen will simply time out / receive nothing; that is
expected, not a bug.
*/
package simba

import (
	"context"
	"fmt"
	"net"

	"github.com/tonymontanov/go-moex/internal/moexlog"
)

// ListenerConfig — one multicast group subscription.
type ListenerConfig struct {
	// GroupAddr — multicast group address:port, e.g. "239.195.20.81:20081".
	GroupAddr string
	// SourceIP — unicast source of the group (src-ip in configuration.xml,
	// e.g. "91.203.253.244"). Set for a source-specific join; empty joins
	// any-source.
	SourceIP string
	// Interface — local NIC name to join the group on (e.g. "eth0"). Empty
	// lets the OS pick the default multicast-capable interface — usually
	// wrong in a multi-NIC colocation host, so it should normally be set.
	Interface string
	// ReadBufferSize — per-read buffer size. SIMBA datagrams are small
	// (order of a few hundred bytes for OrderUpdate/OrderExecution, larger
	// for OrderBookSnapshot fragments) — 64KiB safely covers the UDP max.
	ReadBufferSize int
	// SocketReadBufferBytes — OS-level SO_RCVBUF size. 0 = OS default.
	// Bursty order-log traffic (3500 packets/s average on production, far
	// more at the open) can overflow small default kernel buffers;
	// production colocation deployments should set this explicitly.
	SocketReadBufferBytes int

	Logger moexlog.Logger
}

// Listener — one joined multicast group.
type Listener struct {
	cfg  ListenerConfig
	conn *net.UDPConn
	ssm  bool
}

// Listen resolves cfg.GroupAddr and joins the multicast group on
// cfg.Interface (source-specific if cfg.SourceIP is set).
func Listen(cfg ListenerConfig) (*Listener, error) {
	var groupAddr *net.UDPAddr
	var err error
	groupAddr, err = net.ResolveUDPAddr("udp4", cfg.GroupAddr)
	if err != nil {
		return nil, fmt.Errorf("simba: resolve group addr %q: %w", cfg.GroupAddr, err)
	}
	if groupAddr.IP == nil || !groupAddr.IP.IsMulticast() {
		return nil, fmt.Errorf("simba: %q is not a multicast group", cfg.GroupAddr)
	}

	var iface *net.Interface
	if cfg.Interface != "" {
		iface, err = net.InterfaceByName(cfg.Interface)
		if err != nil {
			return nil, fmt.Errorf("simba: interface %q: %w", cfg.Interface, err)
		}
	}

	var l *Listener = &Listener{cfg: cfg}
	if cfg.SourceIP != "" {
		var src net.IP = net.ParseIP(cfg.SourceIP).To4()
		if src == nil {
			return nil, fmt.Errorf("simba: SourceIP %q is not an IPv4 address", cfg.SourceIP)
		}
		l.conn, err = listenSourceSpecific(groupAddr, src, iface)
		if err == errSSMUnsupported {
			l.logger().Warn("simba: source-specific multicast join unsupported on this platform, falling back to any-source join")
		} else if err != nil {
			return nil, fmt.Errorf("simba: source-specific join %q from %s: %w", cfg.GroupAddr, cfg.SourceIP, err)
		} else {
			l.ssm = true
		}
	}
	if l.conn == nil {
		l.conn, err = net.ListenMulticastUDP("udp4", iface, groupAddr)
		if err != nil {
			return nil, fmt.Errorf("simba: join multicast group %q: %w", cfg.GroupAddr, err)
		}
	}

	if cfg.ReadBufferSize <= 0 {
		l.cfg.ReadBufferSize = 64 * 1024
	}
	if cfg.SocketReadBufferBytes > 0 {
		if err = l.conn.SetReadBuffer(cfg.SocketReadBufferBytes); err != nil {
			l.logger().Warn("simba: SetReadBuffer failed", moexlog.Err(err))
		}
	}
	return l, nil
}

// SourceSpecific reports whether the join was source-specific.
func (l *Listener) SourceSpecific() bool { return l.ssm }

// Close leaves the multicast group and closes the socket.
func (l *Listener) Close() error { return l.conn.Close() }

// Run reads datagrams until ctx is canceled or a read error occurs, calling
// handler synchronously for each one. handler must copy buf if it retains a
// reference beyond the call (the slice is reused across iterations).
//
// Speed contract: handler runs on the read goroutine — keep it O(1)
// (decode + forward to a channel/orderbook engine), exactly like the
// go-okx WS message handler contract.
func (l *Listener) Run(ctx context.Context, handler func(buf []byte)) error {
	var buf []byte = make([]byte, l.cfg.ReadBufferSize)

	go func() {
		<-ctx.Done()
		_ = l.conn.Close()
	}()

	for {
		var n int
		var err error
		n, _, err = l.conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return fmt.Errorf("simba: read from %s: %w", l.cfg.GroupAddr, err)
			}
		}
		handler(buf[:n])
	}
}

func (l *Listener) logger() moexlog.Logger {
	if l.cfg.Logger != nil {
		return l.cfg.Logger
	}
	return moexlog.Noop()
}
