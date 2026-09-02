//go:build linux

package simba

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"syscall"
)

var errSSMUnsupported = errors.New("ssm unsupported")

// ipAddSourceMembership — IP_ADD_SOURCE_MEMBERSHIP (linux/in.h); not
// exported by the syscall package.
const ipAddSourceMembership = 39

// listenSourceSpecific binds a UDP socket to group:port with SO_REUSEADDR
// and joins the group source-specifically (struct ip_mreq_source) on the
// interface's first IPv4 address (INADDR_ANY when iface is nil) — what
// MOEX's reference PythonSimbaClient does.
func listenSourceSpecific(group *net.UDPAddr, source net.IP, iface *net.Interface) (*net.UDPConn, error) {
	var ifaceIP net.IP = net.IPv4zero.To4()
	if iface != nil {
		var addrs []net.Addr
		var err error
		addrs, err = iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil {
				ifaceIP = ipn.IP.To4()
				break
			}
		}
	}

	var mreq [12]byte // imr_multiaddr, imr_interface, imr_sourceaddr — network byte order each.
	copy(mreq[0:4], group.IP.To4())
	copy(mreq[4:8], ifaceIP)
	copy(mreq[8:12], source.To4())
	_ = binary.BigEndian

	var joinErr error
	var lc net.ListenConfig = net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var ctrlErr error
			var err error = c.Control(func(fd uintptr) {
				ctrlErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
			if err != nil {
				return err
			}
			return ctrlErr
		},
	}
	var pc net.PacketConn
	var err error
	pc, err = lc.ListenPacket(context.Background(), "udp4", group.String())
	if err != nil {
		return nil, err
	}
	var conn *net.UDPConn = pc.(*net.UDPConn)
	var raw syscall.RawConn
	raw, err = conn.SyscallConn()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	err = raw.Control(func(fd uintptr) {
		joinErr = syscall.SetsockoptString(int(fd), syscall.IPPROTO_IP, ipAddSourceMembership, string(mreq[:]))
	})
	if err == nil {
		err = joinErr
	}
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}
