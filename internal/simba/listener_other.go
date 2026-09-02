//go:build !linux

package simba

import (
	"errors"
	"net"
)

var errSSMUnsupported = errors.New("ssm unsupported")

// listenSourceSpecific is only implemented on Linux (raw
// IP_ADD_SOURCE_MEMBERSHIP); other platforms fall back to an any-source
// join in Listen.
func listenSourceSpecific(group *net.UDPAddr, source net.IP, iface *net.Interface) (*net.UDPConn, error) {
	return nil, errSSMUnsupported
}
