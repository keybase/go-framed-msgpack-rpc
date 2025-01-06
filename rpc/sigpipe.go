//go:build !darwin
// +build !darwin

package rpc

import "net"

func DisableSigPipe(_ net.Conn) error {
	return nil
}
