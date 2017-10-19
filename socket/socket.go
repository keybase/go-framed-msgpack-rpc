// Copyright 2017 Keybase, Inc. All rights reserved. Use of
// this source code is governed by the included BSD license.

package socket

import (
	"net"
	"sync"

	"github.com/keybase/go-framed-msgpack-rpc/rpc"
)

type Socket interface {
	BindToSocket() (net.Listener, error)
	DialSocket() (net.Conn, error)
}

type SocketInfo struct {
	bindFile  string
	dialFiles []string
}

var _ Socket = (*SocketInfo)(nil)

type SocketWrapper struct {
	mtx  sync.Mutex
	si   SocketInfo
	conn net.Conn
	xp   rpc.Transporter
}

func NewTransportFromSocket(c net.Conn, wef rpc.WrapErrorFunc) rpc.Transporter {
	return rpc.NewTransport(c, rpc.NewSimpleLogFactory(nil, nil), wef)
}

// ResetSocket clears and returns a new socket
func (g *GlobalContext) ResetSocket(reset bool) (
	net.Conn, rpc.Transporter, bool, error) {
	sw.mtx.Lock()
	defer sw.mtx.Unlock()

	return g.getSocketLocked(reset)
}

func (sw *SocketWrapper) getSocketLocked(reset bool) (
	conn net.Conn, xp rpc.Transporter, isNew bool, err error) {
	if sw.xp != nil {
		if !reset && sw.xp.IsConnected() && sw.err == nil {
			return sw.conn, sw.xp, false, nil
		}
		if sw.conn != nil {
			sw.conn.Close()
		}
	}

	sw.conn, sw.err = sw.si.DialSocket()
	isNew = true
	if sw.err == nil {
		sw.xp = NewTransportFromSocket(g, sw.conn)
	}

	return sw.conn, sw.xp, isNew, sw.err
}

func (sw *SocketWrapper) GetSocket(clearError bool) (
	conn net.Conn, xp rpc.Transporter, isNew bool, err error) {

	sw.mtx.Lock()
	defer sw.mtx.Unlock()

	return sw.getSocketLocked(clearError)
}
