// Copyright 2018 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

//go:build memtest
// +build memtest

package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/keybase/backoff"
	"github.com/stretchr/testify/require"
)

type nullLogOutput struct{}

func (nullLogOutput) Info(fmt string, args ...interface{})              {}
func (nullLogOutput) Error(fmt string, args ...interface{})             {}
func (nullLogOutput) Debug(fmt string, args ...interface{})             {}
func (nullLogOutput) Warning(fmt string, args ...interface{})           {}
func (nullLogOutput) Profile(fmt string, args ...interface{})           {}
func (n nullLogOutput) CloneWithAddedDepth(int) LogOutputWithDepthAdder { return n }

func startServer(t *testing.T) {
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", testPort))
	require.NoError(t, err)
	go func() {
		for {
			c, err := listener.Accept()
			require.NoError(t, err)
			xp := NewTransport(c, NewSimpleLogFactory(nullLogOutput{}, nil), nil)
			srv := NewServer(xp, nil)
			srv.Run()
		}
	}()
}

type memoryLeakTester struct {
	numConnects int
}

// HandlerName implements the ConnectionHandler interface.
func (memoryLeakTester) HandlerName() string {
	return "memoryLeakTester"
}

// OnConnect implements the ConnectionHandler interface.
func (mlt *memoryLeakTester) OnConnect(context.Context, *Connection, GenericClient, *Server) error {
	mlt.numConnects++
	runtime.GC()
	if mlt.numConnects%100 == 0 {
		for _, p := range pprof.Profiles() {
			if p.Name() != "goroutine" {
				continue
			}
			fmt.Printf("\n======== START Profile: %s ========\n\n", p.Name())
			_ = p.WriteTo(os.Stdout, 2)
			fmt.Printf("\n======== END   Profile: %s ========\n\n", p.Name())
		}
	}
	return errors.New("nope")
}

// OnConnectError implements the ConnectionHandler interface.
func (mlt *memoryLeakTester) OnConnectError(error, time.Duration) {
}

// OnDoCommandError implements the ConnectionHandler interace
func (mlt *memoryLeakTester) OnDoCommandError(error, time.Duration) {
}

// OnDisconnected implements the ConnectionHandler interface.
func (mlt *memoryLeakTester) OnDisconnected(context.Context, DisconnectStatus) {
}

func (mlt *memoryLeakTester) ShouldRetry(name string, err error) bool {
	return true
}

func (mlt *memoryLeakTester) ShouldRetryOnConnect(err error) bool {
	return true
}

func TestMemoryLeak(t *testing.T) {
	output := nullLogOutput{}
	startServer(t)
	reconnectBackoffFn := func() backoff.BackOff {
		return backoff.NewConstantBackOff(1 * time.Millisecond)
	}
	opts := ConnectionOpts{
		WrapErrorFunc:    testWrapError,
		TagsFunc:         testLogTags,
		ReconnectBackoff: reconnectBackoffFn,
	}
	connTransport := NewConnectionTransport(&FMPURI{
		Scheme:   "tcp",
		HostPort: "127.0.0.1:" + strconv.Itoa(testPort),
	}, NewSimpleLogFactory(nullLogOutput{}, nil), nil)
	conn := NewConnectionWithTransport(&memoryLeakTester{}, connTransport, nil, output, opts)
	conn.getReconnectChan()
	select {}
}
