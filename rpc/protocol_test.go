package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/foks-proj/go-ctxlog"
	telnet "github.com/reiver/go-telnet"
	"github.com/stretchr/testify/require"
)

var testPort = 8089
var testHostPort = fmt.Sprintf("127.0.0.1:%d", testPort)

type longCallResult struct {
	res interface{}
	err error
}

func prepServer(t *testing.T, listener chan error) error {
	server := &server{port: testPort}

	serverReady := make(chan struct{})
	var err error
	go func() {
		err = server.Run(t, serverReady, listener)
	}()
	<-serverReady
	// TODO: Fix the race here -- when serverReady is closed by
	// server.Run, err is in an indeterminate state.
	return err
}

func prepClient(t *testing.T) (TestClient, net.Conn) {
	c, err := net.Dial("tcp", testHostPort)
	require.NoError(t, err, "a dialer error occurred")

	lf := NewSimpleLogFactory(&testLogOutput{t: t}, nil)
	instrumenterStorage := NewMemoryInstrumentationStorage()
	xp := NewTransport(context.Background(), c, lf, instrumenterStorage, nil, testMaxFrameLength)
	return TestClient{GenericClient: NewClient(xp, nil, nil)}, c
}

func prepTest(t *testing.T) (TestClient, chan error, net.Conn) {
	listener := make(chan error)
	err := prepServer(t, listener)
	require.NoError(t, err)
	cli, conn := prepClient(t)
	return cli, listener, conn
}

func endTest(t *testing.T, c net.Conn, listener chan error) {
	c.Close()
	err := <-listener
	require.EqualError(t, err, io.EOF.Error(), "expected EOF")
}

func TestCall(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	B := 34
	numRuns := 15
	done := make(chan int)
	for A := 0; A < numRuns; A++ {
		go func(A, B int) {
			res, err := cli.Add(context.Background(), AddArgs{A: A, B: B})
			require.NoError(t, err, "an error occurred while adding parameters")
			require.Equal(t, A+B, res, "Result should be the two parameters added together")
			done <- 0
		}(A, B)
	}
	for i := 0; i < numRuns; i++ {
		<-done
	}
}

func TestBrokenCall(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	err := cli.BrokenMethod()
	require.EqualError(t, err, "method 'broken' not found in protocol 'test.1.testp'")

	err = cli.BrokenProtocol()
	require.EqualError(t, err, "protocol not found: test.2.testp")
}

func TestCallLargeFrame(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	var padding [testMaxFrameLength]byte
	_, err := cli.Add(context.Background(), AddArgs{A: 1, B: 1, Padding: padding[:]})
	require.EqualError(t, err, "frame length too big: 1062 > 1024")

	// Shouldn't close the whole connection.
	_, err = cli.Add(context.Background(), AddArgs{A: 1, B: 1})
	require.NoError(t, err)
}

func TestNotify(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	pi := 31415

	err := cli.UpdateConstants(context.Background(), Constants{Pi: pi})
	require.NoError(t, err, "Unexpected error on notify: %v", err)

	constants, err := cli.GetConstants(context.Background())
	require.NoError(t, err, "Unexpected error on GetConstants: %v", err)
	require.Equal(t, pi, constants.Pi, "we set the constant properly via Notify")
}

func TestLongCall(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	ctx := context.Background()
	ctx = ctxlog.AddTagsToContext(ctx, ctxlog.CtxLogTags{"hello": []string{"world"}})

	longResult, err := cli.LongCall(ctx)
	require.NoError(t, err, "call should have succeeded")
	require.Equal(t, longResult, 100, "call should have succeeded")
}

func TestCallCompressed(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	ctx := context.Background()
	ctx = ctxlog.AddTagsToContext(ctx, ctxlog.CtxLogTags{"hello": []string{"world"}})

	nargs := NArgs{N: 50}
	verifyRes := func(res []*Constants, err error) {
		require.NoError(t, err, "call should have succeeded")
		require.Len(t, res, nargs.N)
		for i := 0; i < nargs.N; i++ {
			require.NotNil(t, res[i])
			require.Equal(t, Constants{}, *res[i])
		}
	}

	numRuns := 15
	doWithAllCompressionTypes(func(ctype CompressionType) {
		done := make(chan int)
		for i := 0; i < numRuns; i++ {
			go func(i int) {
				res := []*Constants{}
				err := cli.CallCompressed(ctx, newMethodV1("test.1.testp.GetNConstants"), nargs, &res, ctype, 0)
				verifyRes(res, err)
				done <- i
			}(i)
		}
		for i := 0; i < numRuns; i++ {
			<-done
		}
	})

	// Runs with gzip compression mode by default
	res, err := cli.GetNConstants(ctx, nargs)
	verifyRes(res, err)

	// Also test CallCompressed w/CompressionNone and regular Call work identically
	res = []*Constants{}
	err = cli.CallCompressed(ctx, newMethodV1("test.1.testp.GetNConstants"), nargs, &res, CompressionNone, 0)
	verifyRes(res, err)

	res = []*Constants{}
	err = cli.Call(ctx, newMethodV1("test.1.testp.GetNConstants"), nargs, &res, 0)
	verifyRes(res, err)
}

func TestLongCallCancel(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	ctx, cancel := context.WithCancel(context.Background())
	ctx = ctxlog.AddTagsToContext(ctx, ctxlog.CtxLogTags{"hello": []string{"world"}})

	resultCh := make(chan longCallResult)
	runInBg(func() error {
		var longResult interface{}
		var err error
		longResult, err = cli.LongCall(ctx)
		resultCh <- longCallResult{longResult, err}
		longResult, err = cli.LongCallResult(context.Background())
		resultCh <- longCallResult{longResult, err}
		longResult, err = cli.LongCallDebugTags(context.Background())
		resultCh <- longCallResult{longResult, err}
		return nil
	})
	// TODO figure out a way to avoid this sleep
	time.Sleep(time.Millisecond)
	cancel()
	res := <-resultCh
	require.EqualError(t, res.err, context.Canceled.Error())
	require.Equal(t, 0, res.res, "call should be canceled")

	res = <-resultCh
	require.Nil(t, res.err, "call should have succeeded")
	require.Equal(t, -1, res.res, "canceled call should have set the longCallResult to canceled")

	res = <-resultCh
	require.Nil(t, res.err, "call should have succeeded")
	require.Equal(t, ctxlog.CtxLogTags{"hello": []interface{}{"world"}, "server": "test123"}, res.res, "canceled call should have set the debug tags")
}

func TestLongCallTimeout(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	resultCh := make(chan longCallResult)
	runInBg(func() error {
		var res int
		// specify a millisecond timeout
		err := cli.Call(context.Background(), newMethodV1("test.1.testp.LongCall"), nil, &res, time.Millisecond)
		resultCh <- longCallResult{res, err}
		return nil
	})
	res := <-resultCh
	require.EqualError(t, res.err, context.DeadlineExceeded.Error())
	require.Equal(t, 0, res.res, "call should have timed out")
}

func TestClosedConnection(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	resultCh := make(chan longCallResult)
	runInBg(func() error {
		var longResult interface{}
		var err error
		longResult, err = cli.LongCall(context.Background())
		resultCh <- longCallResult{longResult, err}
		return nil
	})
	// TODO figure out a way to avoid this sleep
	time.Sleep(time.Millisecond)
	conn.Close()
	res := <-resultCh
	require.EqualError(t, res.err, io.EOF.Error())
	require.Equal(t, 0, res.res)
}

func TestKillClient(t *testing.T) {
	listener := make(chan error)
	err := prepServer(t, listener)
	require.NoError(t, err)
	defer func() {
		err := <-listener
		require.EqualError(t, err, io.EOF.Error(), "expected EOF")
	}()

	conn, err := telnet.DialTo(testHostPort)
	require.NoError(t, err)
	// Write the control character sequence {IAC, IP} to the connection. This
	// is analogous to sending a ctrl-C over telnet.
	_, err = conn.Write([]byte{255, 244})
	require.NoError(t, err)
	err = conn.Close()
	require.NoError(t, err)
}
