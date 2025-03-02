package rpc

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func dispatchTestCallWithContextAndCompressionType(ctx context.Context, t *testing.T, ctype CompressionType) (dispatcher, *callContainer, chan error) {
	log := newTestLog(t)
	instrumenterStorage := NewMemoryInstrumentationStorage()

	conn1, conn2 := net.Pipe()
	dispatchOut := newFramedMsgpackEncoder(testMaxFrameLength, conn1)
	calls := newCallContainer()
	pkt := newPacketizer(testMaxFrameLength, conn2, createMessageTestProtocol(t),
		calls, log, instrumenterStorage)

	d := newDispatch(dispatchOut, calls, log, instrumenterStorage)

	done := runInBg(func() error {
		return d.Call(ctx, newMethodV1("abc.hello"), new(interface{}), new(interface{}),
			ctype, nil, nil)
	})

	// Necessary to ensure the call is far enough along to
	// be ready to respond
	_, decoderErr := pkt.NextFrame(context.Background())
	require.NoError(t, decoderErr, "Expected no error")
	return d, calls, done
}

func dispatchTestCallWithContext(ctx context.Context, t *testing.T) (dispatcher, *callContainer, chan error) {
	return dispatchTestCallWithContextAndCompressionType(ctx, t, CompressionNone)
}

func dispatchTestCall(t *testing.T) (dispatcher, *callContainer, chan error) {
	return dispatchTestCallWithContext(context.Background(), t)
}

func sendResponse(c *call, err error) {
	c.resultCh <- &rpcResponseMessage{
		err: err,
		c:   c,
	}
}

func TestDispatchSuccessfulCall(t *testing.T) {
	d, calls, done := dispatchTestCall(t)

	c := calls.RetrieveCall(0)
	require.NotNil(t, c, "Expected c not to be nil")

	sendResponse(c, nil)
	err := <-done
	require.NoError(t, err, "Expected no error")

	d.Close()
}

func TestDispatchSuccessfulCallCompressed(t *testing.T) {
	doWithAllCompressionTypes(func(ctype CompressionType) {
		d, calls, done := dispatchTestCallWithContextAndCompressionType(context.Background(), t, ctype)

		c := calls.RetrieveCall(0)
		require.NotNil(t, c, "Expected c not to be nil")
		require.Equal(t, ctype, c.ctype)

		sendResponse(c, nil)
		err := <-done
		require.NoError(t, err, "Expected no error")

		d.Close()
	})
}

func TestDispatchCanceledBeforeResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	d, calls, done := dispatchTestCallWithContext(ctx, t)

	c := calls.RetrieveCall(0)
	require.NotNil(t, c, "Expected c not to be nil")
	require.Equal(t, CompressionNone, c.ctype)

	cancel()

	// Should not hang.
	sendResponse(c, nil)

	err := <-done
	require.True(t, err == nil || err == context.Canceled,
		"Expected call to complete successfully or be cancelled")

	require.Nil(t, calls.RetrieveCall(0),
		"Expected call to be removed from the container")

	d.Close()
}

func TestDispatchCanceledAfterResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	d, calls, done := dispatchTestCallWithContext(ctx, t)

	c := calls.RetrieveCall(0)
	require.NotNil(t, c, "Expected c not to be nil")

	sendResponse(c, nil)

	cancel()

	err := <-done
	require.True(t, err == nil || err == context.Canceled,
		"Expected call to complete successfully or be cancelled")

	require.Nil(t, calls.RetrieveCall(0),
		"Expected call to be removed from the container")

	d.Close()
}

func TestDispatchEOF(t *testing.T) {
	d, _, done := dispatchTestCall(t)

	d.Close()
	err := <-done
	require.Equal(t, io.EOF, err, "Expected EOF")
}

func TestDispatchCallAfterClose(t *testing.T) {
	d, calls, done := dispatchTestCall(t)

	c := calls.RetrieveCall(0)
	sendResponse(c, nil)

	err := <-done
	require.NoError(t, err)
	d.Close()

	done = runInBg(func() error {
		return d.Call(context.Background(), newMethodV1("whatever"), new(interface{}), new(interface{}),
			CompressionNone, nil, nil)
	})
	err = <-done
	require.Equal(t, io.EOF, err)
}

func TestDispatchCancelEndToEnd(t *testing.T) {
	dispatchConn, _ := net.Pipe()
	enc := newFramedMsgpackEncoder(testMaxFrameLength, dispatchConn)
	cc := newCallContainer()
	log := newTestLog(t)
	instrumenterStorage := NewMemoryInstrumentationStorage()
	d := newDispatch(enc, cc, log, instrumenterStorage)

	ctx1, cancel1 := context.WithCancel(context.Background())

	ch := make(chan error)
	go func() {
		err := d.Call(ctx1, newMethodV1("abc.hello"), nil, new(interface{}),
			CompressionNone, nil, nil)
		ch <- err
	}()

	cancel1()
	err := <-ch
	require.Equal(t, err, context.Canceled)
}
