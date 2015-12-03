package rpc

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func dispatchTestCallWithContext(t *testing.T, ctx context.Context) (dispatcher, chan callRetrieval, chan error) {
	dispatchOut := newBlockingMockCodec()

	logFactory := NewSimpleLogFactory(SimpleLogOutput{}, SimpleLogOptions{})
	callCh := make(chan callRetrieval)
	d := newDispatch(dispatchOut, newBlockingMockCodec(), callCh, logFactory.NewLog(nil))

	done := runInBg(func() error {
		return d.Call(ctx, "whatever", new(interface{}), new(interface{}), nil)
	})

	// Necessary to ensure the call is far enough along to
	// be ready to respond
	decoderErr := decodeToNull(dispatchOut, &message{remainingFields: 5})
	require.Nil(t, decoderErr, "Expected no error")
	return d, callCh, done
}

func dispatchTestCall(t *testing.T) (dispatcher, chan callRetrieval, chan error) {
	return dispatchTestCallWithContext(t, context.Background())
}

func TestDispatchSuccessfulCall(t *testing.T) {
	d, callCh, done := dispatchTestCall(t)

	ch := make(chan *call)
	callCh <- callRetrieval{0, ch}
	c := <-ch
	require.NotNil(t, c, "Expected c not to be nil")

	ok := c.Finish(nil)
	require.True(t, ok, "Expected c.Finish to succeed")
	err := <-done
	require.Nil(t, err, "Expected no error")

	closed := d.Close(nil)
	<-closed
}

func TestDispatchCanceledBeforeResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	d, callCh, done := dispatchTestCallWithContext(t, ctx)

	ch := make(chan *call)
	callCh <- callRetrieval{0, ch}
	c := <-ch
	require.NotNil(t, c, "Expected c not to be nil")

	cancel()

	err := <-done
	_, canceled := err.(CanceledError)
	require.True(t, canceled, "Expected rpc.CanceledError")

	ok := c.Finish(nil)
	require.False(t, ok, "Expected c.Finish to fail")

	closed := d.Close(nil)
	<-closed
}

func TestDispatchCanceledAfterResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	d, callCh, done := dispatchTestCallWithContext(t, ctx)

	ch := make(chan *call)
	callCh <- callRetrieval{0, ch}
	c := <-ch
	require.NotNil(t, c, "Expected c not to be nil")

	ok := c.Finish(nil)
	require.True(t, ok, "Expected c.Finish to succeed")

	cancel()

	err := <-done
	require.Nil(t, err, "Expected no error")

	closed := d.Close(nil)
	<-closed
}

func TestDispatchEOF(t *testing.T) {
	d, _, done := dispatchTestCall(t)

	closed := d.Close(nil)
	<-closed
	err := <-done
	require.Equal(t, io.EOF, err, "Expected EOF")
}

func TestDispatchEncodeError(t *testing.T) {
	dispatchOut := newErrorMockCodec()

	logFactory := NewSimpleLogFactory(SimpleLogOutput{}, SimpleLogOptions{})
	callCh := make(chan callRetrieval)
	d := newDispatch(dispatchOut, newBlockingMockCodec(), callCh, logFactory.NewLog(nil))

	done := runInBg(func() error {
		return d.Call(context.Background(), "whatever", new(interface{}), new(interface{}), nil)
	})

	require.EqualError(t, <-done, "Encode error")
}
