package rpc

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func dispatchTestCall(t *testing.T) (dispatcher, chan callRetrieval, chan error) {
	dispatchOut := newBlockingMockCodec()

	logFactory := NewSimpleLogFactory(SimpleLogOutput{}, SimpleLogOptions{})
	callCh := make(chan callRetrieval)
	d := newDispatch(dispatchOut, newBlockingMockCodec(), callCh, logFactory.NewLog(nil))

	done := runInBg(func() error {
		return d.Call(context.Background(), "whatever", new(interface{}), new(interface{}), nil)
	})

	// Necessary to ensure the call is far enough along to
	// be ready to respond
	decoderErr := decodeToNull(dispatchOut, &message{remainingFields: 4})
	assert.Nil(t, decoderErr, "Expected no error")
	return d, callCh, done
}

func TestDispatchSuccessfulCall(t *testing.T) {
	d, callCh, done := dispatchTestCall(t)

	ch := make(chan *call)
	callCh <- callRetrieval{0, ch}
	c := <-ch
	assert.NotNil(t, c, "Expected c not to be nil")

	c.Finish(nil)
	err := <-done
	assert.Nil(t, err, "Expected no error")

	closed := d.Close(nil)
	<-closed
}

func TestDispatchEOF(t *testing.T) {
	d, _, done := dispatchTestCall(t)

	closed := d.Close(nil)
	<-closed
	err := <-done
	assert.Equal(t, io.EOF, err, "Expected EOF")
}
