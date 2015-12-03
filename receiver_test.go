package rpc

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func testReceive(t *testing.T, callCh chan callRetrieval, p *Protocol, args ...interface{}) error {
	receiveIn := newBlockingMockCodec()
	receiveOut := newBlockingMockCodec()

	logFactory := NewSimpleLogFactory(SimpleLogOutput{}, SimpleLogOptions{})
	r := newReceiveHandler(receiveOut, receiveIn, callCh, logFactory.NewLog(nil), nil)
	if p != nil {
		r.RegisterProtocol(*p)
	}
	go receiveIn.Encode(args)

	errCh := make(chan error)
	go func() {
		var m MethodType
		var seqno seqNumber
		var respErrStr string
		var res interface{}
		decErr := receiveOut.Decode(&m)
		require.Nil(t, decErr, "Expected decode to succeed")
		require.Equal(t, MethodResponse, m, "Expected a response")
		decErr = receiveOut.Decode(&seqno)
		require.Nil(t, decErr, "Expected decode to succeed")
		require.Equal(t, seqNumber(0), seqno, "Expected sequence number to be 0")
		decErr = receiveOut.Decode(&respErrStr)
		require.Nil(t, decErr, "Expected decode to succeed")
		decErr = receiveOut.Decode(&res)
		require.Nil(t, decErr, "Expected decode to succeed")
		errCh <- errors.New(respErrStr)
	}()

	err := r.Receive(len(args))
	if err != nil {
		return err
	}

	return <-errCh
}

func TestReceiveInvalidType(t *testing.T) {
	err := testReceive(
		t,
		make(chan callRetrieval),
		nil,
		"hello",
		seqNumber(0),
		"invalid",
		new(interface{}),
		nil,
	)
	require.EqualError(t, err, "Tried to decode incorrect type. Expected: rpc.MethodType, actual: string", "Expected error attempting to call an invalid method type")
}

func TestReceiveInvalidMethodType(t *testing.T) {
	err := testReceive(
		t,
		make(chan callRetrieval),
		nil,
		(MethodType)(999),
		seqNumber(0),
		"invalid",
		new(interface{}),
		nil,
	)
	require.EqualError(t, err, "dispatcher error: invalid message type", "Expected error attempting to call an invalid method type")
}

func TestReceiveInvalidProtocol(t *testing.T) {
	err := testReceive(
		t,
		make(chan callRetrieval),
		nil,
		MethodCall,
		seqNumber(0),
		"nonexistent.broken",
		new(interface{}),
		nil,
	)
	require.EqualError(t, err, "protocol not found: nonexistent", "Expected error attempting to call a nonexistent method")
}

func TestReceiveInvalidMethod(t *testing.T) {
	err := testReceive(
		t,
		make(chan callRetrieval),
		&Protocol{
			Name:    "existent",
			Methods: make(map[string]ServeHandlerDescription),
		},
		MethodCall,
		seqNumber(0),
		"existent.invalid",
		new(interface{}),
		nil,
	)
	require.EqualError(t, err, "method 'invalid' not found in protocol 'existent'", "Expected error attempting to call a nonexistent method")
}

func TestReceiveWrongMessageLength(t *testing.T) {
	err := testReceive(
		t,
		make(chan callRetrieval),
		nil,
		MethodCall,
		seqNumber(0),
		"invalid",
	)
	require.EqualError(t, err, "dispatcher error: wrong number of fields for message (got n=3, expected n=5)", "Expected error attempting to call a nonexistent method")
}

func TestReceiveResponse(t *testing.T) {
	callCh := make(chan callRetrieval)
	go func() {
		err := testReceive(
			t,
			callCh,
			nil,
			MethodResponse,
			seqNumber(0),
			"",
			"hi",
		)
		require.Nil(t, err, "expected receive to succeed")
	}()

	var result string
	c := newCall(context.Background(), "testmethod", new(interface{}), &result, nil, NilProfiler{})
	callRetrieval := <-callCh
	callRetrieval.ch <- c

	<-c.resultCh
	require.Equal(t, "hi", result, "Expected response to say \"hi\"")
}

func TestReceiveResponseWrongSize(t *testing.T) {
	err := testReceive(
		t,
		make(chan callRetrieval),
		nil,
		MethodResponse,
		seqNumber(0),
		"",
	)
	require.EqualError(t, err, "dispatcher error: wrong number of fields for message (got n=3, expected n=4)", "Expected error attempting to receive the wrong size of response")
}

func TestReceiveResponseNilCall(t *testing.T) {
	callCh := make(chan callRetrieval)
	done := runInBg(func() error {
		receiveIn := newBlockingMockCodec()
		receiveOut := newBlockingMockCodec()

		logFactory := NewSimpleLogFactory(SimpleLogOutput{}, SimpleLogOptions{})
		r := newReceiveHandler(receiveOut, receiveIn, callCh, logFactory.NewLog(nil), nil)

		args := []interface{}{
			MethodResponse,
			seqNumber(0),
			"",
			"hi",
		}
		go receiveIn.Encode(args)

		err := r.Receive(len(args))
		return err
	})

	callRetrieval := <-callCh
	callRetrieval.ch <- nil

	err := <-done
	require.Nil(t, err)
}

func TestReceiveResponseError(t *testing.T) {
	callCh := make(chan callRetrieval)
	done := runInBg(func() error {
		err := testReceive(
			t,
			callCh,
			nil,
			MethodResponse,
			seqNumber(0),
			// Wrong type for error, should be string
			32,
			"hi",
		)
		return err
	})

	var result string
	c := newCall(context.Background(), "testmethod", new(interface{}), &result, nil, NilProfiler{})
	callRetrieval := <-callCh
	callRetrieval.ch <- c

	err := <-c.resultCh
	require.EqualError(t, err, "Tried to decode incorrect type. Expected: string, actual: int", "expected error when passing in a nil call")
	err = <-done
	require.EqualError(t, err, "Tried to decode incorrect type. Expected: string, actual: int", "expected error when passing in a nil call")
}

func TestCloseReceiver(t *testing.T) {
	logFactory := NewSimpleLogFactory(SimpleLogOutput{}, SimpleLogOptions{})
	r := newReceiveHandler(
		newBlockingMockCodec(),
		newBlockingMockCodec(),
		make(chan callRetrieval),
		logFactory.NewLog(nil),
		nil,
	)
	// Buffer so the write doesn't block and thus we don't need a goroutine
	errCh := make(chan error, 1)

	r.AddCloseListener(errCh)
	r.Close(io.EOF)

	err := <-errCh
	require.EqualError(t, err, io.EOF.Error(), "expected EOF error from closing the receiver")
}
