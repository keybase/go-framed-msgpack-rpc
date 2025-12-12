package rpc

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func testReceive(t *testing.T, p *Protocol, rpc rpcMessage) (receiver, chan error, net.Conn, net.Conn) {
	conn1, conn2 := net.Pipe()
	receiveOut := newFramedMsgpackEncoder(testMaxFrameLength, conn2)

	protHandler := createMessageTestProtocol(t)
	if p != nil {
		err := protHandler.registerProtocol(*p)
		require.NoError(t, err)
	}

	log := newTestLog(t)
	instrumenterStorage := NewMemoryInstrumentationStorage()
	pkt := newPacketizer(testMaxFrameLength, conn1, protHandler,
		newCallContainer(), log, instrumenterStorage)
	r := newReceiveHandler(receiveOut, protHandler, log)

	errCh := make(chan error, 1)
	err := r.Receive(rpc)
	if err != nil {
		errCh <- err
	} else {
		go func() {
			_, err := pkt.NextFrame()
			errCh <- err
		}()
	}

	return r, errCh, conn1, conn2
}

func makeCall(seq SeqNumber, name string) *rpcCallMessage {
	return &rpcCallMessage{
		seqno: seq,
		name:  name,
		arg:   nil,
	}
}

func makeResponse(err error, res interface{}) *rpcResponseMessage {
	return &rpcResponseMessage{
		err: err,
		c: &call{
			resultCh: make(chan *rpcResponseMessage),
			res:      res,
		},
	}
}

func TestReceiveResponse(t *testing.T) {
	c := makeResponse(
		nil,
		"hi",
	)

	type result struct {
		r     receiver
		errCh chan error
		conn1 net.Conn
		conn2 net.Conn
	}
	resultCh := make(chan result, 1)

	go func() {
		r, errCh, conn1, conn2 := testReceive(
			t,
			nil,
			c,
		)
		resultCh <- result{r, errCh, conn1, conn2}
	}()

	resp := <-c.ResponseCh()
	require.Equal(t, "hi", resp.Res())

	// Get the connections from the goroutine
	res := <-resultCh

	// Close connections to unblock NextFrame()
	require.NoError(t, res.conn1.Close())
	require.NoError(t, res.conn2.Close())

	// The NextFrame goroutine will get EOF, which is expected
	err := <-res.errCh
	require.Error(t, err) // EOF is expected when we close the connection
}

func TestReceiveResponseNilCall(t *testing.T) {
	c := &rpcResponseMessage{c: &call{}}
	_, errCh, conn1, conn2 := testReceive(
		t,
		nil,
		c,
	)
	defer func() { require.NoError(t, conn1.Close()) }()
	defer func() { require.NoError(t, conn2.Close()) }()
	err := <-errCh

	require.True(t, shouldContinue(err))
	require.EqualError(t, err, "Call not found for sequence number 0")
}

func TestCloseReceiver(t *testing.T) {
	// Call error status
	waitCh := make(chan error, 1)
	p := &Protocol{
		Name: "waiter",
		Methods: map[string]ServeHandlerDescription{
			"wait": {
				MakeArg: func() interface{} {
					return nil
				},
				Handler: func(c context.Context, _ interface{}) (interface{}, error) {
					<-c.Done()
					waitCh <- c.Err()
					return nil, c.Err()
				},
			},
		},
	}
	receiver, errCh, conn1, conn2 := testReceive(
		t,
		p,
		makeCall(
			0,
			"waiter.wait",
		),
	)
	<-receiver.Close()

	// Close connections to unblock the background goroutine
	require.NoError(t, conn1.Close())
	require.NoError(t, conn2.Close())

	// Wait for the background goroutine in testReceive to complete
	<-errCh

	err := <-waitCh
	require.EqualError(t, err, context.Canceled.Error())
}
