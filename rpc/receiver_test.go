package rpc

import (
	"bufio"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func testReceive(t *testing.T, p *Protocol, rpc rpcMessage) (receiver, chan error) {
	conn1, conn2 := net.Pipe()
	receiveOut := newFramedMsgpackEncoder(conn2)

	protHandler := createMessageTestProtocol()
	if p != nil {
		protHandler.registerProtocol(*p)
	}

	log := newTestLog(t)
	pkt := newPacketizer(bufio.NewReader(conn1), protHandler, newCallContainer(), log)

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

	return r, errCh
}

func makeCall(seq SeqNumber, name string, arg interface{}) *rpcCallMessage {
	return &rpcCallMessage{
		seqno: seq,
		name:  name,
		arg:   arg,
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
	go func() {
		_, errCh := testReceive(
			t,
			nil,
			c,
		)
		err := <-errCh
		require.Nil(t, err)
	}()

	resp := <-c.ResponseCh()
	require.Equal(t, "hi", resp.Res())
}

func TestReceiveResponseNilCall(t *testing.T) {
	c := &rpcResponseMessage{c: &call{}}
	_, errCh := testReceive(
		t,
		nil,
		c,
	)
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
				Handler: func(c context.Context, i interface{}) (interface{}, error) {
					<-c.Done()
					waitCh <- c.Err()
					return nil, c.Err()
				},
				MethodType: MethodCall,
			},
		},
	}
	receiver, _ := testReceive(
		t,
		p,
		makeCall(
			0,
			"waiter.wait",
			nil,
		),
	)
	<-receiver.Close()

	err := <-waitCh
	require.EqualError(t, err, context.Canceled.Error())
}
