package rpc

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func createPacketizerTestProtocol() *protocolHandler {
	p := newProtocolHandler(nil)
	p.registerProtocol(Protocol{
		Name: "abc",
		Methods: map[string]ServeHandlerDescription{
			"hello": {
				MakeArg: func() interface{} {
					return nil
				},
				Handler: func(context.Context, interface{}) (interface{}, error) {
					return nil, nil
				},
				MethodType: MethodCall,
			},
		},
	})
	return p
}

func runPacketizerTest(t *testing.T, v []interface{}) (rpcMessage, error) {
	var buf bytes.Buffer
	enc := newFramedMsgpackEncoder(&buf)
	cc := newCallContainer()
	c := cc.NewCall(context.Background(), "foo.bar", new(interface{}), new(string), nil)
	cc.AddCall(c)
	pkt := newPacketHandler(&buf, createPacketizerTestProtocol(), cc)

	err := <-enc.EncodeAndWrite(c.ctx, v, nil)
	require.Nil(t, err, "expected encoding to succeed")

	return pkt.NextFrame()
}

func TestPacketizerDecodeValid(t *testing.T) {
	v := []interface{}{MethodCall, 999, "abc.hello", new(interface{})}

	rpc, err := runPacketizerTest(t, v)
	require.NoError(t, err)
	c, ok := rpc.(*rpcCallMessage)
	require.True(t, ok)
	require.Equal(t, MethodCall, c.Type())
	require.Equal(t, SeqNumber(999), c.SeqNo())
	require.Equal(t, "abc.hello", c.Name())
	require.Equal(t, nil, c.Arg())
}
