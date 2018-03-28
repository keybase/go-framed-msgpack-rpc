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

func TestPacketizerDecodeInvalidFrames(t *testing.T) {
	// Encode a mix of valid and invalid frames.
	v1 := []interface{}{MethodCall, 1, "abc.hello", new(interface{})}
	iv1 := "some string"
	iv2 := 53
	v2 := []interface{}{MethodCall, 2, "abc.hello", new(interface{})}
	iv3 := false
	v3 := []interface{}{MethodCall, 3, "abc.hello", new(interface{})}
	iv4 := []interface{}{"some string"}

	frames := []interface{}{v1, iv1, iv2, v2, iv3, v3, iv4}

	var buf bytes.Buffer
	enc := newFramedMsgpackEncoder(&buf)
	ctx := context.Background()
	for _, frame := range frames {
		err := <-enc.EncodeAndWrite(ctx, frame, nil)
		require.NoError(t, err)
	}

	cc := newCallContainer()
	c := cc.NewCall(ctx, "foo.bar", new(interface{}), new(string), nil)
	cc.AddCall(c)
	pkt := newPacketHandler(&buf, createPacketizerTestProtocol(), cc)

	f1, err := pkt.NextFrame()
	require.NoError(t, err)
	require.Equal(t, &rpcCallMessage{
		seqno: 1,
		name:  "abc.hello",
	}, f1)

	f2, err := pkt.NextFrame()
	require.NoError(t, err)
	require.Equal(t, f2, iv1)
}
