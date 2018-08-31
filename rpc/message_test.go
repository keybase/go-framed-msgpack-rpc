package rpc

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func createMessageTestProtocol() *protocolHandler {
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

func runMessageTest(t *testing.T, v []interface{}) (rpcMessage, error) {
	var buf bytes.Buffer
	enc := newFramedMsgpackEncoder(&buf)
	cc := newCallContainer()
	c := cc.NewCall(context.Background(), "foo.bar", new(interface{}), new(string), nil)
	cc.AddCall(c)

	log := newTestLog(t)
	pkt := newPacketHandler(&buf, createMessageTestProtocol(), cc, log)

	err := <-enc.EncodeAndWrite(c.ctx, v, nil)
	require.Nil(t, err, "expected encoding to succeed")

	return pkt.NextFrame()
}

func TestMessageDecodeValid(t *testing.T) {
	v := []interface{}{MethodCall, 999, "abc.hello", new(interface{})}

	rpc, err := runMessageTest(t, v)
	require.Nil(t, err)
	c, ok := rpc.(*rpcCallMessage)
	require.True(t, ok)
	require.Equal(t, MethodCall, c.Type())
	require.Equal(t, SeqNumber(999), c.SeqNo())
	require.Equal(t, "abc.hello", c.Name())
	require.Equal(t, nil, c.Arg())
}

func TestMessageDecodeValidExtraParams(t *testing.T) {
	tags := CtxRpcTags{"hello": "world"}
	v := []interface{}{MethodCall, 999, "abc.hello", new(interface{}), tags, "foo"}

	rpc, err := runMessageTest(t, v)
	require.Nil(t, err)
	c, ok := rpc.(*rpcCallMessage)
	require.True(t, ok)
	require.Equal(t, MethodCall, c.Type())
	require.Equal(t, SeqNumber(999), c.SeqNo())
	require.Equal(t, "abc.hello", c.Name())
	require.Equal(t, nil, c.Arg())
	resultTags, ok := RpcTagsFromContext(c.Context())
	require.True(t, ok)
	require.Equal(t, tags, resultTags)
}

func TestMessageDecodeValidResponse(t *testing.T) {
	v := []interface{}{MethodResponse, SeqNumber(0), nil, "hi"}

	rpc, err := runMessageTest(t, v)
	require.Nil(t, err)
	r, ok := rpc.(*rpcResponseMessage)
	require.True(t, ok)
	require.Equal(t, MethodResponse, r.Type())
	require.Equal(t, SeqNumber(0), r.SeqNo())
	resAsString, ok := r.Res().(*string)
	require.True(t, ok)
	require.Equal(t, "hi", *resAsString)
	require.True(t, ok)
}

func TestMessageDecodeInvalidType(t *testing.T) {
	v := []interface{}{"hello", SeqNumber(0), "invalid", new(interface{})}

	_, err := runMessageTest(t, v)
	require.Error(t, err)
	require.Contains(t, err.Error(), "RPC error. type: Invalid, method: , length: 4, error: error decoding message field at position 0, error: ")
}

func TestMessageDecodeInvalidMethodType(t *testing.T) {
	v := []interface{}{MethodType(999), SeqNumber(0), "invalid", new(interface{})}

	_, err := runMessageTest(t, v)
	require.EqualError(t, err, "RPC error. type: Method(999), method: , length: 4, error: invalid RPC type")
}

func TestMessageDecodeInvalidProtocol(t *testing.T) {
	v := []interface{}{MethodCall, SeqNumber(0), "nonexistent.broken", new(interface{})}

	_, err := runMessageTest(t, v)
	require.EqualError(t, err, "RPC error. type: Call, method: nonexistent.broken, length: 4, error: protocol not found: nonexistent")
}

func TestMessageDecodeInvalidMethod(t *testing.T) {
	v := []interface{}{MethodCall, SeqNumber(0), "abc.invalid", new(interface{})}

	_, err := runMessageTest(t, v)
	require.EqualError(t, err, "RPC error. type: Call, method: abc.invalid, length: 4, error: method 'invalid' not found in protocol 'abc'")
}

func TestMessageDecodeWrongMessageLength(t *testing.T) {
	v := []interface{}{MethodCall, SeqNumber(0), "abc.invalid"}

	_, err := runMessageTest(t, v)
	require.EqualError(t, err, "RPC error. type: Call, method: , length: 3, error: wrong message length")
}

func TestMessageDecodeResponseNilCall(t *testing.T) {
	v := []interface{}{MethodResponse, SeqNumber(-1), 32, "hi"}

	_, err := runMessageTest(t, v)
	require.EqualError(t, err, "RPC error. type: Response, method: , length: 4, error: Call not found for sequence number -1")
}
