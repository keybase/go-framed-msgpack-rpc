package rpc

import (
	"bytes"
	"context"
	"testing"

	"github.com/foks-proj/go-ctxlog"
	"github.com/stretchr/testify/require"
)

func createMessageTestProtocol(t *testing.T) protocolHandlers {
	p := newProtocolHandler(nil)
	err := p.registerProtocol(Protocol{
		Name: "abc",
		Methods: map[string]ServeHandlerDescription{
			"hello": {
				MakeArg: func() interface{} {
					return nil
				},
				Handler: func(context.Context, interface{}) (interface{}, error) {
					return nil, nil
				},
			},
		},
	})
	require.NoError(t, err)
	return protocolHandlers{v1: p}
}

func runMessageTest(t *testing.T, ctype CompressionType, v []interface{}) (rpcMessage, error) {
	var buf bytes.Buffer
	enc := newFramedMsgpackEncoder(testMaxFrameLength, &buf)
	cc := newCallContainer()
	instrumenterStorage := NewMemoryInstrumentationStorage()
	record := NewNetworkInstrumenter(instrumenterStorage, "foo.bar")
	c := cc.NewCall(context.Background(), newMethodV1("foo.bar"), new(interface{}), new(string), ctype, nil, record)
	cc.AddCall(c)

	log := newTestLog(t)
	pkt := newPacketizer(testMaxFrameLength, &buf, createMessageTestProtocol(t),
		cc, log, instrumenterStorage)

	size, errCh := enc.EncodeAndWrite(c.ctx, v, nil)
	err := <-errCh
	require.NoError(t, err, "expected encoding to succeed")
	require.EqualValues(t, buf.Len(), size)

	return pkt.NextFrame(context.Background())
}

func TestMessageDecodeValid(t *testing.T) {
	v := []interface{}{MethodCall, 999, "abc.hello", new(interface{})}

	rpc, err := runMessageTest(t, CompressionNone, v)
	require.NoError(t, err)
	c, ok := rpc.(*rpcCallMessage)
	require.True(t, ok)
	require.Equal(t, MethodCall, c.Type())
	require.Equal(t, CompressionNone, c.Compression())
	require.Equal(t, SeqNumber(999), c.SeqNo())
	require.Equal(t, newMethodV1("abc.hello"), c.Name())
	require.Equal(t, nil, c.Arg())
}

func TestMessageDecodeValidCompressed(t *testing.T) {
	doWithAllCompressionTypes(func(ctype CompressionType) {
		v := []interface{}{MethodCallCompressed, 999, ctype, "abc.hello", new(interface{})}

		rpc, err := runMessageTest(t, ctype, v)
		require.NoError(t, err)
		c, ok := rpc.(*rpcCallCompressedMessage)
		require.True(t, ok)
		require.Equal(t, MethodCallCompressed, c.Type())
		require.Equal(t, ctype, c.Compression())
		require.Equal(t, SeqNumber(999), c.SeqNo())
		require.Equal(t, newMethodV1("abc.hello"), c.Name())
		require.Equal(t, nil, c.Arg())
	})
}

func TestMessageDecodeValidExtraParams(t *testing.T) {
	tags := ctxlog.CtxLogTags{"hello": "world"}
	v := []interface{}{MethodCall, 999, "abc.hello", new(interface{}), tags, "foo"}

	rpc, err := runMessageTest(t, CompressionNone, v)
	require.NoError(t, err)
	c, ok := rpc.(*rpcCallMessage)
	require.True(t, ok)
	require.Equal(t, MethodCall, c.Type())
	require.Equal(t, SeqNumber(999), c.SeqNo())
	require.Equal(t, CompressionNone, c.Compression())
	require.Equal(t, newMethodV1("abc.hello"), c.Name())
	require.Equal(t, nil, c.Arg())
	resultTags, ok := ctxlog.TagsFromContext(c.Context())
	require.True(t, ok)
	require.Equal(t, tags, resultTags)
}

func TestMessageDecodeValidResponse(t *testing.T) {
	v := []interface{}{MethodResponse, SeqNumber(0), nil, "hi"}
	rpc, err := runMessageTest(t, CompressionNone, v)
	require.NoError(t, err)
	c, ok := rpc.(*rpcResponseMessage)
	require.True(t, ok)
	require.Equal(t, MethodResponse, c.Type())
	require.Equal(t, CompressionNone, c.Compression())
	require.Equal(t, SeqNumber(0), c.SeqNo())
	resAsString, ok := c.Res().(*string)
	require.True(t, ok)
	require.Equal(t, "hi", *resAsString)
	require.True(t, ok)
}

func TestMessageDecodeInvalidType(t *testing.T) {
	v := []interface{}{"hello", SeqNumber(0), "invalid", new(interface{})}

	_, err := runMessageTest(t, CompressionNone, v)
	require.Error(t, err)
	require.Contains(t, err.Error(), "RPC error. type: Invalid, method: , length: 4, compression: none, error: error decoding message field at position 0, error: ")
}

func TestMessageDecodeInvalidMethodType(t *testing.T) {
	v := []interface{}{MethodType(999), SeqNumber(0), "invalid", new(interface{})}

	_, err := runMessageTest(t, CompressionNone, v)
	require.EqualError(t, err, "RPC error. type: Method(999), method: , length: 4, compression: none, error: invalid RPC type")
}

func TestMessageDecodeInvalidProtocol(t *testing.T) {
	v := []interface{}{MethodCall, SeqNumber(0), "nonexistent.broken", new(interface{})}

	_, err := runMessageTest(t, CompressionNone, v)
	require.EqualError(t, err, "RPC error. type: Call, method: nonexistent.broken, length: 4, compression: none, error: protocol not found: nonexistent")
}

func TestMessageDecodeInvalidMethod(t *testing.T) {
	v := []interface{}{MethodCall, SeqNumber(0), "abc.invalid", new(interface{})}

	_, err := runMessageTest(t, CompressionNone, v)
	require.EqualError(t, err, "RPC error. type: Call, method: abc.invalid, length: 4, compression: none, error: method 'invalid' not found in protocol 'abc'")
}

func TestMessageDecodeWrongMessageLength(t *testing.T) {
	v := []interface{}{MethodCall, SeqNumber(0), "abc.invalid"}

	_, err := runMessageTest(t, CompressionNone, v)
	require.EqualError(t, err, "RPC error. type: Call, method: , length: 3, compression: none, error: wrong message length")
}

func TestMessageDecodeResponseNilCall(t *testing.T) {
	v := []interface{}{MethodResponse, SeqNumber(-1), 32, "hi"}

	_, err := runMessageTest(t, CompressionNone, v)
	require.EqualError(t, err, "RPC error. type: Response, method: , length: 4, compression: none, error: Call not found for sequence number -1")
}
