package rpc

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/keybase/go-codec/codec"
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

// TestPacketizerDecodeInvalidFrames makes sure that the packetizer
// can handle invalid frames and skip over them.
func TestPacketizerDecodeInvalidFrames(t *testing.T) {
	// Encode a mix of valid and invalid frames.
	v1 := []interface{}{MethodCall, 1, "abc.hello", new(interface{})}
	iv1 := "some string"
	iv2 := 53
	v2 := []interface{}{MethodNotify, "abc.hello", new(interface{})}
	iv3 := false
	v3 := []interface{}{MethodResponse, 0, "response err", new(interface{})}
	iv4 := []interface{}{"some string"}
	v4 := []interface{}{MethodCancel, 1, "abc.hello"}

	frames := []interface{}{v1, iv1, iv2, v2, iv3, v3, iv4, v4}

	var buf bytes.Buffer
	enc := newFramedMsgpackEncoder(&buf)
	ctx := context.Background()
	for _, frame := range frames {
		err := <-enc.encodeAndWriteInternal(ctx, frame, nil)
		require.NoError(t, err)
	}

	cc := newCallContainer()
	c := cc.NewCall(ctx, "foo.bar", new(interface{}), new(string), nil)
	cc.AddCall(c)

	log := newTestLog(t)
	pkt := newPacketizer(&buf, createPacketizerTestProtocol(), cc, log)

	f1, err := pkt.NextFrame()
	require.NoError(t, err)
	require.Equal(t, &rpcCallMessage{
		seqno: 1,
		name:  "abc.hello",
	}, f1)

	f2, err := pkt.NextFrame()
	require.IsType(t, PacketizerError{}, err)
	require.Nil(t, f2)

	f3, err := pkt.NextFrame()
	require.IsType(t, PacketizerError{}, err)
	require.Nil(t, f3)

	f4, err := pkt.NextFrame()
	require.NoError(t, err)
	require.Equal(t, &rpcNotifyMessage{
		name: "abc.hello",
	}, f4)

	f5, err := pkt.NextFrame()
	require.IsType(t, PacketizerError{}, err)
	require.Nil(t, f5)

	f6, err := pkt.NextFrame()
	require.NoError(t, err)
	require.Equal(t, &rpcResponseMessage{
		c:           c,
		responseErr: errors.New("response err"),
	}, f6)

	f7, err := pkt.NextFrame()
	require.IsType(t, RPCDecodeError{}, err)
	require.Nil(t, f7)

	f8, err := pkt.NextFrame()
	require.NoError(t, err)
	require.Equal(t, &rpcCancelMessage{
		seqno: 1,
		name:  "abc.hello",
	}, f8)
}

type errReader struct {
	err error
}

func (r errReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestPacketizerReaderOpError(t *testing.T) {
	log := newTestLog(t)

	// Taking advantage here of opErr being a nil *net.OpError,
	// but a non-nil error when used by errReader.
	var opErr *net.OpError
	pkt := newPacketizer(errReader{opErr}, createPacketizerTestProtocol(), newCallContainer(), log)

	bytes, err := pkt.loadNextFrame()
	require.Nil(t, bytes)
	require.Equal(t, io.EOF, err)
}

func TestPacketizerDecodeLargeFrame(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	e := codec.NewEncoder(buf, newCodecMsgpackHandle())
	const maxInt = ^uint(0) >> 1
	e.Encode(maxInt)

	cc := newCallContainer()
	log := newTestLog(t)
	pkt := newPacketHandler(buf, createPacketizerTestProtocol(), cc, log)

	_, err := pkt.NextFrame()
	require.Equal(t, io.EOF, err)
}
