package rpc

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/keybase/go-codec/codec"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestFrameReaderReadByte(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x1, 0x2})
	bufReader := bufio.NewReader(buf)

	log := newTestLog(t)
	frameReader := frameReader{bufReader, 3, log}

	b, err := frameReader.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte(0x1), b)
	require.Equal(t, int32(2), frameReader.remaining)

	b, err = frameReader.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte(0x2), b)
	require.Equal(t, int32(1), frameReader.remaining)

	b, err = frameReader.ReadByte()
	require.Equal(t, io.ErrUnexpectedEOF, err)
	require.Equal(t, byte(0), b)
	// Shouldn't decrease.
	require.Equal(t, int32(1), frameReader.remaining)

	buf.WriteByte(0x3)

	b, err = frameReader.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte(0x3), b)
	require.Equal(t, int32(0), frameReader.remaining)

	b, err = frameReader.ReadByte()
	require.Equal(t, io.EOF, err)
	require.Equal(t, byte(0), b)
	require.Equal(t, int32(0), frameReader.remaining)
}

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
//
// TODO: Fail at various stages.
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
	pkt := newPacketizer(testMaxFrameLength, &buf, createPacketizerTestProtocol(), cc, log)

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
	pkt := newPacketizer(testMaxFrameLength, errReader{opErr}, createPacketizerTestProtocol(), newCallContainer(), log)

	msg, err := pkt.NextFrame()
	require.Nil(t, msg)
	require.Equal(t, io.EOF, err)
}

// TODO: Fail at various stages.
func TestPacketizerDecodeLargeFrame(t *testing.T) {
	var buf bytes.Buffer
	e := codec.NewEncoder(&buf, newCodecMsgpackHandle())
	const maxInt = int32(^uint32(0) >> 1)
	e.Encode(maxInt)

	cc := newCallContainer()
	log := newTestLog(t)
	pkt := newPacketizer(maxInt, &buf, createPacketizerTestProtocol(), cc, log)

	_, err := pkt.NextFrame()
	require.Equal(t, io.ErrUnexpectedEOF, err)
}
