package rpc

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/keybase/go-codec/codec"
	"github.com/stretchr/testify/require"
)

func TestFrameReaderReadByte(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x1, 0x2})
	bufReader := bufio.NewReader(buf)

	log := newTestLog(t)
	frameReader := newFrameReader(bufReader, 3, log)

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

func TestFrameReaderRead(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x1, 0x2, 0x3})
	bufReader := bufio.NewReader(buf)

	log := newTestLog(t)
	frameReader := newFrameReader(bufReader, 3, log)

	n, err := frameReader.Read(nil)
	require.NoError(t, err)
	require.Equal(t, 0, n)

	// Full read.
	b := make([]byte, 2)
	n, err = frameReader.Read(b)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []byte{0x1, 0x2}, b)

	// Partial read -- bufio.Reader doesn't return EOF.
	n, err = frameReader.Read(b)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []byte{0x3}, b[:1])

	// Empty read at end.
	n, err = frameReader.Read(b)
	require.Equal(t, err, io.EOF)
	require.Equal(t, 0, n)

	// Empty read not at end.
	frameReader.remaining = 1
	n, err = frameReader.Read(b)
	require.Equal(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, 0, n)
}

func TestFrameReaderDrainSuccess(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x1, 0x2, 0x3})
	bufReader := bufio.NewReader(buf)

	log := newTestLog(t)
	frameReader := newFrameReader(bufReader, 3, log)

	err := frameReader.drain()
	require.NoError(t, err)
	require.Equal(t, 0, buf.Len())
}

func TestFrameReaderDrainFailure(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x1, 0x2, 0x3})
	bufReader := bufio.NewReader(buf)

	log := newTestLog(t)
	frameReader := newFrameReader(bufReader, 4, log)

	err := frameReader.drain()
	require.Equal(t, io.ErrUnexpectedEOF, err)
	require.Equal(t, 0, buf.Len())
}

func createPacketizerTestProtocol(t *testing.T) protocolHandlers {
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

func TestPacketizerDecodeNegativeLength(t *testing.T) {
	var buf bytes.Buffer
	e := codec.NewEncoder(&buf, newCodecMsgpackHandle())
	err := e.Encode(-1)
	require.NoError(t, err)

	cc := newCallContainer()
	log := newTestLog(t)
	instrumenterStorage := NewMemoryInstrumentationStorage()
	pkt := newPacketizer(testMaxFrameLength, &buf, createPacketizerTestProtocol(t),
		cc, log, instrumenterStorage)

	_, err = pkt.NextFrame(context.Background())
	require.Equal(t, NewPacketizerError("invalid frame length: -1"), err)
}

func TestPacketizerDecodeTooLargeLength(t *testing.T) {
	var buf bytes.Buffer
	e := codec.NewEncoder(&buf, newCodecMsgpackHandle())
	err := e.Encode(testMaxFrameLength + 1)
	require.NoError(t, err)

	cc := newCallContainer()
	log := newTestLog(t)
	instrumenterStorage := NewMemoryInstrumentationStorage()
	pkt := newPacketizer(testMaxFrameLength, &buf, createPacketizerTestProtocol(t),
		cc, log, instrumenterStorage)

	_, err = pkt.NextFrame(context.Background())
	require.Equal(t, NewPacketizerError("frame length too big: %d > %d", testMaxFrameLength+1, testMaxFrameLength), err)
}

func TestPacketizerDecodeShortPacket(t *testing.T) {
	var buf bytes.Buffer
	e := codec.NewEncoder(&buf, newCodecMsgpackHandle())
	const maxInt = int32(^uint32(0) >> 1)
	err := e.Encode(maxInt)
	require.NoError(t, err)

	cc := newCallContainer()
	log := newTestLog(t)
	instrumenterStorage := NewMemoryInstrumentationStorage()
	pkt := newPacketizer(maxInt, &buf, createPacketizerTestProtocol(t),
		cc, log, instrumenterStorage)

	// Shouldn't try to allocate a buffer for the packet.
	_, err = pkt.NextFrame(context.Background())
	require.Equal(t, io.ErrUnexpectedEOF, err)
}

func TestPacketizerDecodeBadLengthField(t *testing.T) {
	var buf bytes.Buffer
	e := codec.NewEncoder(&buf, newCodecMsgpackHandle())
	err := e.Encode(testMaxFrameLength)
	require.NoError(t, err)
	buf.WriteByte(0x90)

	cc := newCallContainer()
	log := newTestLog(t)
	instrumenterStorage := NewMemoryInstrumentationStorage()
	pkt := newPacketizer(testMaxFrameLength, &buf, createPacketizerTestProtocol(t),
		cc, log, instrumenterStorage)

	_, err = pkt.NextFrame(context.Background())
	require.Equal(t, NewPacketizerError("wrong message structure prefix (0x90)"), err)
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
	enc := newFramedMsgpackEncoder(testMaxFrameLength, &buf)
	ctx := context.Background()
	sizes := map[int]int64{}
	for i, frame := range frames {
		size, errCh := enc.encodeAndWriteInternal(ctx, frame, nil)
		err := <-errCh
		require.NoError(t, err)
		require.NotZero(t, size)
		sizes[i] = size - 1
	}

	cc := newCallContainer()
	instrumenterStorage := NewMemoryInstrumentationStorage()
	record := NewNetworkInstrumenter(instrumenterStorage, "Response foo.bar")
	c := cc.NewCall(ctx, newMethodV1("foo.bar"), new(interface{}), new(string), CompressionNone, nil, record)
	cc.AddCall(c)

	log := newTestLog(t)
	pkt := newPacketizer(testMaxFrameLength, &buf, createPacketizerTestProtocol(t),
		cc, log, instrumenterStorage)

	f1, err := pkt.NextFrame(context.Background())
	require.NoError(t, err)
	record = f1.(*rpcCallMessage).basicRPCData.instrumenter
	f1.(*rpcCallMessage).basicRPCData.instrumenter = nil
	require.Equal(t, &rpcCallMessage{
		basicRPCData: basicRPCData{ctx: context.Background()},
		seqno:        1,
		name:         newMethodV1("abc.hello"),
	}, f1)
	require.EqualValues(t, sizes[0], record.Size)
	require.Equal(t, InstrumentTag(MethodCall, "abc.hello"), record.tag)

	f2, err := pkt.NextFrame(ctx)
	require.IsType(t, PacketizerError{}, err)
	require.Nil(t, f2)

	f3, err := pkt.NextFrame(ctx)
	require.IsType(t, PacketizerError{}, err)
	require.Nil(t, f3)

	f4, err := pkt.NextFrame(ctx)
	require.NoError(t, err)
	record = f4.(*rpcNotifyMessage).basicRPCData.instrumenter
	f4.(*rpcNotifyMessage).basicRPCData.instrumenter = nil
	require.Equal(t, &rpcNotifyMessage{
		name: newMethodV1("abc.hello"),
	}, f4)
	require.EqualValues(t, sizes[3], record.Size)
	require.Equal(t, InstrumentTag(MethodNotify, "abc.hello"), record.tag)

	f5, err := pkt.NextFrame(ctx)
	require.IsType(t, PacketizerError{}, err)
	require.Nil(t, f5)

	f6, err := pkt.NextFrame(ctx)
	require.NoError(t, err)
	require.NotNil(t, f6.(*rpcResponseMessage).c)
	record = f6.(*rpcResponseMessage).c.instrumenter
	f6.(*rpcResponseMessage).c.instrumenter = nil
	require.Equal(t, &rpcResponseMessage{
		c:           c,
		responseErr: errors.New("response err"),
	}, f6)
	require.EqualValues(t, sizes[5], record.Size)
	require.Equal(t, InstrumentTag(MethodResponse, "foo.bar"), record.tag)

	f7, err := pkt.NextFrame(ctx)
	require.IsType(t, DecodeError{}, err)
	require.Nil(t, f7)

	f8, err := pkt.NextFrame(ctx)
	require.NoError(t, err)
	require.Equal(t, &rpcCancelMessage{
		seqno: 1,
		name:  newMethodV1("abc.hello"),
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
	instrumenterStorage := NewMemoryInstrumentationStorage()

	// Taking advantage here of opErr being a nil *net.OpError,
	// but a non-nil error when used by errReader.
	var opErr *net.OpError
	pkt := newPacketizer(testMaxFrameLength, errReader{opErr}, createPacketizerTestProtocol(t),
		newCallContainer(), log, instrumenterStorage)

	msg, err := pkt.NextFrame(context.Background())
	require.Nil(t, msg)
	require.Equal(t, io.EOF, err)
}
