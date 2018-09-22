package rpc

import (
	"bufio"
	"fmt"
	"io"
	"net"

	"github.com/keybase/go-codec/codec"
)

// lastErrReader stores the last error returned by its child
// reader. It's used by NextFrame below.
type lastErrReader struct {
	reader *bufio.Reader
	err    error
}

func (r *lastErrReader) Read(buf []byte) (int, error) {
	n, err := r.reader.Read(buf)
	r.err = err
	return n, err
}

type packetizer struct {
	lengthDecoder *codec.Decoder
	reader        *lastErrReader
	protocols     *protocolHandler
	calls         *callContainer
	log           LogInterface
}

func newPacketizer(reader io.Reader, protocols *protocolHandler, calls *callContainer, log LogInterface) *packetizer {
	wrappedReader := &lastErrReader{bufio.NewReader(reader), nil}
	return &packetizer{
		lengthDecoder: codec.NewDecoder(wrappedReader, newCodecMsgpackHandle()),
		reader:        wrappedReader,
		protocols:     protocols,
		calls:         calls,
		log:           log,
	}
}

type frameReader struct {
	r         *bufio.Reader
	remaining int32
	log       LogInterface
}

func (l *frameReader) ReadByte() (byte, error) {
	if l.remaining <= 0 {
		return 0, io.ErrUnexpectedEOF
	}

	b, err := l.r.ReadByte()
	l.remaining--

	if err == nil {
		// TODO: Figure out what to do here.
		l.log.FrameRead([]byte{b})
	}

	return b, err
}

func (l *frameReader) Read(p []byte) (int, error) {
	if l.remaining <= 0 {
		return 0, io.ErrUnexpectedEOF
	}

	if len(p) > int(l.remaining) {
		p = p[:l.remaining]
	}

	n, err := l.r.Read(p)
	l.remaining -= int32(n)

	if err == nil {
		// TODO: Figure out what to do here.
		l.log.FrameRead(p[:n])
	}

	return n, err
}

func (l *frameReader) drain() error {
	n, err := l.r.Discard(int(l.remaining))
	l.remaining -= int32(n)

	if err != nil {
		return err
	}

	if l.remaining != 0 {
		return fmt.Errorf("Unexpected remaining %d", l.remaining)
	}

	return nil
}

// NextFrame returns the next message and an error. The error can be:
//
//   - nil, in which case the returned rpcMessage will be non-nil.
//   - a framing error, i.e. having to do with reading the packet
//     length or the packet bytes. This is a fatal error, and the
//     connection must be closed.
//   - an error while decoding the packet. In theory, we can then
//     discard the packet and move on to the next one, but the
//     semantics of doing so aren't clear. Currently we also treat this
//     as a fatal error.
//   - an error while decoding the message, in which case the returned
//     rpcMessage will be non-nil, and its Err() will match this
//     error. We can then process the error and continue with the next
//     packet.
func (p *packetizer) NextFrame() (msg rpcMessage, err error) {
	// Get the packet length.
	var l int32
	if err := p.lengthDecoder.Decode(&l); err != nil {
		// If the connection is reset or has been closed on
		// this side, return EOF. lengthDecoder wraps most
		// errors, so we have to check p.reader.err instead of
		// err.
		if _, ok := p.reader.err.(*net.OpError); ok {
			return nil, io.EOF
		}
		return nil, err
	}
	if l <= 0 {
		return nil, PacketizerError{fmt.Sprintf("invalid frame length: %d", l)}
	}

	// TODO: Have an upper bound.

	r := frameReader{p.reader.reader, l, p.log}
	defer func() {
		drainErr := r.drain()
		if drainErr != nil && err == nil {
			msg = nil
			err = drainErr
		}
	}()

	nb, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	// Interpret the byte as the length field of a fixarray of up
	// to 15 elements: see
	// https://github.com/msgpack/msgpack/blob/master/spec.md#formats-array
	// for details. Do this so we can decode directly into the
	// expected fields without copying.
	if nb < 0x91 || nb > 0x9f {
		return nil, NewPacketizerError("wrong message structure prefix (0x%x)", nb)
	}

	return decodeRPC(int(nb-0x90), &r, p.protocols, p.calls)
}
