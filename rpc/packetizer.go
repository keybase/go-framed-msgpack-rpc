package rpc

import (
	"bufio"
	"fmt"
	"io"
	"net"

	"github.com/keybase/go-codec/codec"
)

// lastErrReader stores the last error returned by its child
// reader. It's used by loadNextFrame below.
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
	fieldDecoder  *fieldDecoder
	protocols     *protocolHandler
	calls         *callContainer
}

func newPacketizer(reader *bufio.Reader, protocols *protocolHandler, calls *callContainer, log LogInterface) *packetizer {
	wrappedReader := &lastErrReader{reader, nil}
	return &packetizer{
		lengthDecoder: codec.NewDecoder(wrappedReader, newCodecMsgpackHandle()),
		reader:        wrappedReader,
		fieldDecoder:  newFieldDecoder(log),
		protocols:     protocols,
		calls:         calls,
	}
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
	nb, l, err := p.loadNextFrame()
	if err != nil {
		return nil, err
	}

	// Interpret the byte as the length field of a fixarray of up
	// to 15 elements: see
	// https://github.com/msgpack/msgpack/blob/master/spec.md#formats-array
	// for details. Do this so we can decode directly into the
	// expected fields without copying.
	if nb < 0x91 || nb > 0x9f {
		// TODO: Clean up
		b := make([]byte, l)
		_, _ = p.reader.Read(b)
		return nil, NewPacketizerError("wrong message structure prefix (0x%x)", nb)
	}
	p.fieldDecoder.Reset(p.reader, l)
	defer func() {
		drainErr := p.fieldDecoder.Drain()
		// TODO: Figure out right semantics here.
		if drainErr != nil && err == nil {
			msg = nil
			err = drainErr
		}
	}()

	return decodeRPC(int(nb-0x90), p.fieldDecoder, p.protocols, p.calls)
}

func (p *packetizer) loadNextFrame() (byte, int32, error) {
	// Get the packet length
	var l int32
	if err := p.lengthDecoder.Decode(&l); err != nil {
		// If the connection is reset or has been closed on
		// this side, return EOF. lengthDecoder wraps most
		// errors, so we have to check p.reader.err instead of
		// err.
		if _, ok := p.reader.err.(*net.OpError); ok {
			return 0, 0, io.EOF
		}
		return 0, 0, err
	}
	if l <= 0 {
		return 0, 0, PacketizerError{fmt.Sprintf("invalid frame length: %d", l)}
	}

	// TODO: Probably gotta drain here, too.

	var nb [1]byte
	// Note that ReadFull drops the error returned from p.reader
	// if enough bytes are read. This isn't a big deal, as if it's
	// a serious error we'll probably run it again on the next
	// frame read.
	lenRead, err := io.ReadFull(p.reader, nb[:])
	if err != nil {
		return 0, 0, err
	}
	if lenRead != 1 {
		return 0, 0, fmt.Errorf("Unable to read desired length. Desired: %d, actual: %d", 1, lenRead)
	}

	return nb[0], l - 1, nil
}
