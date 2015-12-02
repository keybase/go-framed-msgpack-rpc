package rpc

import (
	"github.com/ugorji/go/codec"
	"io"
)

// It might seem like the decoder will race, because we use a shared channel to
// deliver results. However, since channels are FIFO and we only consume one
// element at a time, there is no race.

type decoder interface {
	Decode(interface{}) error
}

type byteReadingDecoder interface {
	decoder
	io.ByteReader
}

type encoder interface {
	Encode(interface{}) <-chan error
}

type encodingBundle struct {
	bytes    []byte
	resultCh chan error
}

type framedMsgpackEncoder struct {
	handle  codec.Handle
	writeCh chan encodingBundle
}

func newMsgPackHandle() *codec.MsgpackHandle {
	return &codec.MsgpackHandle{
		WriteExt:    true,
		RawToString: true,
	}
}

func newFramedMsgpackEncoder(writeCh chan encodingBundle) *framedMsgpackEncoder {
	return &framedMsgpackEncoder{
		handle:  newMsgPackHandle(),
		writeCh: writeCh,
	}
}

func (e *framedMsgpackEncoder) encodeToBytes(i interface{}) (v []byte, err error) {
	enc := codec.NewEncoderBytes(&v, e.handle)
	err = enc.Encode(i)
	return v, err
}

func (e *framedMsgpackEncoder) encodeFrame(i interface{}) ([]byte, error) {
	content, err := e.encodeToBytes(i)
	if err != nil {
		return nil, err
	}
	length, err := e.encodeToBytes(len(content))
	if err != nil {
		return nil, err
	}
	return append(length, content...), nil
}

func (e *framedMsgpackEncoder) Encode(i interface{}) <-chan error {
	ch := make(chan error, 1)
	bytes, err := e.encodeFrame(i)
	if err != nil {
		ch <- err
		return ch
	}
	go func() {
		e.writeCh <- encodingBundle{bytes: bytes, resultCh: ch}
	}()
	return ch
}

type byteResult struct {
	b   byte
	err error
}

type framedMsgpackDecoder struct {
	decoderCh        chan interface{}
	decoderResultCh  chan error
	readByteCh       chan struct{}
	readByteResultCh chan byteResult
}

func newFramedMsgpackDecoder(decoderCh chan interface{}, decoderResultCh chan error, readByteCh chan struct{}, readByteResultCh chan byteResult) *framedMsgpackDecoder {
	return &framedMsgpackDecoder{
		decoderCh:        decoderCh,
		decoderResultCh:  decoderResultCh,
		readByteCh:       readByteCh,
		readByteResultCh: readByteResultCh,
	}
}

func (t *framedMsgpackDecoder) ReadByte() (byte, error) {
	t.readByteCh <- struct{}{}
	// See comment above regarding potential race
	byteRes := <-t.readByteResultCh
	return byteRes.b, byteRes.err
}

func (t *framedMsgpackDecoder) Decode(i interface{}) error {
	t.decoderCh <- i
	// See comment above regarding potential race
	return <-t.decoderResultCh
}
