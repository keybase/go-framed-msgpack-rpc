package rpc

import (
	"bufio"
	"io"
	"net"

	"github.com/keybase/go-codec/codec"
)

type WrapErrorFunc func(error) interface{}

type Transporter interface {
	getDispatcher() (dispatcher, error)
	getReceiver() (receiver, error)

	// Done returns a channel that's closed when incoming frames
	// have finished processing, either due to an error or the
	// underlying connection being closed. Successive calls to
	// Done return the same value. This is safe to call before
	// incoming frames have started processing (i.e., before
	// ReceiveFramesAsync() has been called).
	Done() <-chan struct{}

	// Err returns a non-nil error value after Done is closed.
	// After Done is closed, successive calls to Err return the
	// same value.
	Err() error

	// IsConnected returns false when incoming packets have
	// finished processing. This is exactly equivalent to checking
	// if Done() is closed.
	//
	// TODO: Use a better name.
	IsConnected() bool

	// ReceiveFramesAsync starts processing incoming frames in a
	// background goroutine, if it's not already happening.
	ReceiveFramesAsync()

	RegisterProtocol(p Protocol) error
}

type connDecoder struct {
	decoder
	net.Conn
	Reader *bufio.Reader
}

func newConnDecoder(c net.Conn) *connDecoder {
	br := bufio.NewReader(c)
	mh := &codec.MsgpackHandle{WriteExt: true}

	return &connDecoder{
		Conn:    c,
		Reader:  br,
		decoder: codec.NewDecoder(br, mh),
	}
}

var _ Transporter = (*transport)(nil)

type transport struct {
	cdec       *connDecoder
	enc        *framedMsgpackEncoder
	dispatcher dispatcher
	receiver   receiver
	packetizer packetizer
	protocols  *protocolHandler
	calls      *callContainer
	log        LogInterface
	startCh    chan struct{}
	stopCh     chan struct{}

	// Filled in right before stopCh is closed.
	stopErr error
}

func NewTransport(c net.Conn, l LogFactory, wef WrapErrorFunc) Transporter {
	cdec := newConnDecoder(c)
	if l == nil {
		l = NewSimpleLogFactory(nil, nil)
	}
	log := l.NewLog(cdec.RemoteAddr())

	startCh := make(chan struct{}, 1)
	startCh <- struct{}{}

	ret := &transport{
		cdec:      cdec,
		log:       log,
		startCh:   startCh,
		stopCh:    make(chan struct{}),
		protocols: newProtocolHandler(wef),
		calls:     newCallContainer(),
	}
	enc := newFramedMsgpackEncoder(ret.cdec)
	ret.enc = enc
	ret.dispatcher = newDispatch(enc, ret.calls, log)
	ret.receiver = newReceiveHandler(enc, ret.protocols, log)
	ret.packetizer = newPacketHandler(cdec.Reader, ret.protocols, ret.calls)
	return ret
}

func (t *transport) Done() <-chan struct{} {
	return t.stopCh
}

func (t *transport) Err() error {
	select {
	case <-t.stopCh:
		return t.stopErr
	default:
		return nil
	}
}

func (t *transport) IsConnected() bool {
	select {
	case <-t.stopCh:
		return false
	default:
		return true
	}
}

func (t *transport) ReceiveFramesAsync() {
	select {
	case <-t.startCh:
		// First time calling Run() -- proceed.
	default:
		// Second time calling run -- do nothing.
		return
	}

	go func() {
		t.receiveFrames()
	}()
}

func (t *transport) receiveFrames() {
	// Packetize: do work
	var err error
	for shouldContinue(err) {
		var rpc rpcMessage
		if rpc, err = t.packetizer.NextFrame(); shouldReceive(rpc) {
			t.receiver.Receive(rpc)
		}
	}

	// Log packetizer completion
	t.log.TransportError(err)

	// This must happen before stopCh is closed to have a correct
	// ordering.
	t.stopErr = err

	// Since the receiver might require the transport, we have to
	// close it before terminating our loops
	close(t.stopCh)
	t.dispatcher.Close()
	<-t.receiver.Close()

	// First inform the encoder that it should close
	encoderClosed := t.enc.Close()
	// Unblock any remaining writes
	t.cdec.Close()
	// Wait for the encoder to finish handling the now unblocked writes
	<-encoderClosed
}

func (t *transport) getDispatcher() (dispatcher, error) {
	if !t.IsConnected() {
		return nil, io.EOF
	}
	return t.dispatcher, nil
}

func (t *transport) getReceiver() (receiver, error) {
	if !t.IsConnected() {
		return nil, io.EOF
	}
	return t.receiver, nil
}

func (t *transport) RegisterProtocol(p Protocol) error {
	return t.protocols.registerProtocol(p)
}

func shouldContinue(err error) bool {
	err = unboxRPCError(err)
	switch err.(type) {
	case nil:
		return true
	case CallNotFoundError:
		return true
	case MethodNotFoundError:
		return true
	case ProtocolNotFoundError:
		return true
	default:
		return false
	}
}

func shouldReceive(rpc rpcMessage) bool {
	if rpc == nil {
		return false
	}
	switch rpc.Err().(type) {
	case nil:
		return true
	case MethodNotFoundError:
		return true
	case ProtocolNotFoundError:
		return true
	default:
		return false
	}
}
