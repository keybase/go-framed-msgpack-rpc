package rpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

type ServeHandlerDescription struct {
	MakeArg func() interface{}
	Handler func(ctx context.Context, arg interface{}) (ret interface{}, err error)
}

type MethodType int

const (
	MethodInvalid        MethodType = -1
	MethodCall           MethodType = 0
	MethodResponse       MethodType = 1
	MethodNotify         MethodType = 2
	MethodCancel         MethodType = 3
	MethodCallCompressed MethodType = 4

	// V2 of Call and Notify use a more succint naming scheme. Methods are specified as [<unique-protocol-id>,<method-id>],
	// where the unique protocol ID is a random-ish 32-bit, 48-bit or 64-bit unsigned integer. The method-id is sequetial
	// within the protocol and will be almost always an unsigned 8-bit.
	MethodCallV2   MethodType = 5
	MethodNotifyV2 MethodType = 6
	MethodCancelV2 MethodType = 7
)

func (t MethodType) String() string {
	switch t {
	case MethodInvalid:
		return "Invalid"
	case MethodCall:
		return "Call"
	case MethodResponse:
		return "Response"
	case MethodNotify:
		return "Notify"
	case MethodCancel:
		return "Cancel"
	case MethodCallCompressed:
		return "CallCompressed"
	case MethodCallV2:
		return "Call2"
	case MethodNotifyV2:
		return "Notify2"
	default:
		return fmt.Sprintf("Method(%d)", t)
	}
}

type CompressionType int

const (
	CompressionNone       CompressionType = 0
	CompressionGzip       CompressionType = 1
	CompressionMsgpackzip CompressionType = 2
)

func (t CompressionType) String() string {
	switch t {
	case CompressionNone:
		return "none"
	case CompressionGzip:
		return "gzip"
	case CompressionMsgpackzip:
		return "msgpackzip"
	default:
		return fmt.Sprintf("Compression(%d)", t)
	}
}

func (t CompressionType) NewCompressor() compressor {
	switch t {
	case CompressionGzip:
		return newGzipCompressor()
	case CompressionMsgpackzip:
		return newMsgpackzipCompressor()
	default:
		return nil
	}
}

type ErrorUnwrapper interface {
	MakeArg() interface{}
	UnwrapError(arg interface{}) (appError error, dispatchError error)
}

type Protocol struct {
	Name      string
	Methods   map[string]ServeHandlerDescription
	WrapError WrapErrorFunc
}

type ProtocolV2 struct {
	Name      string
	ID        ProtocolUniqueID
	Methods   map[Position]ServeHandlerDescriptionV2
	WrapError WrapErrorFunc
}

type ServeHandlerDescriptionV2 struct {
	ServeHandlerDescription
	Name string
}

type protocolMap map[string]Protocol
type protocolMapV2 map[ProtocolUniqueID]ProtocolV2

type SeqNumber int

type protocolHandler struct {
	wef       WrapErrorFunc
	mtx       sync.RWMutex
	protocols protocolMap
	killWith  error
}

type protocolHandlerV2 struct {
	wef       WrapErrorFunc
	mtx       sync.RWMutex
	protocols protocolMapV2
	killWith  error
}

type protocolHandlers struct {
	v1 *protocolHandler
	v2 *protocolHandlerV2
}

func (p protocolHandlers) killIncoming(err error) {
	if p.v1 != nil {
		p.v1.killIncoming(err)
	}
	if p.v2 != nil {
		p.v2.killIncoming(err)
	}
}

func newProtocolHandler(wef WrapErrorFunc) *protocolHandler {
	return &protocolHandler{
		wef:       wef,
		protocols: make(protocolMap),
	}
}

func newProtocolV2Handler(wef WrapErrorFunc) *protocolHandlerV2 {
	return &protocolHandlerV2{
		wef:       wef,
		protocols: make(protocolMapV2),
	}
}

func (h *protocolHandler) killIncoming(err error) {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	h.killWith = err
}

func (h *protocolHandler) registerProtocol(p Protocol) error {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	if _, found := h.protocols[p.Name]; found {
		return newAlreadyRegisteredError(p.Name)
	}
	h.protocols[p.Name] = p
	return nil
}

func (h *protocolHandlerV2) registerProtocol(p ProtocolV2) error {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	if _, found := h.protocols[p.ID]; found {
		return newAlreadyRegisteredV2Error(p.ID, p.Name)
	}
	h.protocols[p.ID] = p
	return nil
}

func (h *protocolHandlerV2) killIncoming(err error) {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	h.killWith = err
}

func (h *protocolHandler) findServeHandler(method Methoder) (*ServeHandlerDescription, WrapErrorFunc, error) {
	h.mtx.RLock()
	defer h.mtx.RUnlock()
	name := method.String()

	p, m := splitMethodName(name)
	prot, found := h.protocols[p]
	if !found {
		return nil, h.wef, newProtocolNotFoundError(p)
	}
	srv, found := prot.Methods[m]
	if !found {
		return nil, h.wef, newMethodNotFoundError(p, m)
	}
	if h.killWith != nil {
		srv.Handler = func(_ context.Context, _ interface{}) (ret interface{}, err error) {
			return nil, h.killWith
		}
	}
	return &srv, prot.WrapError, nil
}

func (h *protocolHandlerV2) findServeHandler(meth MethodV2) (*ServeHandlerDescriptionV2, WrapErrorFunc, error) {
	h.mtx.RLock()
	defer h.mtx.RUnlock()
	prot, found := h.protocols[meth.puid]
	if !found {
		return nil, h.wef, NewProtocolV2NotFoundError(meth.puid)
	}
	srv, found := prot.Methods[meth.method]
	if !found {
		return nil, h.wef, NewMethodV2NotFoundError(meth.puid, meth.method, prot.Name)
	}
	if h.killWith != nil {
		srv.Handler = func(_ context.Context, _ interface{}) (ret interface{}, err error) {
			return nil, h.killWith
		}
	}
	return &srv, prot.WrapError, nil
}

func (h *protocolHandler) getArg(name Methoder) (interface{}, error) {
	handler, _, err := h.findServeHandler(name)
	if err != nil {
		return nil, err
	}
	return handler.MakeArg(), nil
}

func (h *protocolHandlerV2) getArg(meth MethodV2) (interface{}, error) {
	handler, _, err := h.findServeHandler(meth)
	if err != nil {
		return nil, err
	}
	return handler.MakeArg(), nil
}

type ProtocolUniqueID uint64
type TypeUniqueID uint64
type Position uint64

func (t TypeUniqueID) EncodeToBytes(b []byte) {
	binary.BigEndian.PutUint64(b, uint64(t))
}
func (t TypeUniqueID) Encode(w io.Writer) error {
	var b [8]byte
	t.EncodeToBytes(b[:])
	n, err := w.Write(b[:])
	if n != 8 {
		return errors.New("short buffer write")
	}
	return err
}

type MethodV1 struct {
	s string
}

func (m *MethodV1) String() string { return m.s }
func (m *MethodV1) appendForEncoding(v []interface{}) []interface{} {
	return append(v, m.s)
}
func (m *MethodV1) decodeInto(d *fieldDecoder) error {
	return d.Decode(&m.s)
}

func (m *MethodV1) getArg(p protocolHandlers) (interface{}, error) {
	return p.v1.getArg(m)
}
func (m *MethodV1) findServeHandler(p protocolHandlers) (*ServeHandlerDescription, WrapErrorFunc, error) {
	return p.v1.findServeHandler(m)
}

func (m *MethodV1) numFields() int { return 1 }

var _ Methoder = (*MethodV1)(nil)

func newMethodV1(s string) *MethodV1 {
	return &MethodV1{s: s}
}

type Methoder interface {
	String() string
	appendForEncoding(v []interface{}) []interface{}
	decodeInto(d *fieldDecoder) error
	CallMethodType() MethodType
	CancelMethodType() MethodType
	NotifyMethodType() MethodType
	getArg(p protocolHandlers) (interface{}, error)
	findServeHandler(p protocolHandlers) (*ServeHandlerDescription, WrapErrorFunc, error)
	numFields() int
}

type MethodV2 struct {
	puid   ProtocolUniqueID
	method Position
	name   string
}

func NewMethodV2(p ProtocolUniqueID, m Position, n string) *MethodV2 {
	return &MethodV2{puid: p, method: m, name: n}
}

func (m *MethodV2) String() string {
	return m.name
}

func (m *MethodV2) numFields() int { return 2 }

func (m *MethodV1) CallMethodType() MethodType   { return MethodCall }
func (m *MethodV2) CallMethodType() MethodType   { return MethodCallV2 }
func (m *MethodV1) CancelMethodType() MethodType { return MethodCancel }
func (m *MethodV2) CancelMethodType() MethodType { return MethodCancelV2 }
func (m *MethodV1) NotifyMethodType() MethodType { return MethodNotify }
func (m *MethodV2) NotifyMethodType() MethodType { return MethodNotifyV2 }

func (m *MethodV2) appendForEncoding(v []interface{}) []interface{} {
	return append(v, m.puid, m.method)
}

func (m *MethodV2) decodeInto(d *fieldDecoder) error {
	if err := d.Decode(&m.puid); err != nil {
		return err
	}
	if err := d.Decode(&m.method); err != nil {
		return err
	}
	return nil
}

func (m *MethodV2) getArg(p protocolHandlers) (interface{}, error) {
	return p.v2.getArg(*m)
}

func (m *MethodV2) findServeHandler(p protocolHandlers) (*ServeHandlerDescription, WrapErrorFunc, error) {
	se, wef, err := p.v2.findServeHandler(*m)
	if err != nil {
		return nil, nil, err
	}
	return &se.ServeHandlerDescription, wef, nil
}

var _ Methoder = (*MethodV2)(nil)

type Encoder interface {
	Encode(interface{}) error
}

type Decoder interface {
	Decode(interface{}) error
}

type Decodable interface {
	Decode(Decoder) error
}

type DecoderFactory interface {
	NewDecoderBytes(interface{}, []byte) Decoder
}

type EncoderFactory interface {
	NewEncoderBytes(out *[]byte) Encoder
}
