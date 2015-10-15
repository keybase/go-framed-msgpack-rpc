package rpc

import (
	"golang.org/x/net/context"
)

type request interface {
	Message() *message
	Reply(encoder, LogInterface) error
	Serve(byteReadingDecoder, encoder, *ServeHandlerDescription, WrapErrorFunc, LogInterface) context.CancelFunc
	LogInvocation(log LogInterface, err error, arg interface{})
	LogCompletion(log LogInterface, err error)
}

type requestImpl struct {
	message
}

func (req *requestImpl) Message() *message {
	return &req.message
}

func (r *requestImpl) LogInvocation(LogInterface, error, interface{}) {}
func (r *requestImpl) LogCompletion(LogInterface, error)              {}
func (r *requestImpl) Reply(encoder, LogInterface) error              { return nil }
func (r *requestImpl) Serve(byteReadingDecoder, encoder, *ServeHandlerDescription, WrapErrorFunc, LogInterface) context.CancelFunc {
	return nil
}

func (req *requestImpl) getArg(receiver decoder, handler *ServeHandlerDescription) (interface{}, error) {
	arg := handler.MakeArg()
	err := decodeMessage(receiver, req.Message(), arg)
	return arg, err
}

type callRequest struct {
	requestImpl
}

func newCallRequest() *callRequest {
	r := &callRequest{
		requestImpl: requestImpl{
			message: message{
				remainingFields: 3,
			},
		},
	}
	r.decodeSlots = []interface{}{
		&r.seqno,
		&r.method,
	}
	return r
}

func (r *callRequest) LogInvocation(log LogInterface, err error, arg interface{}) {
	log.ServerCall(r.seqno, r.method, err, arg)
}

func (r *callRequest) LogCompletion(log LogInterface, err error) {
	log.ServerReply(r.seqno, r.method, err, r.res)
}

func (r *callRequest) Reply(enc encoder, log LogInterface) error {
	v := []interface{}{
		MethodResponse,
		r.seqno,
		r.err,
		r.res,
	}
	err := enc.Encode(v)
	if err != nil {
		log.Warning("Reply error for %d: %s", r.seqno, err.Error())
	}
	return err
}

func (r *callRequest) Serve(receiver byteReadingDecoder, transmitter encoder, handler *ServeHandlerDescription, wrapErrorFunc WrapErrorFunc, log LogInterface) context.CancelFunc {

	prof := log.StartProfiler("serve %s", r.method)
	arg, err := r.getArg(receiver, handler)
	ctx, cancelFunc := context.WithCancel(context.Background())

	go func() {
		r.LogInvocation(log, err, arg)
		if err != nil {
			r.err = wrapError(wrapErrorFunc, err)
		} else {
			res, err := handler.Handler(ctx, arg)
			r.err = wrapError(wrapErrorFunc, err)
			r.res = res
		}
		prof.Stop()
		r.LogCompletion(log, err)
		r.Reply(transmitter, log)
	}()
	return cancelFunc
}

type notifyRequest struct {
	requestImpl
}

func newNotifyRequest() *notifyRequest {
	r := &notifyRequest{
		requestImpl: requestImpl{
			message: message{
				remainingFields: 2,
			},
		},
	}
	r.decodeSlots = []interface{}{
		&r.method,
	}
	return r
}

func (r *notifyRequest) LogInvocation(log LogInterface, err error, arg interface{}) {
	log.ServerNotifyCall(r.method, err, arg)
}

func (r *notifyRequest) LogCompletion(log LogInterface, err error) {
	log.ServerNotifyComplete(r.method, err)
}

func (r *notifyRequest) Serve(receiver byteReadingDecoder, transmitter encoder, handler *ServeHandlerDescription, wrapErrorFunc WrapErrorFunc, log LogInterface) context.CancelFunc {

	prof := log.StartProfiler("serve-notify %s", r.method)
	arg, err := r.getArg(receiver, handler)
	ctx, cancelFunc := context.WithCancel(context.Background())

	go func() {
		r.LogInvocation(log, err, arg)
		if err == nil {
			_, err = handler.Handler(ctx, arg)
		}
		prof.Stop()
		r.LogCompletion(log, err)
	}()
	return cancelFunc
}

type cancelRequest struct {
	requestImpl
}

func newCancelRequest() *cancelRequest {
	r := &cancelRequest{
		requestImpl: requestImpl{
			message: message{
				remainingFields: 2,
			},
		},
	}
	r.decodeSlots = []interface{}{
		&r.seqno,
		&r.method,
	}
	return r
}

func (r *cancelRequest) LogInvocation(log LogInterface, err error, arg interface{}) {
	log.ServerCancelCall(r.seqno, r.method)
}

func newRequest(methodType MethodType) request {
	switch methodType {
	case MethodCall:
		return newCallRequest()
	case MethodNotify:
		return newNotifyRequest()
	case MethodCancel:
		return newCancelRequest()
	}
	return nil
}
