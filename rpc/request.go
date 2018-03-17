package rpc

import (
	"golang.org/x/net/context"
	"reflect"
)

type request interface {
	rpcMessage
	CancelFunc() context.CancelFunc
	Reply(encoder, interface{}, interface{}) error
	Serve(encoder, *ServeHandlerDescription, WrapErrorFunc)
	LogInvocation(err error)
	LogCompletion(res interface{}, err error)
}

type requestImpl struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	log        LogInterface
}

func (req *requestImpl) CancelFunc() context.CancelFunc {
	return req.cancelFunc
}

type callRequest struct {
	*rpcCallMessage
	requestImpl
}

func newCallRequest(rpc *rpcCallMessage, log LogInterface) *callRequest {
	ctx, cancel := context.WithCancel(rpc.Context())
	return &callRequest{
		rpcCallMessage: rpc,
		requestImpl: requestImpl{
			ctx:        ctx,
			cancelFunc: cancel,
			log:        log,
		},
	}
}

func (r *callRequest) LogInvocation(err error) {
	r.log.ServerCall(r.SeqNo(), r.Name(), err, r.Arg())
}

func (r *callRequest) LogCompletion(res interface{}, err error) {
	r.log.ServerReply(r.SeqNo(), r.Name(), err, res)
}

func (r *callRequest) Reply(enc encoder, res interface{}, errArg interface{}) (err error) {
	v := []interface{}{
		MethodResponse,
		r.SeqNo(),
		errArg,
		res,
	}
	errCh := enc.EncodeAndWrite(r.ctx, v)
	select {
	case err := <-errCh:
		if err != nil {
			r.log.Warning("Reply error for %d: %s", r.SeqNo(), err.Error())
		}
	case <-r.ctx.Done():
		r.log.Info("Call canceled after reply sent. Seq: %d", r.SeqNo())
	}
	return err
}

func (r *callRequest) passSeqNumber(arg interface{}) {

	// Recall that if the RPC takes an argument of type Foo,
	// we'll have arg of type *[]Foo, but expressed as an interface{}
	// So we need to access (*arg)[0] via type reflection magic.

	// Nothing to do if our argument is `nil`
	if arg == nil {
		return
	}

	// Convert from an interface{} to a reflect.Value
	v := reflect.ValueOf(arg)
	if v.IsNil() {
		return
	}

	// Take *arg to get an Array or Slice.
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return
	}

	// If the Slice (or Array) has less than 1 element, then no reason to go further.
	if v.Len() < 1 {
		return
	}

	// Take the 0th element of the array, then take a pointer to that value,
	// and the convert to an interface.
	tmp := v.Index(0).Addr().Interface()

	// Now we can check (*arg)[0] to see if it's of type SeqNumberReceiver.
	// If so, then we send it the sequence number of the current RPC.
	snr, ok := (tmp).(SeqNumberReceiver)
	if !ok {
		return
	}
	snr.SetSeqNumber(r.SeqNo())
}

func (r *callRequest) Serve(transmitter encoder, handler *ServeHandlerDescription, wrapErrorFunc WrapErrorFunc) {

	prof := r.log.StartProfiler("serve %s", r.Name())
	arg := r.Arg()
	r.passSeqNumber(arg)

	r.LogInvocation(nil)
	res, err := handler.Handler(r.ctx, arg)
	prof.Stop()
	r.LogCompletion(res, err)

	r.Reply(transmitter, res, wrapError(wrapErrorFunc, err))
}

type notifyRequest struct {
	*rpcNotifyMessage
	requestImpl
}

func newNotifyRequest(rpc *rpcNotifyMessage, log LogInterface) *notifyRequest {
	ctx, cancel := context.WithCancel(rpc.Context())
	return &notifyRequest{
		rpcNotifyMessage: rpc,
		requestImpl: requestImpl{
			ctx:        ctx,
			cancelFunc: cancel,
			log:        log,
		},
	}
}

func (r *notifyRequest) LogInvocation(err error) {
	r.log.ServerNotifyCall(r.Name(), err, r.Arg())
}

func (r *notifyRequest) LogCompletion(_ interface{}, err error) {
	r.log.ServerNotifyComplete(r.Name(), err)
}

func (r *notifyRequest) Serve(transmitter encoder, handler *ServeHandlerDescription, wrapErrorFunc WrapErrorFunc) {

	prof := r.log.StartProfiler("serve-notify %s", r.Name())
	arg := r.Arg()

	r.LogInvocation(nil)
	_, err := handler.Handler(r.ctx, arg)
	prof.Stop()
	r.LogCompletion(nil, err)
}

func (r *notifyRequest) Reply(enc encoder, res interface{}, errArg interface{}) (err error) {
	return nil
}
