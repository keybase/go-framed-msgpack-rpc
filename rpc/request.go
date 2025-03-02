package rpc

import (
	"context"
)

type request interface {
	rpcMessage
	CancelFunc() context.CancelFunc
	Reply(*framedMsgpackEncoder, interface{}, interface{}) error
	Serve(*framedMsgpackEncoder, *ServeHandlerDescription, WrapErrorFunc)
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
	r.log.ServerCall(r.SeqNo(), r.Name().String(), err, r.Arg())
}

func (r *callRequest) LogCompletion(res interface{}, err error) {
	r.log.ServerReply(r.SeqNo(), r.Name().String(), err, res)
}

func (r *callRequest) Reply(enc *framedMsgpackEncoder, res interface{}, errArg interface{}) (err error) {
	v := []interface{}{
		MethodResponse,
		r.SeqNo(),
		errArg,
		res,
	}

	size, errCh := enc.EncodeAndWrite(r.ctx, v, nil)
	defer func() { _ = r.RecordAndFinish(r.ctx, size) }()

	select {
	case err := <-errCh:
		if err != nil {
			r.log.Warnw("reply error",
				LogField{"seqno", r.SeqNo()},
				LogField{"err", err.Error()},
			)
		}
	case <-r.ctx.Done():
		r.log.Infow("call canceled after reply sent", LogField{"seqno", r.SeqNo()})
	}
	return err
}

func (r *callRequest) Serve(transmitter *framedMsgpackEncoder, handler *ServeHandlerDescription, wrapErrorFunc WrapErrorFunc) {

	prof := r.log.StartProfiler("serve %s", r.Name())
	arg := r.Arg()

	r.LogInvocation(nil)
	res, err := handler.Handler(r.ctx, arg)
	prof.Stop()
	r.LogCompletion(res, err)

	if err := r.Reply(transmitter, res, wrapError(wrapErrorFunc, err)); err != nil {
		r.log.Infow("Unable to reply", LogField{"err", err})
	}
}

type callCompressedRequest struct {
	*rpcCallCompressedMessage
	requestImpl
}

func newCallCompressedRequest(rpc *rpcCallCompressedMessage, log LogInterface) *callCompressedRequest {
	ctx, cancel := context.WithCancel(rpc.Context())
	return &callCompressedRequest{
		rpcCallCompressedMessage: rpc,
		requestImpl: requestImpl{
			ctx:        ctx,
			cancelFunc: cancel,
			log:        log,
		},
	}
}

func (r *callCompressedRequest) LogInvocation(err error) {
	r.log.ServerCallCompressed(r.SeqNo(), r.Name().String(), err, r.Arg(), r.Compression())
}

func (r *callCompressedRequest) LogCompletion(res interface{}, err error) {
	r.log.ServerReplyCompressed(r.SeqNo(), r.Name().String(), err, res, r.Compression())
}

func (r *callCompressedRequest) Reply(enc *framedMsgpackEncoder, res interface{}, errArg interface{}) (err error) {
	res, err = enc.compressData(r.Compression(), res)
	if err != nil {
		return err
	}
	v := []interface{}{
		MethodResponse,
		r.SeqNo(),
		errArg,
		res,
	}

	size, errCh := enc.EncodeAndWrite(r.ctx, v, nil)
	defer func() { _ = r.RecordAndFinish(r.ctx, size) }()

	select {
	case err := <-errCh:
		if err != nil {
			r.log.Warnw("reply error", LogField{"seqno", r.SeqNo()}, LogField{"err", err})
		}
	case <-r.ctx.Done():
		r.log.Infow("call canceled after reply sent", LogField{"seqno", r.SeqNo()})
	}
	return err
}

func (r *callCompressedRequest) Serve(transmitter *framedMsgpackEncoder, handler *ServeHandlerDescription, wrapErrorFunc WrapErrorFunc) {

	prof := r.log.StartProfiler("serve-compressed %s", r.Name())
	arg := r.Arg()

	r.LogInvocation(nil)
	res, err := handler.Handler(r.ctx, arg)
	prof.Stop()
	r.LogCompletion(res, err)

	if err := r.Reply(transmitter, res, wrapError(wrapErrorFunc, err)); err != nil {
		r.log.Infow("unable to reply", LogField{"err", err})
	}
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
	r.log.ServerNotifyCall(r.Name().String(), err, r.Arg())
}

func (r *notifyRequest) LogCompletion(_ interface{}, err error) {
	r.log.ServerNotifyComplete(r.Name().String(), err)
}

func (r *notifyRequest) Serve(_ *framedMsgpackEncoder, handler *ServeHandlerDescription, _ WrapErrorFunc) {

	prof := r.log.StartProfiler("serve-notify %s", r.Name())
	arg := r.Arg()

	r.LogInvocation(nil)
	_, err := handler.Handler(r.ctx, arg)
	prof.Stop()
	r.LogCompletion(nil, err)
}

func (r *notifyRequest) Reply(_ *framedMsgpackEncoder, _ interface{}, _ interface{}) (err error) {
	return nil
}
