package rpc

import (
	"context"
	"sync/atomic"
)

type task struct {
	seqid      SeqNumber
	cancelFunc context.CancelFunc
}

type receiver interface {
	Receive(rpcMessage) error
	Close() <-chan struct{}
}

type receiveHandler struct {
	writer      *framedMsgpackEncoder
	protHandler *protocolHandler

	// Stops all loops when closed.
	stopCh chan struct{}
	// Closed once taskLoop exits.
	closedCh chan struct{}

	// Task loop channels.
	taskBeginCh  chan *task
	taskCancelCh chan SeqNumber
	taskEndCh    chan SeqNumber

	// notifySeq generates unique negative task IDs for notify handlers in the
	// taskLoop map. Notify messages all carry SeqNo=-1 on the wire; without
	// unique IDs the taskLoop map key would collide and cancel funcs would leak.
	// Initialized to -1 so the first Add(-1) yields -2, avoiding the wire SeqNo
	// of -1 that a malicious peer could send as a cancel message.
	notifySeq atomic.Int64

	log LogInterface
}

func newReceiveHandler(enc *framedMsgpackEncoder, protHandler *protocolHandler,
	l LogInterface,
) *receiveHandler {
	r := &receiveHandler{
		writer:      enc,
		protHandler: protHandler,
		stopCh:      make(chan struct{}),
		closedCh:    make(chan struct{}),

		taskBeginCh:  make(chan *task),
		taskCancelCh: make(chan SeqNumber),
		taskEndCh:    make(chan SeqNumber),

		log: l,
	}
	r.notifySeq.Store(-1)
	go r.taskLoop()
	return r
}

func (r *receiveHandler) taskLoop() {
	tasks := make(map[SeqNumber]context.CancelFunc)
	for {
		select {
		case <-r.stopCh:
			for _, cancelFunc := range tasks {
				cancelFunc()
			}
			close(r.closedCh)
			return
		case t := <-r.taskBeginCh:
			tasks[t.seqid] = t.cancelFunc
		case seqid := <-r.taskCancelCh:
			if cancelFunc, ok := tasks[seqid]; ok {
				cancelFunc()
			}
			delete(tasks, seqid)
		case seqid := <-r.taskEndCh:
			if cancelFunc, ok := tasks[seqid]; ok {
				cancelFunc()
			}
			delete(tasks, seqid)
		}
	}
}

func (r *receiveHandler) Receive(rpc rpcMessage) error {
	switch message := rpc.(type) {
	case *rpcNotifyMessage:
		return r.receiveNotify(message)
	case *rpcCallMessage:
		return r.receiveCall(message)
	case *rpcResponseMessage:
		return r.receiveResponse(message)
	case *rpcCancelMessage:
		return r.receiveCancel(message)
	case *rpcCallCompressedMessage:
		return r.receiveCallCompressed(message)
	default:
		return NewReceiverError("invalid message type, %d", rpc.Type())
	}
}

func (r *receiveHandler) receiveNotify(rpc *rpcNotifyMessage) error {
	req := newNotifyRequest(rpc, r.log)
	return r.handleReceiveDispatch(req)
}

func (r *receiveHandler) receiveCall(rpc *rpcCallMessage) error {
	req := newCallRequest(rpc, r.log)
	return r.handleReceiveDispatch(req)
}

func (r *receiveHandler) receiveCallCompressed(rpc *rpcCallCompressedMessage) error {
	req := newCallCompressedRequest(rpc, r.log)
	return r.handleReceiveDispatch(req)
}

func (r *receiveHandler) receiveCancel(rpc *rpcCancelMessage) error {
	r.log.ServerCancelCall(rpc.SeqNo(), rpc.Name())
	select {
	case r.taskCancelCh <- rpc.SeqNo():
	case <-r.stopCh:
	}
	return nil
}

func (r *receiveHandler) handleReceiveDispatch(req request) error {
	if req.Err() != nil {
		req.LogInvocation(req.Err())
		return req.Reply(r.writer, nil, wrapError(r.protHandler.wef, req.Err()))
	}
	serveHandler, wrapErrorFunc, se := r.protHandler.findServeHandler(req.Name())
	if se != nil {
		req.LogInvocation(se)
		return req.Reply(r.writer, nil, wrapError(wrapErrorFunc, se))
	}
	// Compute the taskLoop key. Call messages use their SeqNo (always >= 0).
	// Notify messages all carry SeqNo=-1 on the wire; assign each a unique
	// negative ID (starting at -2) so their cancel funcs don't overwrite each
	// other in the map and don't alias a malicious cancel with SeqNo=-1.
	taskID := req.SeqNo()
	if taskID < 0 {
		taskID = SeqNumber(r.notifySeq.Add(-1))
	}
	select {
	case r.taskBeginCh <- &task{taskID, req.CancelFunc()}:
	case <-r.stopCh:
		req.CancelFunc()()
		return nil
	case <-req.Context().Done():
		req.CancelFunc()()
		return nil
	}
	go func() {
		req.Serve(r.writer, serveHandler, wrapErrorFunc)
		select {
		case r.taskEndCh <- taskID:
		case <-r.stopCh:
		}
	}()
	return nil
}

func (r *receiveHandler) receiveResponse(rpc *rpcResponseMessage) (err error) {
	callResponseCh := rpc.ResponseCh()

	if callResponseCh == nil {
		r.log.UnexpectedReply(rpc.SeqNo())
		return newCallNotFoundError(rpc.SeqNo())
	}

	callResponseCh <- rpc
	return nil
}

func (r *receiveHandler) Close() <-chan struct{} {
	close(r.stopCh)
	return r.closedCh
}
