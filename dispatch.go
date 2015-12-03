package rpc

import (
	"fmt"
	"io"

	"golang.org/x/net/context"
)

type dispatcher interface {
	Call(ctx context.Context, name string, arg interface{}, res interface{}, u ErrorUnwrapper) error
	Notify(ctx context.Context, name string, arg interface{}) error
	Close(err error) chan struct{}
}

type dispatch struct {
	writer encoder
	reader byteReadingDecoder

	seqid seqNumber

	// Stops all loops when closed
	stopCh chan struct{}
	// Closed once all loops are finished
	closedCh chan struct{}

	callCh     chan *call
	callRespCh chan *call
	rmCallCh   chan callRetrieval

	// Task loop channels
	taskBeginCh  chan *task
	taskCancelCh chan int
	taskEndCh    chan int

	log LogInterface
}

func newDispatch(enc encoder, dec byteReadingDecoder, callRetrievalCh chan callRetrieval, l LogInterface) *dispatch {
	d := &dispatch{
		writer:     enc,
		reader:     dec,
		callCh:     make(chan *call),
		callRespCh: make(chan *call),
		rmCallCh:   callRetrievalCh,
		stopCh:     make(chan struct{}),
		closedCh:   make(chan struct{}),

		taskBeginCh:  make(chan *task),
		taskCancelCh: make(chan int),
		taskEndCh:    make(chan int),

		seqid: 0,
		log:   l,
	}
	go d.callLoop()
	return d
}

type call struct {
	ctx context.Context

	// resultCh serializes the possible results generated for this
	// call, with the first one becoming the true result.
	resultCh chan error

	// doneCh is closed when the true result for this call is
	// chosen.
	doneCh chan struct{}

	method         string
	seqid          seqNumber
	arg            interface{}
	res            interface{}
	errorUnwrapper ErrorUnwrapper
	profiler       Profiler
}

func newCall(ctx context.Context, m string, arg interface{}, res interface{}, u ErrorUnwrapper, p Profiler) *call {
	return &call{
		ctx:            ctx,
		resultCh:       make(chan error),
		doneCh:         make(chan struct{}),
		method:         m,
		arg:            arg,
		res:            res,
		errorUnwrapper: u,
		profiler:       p,
	}
}

// Finish tries to set the given error as the result of this call, and
// returns whether or not this was successful.
func (c *call) Finish(err error) bool {
	select {
	case c.resultCh <- err:
		close(c.doneCh)
		return true
	case <-c.doneCh:
		return false
	}
}

func (d *dispatch) callLoop() {
	calls := make(map[seqNumber]*call)
	for {
		select {
		case <-d.stopCh:
			for _, c := range calls {
				_ = c.Finish(io.EOF)
			}
			close(d.closedCh)
			return
		case c := <-d.callCh:
			d.handleCall(calls, c)
		case cr := <-d.rmCallCh:
			call := calls[cr.seqid]
			delete(calls, cr.seqid)
			cr.ch <- call
		}
	}
}

func (d *dispatch) handleCallCancel(c *call, err error) bool {
	// TODO: Make err a part of the error passed to c.Finish.
	// Return the call
	setResult := c.Finish(newCanceledError(c.method, c.seqid))
	if !setResult {
		return false
	}

	// Dispatch cancellation
	cancelErrCh := d.writer.Encode([]interface{}{MethodCancel, c.seqid, c.method})

	// TODO: Make a single goroutine to handle this.
	go func() {
		err := <-cancelErrCh
		d.log.ClientCancel(c.seqid, c.method, err)
	}()

	return true
}

func (d *dispatch) handleCall(calls map[seqNumber]*call, c *call) {
	c.seqid = d.nextSeqid()

	errCh := d.dispatchMessage(c.ctx, MethodCall, c.seqid, c.method, c.arg)
	d.log.ClientCall(c.seqid, c.method, c.arg)
	d.log.Info("client call")

	// Wait for a cancel or encoding result
	select {
	case <-c.ctx.Done():
		setResult := d.handleCallCancel(c, c.ctx.Err())
		if !setResult {
			panic(fmt.Sprintf("d.handleCallCancel unexpectedly returned false for method=%s, seqid=%d", c.method, c.seqid))
		}
		return

	case err := <-errCh:
		if err != nil {
			setResult := c.Finish(err)
			if !setResult {
				panic(fmt.Sprintf("c.Finish unexpectedly returned false for method=%s, seqid=%d", c.method, c.seqid))
			}
			return
		}
	}

	calls[c.seqid] = c

	go func() {
		// Wait for a cancel or response
		select {
		case <-c.ctx.Done():
			setResult := d.handleCallCancel(c, c.ctx.Err())
			if !setResult {
				d.log.Info("call has already been processed: method=%s, seqid=%d", c.method, c.seqid)
				return
			}

			// Ensure the call is removed
			ch := make(chan *call)
			d.rmCallCh <- callRetrieval{c.seqid, ch}
			<-ch
		case <-c.doneCh:
		}
	}()
}

func (d *dispatch) nextSeqid() seqNumber {
	ret := d.seqid
	d.seqid++
	return ret
}

func (d *dispatch) Call(ctx context.Context, name string, arg interface{}, res interface{}, u ErrorUnwrapper) error {
	profiler := d.log.StartProfiler("call %s", name)
	call := newCall(ctx, name, arg, res, u, profiler)
	d.callCh <- call
	return <-call.resultCh
}

func (d *dispatch) Notify(ctx context.Context, name string, arg interface{}) error {
	errCh := d.dispatchMessage(ctx, MethodNotify, name, arg)
	select {
	case err := <-errCh:
		d.log.ClientNotify(name, err, arg)
		return err
	case <-ctx.Done():
		d.log.ClientCancel(-1, name, nil)
	}
	return nil
}

func (d *dispatch) Close(err error) chan struct{} {
	close(d.stopCh)
	return d.closedCh
}

func (d *dispatch) dispatchMessage(ctx context.Context, args ...interface{}) <-chan error {
	rpcTags, _ := RpcTagsFromContext(ctx)
	return d.writer.Encode(append(args, rpcTags))
}

func wrapError(f WrapErrorFunc, e error) interface{} {
	if f != nil {
		return f(e)
	}
	if e == nil {
		return nil
	}
	return e.Error()
}
