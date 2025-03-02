package rpc

import (
	"context"
	"io"

	"github.com/foks-proj/go-ctxlog"
)

type dispatcher interface {
	Call(ctx context.Context, name Methoder, arg interface{}, res interface{},
		ctype CompressionType, u ErrorUnwrapper, sendNotifier SendNotifier) error
	Notify(ctx context.Context, name Methoder, arg interface{}, sendNotifier SendNotifier) error
	Close()
}

type dispatch struct {
	writer *framedMsgpackEncoder
	calls  *callContainer

	// Stops all loops when closed
	stopCh chan struct{}
	// Closed once all loops are finished
	closedCh chan struct{}

	instrumenterStorage NetworkInstrumenterStorage
	log                 LogInterface
}

func newDispatch(enc *framedMsgpackEncoder, calls *callContainer,
	l LogInterface, instrumenterStorage NetworkInstrumenterStorage) *dispatch {
	d := &dispatch{
		writer:   enc,
		calls:    calls,
		stopCh:   make(chan struct{}),
		closedCh: make(chan struct{}),

		log:                 l,
		instrumenterStorage: instrumenterStorage,
	}
	return d
}

func currySendNotifier(sendNotifier SendNotifier, seqid SeqNumber) func() {
	if sendNotifier == nil {
		return nil
	}
	return func() {
		sendNotifier(seqid)
	}
}

func (d *dispatch) Call(ctx context.Context, name Methoder, arg interface{}, res interface{},
	ctype CompressionType, u ErrorUnwrapper, sendNotifier SendNotifier) error {
	profiler := d.log.StartProfiler("call %s", name.String())
	defer profiler.Stop()

	var methodType MethodType
	switch ctype {
	case CompressionNone:
		methodType = name.CallMethodType()
	default:
		methodType = MethodCallCompressed
	}

	record := NewNetworkInstrumenter(d.instrumenterStorage, InstrumentTag(methodType, name.String()))
	c := d.calls.NewCall(ctx, name, arg, res, ctype, u, record)

	// Have to add call before encoding otherwise we'll race the response
	d.calls.AddCall(c)
	defer d.calls.RemoveCall(c.seqid)

	var v []interface{}
	var logCall func()
	switch ctype {
	case CompressionNone:
		v = []interface{}{methodType, c.seqid}
		v = c.method.appendForEncoding(v)
		v = append(v, c.arg)
		logCall = func() { d.log.ClientCall(c.seqid, c.method.String(), c.arg) }
	default:
		arg, err := d.writer.compressData(c.ctype, c.arg)
		if err != nil {
			return err
		}
		v = []interface{}{methodType, c.seqid, c.ctype}
		v = c.method.appendForEncoding(v)
		v = append(v, arg)
		logCall = func() { d.log.ClientCallCompressed(c.seqid, c.method.String(), c.arg, c.ctype) }
	}

	rpcTags, _ := ctxlog.TagsFromContext(ctx)
	if len(rpcTags) > 0 {
		v = append(v, rpcTags)
	}
	size, errCh := d.writer.EncodeAndWrite(ctx, v, currySendNotifier(sendNotifier, c.seqid))
	defer func() { _ = record.RecordAndFinish(ctx, size) }()

	// Wait for result from encode
	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
	case <-c.ctx.Done():
		return d.handleCancel(ctx, c)
	case <-d.stopCh:
		return io.EOF
	}

	logCall()

	// Wait for result from call
	select {
	case res := <-c.resultCh:
		d.log.ClientReply(c.seqid, c.method.String(), res.ResponseErr(), res.Res())
		return res.ResponseErr()
	case <-c.ctx.Done():
		return d.handleCancel(ctx, c)
	case <-d.stopCh:
		return io.EOF
	}
}

func (d *dispatch) Notify(ctx context.Context, name Methoder, arg interface{}, sendNotifier SendNotifier) error {
	rpcTags, _ := ctxlog.TagsFromContext(ctx)
	v := []interface{}{name.NotifyMethodType()}
	v = name.appendForEncoding(v)
	v = append(v, arg)
	if len(rpcTags) > 0 {
		v = append(v, rpcTags)
	}

	size, errCh := d.writer.EncodeAndWrite(ctx, v, currySendNotifier(sendNotifier, SeqNumber(-1)))
	record := NewNetworkInstrumenter(d.instrumenterStorage, InstrumentTag(MethodNotify, name.String()))
	defer func() { _ = record.RecordAndFinish(ctx, size) }()

	select {
	case err := <-errCh:
		if err == nil {
			d.log.ClientNotify(name.String(), arg)
		}
		return err
	case <-d.stopCh:
		return io.EOF
	case <-ctx.Done():
		d.log.ClientCancel(-1, name.String(), nil)
		return ctx.Err()
	}
}

func (d *dispatch) Close() {
	close(d.stopCh)
}

func (d *dispatch) handleCancel(ctx context.Context, c *call) error {
	d.log.ClientCancel(c.seqid, c.method.String(), nil)
	v := []interface{}{c.method.CancelMethodType(), c.seqid}
	v = c.method.appendForEncoding(v)
	size, errCh := d.writer.EncodeAndWriteAsync(v)
	record := NewNetworkInstrumenter(d.instrumenterStorage, InstrumentTag(MethodCancel, c.method.String()))
	defer func() { _ = record.RecordAndFinish(ctx, size) }()
	select {
	case err := <-errCh:
		if err != nil {
			d.log.Infow("error while dispatching cancellation", LogField{"err", err.Error()})
		}
	default:
		// Don't block on receiving the error from the Encode
	}
	return c.ctx.Err()
}
