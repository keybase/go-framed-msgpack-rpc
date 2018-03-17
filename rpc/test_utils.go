package rpc

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"golang.org/x/net/context"
)

type server struct {
	port int
	prot *testProtocol
}

func (s *server) Run(ready chan struct{}, externalListener chan error) (err error) {
	var listener net.Listener
	o := SimpleLogOutput{}
	lf := NewSimpleLogFactory(o, nil)
	o.Info(fmt.Sprintf("Listening on port %d...", s.port))
	if listener, err = net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", s.port)); err != nil {
		return
	}
	close(ready)
	for {
		var c net.Conn
		if c, err = listener.Accept(); err != nil {
			externalListener <- io.EOF
			return err
		}
		xp := NewTransport(c, lf, nil)
		srv := NewServer(xp, nil)
		s.prot = newTestProtocol(c)
		srv.Register(createTestProtocol(s.prot))
		done := srv.Run()
		go func() {
			<-done
			listener.Close()
		}()
	}
}

type testProtocol struct {
	c              net.Conn
	constants      Constants
	longCallResult int
	debugTags      CtxRpcTags
	notifyCh       chan struct{}
	lastSeqNumber      SeqNumber
}

func newTestProtocol(c net.Conn) *testProtocol {
	return &testProtocol{
		c:              c,
		constants:      Constants{},
		longCallResult: 0,
		notifyCh:       make(chan struct{}, 1),
	}
}

func (a *testProtocol) Add(_ context.Context, args AddArgs) (ret AddRes, err error) {
	ret.I = args.A + args.B
	a.lastSeqNumber = args.seqno
	return
}

func (a *testProtocol) UpdateConstants(args *Constants) error {
	a.constants = *args
	close(a.notifyCh)
	return nil
}

func (a *testProtocol) GetConstants() (*Constants, error) {
	<-a.notifyCh
	return &a.constants, nil
}

func (a *testProtocol) LongCall(ctx context.Context) (int, error) {
	defer func() {
		close(a.notifyCh)
	}()
	tags, _ := RpcTagsFromContext(ctx)
	a.debugTags = tags
	a.longCallResult = 0
	for i := 0; i < 100; i++ {
		select {
		case <-time.After(time.Millisecond):
			a.longCallResult++
		case <-ctx.Done():
			a.longCallResult = -1
			// There is no way to get this value out right now
			return a.longCallResult, errors.New("terminated")
		}
	}
	return a.longCallResult, nil
}

func (a *testProtocol) LongCallResult(ctx context.Context) (int, error) {
	<-a.notifyCh
	return a.longCallResult, nil
}

func (a *testProtocol) LongCallDebugTags(ctx context.Context) (CtxRpcTags, error) {
	return a.debugTags, nil
}

//---------------------------------------------------------------
// begin autogen code

type AddArgs struct {
	A int
	B int
	seqno SeqNumber
}

func (a *AddArgs) SetSeqNumber(s SeqNumber) {
	a.seqno = s
}

type AddRes struct {
	I int
	seqno SeqNumber
}

func (a *AddRes) SetSeqNumber(s SeqNumber) {
	a.seqno = s
}

type Constants struct {
	Pi int
}

type TestInterface interface {
	Add(context.Context, AddArgs) (AddRes, error)
	UpdateConstants(*Constants) error
	GetConstants() (*Constants, error)
	LongCall(context.Context) (int, error)
	LongCallResult(context.Context) (int, error)
	LongCallDebugTags(context.Context) (CtxRpcTags, error)
}

func createTestProtocol(i TestInterface) Protocol {
	return Protocol{
		Name: "test.1.testp",
		Methods: map[string]ServeHandlerDescription{
			"add": {
				MakeArg: func() interface{} {
					ret := make([]AddArgs, 1)
					return &ret
				},
				Handler: func(ctx context.Context, args interface{}) (interface{}, error) {
					typedArgs, ok := args.(*[]AddArgs)
					if !ok {
						return nil, NewTypeError((*[]AddArgs)(nil), args)
					}
					return i.Add(ctx, (*typedArgs)[0])
				},
				MethodType: MethodCall,
			},
			"GetConstants": {
				MakeArg: func() interface{} {
					return new(interface{})
				},
				Handler: func(_ context.Context, _ interface{}) (interface{}, error) {
					return i.GetConstants()
				},
				MethodType: MethodCall,
			},
			"updateConstants": {
				MakeArg: func() interface{} {
					return new(Constants)
				},
				Handler: func(_ context.Context, args interface{}) (interface{}, error) {
					constants, ok := args.(*Constants)
					if !ok {
						return nil, NewTypeError((*Constants)(nil), args)
					}
					err := i.UpdateConstants(constants)
					return nil, err
				},
				MethodType: MethodNotify,
			},
			"LongCall": {
				MakeArg: func() interface{} {
					return new(interface{})
				},
				Handler: func(ctx context.Context, _ interface{}) (interface{}, error) {
					return i.LongCall(ctx)
				},
				MethodType: MethodCall,
			},
			"LongCallResult": {
				MakeArg: func() interface{} {
					return new(interface{})
				},
				Handler: func(ctx context.Context, _ interface{}) (interface{}, error) {
					return i.LongCallResult(ctx)
				},
				MethodType: MethodCall,
			},
			"LongCallDebugTags": {
				MakeArg: func() interface{} {
					return new(interface{})
				},
				Handler: func(ctx context.Context, _ interface{}) (interface{}, error) {
					return i.LongCallDebugTags(ctx)
				},
				MethodType: MethodCall,
			},
		},
	}
}

// end autogen code
//---------------------------------------------------------------

//---------------------------------------------------------------------
// Client

type TestClient struct {
	GenericClient
}

func (a TestClient) Add(ctx context.Context, __arg AddArgs) (ret AddRes, err error) {
	err = a.Call(ctx, "test.1.testp.add", []interface{}{__arg}, &ret)
	return
}

func (a TestClient) BrokenMethod() (err error) {
	err = a.Call(context.Background(), "test.1.testp.broken", nil, nil)
	return
}

func (a TestClient) BrokenProtocol() (err error) {
	err = a.Call(context.Background(), "test.2.testp.broken", nil, nil)
	return
}

func (a TestClient) UpdateConstants(ctx context.Context, arg Constants) (err error) {
	err = a.Notify(ctx, "test.1.testp.updateConstants", arg)
	return
}

func (a TestClient) GetConstants(ctx context.Context) (ret Constants, err error) {
	err = a.Call(ctx, "test.1.testp.GetConstants", nil, &ret)
	return
}

func (a TestClient) LongCall(ctx context.Context) (ret int, err error) {
	err = a.Call(ctx, "test.1.testp.LongCall", nil, &ret)
	return
}

func (a TestClient) LongCallResult(ctx context.Context) (ret int, err error) {
	err = a.Call(ctx, "test.1.testp.LongCallResult", nil, &ret)
	return
}

func (a TestClient) LongCallDebugTags(ctx context.Context) (ret CtxRpcTags, err error) {
	err = a.Call(ctx, "test.1.testp.LongCallDebugTags", nil, &ret)
	return
}
