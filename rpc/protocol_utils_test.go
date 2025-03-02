package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/foks-proj/go-ctxlog"
	"github.com/stretchr/testify/require"
)

type server struct {
	port int
}

func (s *server) Run(t *testing.T, ready chan struct{}, externalListener chan error) (err error) {
	var listener net.Listener
	o := testLogOutput{t: t}
	lf := NewSimpleLogFactory(&o, nil)
	instrumenterStorage := NewMemoryInstrumentationStorage()
	o.Infof(fmt.Sprintf("Listening on port %d...", s.port))
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
		ctx := ctxlog.WithLogTagWithValue(context.Background(), "server", "test123")
		xp := NewTransport(ctx, c, lf, instrumenterStorage, nil, testMaxFrameLength)
		srv := NewServer(xp, nil)
		err := srv.Register(createTestProtocol(newTestProtocol(c)))
		require.NoError(t, err)
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
	debugTags      ctxlog.CtxLogTags
	notifyCh       chan struct{}
}

func newTestProtocol(c net.Conn) *testProtocol {
	return &testProtocol{
		c:              c,
		constants:      Constants{},
		longCallResult: 0,
		notifyCh:       make(chan struct{}, 1),
	}
}

func (a *testProtocol) Add(args *AddArgs) (ret int, err error) {
	ret = args.A + args.B
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

func (a *testProtocol) GetNConstants(nargs *NArgs) (ret []*Constants, err error) {
	if nargs == nil {
		return nil, nil
	}
	for i := 0; i < nargs.N; i++ {
		ret = append(ret, &a.constants)
	}
	return ret, nil
}

func (a *testProtocol) LongCall(ctx context.Context) (int, error) {
	defer func() {
		close(a.notifyCh)
	}()
	tags, _ := ctxlog.TagsFromContext(ctx)
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

func (a *testProtocol) LongCallResult(_ context.Context) (int, error) {
	<-a.notifyCh
	return a.longCallResult, nil
}

func (a *testProtocol) LongCallDebugTags(_ context.Context) (ctxlog.CtxLogTags, error) {
	return a.debugTags, nil
}

//---------------------------------------------------------------
// begin autogen code

type AddArgs struct {
	A       int
	B       int
	Padding []byte
}

type NArgs struct {
	N int
}

type Constants struct {
	Pi int
}

type TestInterface interface {
	Add(*AddArgs) (int, error)
	UpdateConstants(*Constants) error
	GetConstants() (*Constants, error)
	GetNConstants(*NArgs) ([]*Constants, error)
	LongCall(context.Context) (int, error)
	LongCallResult(context.Context) (int, error)
	LongCallDebugTags(context.Context) (ctxlog.CtxLogTags, error)
}

func createTestProtocol(i TestInterface) Protocol {
	return Protocol{
		Name: "test.1.testp",
		Methods: map[string]ServeHandlerDescription{
			"add": {
				MakeArg: func() interface{} {
					return new(AddArgs)
				},
				Handler: func(_ context.Context, args interface{}) (interface{}, error) {
					addArgs, ok := args.(*AddArgs)
					if !ok {
						return nil, NewTypeError((*AddArgs)(nil), args)
					}
					return i.Add(addArgs)
				},
			},
			"GetConstants": {
				MakeArg: func() interface{} {
					return new(interface{})
				},
				Handler: func(_ context.Context, _ interface{}) (interface{}, error) {
					return i.GetConstants()
				},
			},
			"GetNConstants": {
				MakeArg: func() interface{} {
					return new(NArgs)
				},
				Handler: func(_ context.Context, args interface{}) (interface{}, error) {
					nargs, ok := args.(*NArgs)
					if !ok {
						return nil, NewTypeError((*NArgs)(nil), args)
					}
					return i.GetNConstants(nargs)
				},
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
			},
			"LongCall": {
				MakeArg: func() interface{} {
					return new(interface{})
				},
				Handler: func(ctx context.Context, _ interface{}) (interface{}, error) {
					return i.LongCall(ctx)
				},
			},
			"LongCallResult": {
				MakeArg: func() interface{} {
					return new(interface{})
				},
				Handler: func(ctx context.Context, _ interface{}) (interface{}, error) {
					return i.LongCallResult(ctx)
				},
			},
			"LongCallDebugTags": {
				MakeArg: func() interface{} {
					return new(interface{})
				},
				Handler: func(ctx context.Context, _ interface{}) (interface{}, error) {
					return i.LongCallDebugTags(ctx)
				},
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

func (a TestClient) Add(ctx context.Context, arg AddArgs) (ret int, err error) {
	err = a.Call(ctx, newMethodV1("test.1.testp.add"), arg, &ret, 0)
	return ret, err
}
func (a TestClient) BrokenMethod() (err error) {
	return a.Call(context.Background(), newMethodV1("test.1.testp.broken"), nil, nil, 0)
}

func (a TestClient) BrokenProtocol() error {
	return a.Call(context.Background(), newMethodV1("test.2.testp.broken"), nil, nil, 0)
}

func (a TestClient) UpdateConstants(ctx context.Context, arg Constants) error {
	return a.Notify(ctx, newMethodV1("test.1.testp.updateConstants"), arg, 0)
}

func (a TestClient) GetConstants(ctx context.Context) (ret Constants, err error) {
	err = a.Call(ctx, newMethodV1("test.1.testp.GetConstants"), nil, &ret, 0)
	return ret, err
}

func (a TestClient) GetNConstants(ctx context.Context, nargs NArgs) (ret []*Constants, err error) {
	err = a.CallCompressed(ctx, newMethodV1("test.1.testp.GetNConstants"), nargs, &ret, CompressionGzip, 0)
	return ret, err
}

func (a TestClient) LongCall(ctx context.Context) (ret int, err error) {
	err = a.Call(ctx, newMethodV1("test.1.testp.LongCall"), nil, &ret, 0)
	return ret, err
}

func (a TestClient) LongCallResult(ctx context.Context) (ret int, err error) {
	err = a.Call(ctx, newMethodV1("test.1.testp.LongCallResult"), nil, &ret, 0)
	return ret, err
}

func (a TestClient) LongCallDebugTags(ctx context.Context) (ret ctxlog.CtxLogTags, err error) {
	err = a.Call(ctx, newMethodV1("test.1.testp.LongCallDebugTags"), nil, &ret, 0)
	return ret, err
}
