package rpc

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"time"

	"golang.org/x/net/context"
)

type server struct {
	port int
}

type testProtocol struct {
	c         net.Conn
	constants Constants
}

func (a *testProtocol) Add(args *AddArgs) (ret int, err error) {
	ret = args.A + args.B
	return
}

func (a *testProtocol) DivMod(args *DivModArgs) (ret *DivModRes, err error) {
	ret = &DivModRes{}
	if args.B == 0 {
		err = errors.New("Cannot divide by 0")
	} else {
		ret.Q = args.A / args.B
		ret.R = args.A % args.B
	}
	return
}

func (a *testProtocol) UpdateConstants(args *Constants) error {
	a.constants = *args
	return nil
}

func (a *testProtocol) GetConstants() (*Constants, error) {
	return &a.constants, nil
}

func (a *testProtocol) LongCall(ctx context.Context) (int, error) {
	for i := 0; i < 100; i++ {
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			// There is no way to get this value out right now
			return 999, nil
		}
	}
	return 1, nil
}

//---------------------------------------------------------------
// begin autogen code

type AddArgs struct {
	A int
	B int
}

type DivModArgs struct {
	A int
	B int
}

type DivModRes struct {
	Q int
	R int
}

type Constants struct {
	Pi int
}

type TestInterface interface {
	Add(*AddArgs) (int, error)
	DivMod(*DivModArgs) (*DivModRes, error)
	UpdateConstants(*Constants) error
	GetConstants() (*Constants, error)
	LongCall(context.Context) (int, error)
}

func newTestProtocol(i TestInterface) Protocol {
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
				MethodType: MethodCall,
			},
			"divMod": {
				MakeArg: func() interface{} {
					return new(DivModArgs)
				},
				Handler: func(_ context.Context, args interface{}) (interface{}, error) {
					divModArgs, ok := args.(*DivModArgs)
					if !ok {
						return nil, NewTypeError((*DivModArgs)(nil), args)
					}
					return i.DivMod(divModArgs)
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
		},
	}
}

// end autogen code
//---------------------------------------------------------------

func (s *server) Run(ready chan struct{}) (err error) {
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
			return
		}
		xp := NewTransport(c, lf, nil)
		srv := NewServer(xp, nil)
		srv.Register(newTestProtocol(&testProtocol{c, Constants{}}))
		srv.Run(true)
	}
	return nil
}

//---------------------------------------------------------------------
// Client

type GenericClient interface {
	Call(ctx context.Context, method string, arg interface{}, res interface{}) error
	Notify(ctx context.Context, method string, arg interface{}) error
}

type TestClient struct {
	GenericClient
}

func (a TestClient) Add(ctx context.Context, arg AddArgs) (ret int, err error) {
	err = a.Call(ctx, "test.1.testp.add", arg, &ret)
	return
}

func (a TestClient) Broken() (err error) {
	err = a.Call(nil, "test.1.testp.broken", nil, nil)
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

type mockCodec struct {
	elems []interface{}
}

func newMockCodec(elems ...interface{}) *mockCodec {
	return &mockCodec{
		elems: elems,
	}
}

func (md *mockCodec) Decode(i interface{}) error {
	if len(md.elems) == 0 {
		return errors.New("Tried to decode too many elements")
	}
	v := reflect.ValueOf(i).Elem()
	d := reflect.ValueOf(md.elems[0])
	if !d.Type().AssignableTo(v.Type()) {
		return errors.New("Tried to decode incorrect type")
	}
	v.Set(d)
	md.elems = md.elems[1:]
	return nil
}

func (md *mockCodec) ReadByte() (b byte, err error) {
	err = md.Decode(&b)
	return b, err
}

func (md *mockCodec) Encode(i interface{}) error {
	v := reflect.ValueOf(i)
	if v.Kind() == reflect.Slice {
		for i := 0; i < v.Len(); i++ {
			md.elems = append(md.elems, v.Index(i).Interface())
		}
		return nil
	}
	return errors.New("only support encoding slices")
}

type mockErrorUnwrapper struct{}

func (eu *mockErrorUnwrapper) MakeArg() interface{} {
	return new(int)
}

func (eu *mockErrorUnwrapper) UnwrapError(i interface{}) (appErr error, dispatchErr error) {
	return nil, nil
}
