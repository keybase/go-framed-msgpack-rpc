package main

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/keybase/go-framed-msgpack-rpc"
	"golang.org/x/net/context"
)

type Server struct {
	port int
}

type TestServer struct {
	c         net.Conn
	constants Constants
}

func (a *TestServer) Add(args *AddArgs) (ret int, err error) {
	ret = args.A + args.B
	return
}

func (a *TestServer) DivMod(args *DivModArgs) (ret *DivModRes, err error) {
	ret = &DivModRes{}
	if args.B == 0 {
		err = errors.New("Cannot divide by 0")
	} else {
		ret.Q = args.A / args.B
		ret.R = args.A % args.B
	}
	return
}

func (a *TestServer) UpdateConstants(args *Constants) error {
	a.constants = *args
	return nil
}

func (a *TestServer) GetConstants() (*Constants, error) {
	return &a.constants, nil
}

func (a *TestServer) LongCall(ctx context.Context) (int, error) {
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

func TestProtocol(i TestInterface) rpc.Protocol {
	return rpc.Protocol{
		Name: "test.1.testp",
		Methods: map[string]rpc.ServeHandlerDescription{
			"add": {
				MakeArg: func() interface{} {
					return new(AddArgs)
				},
				Handler: func(_ context.Context, args interface{}) (interface{}, error) {
					addArgs, ok := args.(*AddArgs)
					if !ok {
						return nil, rpc.NewTypeError((*AddArgs)(nil), args)
					}
					return i.Add(addArgs)
				},
				MethodType: rpc.MethodCall,
			},
			"divMod": {
				MakeArg: func() interface{} {
					return new(DivModArgs)
				},
				Handler: func(_ context.Context, args interface{}) (interface{}, error) {
					divModArgs, ok := args.(*DivModArgs)
					if !ok {
						return nil, rpc.NewTypeError((*DivModArgs)(nil), args)
					}
					return i.DivMod(divModArgs)
				},
				MethodType: rpc.MethodCall,
			},
			"GetConstants": {
				MakeArg: func() interface{} {
					return new(interface{})
				},
				Handler: func(_ context.Context, _ interface{}) (interface{}, error) {
					return i.GetConstants()
				},
				MethodType: rpc.MethodCall,
			},
			"updateConstants": {
				MakeArg: func() interface{} {
					return new(Constants)
				},
				Handler: func(_ context.Context, args interface{}) (interface{}, error) {
					constants, ok := args.(*Constants)
					if !ok {
						return nil, rpc.NewTypeError((*Constants)(nil), args)
					}
					err := i.UpdateConstants(constants)
					return nil, err
				},
				MethodType: rpc.MethodNotify,
			},
			"LongCall": {
				MakeArg: func() interface{} {
					return new(interface{})
				},
				Handler: func(ctx context.Context, _ interface{}) (interface{}, error) {
					return i.LongCall(ctx)
				},
				MethodType: rpc.MethodCall,
			},
		},
	}
}

// end autogen code
//---------------------------------------------------------------

func (s *Server) Run(ready chan struct{}) (err error) {
	var listener net.Listener
	o := rpc.SimpleLogOutput{}
	lf := rpc.NewSimpleLogFactory(o, nil)
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
		xp := rpc.NewTransport(c, lf, nil)
		srv := rpc.NewServer(xp, nil)
		srv.Register(TestProtocol(&TestServer{c, Constants{}}))
		srv.Run(true)
	}
	return nil
}
