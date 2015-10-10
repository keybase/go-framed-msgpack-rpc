package main

import (
	"fmt"
	"net"
	"time"

	rpc "github.com/keybase/go-framed-msgpack-rpc"
	"golang.org/x/net/context"
)

type GenericClient interface {
	Call(ctx context.Context, method string, arg interface{}, res interface{}) error
	Notify(ctx context.Context, method string, arg interface{}) error
}

//---------------------------------------------------------------------

type TestClient struct {
	GenericClient
}

func (a TestClient) Add(ctx context.Context, arg AddArgs) (ret int, err error) {
	err = a.Call(ctx, "test.1.arith.add", arg, &ret)
	return
}

func (a TestClient) Broken() (err error) {
	err = a.Call(nil, "test.1.arith.broken", nil, nil)
	return
}

func (a TestClient) UpdateConstants(ctx context.Context, arg Constants) (err error) {
	err = a.Notify(ctx, "test.1.arith.updateConstants", arg)
	return
}

func (a TestClient) GetConstants(ctx context.Context) (ret Constants, err error) {
	err = a.Call(ctx, "test.1.arith.GetConstants", nil, &ret)
	return
}

func (a TestClient) LongCall(ctx context.Context) (ret int, err error) {
	err = a.Call(ctx, "test.1.arith.LongCall", nil, &ret)
	return
}

//---------------------------------------------------------------------

type Client struct {
	port int
}

func (s *Client) Run() (err error) {
	var c net.Conn
	if c, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.port)); err != nil {
		return
	}

	xp := rpc.NewTransport(c, nil, nil)
	cli := TestClient{GenericClient: rpc.NewClient(xp, nil)}

	for A := 10; A < 23; A += 2 {
		var res int
		if res, err = cli.Add(context.Background(), AddArgs{A: A, B: 34}); err != nil {
			return
		}
		fmt.Printf("result is -> %v\n", res)
	}

	err = cli.Broken()
	fmt.Printf("for broken: %v\n", err)

	if err = cli.UpdateConstants(context.Background(), Constants{Pi: 314}); err != nil {
		return err
	}
	time.Sleep(3 * time.Millisecond)
	var constants Constants
	if constants, err = cli.GetConstants(context.Background()); err != nil {
		return
	} else {
		fmt.Printf("constants -> %v\n", constants)
	}

	return nil
}
