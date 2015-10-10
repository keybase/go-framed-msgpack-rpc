package main

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	rpc "github.com/keybase/go-framed-msgpack-rpc"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

var testPort int = 8089

func TestMain(m *testing.M) {
	err := prepServer()
	if err != nil {
		fmt.Println("A server error occurred")
		os.Exit(-1)
	}
	os.Exit(m.Run())
}

func prepServer() error {
	server := &Server{port: testPort}

	serverReady := make(chan struct{})
	var err error
	go func() {
		err = server.Run(serverReady)
	}()
	<-serverReady
	return err
}

func prepTest(t *testing.T) TestClient {
	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", testPort))
	assert.Nil(t, err, "a dialer error occurred")

	xp := rpc.NewTransport(c, nil, nil)
	return TestClient{GenericClient: rpc.NewClient(xp, nil)}
}

func TestCall(t *testing.T) {
	cli := prepTest(t)

	B := 34
	for A := 10; A < 23; A += 2 {
		res, err := cli.Add(context.Background(), AddArgs{A: A, B: B})
		assert.Nil(t, err, "an error occurred while adding parameters")
		assert.Equal(t, A+B, res, "Result should be the two parameters added together")
	}
}

func TestBrokenCall(t *testing.T) {
	cli := prepTest(t)

	err := cli.Broken()
	assert.Error(t, err, "Called nonexistent method, expected error")
}

func TestNotify(t *testing.T) {
	cli := prepTest(t)

	pi := 31415

	err := cli.UpdateConstants(context.Background(), Constants{Pi: pi})
	assert.Nil(t, err, "Unexpected error on notify: %v", err)

	time.Sleep(3 * time.Millisecond)
	constants, err := cli.GetConstants(context.Background())
	assert.Nil(t, err, "Unexpected error on GetConstants: %v", err)
	assert.Equal(t, pi, constants.Pi, "we set the constant properly via Notify")
}

func TestLongCall(t *testing.T) {
	cli := prepTest(t)

	longResult, err := cli.LongCall(context.Background())
	assert.Nil(t, err, "call should have succeeded")
	assert.Equal(t, longResult, 1, "call should have succeeded")
}

func TestLongCallCancel(t *testing.T) {
	cli := prepTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	var longResult int
	var err error
	done := false
	go func() {
		longResult, err = cli.LongCall(ctx)
		done = true
	}()
	cancel()
	time.Sleep(3 * time.Millisecond)
	assert.True(t, done, "call should be completed via cancel")
	assert.Error(t, err, "call should be canceled")
	assert.Equal(t, longResult, 0, "call should be canceled")
}
