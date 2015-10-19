package rpc

import (
	"fmt"
	"net"
	"os"
	"testing"

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
	server := &server{port: testPort}

	serverReady := make(chan struct{})
	var err error
	go func() {
		err = server.Run(serverReady)
	}()
	<-serverReady
	return err
}

func prepClient(t *testing.T) TestClient {
	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", testPort))
	assert.Nil(t, err, "a dialer error occurred")

	xp := NewTransport(c, nil, nil)
	return TestClient{GenericClient: NewClient(xp, nil)}
}

func TestCall(t *testing.T) {
	cli := prepClient(t)

	B := 34
	for A := 10; A < 23; A += 2 {
		res, err := cli.Add(context.Background(), AddArgs{A: A, B: B})
		assert.Nil(t, err, "an error occurred while adding parameters")
		assert.Equal(t, A+B, res, "Result should be the two parameters added together")
	}
}

func TestBrokenCall(t *testing.T) {
	cli := prepClient(t)

	err := cli.Broken()
	assert.Error(t, err, "Called nonexistent method, expected error")
}

func TestNotify(t *testing.T) {
	cli := prepClient(t)

	pi := 31415

	err := cli.UpdateConstants(context.Background(), Constants{Pi: pi})
	assert.Nil(t, err, "Unexpected error on notify: %v", err)

	constants, err := cli.GetConstants(context.Background())
	assert.Nil(t, err, "Unexpected error on GetConstants: %v", err)
	assert.Equal(t, pi, constants.Pi, "we set the constant properly via Notify")
}

func TestLongCall(t *testing.T) {
	cli := prepClient(t)

	longResult, err := cli.LongCall(context.Background())
	assert.Nil(t, err, "call should have succeeded")
	assert.Equal(t, longResult, 1, "call should have succeeded")
}

func TestLongCallCancel(t *testing.T) {
	cli := prepClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	var longResult int
	var err error
	wait := make(chan struct{})
	go func() {
		longResult, err = cli.LongCall(ctx)
		close(wait)
	}()
	cancel()
	<-wait
	assert.Error(t, err, "call should be canceled")
	assert.Equal(t, longResult, 0, "call should be canceled")
}
