package rpc

import (
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

var testPort int = 8089

func prepServer(listener chan error) error {
	server := &server{port: testPort}

	serverReady := make(chan struct{})
	var err error
	go func() {
		err = server.Run(serverReady, listener)
	}()
	<-serverReady
	return err
}

func prepClient(t *testing.T) (TestClient, net.Conn) {
	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", testPort))
	assert.Nil(t, err, "a dialer error occurred")

	xp := NewTransport(c, nil, nil)
	return TestClient{GenericClient: NewClient(xp, nil)}, c
}

func prepTest(t *testing.T) (TestClient, chan error, net.Conn) {
	listener := make(chan error)
	prepServer(listener)
	cli, conn := prepClient(t)
	return cli, listener, conn
}

func endTest(t *testing.T, c net.Conn, listener chan error) {
	c.Close()
	err := <-listener
	assert.EqualError(t, err, io.EOF.Error(), "expected EOF")
}

func TestCall(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	B := 34
	for A := 10; A < 23; A += 2 {
		res, err := cli.Add(context.Background(), AddArgs{A: A, B: B})
		assert.Nil(t, err, "an error occurred while adding parameters")
		assert.Equal(t, A+B, res, "Result should be the two parameters added together")
	}
}

func TestBrokenCall(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	err := cli.Broken()
	assert.Error(t, err, "Called nonexistent method, expected error")
}

func TestNotify(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	pi := 31415

	err := cli.UpdateConstants(context.Background(), Constants{Pi: pi})
	assert.Nil(t, err, "Unexpected error on notify: %v", err)

	constants, err := cli.GetConstants(context.Background())
	assert.Nil(t, err, "Unexpected error on GetConstants: %v", err)
	assert.Equal(t, pi, constants.Pi, "we set the constant properly via Notify")
}

func TestLongCall(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	longResult, err := cli.LongCall(context.Background())
	assert.Nil(t, err, "call should have succeeded")
	assert.Equal(t, longResult, 1, "call should have succeeded")
}

func TestLongCallCancel(t *testing.T) {
	cli, listener, conn := prepTest(t)
	defer endTest(t, conn, listener)

	ctx, cancel := context.WithCancel(context.Background())
	var longResult int
	var err error
	wait := runInBg(func() error {
		longResult, err = cli.LongCall(ctx)
		return err
	})
	cancel()
	<-wait
	assert.Error(t, err, "call should be canceled")
	assert.Equal(t, longResult, 0, "call should be canceled")
}
