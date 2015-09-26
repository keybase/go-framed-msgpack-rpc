package main

import (
	"fmt"
	"net"
	"testing"
	"time"


	"github.com/keybase/go-framed-msgpack-rpc/rpc2"
	"github.com/stretchr/testify/assert"
)

func TestProtocol(t *testing.T) {
	assert.True(t, true, "very true")

	port := 8089
	server := &Server{port: 8089}

	serverReady := make(chan struct{})
	go func() {
		err := server.Run(serverReady)
		assert.Nil(t, err, "a server error occurred")
	}()
	<-serverReady

	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return
	}

	xp := rpc2.NewTransport(c, nil, nil)
	cli := ArithClient{GenericClient: rpc2.NewClient(xp, nil)}

	B := 34
	for A := 10; A < 23; A += 2 {
		var res int
		if res, err = cli.Add(AddArgs{A: A, B: B}); err != nil {
			return
		}
		assert.Equal(t, A+B, res, "Result should be the two parameters added together")
	}

	err = cli.Broken()
	assert.Error(t, err, "Called nonexistent method, expected error")

	xp.Close()
	time.Sleep(1000)
	assert.Equal(t, 1, server.nEOFs, "EOF hook worked as planned")
}
