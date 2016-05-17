package rpc

import (
	"io"
	"net"
	"time"

	"golang.org/x/net/context"
)

type testConnectionHandler struct{}

var _ ConnectionHandler = testConnectionHandler{}

func (testConnectionHandler) OnConnect(context.Context, *Connection, GenericClient, *Server) error {
	return nil
}

func (testConnectionHandler) OnConnectError(err error, reconnectThrottleDuration time.Duration) {
}

func (testConnectionHandler) OnDoCommandError(err error, nextTime time.Duration) {
}

func (testConnectionHandler) OnDisconnected(ctx context.Context, status DisconnectStatus) {
}

func (testConnectionHandler) ShouldRetry(name string, err error) bool {
	return false
}

func (testConnectionHandler) ShouldRetryOnConnect(err error) bool {
	return false
}

func (testConnectionHandler) HandlerName() string {
	return "testConnectionHandler"
}

type singleTransport struct {
	t Transporter
}

var _ ConnectionTransport = singleTransport{}

// Dial is an implementation of the ConnectionTransport interface.
func (st singleTransport) Dial(ctx context.Context) (Transporter, error) {
	if !st.t.IsConnected() {
		return nil, io.EOF
	}
	return st.t, nil
}

// IsConnected is an implementation of the ConnectionTransport interface.
func (st singleTransport) IsConnected() bool {
	return st.t.IsConnected()
}

// Finalize is an implementation of the ConnectionTransport interface.
func (st singleTransport) Finalize() {}

// Close is an implementation of the ConnectionTransport interface.
func (st singleTransport) Close() {}

func MakeConnectionForTest(output LogOutput) (net.Conn, *Connection) {
	clientConn, serverConn := net.Pipe()
	transporter := NewTransport(clientConn, nil, testWrapError)
	st := singleTransport{transporter}
	conn := NewConnectionWithTransport(testConnectionHandler{}, st,
		testErrorUnwrapper{}, true, testWrapError, output, testLogTags)
	return serverConn, conn
}
