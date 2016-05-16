package rpc

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"golang.org/x/net/context"
)

type unitTester struct {
	numConnects      int
	numConnectErrors int
	numDisconnects   int
	doneChan         chan bool
	errToThrow       error
	alwaysFail       bool
}

// HandlerName implements the ConnectionHandler interface.
func (unitTester) HandlerName() string {
	return "unitTester"
}

// OnConnect implements the ConnectionHandler interface.
func (ut *unitTester) OnConnect(context.Context, *Connection, GenericClient, *Server) error {
	ut.numConnects++
	return nil
}

// OnConnectError implements the ConnectionHandler interface.
func (ut *unitTester) OnConnectError(error, time.Duration) {
	ut.numConnectErrors++
}

// OnDoCommandError implements the ConnectionHandler interace
func (ut *unitTester) OnDoCommandError(error, time.Duration) {
}

// OnDisconnected implements the ConnectionHandler interface.
func (ut *unitTester) OnDisconnected(context.Context, DisconnectStatus) {
	ut.numDisconnects++
}

// ShouldRetry implements the ConnectionHandler interface.
func (ut *unitTester) ShouldRetry(name string, err error) bool {
	_, isThrottle := err.(throttleError)
	return isThrottle
}

var errCanceled = errors.New("Canceled!")

// ShouldRetryOnConnect implements the ConnectionHandler interface.
func (ut *unitTester) ShouldRetryOnConnect(err error) bool {
	return err != errCanceled
}

// Dial implements the ConnectionTransport interface.
func (ut *unitTester) Dial(ctx context.Context) (
	Transporter, error) {
	if ut.alwaysFail || ut.numConnectErrors == 0 {
		return nil, ut.errToThrow
	}
	return nil, nil
}

// IsConnected implements the ConnectionTransport interface.
func (ut *unitTester) IsConnected() bool {
	return ut.numConnects == 1
}

// Finalize implements the ConnectionTransport interface.
func (ut *unitTester) Finalize() {
	// Do this here so that we guarantee that conn.client is
	// non-nil, and therefore conn.IsConnected() before we're
	// done.
	ut.doneChan <- true
}

// Close implements the ConnectionTransport interface.
func (ut *unitTester) Close() {
}

// Did the test pass?
func (ut *unitTester) Err() error {
	if ut.numConnects != 1 {
		return fmt.Errorf("expected 1 connect, got: %d", ut.numConnects)
	}
	if ut.numConnectErrors != 1 {
		return fmt.Errorf("expected 1 connect error, got: %d", ut.numConnectErrors)
	}
	if ut.numDisconnects != 1 {
		return fmt.Errorf("expected 1 disconnected error, got: %d", ut.numDisconnects)
	}
	return nil
}

type testStatus struct {
	Code int
}
type testUnwrapper struct{}

func testWrapError(err error) interface{} {
	return &testStatus{}
}

func testLogTags(ctx context.Context) (map[interface{}]string, bool) {
	return nil, false
}

type throttleError struct {
	Err error
}

func (e throttleError) ToStatus() (s testStatus) {
	s.Code = 15
	return
}

func (e throttleError) Error() string {
	return e.Err.Error()
}

type testErrorUnwrapper struct{}

var _ ErrorUnwrapper = testErrorUnwrapper{}

func (eu testErrorUnwrapper) MakeArg() interface{} {
	return &testStatus{}
}

func (eu testErrorUnwrapper) UnwrapError(arg interface{}) (appError error, dispatchError error) {
	s, ok := arg.(*testStatus)
	if !ok {
		return nil, errors.New("Error converting arg to testStatus object")
	}
	if s == nil || s.Code == 0 {
		return nil, nil
	}

	switch s.Code {
	case 15:
		appError = throttleError{errors.New("throttle")}
		break
	default:
		panic("Unknown testing error")
	}
	return appError, nil
}

type testLogOutput struct {
	t *testing.T
}

func (t testLogOutput) log(ch string, fmts string, args []interface{}) {
	fmts = fmt.Sprintf("[%s] %s\n", ch, fmts)
	t.t.Logf(fmts, args...)
}

func (t testLogOutput) Info(fmt string, args ...interface{})    { t.log("I", fmt, args) }
func (t testLogOutput) Error(fmt string, args ...interface{})   { t.log("E", fmt, args) }
func (t testLogOutput) Debug(fmt string, args ...interface{})   { t.log("D", fmt, args) }
func (t testLogOutput) Warning(fmt string, args ...interface{}) { t.log("W", fmt, args) }
func (t testLogOutput) Profile(fmt string, args ...interface{}) { t.log("P", fmt, args) }

// Test a basic reconnect flow.
func TestReconnectBasic(t *testing.T) {
	unitTester := &unitTester{
		doneChan:   make(chan bool),
		errToThrow: errors.New("intentional error to trigger reconnect"),
	}
	output := testLogOutput{t}
	conn := NewConnectionWithTransport(unitTester, unitTester,
		testErrorUnwrapper{}, false, testWrapError, output, testLogTags)
	conn.reconnectBackoff.InitialInterval = 5 * time.Millisecond

	// start connecting now
	conn.getReconnectChan()

	defer conn.Shutdown()
	timeout := time.After(2 * time.Second)
	select {
	case <-unitTester.doneChan:
		break
	case <-timeout:
		break
	}
	if err := unitTester.Err(); err != nil {
		t.Fatal(err)
	}
}

// Test when a user cancels a connection.
func TestReconnectCanceled(t *testing.T) {
	cancelErr := errCanceled
	unitTester := &unitTester{
		doneChan:   make(chan bool),
		errToThrow: cancelErr,
		alwaysFail: true,
	}
	output := testLogOutput{t}
	conn := NewConnectionWithTransport(unitTester, unitTester,
		testErrorUnwrapper{}, true, testWrapError, output, testLogTags)
	defer conn.Shutdown()
	// Test that any command fails with the expected error.
	err := conn.DoCommand(context.Background(), "test",
		func(GenericClient) error { return nil })
	if err != cancelErr {
		t.Fatalf("Error wasn't InputCanceled: %v", err)
	}
}

// Test DoCommand with throttling.
func TestDoCommandThrottle(t *testing.T) {
	unitTester := &unitTester{
		doneChan: make(chan bool),
	}

	throttleErr := errors.New("throttle")
	output := testLogOutput{t}
	conn := NewConnectionWithTransport(unitTester, unitTester,
		testErrorUnwrapper{}, true, testWrapError, output, testLogTags)
	defer conn.Shutdown()
	<-unitTester.doneChan

	conn.doCommandBackoff.InitialInterval = 5 * time.Millisecond

	throttle := true
	ctx := context.Background()
	err := conn.DoCommand(ctx, "test", func(GenericClient) error {
		if throttle {
			throttle = false
			err, _ := conn.errorUnwrapper.UnwrapError(
				throttleError{Err: throttleErr}.ToStatus())
			return err
		}
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}

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

type sharedTransport struct {
	t Transporter
}

var _ ConnectionTransport = sharedTransport{}

// Dial is an implementation of the ConnectionTransport interface.
func (st sharedTransport) Dial(ctx context.Context) (Transporter, error) {
	if !st.t.IsConnected() {
		return nil, io.EOF
	}
	return st.t, nil
}

// IsConnected is an implementation of the ConnectionTransport interface.
func (st sharedTransport) IsConnected() bool {
	return st.t.IsConnected()
}

// Finalize is an implementation of the ConnectionTransport interface.
func (st sharedTransport) Finalize() {}

// Close is an implementation of the ConnectionTransport interface.
func (st sharedTransport) Close() {}

func makeConnectionForTest(t *testing.T) (net.Conn, *Connection) {
	clientConn, serverConn := net.Pipe()
	transporter := NewTransport(clientConn, nil, testWrapError)
	st := sharedTransport{transporter}
	output := testLogOutput{t}
	conn := NewConnectionWithTransport(testConnectionHandler{}, st,
		testErrorUnwrapper{}, true, testWrapError, output, testLogTags)
	return serverConn, conn
}

func TestConnectionClientCallError(t *testing.T) {
	serverConn, conn := makeConnectionForTest(t)
	defer conn.Shutdown()

	c := connectionClient{conn}
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Call(context.Background(), "callRpc", nil, nil)
	}()
	serverConn.Close()
	err := <-errCh
	require.Error(t, err)
}

func TestConnectionClientNotifyError(t *testing.T) {
	serverConn, conn := makeConnectionForTest(t)
	defer conn.Shutdown()

	c := connectionClient{conn}
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Notify(context.Background(), "notifyRpc", nil)
	}()
	serverConn.Close()
	err := <-errCh
	require.Error(t, err)
}

func TestConnectionClientCallCancel(t *testing.T) {
	serverConn, conn := makeConnectionForTest(t)
	defer conn.Shutdown()

	c := connectionClient{conn}
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		errCh <- c.Call(ctx, "callRpc", nil, nil)
	}()

	// Wait for Call to make progress.
	n, err := serverConn.Read([]byte{1})
	require.Equal(t, n, 1)
	require.NoError(t, err)

	cancel()

	err = <-errCh
	require.Equal(t, err, ctx.Err())
}

func TestConnectionClientNotifyCancel(t *testing.T) {
	serverConn, conn := makeConnectionForTest(t)
	defer conn.Shutdown()

	c := connectionClient{conn}
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		errCh <- c.Notify(ctx, "notifyRpc", nil)
	}()

	// Wait for Notify to make progress.
	n, err := serverConn.Read([]byte{1})
	require.Equal(t, n, 1)
	require.NoError(t, err)

	cancel()

	err = <-errCh
	require.Equal(t, err, ctx.Err())
}
