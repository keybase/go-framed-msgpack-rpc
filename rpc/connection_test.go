package rpc

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/keybase/backoff"
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
func (ut *unitTester) ShouldRetry(_ string, err error) bool {
	_, isThrottle := err.(throttleError)
	return isThrottle
}

var errCanceled = errors.New("canceled")

// ShouldRetryOnConnect implements the ConnectionHandler interface.
func (ut *unitTester) ShouldRetryOnConnect(err error) bool {
	return err != errCanceled
}

// Dial implements the ConnectionTransport interface.
func (ut *unitTester) Dial(_ context.Context) (
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

func (ut *unitTester) WaitForDoneOrBust(t *testing.T,
	timeout time.Duration, opName string) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ut.doneChan:
		break
	case <-timer.C:
		require.Fail(t, fmt.Sprintf("%s timeout", opName))
	}
}

// Test a basic reconnect flow.
func TestReconnectBasic(t *testing.T) {
	unitTester := &unitTester{
		doneChan:   make(chan bool),
		errToThrow: errors.New("intentional error to trigger reconnect"),
	}
	output := testLogOutput{t: t}
	t.Cleanup(func() { output.MarkDone() })
	reconnectBackoffFn := func() backoff.BackOff {
		reconnectBackoff := backoff.NewExponentialBackOff()
		reconnectBackoff.InitialInterval = 5 * time.Millisecond
		return reconnectBackoff
	}
	opts := ConnectionOpts{
		WrapErrorFunc:    testWrapError,
		TagsFunc:         testLogTags,
		ReconnectBackoff: reconnectBackoffFn,
	}
	conn := NewConnectionWithTransport(unitTester, unitTester,
		testErrorUnwrapper{}, &output, opts)

	// start connecting now
	conn.getReconnectChan()

	defer conn.Shutdown()
	timeout := time.After(2 * time.Second)
	select {
	case <-unitTester.doneChan:
	case <-timeout:
	}
	err := unitTester.Err()
	require.NoError(t, err)
}

// Test a basic reconnect flow.
func TestForceReconnect(t *testing.T) {
	unitTester := &unitTester{
		doneChan:   make(chan bool),
		errToThrow: errors.New("intentional error to trigger reconnect"),
	}
	output := testLogOutput{t: t}
	t.Cleanup(func() { output.MarkDone() })
	reconnectBackoffFn := func() backoff.BackOff {
		reconnectBackoff := backoff.NewExponentialBackOff()
		reconnectBackoff.InitialInterval = 5 * time.Millisecond
		return reconnectBackoff
	}
	opts := ConnectionOpts{
		WrapErrorFunc:    testWrapError,
		TagsFunc:         testLogTags,
		ReconnectBackoff: reconnectBackoffFn,
	}
	conn := NewConnectionWithTransport(unitTester, unitTester,
		testErrorUnwrapper{}, &output, opts)
	ch := make(chan struct{})
	conn.setReconnectCompleteForTest(ch)

	defer conn.Shutdown()
	unitTester.WaitForDoneOrBust(t, 2*time.Second, "initial reconnect")

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		require.Fail(t, "intial reconnect never completed")
	}

	forceReconnectErrCh := make(chan error)
	go func() {
		forceReconnectErrCh <- conn.ForceReconnect(context.Background())
	}()
	unitTester.WaitForDoneOrBust(t, 2*time.Second, "forced reconnect")
	require.NoError(t, <-forceReconnectErrCh)
}

// Test when a user cancels a connection.
func TestReconnectCanceled(t *testing.T) {
	cancelErr := errCanceled
	unitTester := &unitTester{
		doneChan:   make(chan bool),
		errToThrow: cancelErr,
		alwaysFail: true,
	}
	output := testLogOutput{t: t}
	t.Cleanup(func() { output.MarkDone() })
	opts := ConnectionOpts{
		WrapErrorFunc: testWrapError,
		TagsFunc:      testLogTags,
	}
	conn := NewConnectionWithTransport(unitTester, unitTester,
		testErrorUnwrapper{}, &output, opts)
	defer conn.Shutdown()
	// Test that any command fails with the expected error.
	err := conn.DoCommand(context.Background(), "test", 0,
		func(GenericClient) error { return nil })
	require.Equal(t, err, cancelErr)
}

// Test DoCommand with throttling.
func TestDoCommandThrottle(t *testing.T) {
	unitTester := &unitTester{
		doneChan: make(chan bool),
	}

	throttleErr := errors.New("throttle")
	output := testLogOutput{t: t}
	t.Cleanup(func() { output.MarkDone() })
	commandBackoffFn := func() backoff.BackOff {
		commandBackoff := backoff.NewExponentialBackOff()
		commandBackoff.InitialInterval = 5 * time.Millisecond
		return commandBackoff
	}
	opts := ConnectionOpts{
		WrapErrorFunc:  testWrapError,
		TagsFunc:       testLogTags,
		CommandBackoff: commandBackoffFn,
	}
	conn := NewConnectionWithTransport(unitTester, unitTester,
		testErrorUnwrapper{}, &output, opts)
	defer conn.Shutdown()
	<-unitTester.doneChan

	throttle := true
	ctx := context.Background()
	err := conn.DoCommand(ctx, "test", 0, func(GenericClient) error {
		if throttle {
			throttle = false
			err, _ := conn.errorUnwrapper.UnwrapError(
				throttleError{Err: throttleErr}.ToStatus())
			return err
		}
		return nil
	})
	require.NoError(t, err)
}

func TestConnectionClientCallError(t *testing.T) {
	serverConn, conn := MakeConnectionForTest(t)
	defer conn.Shutdown()

	c := connectionClient{conn}
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Call(context.Background(), "callRpc", nil, nil, 0)
	}()
	serverConn.Close()
	err := <-errCh
	require.Error(t, err)
}

func TestConnectionClientCallCompressedError(t *testing.T) {
	doWithAllCompressionTypes(func(ctype CompressionType) {
		serverConn, conn := MakeConnectionForTest(t)
		defer conn.Shutdown()

		c := connectionClient{conn}
		errCh := make(chan error, 1)
		go func() {
			errCh <- c.CallCompressed(context.Background(), "callRpc", nil, nil, ctype, 0)
		}()
		serverConn.Close()
		err := <-errCh
		require.Error(t, err)
	})
}

func TestConnectionClientNotifyError(t *testing.T) {
	serverConn, conn := MakeConnectionForTest(t)
	defer conn.Shutdown()

	c := connectionClient{conn}
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Notify(context.Background(), "notifyRpc", nil, 0)
	}()
	serverConn.Close()
	err := <-errCh
	require.Error(t, err)
}

func TestConnectionClientCallCancel(t *testing.T) {
	serverConn, conn := MakeConnectionForTest(t)
	defer conn.Shutdown()

	c := connectionClient{conn}
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		errCh <- c.Call(ctx, "callRpc", nil, nil, 0)
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
	serverConn, conn := MakeConnectionForTest(t)
	defer conn.Shutdown()

	c := connectionClient{conn}
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		errCh <- c.Notify(ctx, "notifyRpc", nil, 0)
	}()

	// Wait for Notify to make progress.
	n, err := serverConn.Read([]byte{1})
	require.Equal(t, n, 1)
	require.NoError(t, err)

	cancel()

	err = <-errCh
	require.Equal(t, err, ctx.Err())
}

type mockedDialable struct {
	mutex            sync.Mutex
	dialWasCalled    bool
	setoptsWasCalled bool
}

func (md *mockedDialable) SetOpts(_ time.Duration, _ time.Duration) {
	md.mutex.Lock()
	md.setoptsWasCalled = true
	md.mutex.Unlock()
}

func (md *mockedDialable) Dial(_ context.Context, _ string, _ string) (net.Conn, error) {
	md.mutex.Lock()
	md.dialWasCalled = true
	md.mutex.Unlock()
	return nil, fmt.Errorf("This is a mock")
}

func TestDialableTransport(t *testing.T) {
	unitTester := &unitTester{
		doneChan: make(chan bool),
	}
	output := testLogOutput{t: t}
	t.Cleanup(func() { output.MarkDone() })
	opts := ConnectionOpts{
		WrapErrorFunc:  testWrapError,
		TagsFunc:       testLogTags,
		DontConnectNow: true,
	}

	uriStr := "fmprpc://localhost:8080"
	uri, err := ParseFMPURI(uriStr)
	require.NoError(t, err)

	wef := func(err error) interface{} {
		require.NoError(t, err)
		return err
	}

	md := mockedDialable{dialWasCalled: false, setoptsWasCalled: false}

	instrumenterStorage := NewMemoryInstrumentationStorage()
	ct := NewConnectionTransportWithDialable(uri, nil, instrumenterStorage, wef, DefaultMaxFrameLength, &md)
	conn := NewConnectionWithTransport(unitTester, ct,
		testErrorUnwrapper{}, &output, opts)
	require.Error(t, conn.connect(context.TODO()))
	conn.Shutdown()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-unitTester.doneChan:
		break
	case <-timer.C:
		break
	}

	// Set opts isn't called since no timeout or backoff was specified
	md.mutex.Lock()
	require.True(t, md.dialWasCalled)
	md.mutex.Unlock()
}

func TestDialableTLSConn(t *testing.T) {
	unitTester := &unitTester{
		doneChan: make(chan bool),
	}
	output := testLogOutput{t: t}
	t.Cleanup(func() { output.MarkDone() })
	opts := ConnectionOpts{
		WrapErrorFunc:  testWrapError,
		TagsFunc:       testLogTags,
		DontConnectNow: true,
	}

	uriStr := "fmprpc+tls://localhost:8080"
	uri, err := ParseFMPURI(uriStr)
	require.NoError(t, err)

	md := mockedDialable{dialWasCalled: false, setoptsWasCalled: false}
	instrumenterStorage := NewMemoryInstrumentationStorage()
	conn := NewTLSConnectionWithDialable(NewFixedRemote(uri.HostPort),
		nil, testErrorUnwrapper{},
		unitTester, nil,
		instrumenterStorage, &output, DefaultMaxFrameLength, opts,
		&md)

	require.Error(t, conn.connect(context.TODO()))
	conn.Shutdown()

	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()
	select {
	case <-unitTester.doneChan:
		break
	case <-timer.C:
		break
	}

	md.mutex.Lock()
	require.True(t, md.dialWasCalled)
	require.True(t, md.setoptsWasCalled)
	md.mutex.Unlock()
}

// closeTrackingTransport wraps a Transporter and counts Close() calls
type closeTrackingTransport struct {
	Transporter
	closeCount int
	mu         sync.Mutex
}

func (t *closeTrackingTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closeCount++
	t.Transporter.Close()
}

func (t *closeTrackingTransport) getCloseCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closeCount
}

// trackingConnectionTransport wraps a ConnectionTransport and tracks the created Transporter
type trackingConnectionTransport struct {
	ConnectionTransport
	tracked *closeTrackingTransport
	mu      sync.Mutex
}

func (t *trackingConnectionTransport) Dial(ctx context.Context) (Transporter, error) {
	transport, err := t.ConnectionTransport.Dial(ctx)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.tracked = &closeTrackingTransport{Transporter: transport}
	return t.tracked, nil
}

func (t *trackingConnectionTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tracked != nil {
		t.tracked.Close()
	}
	t.ConnectionTransport.Close()
}

func (t *trackingConnectionTransport) getCloseCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tracked != nil {
		return t.tracked.getCloseCount()
	}
	return 0
}

// newTestTransport creates a simple ConnectionTransport that creates pipe-based transports
func newTestTransport(logOutput LogOutput) ConnectionTransport {
	return ConnectionFunc(func(_ context.Context) (Transporter, error) {
		serverConn, clientConn := net.Pipe()
		serverConn.Close()
		return NewTransport(clientConn,
			NewSimpleLogFactory(logOutput, nil), nil, testWrapError, testMaxFrameLength), nil
	})
}

// ConnectionFunc is a function adapter for ConnectionTransport.Dial
type ConnectionFunc func(context.Context) (Transporter, error)

func (f ConnectionFunc) Dial(ctx context.Context) (Transporter, error) { return f(ctx) }
func (f ConnectionFunc) IsConnected() bool                             { return false }
func (f ConnectionFunc) Finalize()                                     {}
func (f ConnectionFunc) Close()                                        {}

// failingConnectionHandler always returns an error from OnConnect
type failingConnectionHandler struct {
	testConnectionHandler
}

func (failingConnectionHandler) OnConnect(context.Context, *Connection, GenericClient, *Server) error {
	return errors.New("intentional OnConnect failure")
}

// successfulConnectionHandler always succeeds on OnConnect
type successfulConnectionHandler struct {
	testConnectionHandler
}

func (successfulConnectionHandler) OnConnect(context.Context, *Connection, GenericClient, *Server) error {
	return nil
}

// TestTransportClosedOnConnectFailure verifies that when OnConnect fails,
// the transport is closed immediately by the defer in connect() to prevent
// zombie RPC servers from remaining active.
func TestTransportClosedOnConnectFailure(t *testing.T) {
	logOutput := &testLogOutput{t: t}
	t.Cleanup(func() { logOutput.MarkDone() })
	tracked := &trackingConnectionTransport{
		ConnectionTransport: newTestTransport(logOutput),
	}

	conn := NewConnectionWithTransport(
		failingConnectionHandler{},
		tracked,
		testErrorUnwrapper{},
		logOutput,
		ConnectionOpts{
			WrapErrorFunc:  testWrapError,
			TagsFunc:       testLogTags,
			DontConnectNow: true,
		},
	)

	// Attempt to connect - should fail in OnConnect
	err := conn.connect(context.Background())
	require.Error(t, err, "connect should fail when OnConnect fails")

	// Verify transport was closed immediately by the defer
	require.Equal(t, 1, tracked.getCloseCount(),
		"transport should be closed immediately by defer when OnConnect fails")

	conn.Shutdown()
}

// TestMultipleConnectionsAfterFailure simulates the production scenario:
// First connection fails, Shutdown() is called, then a SECOND Connection is created.
// Without the defer fix, TWO transports would remain active, causing duplicate broadcasts.
func TestMultipleConnectionsAfterFailure(t *testing.T) {
	logOutput := &testLogOutput{t: t}
	t.Cleanup(func() { logOutput.MarkDone() })

	// First Connection (simulates failing Connection)
	firstTracked := &trackingConnectionTransport{
		ConnectionTransport: newTestTransport(logOutput),
	}
	firstConn := NewConnectionWithTransport(
		failingConnectionHandler{},
		firstTracked,
		testErrorUnwrapper{},
		logOutput,
		ConnectionOpts{
			WrapErrorFunc:  testWrapError,
			TagsFunc:       testLogTags,
			DontConnectNow: true,
		},
	)

	// First connection attempt fails
	err := firstConn.connect(context.Background())
	require.Error(t, err, "first connect should fail")

	// The defer fix ensures this transport is closed immediately
	require.Equal(t, 1, firstTracked.getCloseCount(),
		"first transport should be closed by defer immediately")

	// Simulate gregorHandler.Shutdown() being called (as in production logs)
	firstConn.Shutdown()

	// Shutdown closes the ConnectionTransport again, so closeCount becomes 2
	require.Equal(t, 2, firstTracked.getCloseCount(),
		"first transport closed by defer (1) + Shutdown (1) = 2")

	// Second Connection (simulates successful Connection)
	secondTracked := &trackingConnectionTransport{
		ConnectionTransport: newTestTransport(logOutput),
	}
	secondConn := NewConnectionWithTransport(
		successfulConnectionHandler{},
		secondTracked,
		testErrorUnwrapper{},
		logOutput,
		ConnectionOpts{
			WrapErrorFunc:  testWrapError,
			TagsFunc:       testLogTags,
			DontConnectNow: true,
		},
	)

	// Second connection succeeds
	err = secondConn.connect(context.Background())
	require.NoError(t, err, "second connect should succeed")

	// Verify final state: first transport closed, second transport active
	require.Equal(t, 2, firstTracked.getCloseCount(),
		"first transport must remain closed")
	require.Equal(t, 0, secondTracked.getCloseCount(),
		"second transport should be active - no Close() called yet")

	secondConn.Shutdown()
}
