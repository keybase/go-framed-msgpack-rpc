package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/keybase/backoff"
	"github.com/stretchr/testify/require"
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
func (ut *unitTester) ShouldRetry(_ Methoder, err error) bool {
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
	opts := ConnectionOpts{
		WrapErrorFunc: testWrapError,
		TagsFunc:      testLogTags,
	}
	conn := NewConnectionWithTransport(unitTester, unitTester,
		testErrorUnwrapper{}, &output, opts)
	defer conn.Shutdown()
	// Test that any command fails with the expected error.
	err := conn.DoCommand(context.Background(), newMethodV1("test"), 0,
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
	err := conn.DoCommand(ctx, newMethodV1("test"), 0, func(GenericClient) error {
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
		errCh <- c.Call(context.Background(), newMethodV1("callRpc"), nil, nil, 0)
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
			errCh <- c.CallCompressed(context.Background(), newMethodV1("callRpc"), nil, nil, ctype, 0)
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
		errCh <- c.Notify(context.Background(), newMethodV1("notifyRpc"), nil, 0)
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
		errCh <- c.Call(ctx, newMethodV1("callRpc"), nil, nil, 0)
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
		errCh <- c.Notify(ctx, newMethodV1("notifyRpc"), nil, 0)
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
	opts := ConnectionOpts{
		WrapErrorFunc:  testWrapError,
		TagsFunc:       testLogTags,
		DontConnectNow: true,
	}

	uriStr := "sprpc://localhost:8080"
	uri, err := ParseSPURI(uriStr)
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
	opts := ConnectionOpts{
		WrapErrorFunc:  testWrapError,
		TagsFunc:       testLogTags,
		DontConnectNow: true,
	}

	uriStr := "sprpc+tls://localhost:8080"
	uri, err := ParseSPURI(uriStr)
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
