package rpc

import (
	"context"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// setupReceiverTest creates a receiver with a protocol for testing
func setupReceiverTest(t *testing.T, p *Protocol) (receiver, net.Conn, net.Conn) {
	conn1, conn2 := net.Pipe()
	receiveOut := newFramedMsgpackEncoder(testMaxFrameLength, conn2)
	protHandler := createMessageTestProtocol(t)
	if p != nil {
		err := protHandler.registerProtocol(*p)
		require.NoError(t, err)
	}
	log := newTestLog(t)
	r := newReceiveHandler(receiveOut, protHandler, log)
	return r, conn1, conn2
}

// checkGoroutineLeak verifies no significant goroutine leak occurred
func checkGoroutineLeak(t *testing.T, baseline int, maxAllowedLeak int) {
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	current := runtime.NumGoroutine()
	leaked := current - baseline

	t.Logf("Baseline: %d, Current: %d, Leaked: %d", baseline, current, leaked)

	if leaked > maxAllowedLeak {
		buf := make([]byte, 1<<20)
		stackSize := runtime.Stack(buf, true)
		t.Logf("Stack traces:\n%s", buf[:stackSize])
		t.Fatalf("Goroutine leak detected: %d leaked (max allowed: %d)", leaked, maxAllowedLeak)
	}
}

// TestReceiverGoroutineLeakOnClose verifies that goroutines don't leak
// when the receiver is closed while RPC handlers are still running.
func TestReceiverGoroutineLeakOnClose(t *testing.T) {
	numCalls := 50
	handlerStarted := make(chan struct{}, numCalls)
	handlerCanFinish := make(chan struct{})
	var handlerFinished int32

	p := &Protocol{
		Name: "slowservice",
		Methods: map[string]ServeHandlerDescription{
			"slowcall": {
				MakeArg: func() any { return nil },
				Handler: func(ctx context.Context, _ any) (any, error) {
					handlerStarted <- struct{}{}
					select {
					case <-handlerCanFinish:
						atomic.AddInt32(&handlerFinished, 1)
						return "done", nil
					case <-ctx.Done():
						atomic.AddInt32(&handlerFinished, 1)
						return nil, ctx.Err()
					}
				},
			},
		},
	}

	r, conn1, conn2 := setupReceiverTest(t, p)

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	// Start multiple RPCs
	for i := range numCalls {
		err := r.Receive(makeCall(SeqNumber(i), "slowservice.slowcall"))
		require.NoError(t, err)
	}

	// Wait for all handlers to start
	for i := range numCalls {
		select {
		case <-handlerStarted:
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for handler %d to start", i)
		}
	}

	// Close receiver while handlers are running
	closeCh := r.Close()
	require.NoError(t, conn1.Close())
	require.NoError(t, conn2.Close())
	close(handlerCanFinish)

	select {
	case <-closeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for receiver to close")
	}

	checkGoroutineLeak(t, baseline, 10)
	require.Equal(t, int32(numCalls), atomic.LoadInt32(&handlerFinished))
}

// TestReceiverTaskEndChDoesNotBlockOnClose verifies the specific fix:
// that sending to taskEndCh doesn't block when the receiver is closed
func TestReceiverTaskEndChDoesNotBlockOnClose(t *testing.T) {
	handlerDone := make(chan struct{})
	var goroutineExited int32

	p := &Protocol{
		Name: "testservice",
		Methods: map[string]ServeHandlerDescription{
			"testcall": {
				MakeArg: func() any { return nil },
				Handler: func(_ context.Context, _ any) (any, error) {
					close(handlerDone)
					time.Sleep(10 * time.Millisecond)
					return "result", nil
				},
			},
		},
	}

	r, conn1, conn2 := setupReceiverTest(t, p)

	err := r.Receive(makeCall(1, "testservice.testcall"))
	require.NoError(t, err)

	<-handlerDone

	closeCh := r.Close()
	require.NoError(t, conn1.Close())
	require.NoError(t, conn2.Close())

	go func() {
		<-closeCh
		time.Sleep(200 * time.Millisecond)
		atomic.StoreInt32(&goroutineExited, 1)
	}()

	time.Sleep(500 * time.Millisecond)

	require.Equal(t, int32(1), atomic.LoadInt32(&goroutineExited),
		"Goroutine should have exited, but appears blocked on taskEndCh send")
}

// TestReceiverContextCancellationExitPath verifies that goroutines also
// exit cleanly when the context is cancelled
func TestReceiverContextCancellationExitPath(t *testing.T) {
	handlerStarted := make(chan struct{})
	var handlerExited int32

	p := &Protocol{
		Name: "cancelservice",
		Methods: map[string]ServeHandlerDescription{
			"cancelcall": {
				MakeArg: func() any { return nil },
				Handler: func(ctx context.Context, _ any) (any, error) {
					close(handlerStarted)
					<-ctx.Done()
					atomic.StoreInt32(&handlerExited, 1)
					return nil, ctx.Err()
				},
			},
		},
	}

	r, conn1, conn2 := setupReceiverTest(t, p)

	err := r.Receive(makeCall(1, "cancelservice.cancelcall"))
	require.NoError(t, err)

	<-handlerStarted

	closeCh := r.Close()
	require.NoError(t, conn1.Close())
	require.NoError(t, conn2.Close())

	select {
	case <-closeCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for receiver to close")
	}

	time.Sleep(200 * time.Millisecond)

	require.Equal(t, int32(1), atomic.LoadInt32(&handlerExited),
		"Handler should have exited via context cancellation")
}

// TestReceiverMultipleSimultaneousRPCs tests the realistic scenario of
// many RPCs in flight when connection closes
func TestReceiverMultipleSimultaneousRPCs(t *testing.T) {
	numCalls := 100
	running := make(chan struct{}, numCalls)

	p := &Protocol{
		Name: "loadtest",
		Methods: map[string]ServeHandlerDescription{
			"work": {
				MakeArg: func() any { return nil },
				Handler: func(ctx context.Context, _ any) (any, error) {
					running <- struct{}{}
					select {
					case <-time.After(100 * time.Millisecond):
						return "completed", nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				},
			},
		},
	}

	r, conn1, conn2 := setupReceiverTest(t, p)

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	// Start many RPCs
	for i := range numCalls {
		err := r.Receive(makeCall(SeqNumber(i), "loadtest.work"))
		require.NoError(t, err)
	}

	// Wait for all handlers to start
	for range numCalls {
		select {
		case <-running:
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for handlers to start")
		}
	}

	// Close while handlers are running
	closeCh := r.Close()
	require.NoError(t, conn1.Close())
	require.NoError(t, conn2.Close())

	select {
	case <-closeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for receiver to close")
	}

	// With 100 simultaneous RPCs, without the fix we'd leak ~200 goroutines
	checkGoroutineLeak(t, baseline, 15)
}
