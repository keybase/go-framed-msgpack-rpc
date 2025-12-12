package rpc

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type testLogOutput struct {
	sync.Mutex
	t    TestLogger
	done atomic.Bool
}

func (t *testLogOutput) log(ch string, fmts string, args []interface{}) {
	// Don't log if test is done to avoid data races
	if t.done.Load() {
		return
	}

	t.t.Helper()
	fmts = fmt.Sprintf("[%s] %s", ch, fmts)
	t.Lock()
	defer t.Unlock()

	// Double-check after acquiring lock
	if t.done.Load() {
		return
	}
	t.t.Logf(fmts, args...)
}

// MarkDone marks this logger as done, preventing further logging.
// This should be called when the test completes to avoid data races
// with background goroutines that may still be running.
func (t *testLogOutput) MarkDone() {
	t.done.Store(true)
}

func (t *testLogOutput) Info(fmt string, args ...interface{}) {
	t.t.Helper()
	t.log("I", fmt, args)
}

func (t *testLogOutput) Error(fmt string, args ...interface{}) {
	t.t.Helper()
	t.log("E", fmt, args)
}

func (t *testLogOutput) Debug(fmt string, args ...interface{}) {
	t.t.Helper()
	t.log("D", fmt, args)
}

func (t *testLogOutput) Warning(fmt string, args ...interface{}) {
	t.t.Helper()
	t.log("W", fmt, args)
}

func (t *testLogOutput) Profile(fmt string, args ...interface{}) {
	t.t.Helper()
	t.log("P", fmt, args)
}

func (t *testLogOutput) CloneWithAddedDepth(_ int) LogOutputWithDepthAdder { return t }

func newTestLog(t TestLogger) SimpleLog {
	log := &testLogOutput{t: t}
	// If t has a Cleanup method (like *testing.T), register cleanup
	if tc, ok := t.(interface{ Cleanup(func()) }); ok {
		tc.Cleanup(func() { log.MarkDone() })
	}
	return SimpleLog{nil, log, SimpleLogOptions{}}
}
