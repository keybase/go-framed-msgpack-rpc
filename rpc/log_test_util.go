package rpc

import (
	"fmt"
	"sync"
)

type testLogOutput struct {
	sync.Mutex
	t    TestLogger
	done bool
}

// logLocked writes a log entry while the caller holds t's lock.
func (t *testLogOutput) logLocked(ch string, fmts string, args []any) {
	t.t.Helper()
	fmts = fmt.Sprintf("[%s] %s", ch, fmts)
	t.t.Logf(fmts, args...)
}

// MarkDone marks this logger as done, preventing further logging.
// This should be called when the test completes to avoid data races
// with background goroutines that may still be running.
func (t *testLogOutput) MarkDone() {
	t.Lock()
	defer t.Unlock()
	t.done = true
}

func (t *testLogOutput) Info(fmt string, args ...any) {
	t.Lock()
	defer t.Unlock()
	if !t.done {
		t.t.Helper()
		t.logLocked("I", fmt, args)
	}
}

func (t *testLogOutput) Error(fmt string, args ...any) {
	t.Lock()
	defer t.Unlock()
	if !t.done {
		t.t.Helper()
		t.logLocked("E", fmt, args)
	}
}

func (t *testLogOutput) Debug(fmt string, args ...any) {
	t.Lock()
	defer t.Unlock()
	if !t.done {
		t.t.Helper()
		t.logLocked("D", fmt, args)
	}
}

func (t *testLogOutput) Warning(fmt string, args ...any) {
	t.Lock()
	defer t.Unlock()
	if !t.done {
		t.t.Helper()
		t.logLocked("W", fmt, args)
	}
}

func (t *testLogOutput) Profile(fmt string, args ...any) {
	t.Lock()
	defer t.Unlock()
	if !t.done {
		t.t.Helper()
		t.logLocked("P", fmt, args)
	}
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
