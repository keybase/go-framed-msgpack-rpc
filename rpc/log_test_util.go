package rpc

import (
	"fmt"
	"sync"
)

type testLogOutput struct {
	sync.Mutex
	t TestLogger
}

func (t *testLogOutput) log(ch string, fmts string, args []interface{}) {
	t.t.Helper()
	fmts = fmt.Sprintf("[%s] %s", ch, fmts)
	t.Lock()
	defer t.Unlock()
	t.t.Logf(fmts, args...)
}
func (t *testLogOutput) logw(ch string, msg string, fields []LogField) {
	t.t.Helper()
	fs := LogFieldsToString(fields, " ")
	msg = fmt.Sprintf("[%s] %s%s", ch, msg, fs)
	t.Lock()
	defer t.Unlock()
	t.t.Logf(msg)
}

func (t *testLogOutput) Infof(fmt string, args ...interface{}) {
	t.t.Helper()
	t.log("I", fmt, args)
}

func (t *testLogOutput) Infow(msg string, fields ...LogField) {
	t.t.Helper()
	t.logw("I", msg, fields)
}

func (t *testLogOutput) Errorf(fmt string, args ...interface{}) {
	t.t.Helper()
	t.log("E", fmt, args)
}

func (t *testLogOutput) Errorw(fmt string, args ...LogField) {
	t.t.Helper()
	t.logw("E", fmt, args)
}

func (t *testLogOutput) Debugf(fmt string, args ...interface{}) {
	t.t.Helper()
	t.log("D", fmt, args)
}

func (t *testLogOutput) Debugw(fmt string, args ...LogField) {
	t.t.Helper()
	t.logw("D", fmt, args)
}

func (t *testLogOutput) Warnf(fmt string, args ...interface{}) {
	t.t.Helper()
	t.log("W", fmt, args)
}

func (t *testLogOutput) Warnw(fmt string, args ...LogField) {
	t.t.Helper()
	t.logw("W", fmt, args)
}

func (t *testLogOutput) Profilef(fmt string, args ...interface{}) {
	t.t.Helper()
	t.log("P", fmt, args)
}

func (t *testLogOutput) CloneWithAddedDepth(_ int) LogOutputWithDepthAdder { return t }

func (t *testLogOutput) Profilew(fmt string, args ...LogField) {
	t.t.Helper()
	t.logw("P", fmt, args)
}

func newTestLog(t TestLogger) SimpleLog {
	log := testLogOutput{t: t}
	return SimpleLog{nil, &log, SimpleLogOptions{}}
}
