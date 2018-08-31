package rpc

import "fmt"

// TestLogger is an interface for things, like *testing.T, that have a
// Logf function.
type TestLogger interface {
	Logf(format string, args ...interface{})
}

type testLogOutput struct {
	t TestLogger
}

func (t testLogOutput) log(ch string, fmts string, args []interface{}) {
	fmts = fmt.Sprintf("[%s] %s", ch, fmts)
	t.t.Logf(fmts, args...)
}

func (t testLogOutput) Info(fmt string, args ...interface{})                  { t.log("I", fmt, args) }
func (t testLogOutput) Error(fmt string, args ...interface{})                 { t.log("E", fmt, args) }
func (t testLogOutput) Debug(fmt string, args ...interface{})                 { t.log("D", fmt, args) }
func (t testLogOutput) Warning(fmt string, args ...interface{})               { t.log("W", fmt, args) }
func (t testLogOutput) Profile(fmt string, args ...interface{})               { t.log("P", fmt, args) }
func (t testLogOutput) CloneWithAddedDepth(depth int) LogOutputWithDepthAdder { return t }
