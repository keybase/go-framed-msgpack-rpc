package rpc

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

type Profiler interface {
	Stop()
}

type LogInterface interface {
	TransportStart()
	TransportError(error)
	// The passed-in slice should not be mutated.
	FrameRead([]byte)
	ClientCall(SeqNumber, string, any)
	ServerCall(SeqNumber, string, error, any)
	ServerReply(SeqNumber, string, error, any)
	ClientCallCompressed(SeqNumber, string, any, CompressionType)
	ServerCallCompressed(SeqNumber, string, error, any, CompressionType)
	ServerReplyCompressed(SeqNumber, string, error, any, CompressionType)
	ClientNotify(string, any)
	ServerNotifyCall(string, error, any)
	ServerNotifyComplete(string, error)
	ClientCancel(SeqNumber, string, error)
	ServerCancelCall(SeqNumber, string)
	ClientReply(SeqNumber, string, error, any)
	StartProfiler(format string, args ...any) Profiler
	UnexpectedReply(SeqNumber)
	Warning(format string, args ...any)
	Info(format string, args ...any)
}

type LogFactory interface {
	NewLog(net.Addr) LogInterface
}

type LogOutput interface {
	Error(s string, args ...any)
	Warning(s string, args ...any)
	Info(s string, args ...any)
	Debug(s string, args ...any)
	Profile(s string, args ...any)
}

type LogOutputWithDepthAdder interface {
	LogOutput
	CloneWithAddedDepth(depth int) LogOutputWithDepthAdder
}

type LogOptions interface {
	ShowAddress() bool
	ShowArg() bool
	ShowResult() bool
	Profile() bool
	FrameTrace() bool
	ClientTrace() bool
	ServerTrace() bool
	TransportStart() bool
}

//-------------------------------------------------

type SimpleLogFactory struct {
	out  LogOutput
	opts LogOptions
}

type SimpleLog struct {
	Addr net.Addr
	Out  LogOutput
	Opts LogOptions
}

type (
	SimpleLogOutput  struct{}
	SimpleLogOptions struct{}
)

func (s SimpleLogOutput) log(ch string, fmts string, args []any) {
	fmts = fmt.Sprintf("[%s] %s\n", ch, fmts)
	fmt.Fprintf(os.Stderr, fmts, args...)
}

func (s SimpleLogOutput) Info(fmt string, args ...any)    { s.log("I", fmt, args) }
func (s SimpleLogOutput) Error(fmt string, args ...any)   { s.log("E", fmt, args) }
func (s SimpleLogOutput) Debug(fmt string, args ...any)   { s.log("D", fmt, args) }
func (s SimpleLogOutput) Warning(fmt string, args ...any) { s.log("W", fmt, args) }
func (s SimpleLogOutput) Profile(fmt string, args ...any) { s.log("P", fmt, args) }

func (so SimpleLogOptions) ShowAddress() bool    { return true }
func (so SimpleLogOptions) ShowArg() bool        { return true }
func (so SimpleLogOptions) ShowResult() bool     { return true }
func (so SimpleLogOptions) Profile() bool        { return true }
func (so SimpleLogOptions) FrameTrace() bool     { return true }
func (so SimpleLogOptions) ClientTrace() bool    { return true }
func (so SimpleLogOptions) ServerTrace() bool    { return true }
func (so SimpleLogOptions) TransportStart() bool { return true }

type StandardLogOptions struct {
	frameTrace     bool
	clientTrace    bool
	serverTrace    bool
	profile        bool
	verboseTrace   bool
	connectionInfo bool
	noAddress      bool
}

func (s *StandardLogOptions) ShowAddress() bool    { return !s.noAddress }
func (s *StandardLogOptions) ShowArg() bool        { return s.verboseTrace }
func (s *StandardLogOptions) ShowResult() bool     { return s.verboseTrace }
func (s *StandardLogOptions) Profile() bool        { return s.profile }
func (s *StandardLogOptions) FrameTrace() bool     { return s.frameTrace }
func (s *StandardLogOptions) ClientTrace() bool    { return s.clientTrace }
func (s *StandardLogOptions) ServerTrace() bool    { return s.serverTrace }
func (s *StandardLogOptions) TransportStart() bool { return s.connectionInfo }

func NewStandardLogOptions(opts string, log LogOutput) LogOptions {
	var s StandardLogOptions
	for _, c := range opts {
		switch c {
		case 'A':
			s.noAddress = true
		case 'f':
			s.frameTrace = true
		case 'c':
			s.clientTrace = true
		case 's':
			s.serverTrace = true
		case 'v':
			s.verboseTrace = true
		case 'i':
			s.connectionInfo = true
		case 'p':
			s.profile = true
		default:
			log.Warning("Unknown logging flag: %c", c)
		}
	}
	return &s
}

func NewSimpleLogFactory(out LogOutput, opts LogOptions) SimpleLogFactory {
	if out == nil {
		out = SimpleLogOutput{}
	}
	if opts == nil {
		opts = SimpleLogOptions{}
	}
	ret := SimpleLogFactory{out, opts}
	return ret
}

func (s SimpleLogFactory) NewLog(a net.Addr) LogInterface {
	ret := SimpleLog{a, s.out, s.opts}
	ret.TransportStart()
	return ret
}

func AddrToString(addr net.Addr) string {
	if addr == nil {
		return "-"
	}
	c := addr.String()
	if len(c) == 0 {
		return addr.Network()
	}
	return addr.Network() + "://" + c
}

func (s SimpleLog) TransportStart() {
	if s.Opts.TransportStart() {
		s.Out.Debug(s.msg(true, "New connection"))
	}
}

func (s SimpleLog) TransportError(e error) {
	if e != io.EOF {
		s.Out.Error(s.msg(true, "Error in transport: %s", e.Error()))
	} else if s.Opts.TransportStart() {
		s.Out.Debug(s.msg(true, "EOF"))
	}
}

func (s SimpleLog) FrameRead(bytes []byte) {
	if s.Opts.FrameTrace() {
		s.Out.Debug(s.msg(false, "Frame read: %x", bytes))
	}
}

// Call
func (s SimpleLog) ClientCall(q SeqNumber, meth string, arg any) {
	if s.Opts.ClientTrace() {
		s.trace("call", "arg", s.Opts.ShowArg(), q, meth, nil, arg, nil)
	}
}

func (s SimpleLog) ServerCall(q SeqNumber, meth string, err error, arg any) {
	if s.Opts.ServerTrace() {
		s.trace("serve", "arg", s.Opts.ShowArg(), q, meth, err, arg, nil)
	}
}

func (s SimpleLog) ServerReply(q SeqNumber, meth string, err error, res any) {
	if s.Opts.ServerTrace() {
		s.trace("reply", "res", s.Opts.ShowResult(), q, meth, err, res, nil)
	}
}

// CallCompressed
func (s SimpleLog) ClientCallCompressed(q SeqNumber, meth string, arg any, ctype CompressionType) {
	if s.Opts.ClientTrace() {
		s.trace("call-compressed", "arg", s.Opts.ShowArg(), q, meth, nil, arg, &ctype)
	}
}

func (s SimpleLog) ServerCallCompressed(q SeqNumber, meth string, err error, arg any, ctype CompressionType) {
	if s.Opts.ServerTrace() {
		s.trace("serve-compressed", "arg", s.Opts.ShowArg(), q, meth, err, arg, &ctype)
	}
}

func (s SimpleLog) ServerReplyCompressed(q SeqNumber, meth string, err error, res any, ctype CompressionType) {
	if s.Opts.ServerTrace() {
		s.trace("reply-compressed", "res", s.Opts.ShowResult(), q, meth, err, res, &ctype)
	}
}

// Notify
func (s SimpleLog) ClientNotify(meth string, arg any) {
	if s.Opts.ClientTrace() {
		s.trace("notify", "arg", s.Opts.ShowArg(), 0, meth, nil, arg, nil)
	}
}

func (s SimpleLog) ServerNotifyCall(meth string, err error, arg any) {
	if s.Opts.ServerTrace() {
		s.trace("serve-notify", "arg", s.Opts.ShowArg(), 0, meth, err, arg, nil)
	}
}

func (s SimpleLog) ServerNotifyComplete(meth string, err error) {
	if s.Opts.ServerTrace() {
		s.trace("complete", "", false, 0, meth, err, nil, nil)
	}
}

// Cancel
func (s SimpleLog) ClientCancel(q SeqNumber, meth string, err error) {
	if s.Opts.ClientTrace() {
		s.trace("cancel", "", false, q, meth, err, nil, nil)
	}
}

func (s SimpleLog) ServerCancelCall(q SeqNumber, meth string) {
	if s.Opts.ServerTrace() {
		s.trace("serve-cancel", "", false, q, meth, nil, nil, nil)
	}
}

func (s SimpleLog) ClientReply(q SeqNumber, meth string, err error, res any) {
	if s.Opts.ClientTrace() {
		s.trace("reply", "res", s.Opts.ShowResult(), q, meth, err, res, nil)
	}
}

func (s SimpleLog) trace(which string, objname string, verbose bool, q SeqNumber,
	meth string, err error, obj any, ctype *CompressionType,
) {
	args := []any{which, q}
	fmts := "%s(%d):"
	if len(meth) > 0 {
		fmts += " method=%s;"
		args = append(args, meth)
	}
	if ctype != nil {
		fmts += " ctype=%s;"
		args = append(args, ctype)
	}

	fmts += " err=%s;"
	var es string
	if err == nil {
		es = "null"
	} else {
		es = err.Error()
	}
	args = append(args, es)
	if verbose {
		fmts += " %s=%s;"
		eb, err := json.Marshal(obj)
		var es string
		if err != nil {
			es = fmt.Sprintf(`{"error": "%s"}`, err.Error())
		} else {
			es = string(eb)
		}
		args = append(args, objname)
		args = append(args, es)
	}
	s.Out.Debug(s.msg(false, fmts, args...))
}

func (s SimpleLog) StartProfiler(format string, args ...any) Profiler {
	if s.Opts.Profile() {
		return &SimpleProfiler{
			start: time.Now(),
			msg:   fmt.Sprintf(format, args...),
			log:   s,
		}
	}
	return NilProfiler{}
}

func (s SimpleLog) UnexpectedReply(seqno SeqNumber) {
	s.Out.Warning(s.msg(false, "Unexpected seqno %d in incoming reply", seqno))
}

func (s SimpleLog) Warning(format string, args ...any) {
	s.Out.Warning(s.msg(false, format, args...))
}

func (s SimpleLog) Info(format string, args ...any) {
	s.Out.Info(s.msg(false, format, args...))
}

func (s SimpleLog) msg(force bool, format string, args ...any) string {
	m1 := fmt.Sprintf(format, args...)
	if s.Opts.ShowAddress() || force {
		m2 := fmt.Sprintf("{%s} %s", AddrToString(s.Addr), m1)
		m1 = m2
	}
	return m1
}

type SimpleProfiler struct {
	start time.Time
	msg   string
	log   SimpleLog
}

func (s *SimpleProfiler) Stop() {
	stop := time.Now()
	diff := stop.Sub(s.start)
	s.log.Out.Profile(s.log.msg(false, "%s ran in %dms", s.msg, diff/time.Millisecond))
}

// Callers shouldn't have to worry about whether an interface is satisfied or not
type NilProfiler struct{}

func (n NilProfiler) Stop() {}
