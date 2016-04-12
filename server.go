package rpc

type Server struct {
	xp        Transporter
	wrapError WrapErrorFunc
}

func NewServer(xp Transporter, f WrapErrorFunc) *Server {
	return &Server{xp, f}
}

func (s *Server) Register(p Protocol) error {
	p.WrapError = s.wrapError
	return s.xp.RegisterProtocol(p)
}

// RunAsync starts processing incoming RPC messages asynchronously, if
// it hasn't been started already.
//
// If you want to know when said processing is done, and any
// associated error, use Transport.Done() and Transport.Err().
func (s *Server) RunAsync() {
	s.xp.ReceiveFramesAsync()
}
