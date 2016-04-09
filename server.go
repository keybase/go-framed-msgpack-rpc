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

// Run starts processing incoming RPC messages synchronously. Always
// returns a non-nil error, e.g. io.EOF if the connection was closed.
func (s *Server) Run() error {
	return s.xp.Run()
}
