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

func (s *Server) Run() error {
	return s.xp.Run()
}

func (s *Server) RunAsync() <-chan error {
	return s.xp.RunAsync()
}
