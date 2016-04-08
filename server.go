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

// AddCloseListener supplies a channel listener to which
// the server will send an error when a connection closes
func (s *Server) AddCloseListener(ch chan error) error {
	s.xp.AddCloseListener(ch)
	return nil
}

func (s *Server) Run() error {
	return s.xp.Run()
}

func (s *Server) RunAsync() <-chan error {
	return s.xp.RunAsync()
}
