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

// Run starts processing incoming RPC messages synchronously.
//
// Note that an error is returned immediately if incoming messages are
// already being processed, e.g. if the underlying transport has been
// used to make an RPC call (which has to wait for a
// reply). Otherwise, the returned error (which is always non-nil,
// e.g. io.EOF if the connection was closed) is due to an error
// processing a frame or the connection being closed.
func (s *Server) Run() {
	s.xp.ReceiveFrames()
}
