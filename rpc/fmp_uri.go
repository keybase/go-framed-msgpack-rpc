package rpc

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
)

const (
	spSchemeStandard = "sprpc"
	spSchemeTLS      = "sprpc+tls"
)

// SPURI represents a URI with an FMP scheme.
type SPURI struct {
	Scheme   string
	HostPort string
	Host     string
}

// ParseSPURI parses an FMPURI.
func ParseSPURI(s string) (*SPURI, error) {
	uri, err := url.Parse(s)
	if err != nil {
		return nil, err
	}

	f := &SPURI{Scheme: uri.Scheme, HostPort: uri.Host}

	switch f.Scheme {
	case spSchemeStandard, spSchemeTLS:
	default:
		return nil, fmt.Errorf("invalid framed msgpack rpc scheme %s", uri.Scheme)
	}

	host, _, err := net.SplitHostPort(f.HostPort)
	if err != nil {
		return nil, err
	}
	if len(host) == 0 {
		return nil, fmt.Errorf("missing host in address %s", f.HostPort)
	}
	f.Host = host

	return f, nil
}

func (f *SPURI) UseTLS() bool {
	return f.Scheme == spSchemeTLS
}

func (f *SPURI) String() string {
	return fmt.Sprintf("%s://%s", f.Scheme, f.HostPort)
}

func (f *SPURI) DialWithConfig(config *tls.Config) (net.Conn, error) {
	network, addr := "tcp", f.HostPort
	if f.UseTLS() {
		return tls.Dial(network, addr, config)
	}
	return net.Dial(network, addr)
}

func (f *SPURI) Dial() (net.Conn, error) {
	return f.DialWithConfig(nil)
}
