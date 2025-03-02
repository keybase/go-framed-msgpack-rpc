package rpc

import (
	"net"
	"testing"
)

type spURITest struct {
	in  string
	out *SPURI
	err string
	tls bool
}

var addrErr = net.AddrError{Err: "missing port in address", Addr: "gregor.api.keybase.io"}

var spURITests = []spURITest{
	{in: "sprpc://gregor.api.keybase.io:80", out: &SPURI{Scheme: spSchemeStandard, HostPort: "gregor.api.keybase.io:80", Host: "gregor.api.keybase.io"}},
	{in: "sprpc+tls://gregor.api.keybase.io:443", out: &SPURI{Scheme: spSchemeTLS, HostPort: "gregor.api.keybase.io:443", Host: "gregor.api.keybase.io"}, tls: true},
	{in: "sprpc+tls://gregor.api.keybase.io:80", out: &SPURI{Scheme: spSchemeTLS, HostPort: "gregor.api.keybase.io:80", Host: "gregor.api.keybase.io"}, tls: true},
	{in: "sprpc://gregor.api.keybase.io:443", out: &SPURI{Scheme: spSchemeStandard, HostPort: "gregor.api.keybase.io:443", Host: "gregor.api.keybase.io"}},
	{in: "https://gregor.api.keybase.io:443", err: "invalid framed msgpack rpc scheme https"},
	{in: "sprpc://gregor.api.keybase.io", err: addrErr.Error()},
	{in: "sprpc+tls://gregor.api.keybase.io", err: addrErr.Error()},
	{in: "sprpc+tls://:443", err: "missing host in address :443"},
}

func TestParseSPURI(t *testing.T) {
	for _, test := range spURITests {
		u, err := ParseSPURI(test.in)
		if err != nil {
			if test.err == "" {
				t.Errorf("Parse(%q) error: %v, expected no error", test.in, err)
			}
			if err.Error() != test.err {
				t.Errorf("Parse(%q) error: %v, expected %v", test.in, err, test.err)
			}
			continue
		} else if test.err != "" {
			t.Errorf("Parse(%q) no error, expected %v", test.in, test.err)
			continue
		}
		if u.Scheme != test.out.Scheme {
			t.Errorf("Parse(%q) scheme: %q, expected %q", test.in, u.Scheme, test.out.Scheme)
		}
		if u.Host != test.out.Host {
			t.Errorf("Parse(%q) host: %q, expected %q", test.in, u.Host, test.out.Host)
		}
		if u.HostPort != test.out.HostPort {
			t.Errorf("Parse(%q) host: %q, expected %q", test.in, u.Host, test.out.Host)
		}
		if u.UseTLS() != test.tls {
			t.Errorf("Parse(%q) use tls: %v, expected %v", test.in, u.UseTLS(), test.tls)
		}
	}
}
