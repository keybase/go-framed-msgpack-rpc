package rpc

import (
	"bytes"
	"encoding/base64"
	"os"
	"testing"

	"github.com/keybase/go-codec/codec"

	"github.com/stretchr/testify/require"
)

// Test an output from objective C that was breaking the server
func TestObjcOutput(t *testing.T) {
	dat, err := os.ReadFile("objc_output.dat")
	require.NoError(t, err, "an error occurred while reading dat file")
	v, err := base64.StdEncoding.DecodeString(string(dat))
	require.NoError(t, err, "an error occurred while decoding base64 dat file")

	buf := bytes.NewBuffer(v)
	var i int
	mh := newCodecMsgpackHandle()
	dec := codec.NewDecoder(buf, mh)
	err = dec.Decode(&i)
	require.NoError(t, err, "an error occurred while decoding an integer")
	require.Equal(t, buf.Len(), i, "Bad frame")

	var a interface{}
	err = dec.Decode(&a)
	require.NoError(t, err, "an error occurred while decoding object")
}
