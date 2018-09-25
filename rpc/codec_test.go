package rpc

import (
	"bytes"
	"testing"

	"github.com/keybase/go-codec/codec"
	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	var buf bytes.Buffer
	mh := newCodecMsgpackHandle()
	enc := codec.NewEncoder(&buf, mh)
	dec := codec.NewDecoder(&buf, mh)

	m := map[string]string{
		"hello": "world",
		"foo":   "bar",
	}
	err := enc.Encode(m)
	require.NoError(t, err, "expected encoding to succeed")

	var targetMap map[string]string
	err = dec.Decode(&targetMap)
	require.NoError(t, err, "expected decoding to succeed")
	require.Equal(t, m, targetMap)

	var zeroMap map[string]string
	var targetMapInterface interface{}
	err = enc.Encode(zeroMap)
	require.NoError(t, err, "expected encoding to succeed")

	err = dec.Decode(&targetMapInterface)
	require.NoError(t, err, "expected decoding to succeed")
	require.Equal(t, 0, len(buf.Bytes()))

	err = enc.Encode([]interface{}{"hello", "world", m})
	require.NoError(t, err, "expected encoding to succeed")
	var a string
	var b string
	var c map[string]string
	i := []interface{}{&a, &b, &c}
	err = dec.Decode(&i)
	require.NoError(t, err, "expected decoding to succeed")
	require.Equal(t, 0, len(buf.Bytes()))
	require.Equal(t, "hello", a)
	require.Equal(t, "world", b)
	require.Equal(t, m, c)
}
