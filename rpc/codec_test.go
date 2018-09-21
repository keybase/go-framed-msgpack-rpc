package rpc

import (
	"bytes"
	"math"
	"testing"

	"github.com/keybase/go-codec/codec"
	"github.com/stretchr/testify/require"
)

// TestFieldDecoder makes sure that fieldDecoder is still usable even
// after Decode returns an error, as long as ResetBytes is called
// first.
func TestFieldDecoder(t *testing.T) {
	// Encode an int into iBytes.
	i := math.MaxInt32
	var buf bytes.Buffer
	err := codec.NewEncoder(&buf, newCodecMsgpackHandle()).Encode(i)
	require.NoError(t, err)
	iBytes := buf.Bytes()
	require.Equal(t, 5, len(iBytes))

	log := newTestLog(t)
	dec := newFieldDecoder(log)

	// Try decoding from an empty slice into an int (should fail).

	var targetInt int
	err = dec.Decode(&targetInt)
	require.Error(t, err)
	require.Equal(t, 0, targetInt)

	// Try decoding from iBytes into an int (should succeed, since
	// we called ResetBytes first).

	dec.Reset(bytes.NewBuffer(iBytes), int32(len(iBytes)))

	err = dec.Decode(&targetInt)
	require.NoError(t, err)
	require.Equal(t, i, targetInt)

	// Then try decoding into a string (should fail).

	var targetString string
	err = dec.Decode(&targetString)
	require.Error(t, err)

	// Then reset again, and try decoding from iBytes into an int
	// again (should succeed, since we called ResetBytes first).

	dec.Reset(bytes.NewBuffer(iBytes), int32(len(iBytes)))

	targetInt = 0
	err = dec.Decode(&targetInt)
	require.NoError(t, err)
	require.Equal(t, i, targetInt)
}

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
	require.Nil(t, err, "expected encoding to succeed")

	var targetMap map[string]string
	err = dec.Decode(&targetMap)
	require.Nil(t, err, "expected decoding to succeed")
	require.Equal(t, m, targetMap)

	var zeroMap map[string]string
	var targetMapInterface interface{}
	err = enc.Encode(zeroMap)
	require.Nil(t, err, "expected encoding to succeed")

	err = dec.Decode(&targetMapInterface)
	require.Nil(t, err, "expected decoding to succeed")
	require.Equal(t, 0, len(buf.Bytes()))

	err = enc.Encode([]interface{}{"hello", "world", m})
	require.Nil(t, err, "expected encoding to succeed")
	var a string
	var b string
	var c map[string]string
	i := []interface{}{&a, &b, &c}
	err = dec.Decode(&i)
	require.Nil(t, err, "expected decoding to succeed")
	require.Equal(t, 0, len(buf.Bytes()))
	require.Equal(t, "hello", a)
	require.Equal(t, "world", b)
	require.Equal(t, m, c)
}
