package rpc

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ugorji/go/codec"
)

// This test determines the behavior of codec with respect to advancing the
// Reader when decoding error scenarios. It seems that the codec advances
// the Reader if Decode fails, but sets its own state to expect a specific
// type for the next Decode, and thus is functionally the same as not
// advancing the Reader.
func TestCodec(t *testing.T) {
	var buf bytes.Buffer
	mh := &codec.MsgpackHandle{WriteExt: true}
	enc := codec.NewEncoder(&buf, mh)
	dec := codec.NewDecoder(&buf, mh)

	var i int = math.MaxInt32
	err := enc.Encode(i)
	assert.Nil(t, err, "expected encoding to succeed")
	assert.Equal(t, 5, len(buf.Bytes()), "expected buffer to contain bytes")

	var targetInt int
	err = dec.Decode(&targetInt)
	assert.Nil(t, err, "expected decoding to succeed")
	assert.Equal(t, math.MaxInt32, targetInt, "expected codec to successfully decode int")
	assert.Equal(t, 0, len(buf.Bytes()), "expected buffer to be empty")

	var targetString string
	enc.Encode(i)
	assert.Equal(t, 5, len(buf.Bytes()), "expected buffer to contain bytes")
	err = dec.Decode(&targetString)
	assert.Error(t, err, "expected error while decoding")
	assert.Contains(t, err.Error(), "Unrecognized descriptor byte", "expected error while decoding")
	assert.Equal(t, 4, len(buf.Bytes()), "expected buffer to have bytes")
	err = dec.Decode(&targetString)
	assert.Error(t, err, "expected error while decoding")
	assert.Contains(t, err.Error(), "Unrecognized descriptor byte", "expected error while decoding")
	assert.Equal(t, 4, len(buf.Bytes()), "expected buffer to have bytes")

	targetInt = 0
	err = dec.Decode(&targetInt)
	assert.Nil(t, err, "expected decoding to succeed")
	assert.Equal(t, math.MaxInt32, targetInt, "expected codec to successfully decode int")
	assert.Equal(t, 0, len(buf.Bytes()), "expected buffer to be empty")
}
