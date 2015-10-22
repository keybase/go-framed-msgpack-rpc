package rpc

import (
	"bytes"
	"encoding/base64"
	"github.com/ugorji/go/codec"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test an output from objective C that was breaking the server
func TestObjcOutput(t *testing.T) {
	dat, err := ioutil.ReadFile("objc_output.dat")
	assert.Nil(t, err, "an error occurred while reading dat file")
	v, err := base64.StdEncoding.DecodeString(string(dat))
	assert.Nil(t, err, "an error occurred while decoding base64 dat file")

	buf := bytes.NewBuffer(v)
	var i int
	mh := codec.MsgpackHandle{WriteExt: true}
	dec := codec.NewDecoder(buf, &mh)
	err = dec.Decode(&i)
	assert.Nil(t, err, "an error occurred while decoding an integer")
	assert.Equal(t, buf.Len(), i, "Bad frame")

	var a interface{}
	err = dec.Decode(&a)
	assert.Nil(t, err, "an error occurred while decoding object")
}
