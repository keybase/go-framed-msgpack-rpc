package rpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidMessage(t *testing.T) {
	m := &message{remainingFields: 4}
	m.decodeSlots = []interface{}{
		&m.method,
		&m.seqno,
	}

	md := &mockCodec{
		elements: []interface{}{
			"testMethod",
			123,
			456,
			789,
		},
	}

	decodeIntoMessage(md, m)
	assert.Equal(t, 2, m.remainingFields, "Decoded the wrong number of fields")

	decodeToNull(md, m)
	assert.Equal(t, 0, m.remainingFields, "Expected message decoding to be finished")
	assert.Equal(t, "testMethod", m.method, "Wrong method name decoded")
	assert.Equal(t, 123, m.seqno, "Wrong sequence number decoded")

	err := decodeMessage(md, m, new(interface{}))
	assert.Error(t, err, "Expected error decoding past end")
}

func TestInvalidMessage(t *testing.T) {
	m := &message{remainingFields: 4}
	m.decodeSlots = []interface{}{
		&m.method,
		&m.seqno,
	}

	md := &mockCodec{
		elements: []interface{}{
			"testMethod",
		},
	}

	err := decodeIntoMessage(md, m)
	assert.Error(t, err, "Expected error decoding past end")
}

func TestDecodeError(t *testing.T) {
	m := &message{remainingFields: 2}
	md := &mockCodec{
		elements: []interface{}{
			123,
			"testError",
		},
	}
	appErr, dispatchErr := decodeError(md, m, &mockErrorUnwrapper{})
	assert.Nil(t, appErr, "Expected app error to be nil")
	assert.Nil(t, dispatchErr, "Expected dispatch error to be nil")
	appErr, dispatchErr = decodeError(md, m, nil)
	assert.Error(t, appErr, "Expected an app error")
	assert.Nil(t, dispatchErr, "Expected dispatch error to be nil")
	appErr, dispatchErr = decodeError(md, m, &mockErrorUnwrapper{})
	assert.Nil(t, appErr, "Expected app error to be nil")
	assert.Error(t, dispatchErr, "Expected a dispatch error")
}
