package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidMessage(t *testing.T) {
	m := &message{remainingFields: 4}
	m.decodeSlots = []interface{}{
		&m.method,
		&m.seqno,
	}

	md := newMockCodec(
		"testMethod",
		seqNumber(123),
		456,
		789,
	)

	err := decodeIntoMessage(md, m)
	require.Nil(t, err, "An error occurred while decoding")
	require.Equal(t, 2, m.remainingFields, "Decoded the wrong number of fields")

	err = decodeToNull(md, m)
	require.Nil(t, err, "An error occurred while decoding")
	require.Equal(t, 0, m.remainingFields, "Expected message decoding to be finished")
	require.Equal(t, "testMethod", m.method, "Wrong method name decoded")
	require.Equal(t, seqNumber(123), m.seqno, "Wrong sequence number decoded")

	err = decodeField(md, m, new(interface{}))
	require.Error(t, err, "Expected error decoding past end")
}

func TestInvalidMessage(t *testing.T) {
	m := &message{remainingFields: 4}
	m.decodeSlots = []interface{}{
		&m.method,
		&m.seqno,
	}

	md := newMockCodec(
		"testMethod",
	)

	err := decodeIntoMessage(md, m)
	require.Error(t, err, "Expected error decoding past end")
}

func TestMessageDecodeError(t *testing.T) {
	m := &message{remainingFields: 2}
	md := newMockCodec(
		123,
		"testError",
	)
	appErr, dispatchErr := decodeError(md, m, &mockErrorUnwrapper{})
	require.Nil(t, appErr, "Expected app error to be nil")
	require.Nil(t, dispatchErr, "Expected dispatch error to be nil")
	appErr, dispatchErr = decodeError(md, m, nil)
	require.Error(t, appErr, "Expected an app error")
	require.Nil(t, dispatchErr, "Expected dispatch error to be nil")
	appErr, dispatchErr = decodeError(md, m, &mockErrorUnwrapper{})
	require.Nil(t, appErr, "Expected app error to be nil")
	require.Error(t, dispatchErr, "Expected a dispatch error")
}
