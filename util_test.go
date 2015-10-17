package rpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func testSplit(t *testing.T, prot string, method string) {
	name := makeMethodName(prot, method)

	splitProt, splitMethod := splitMethodName(name)

	assert.Equal(t, prot, splitProt, "expected the protocol to match")
	assert.Equal(t, method, splitMethod, "expected the method name to match")
}

func TestSplitMethodName(t *testing.T) {
	testSplit(t, "protocol.namespace.subnamespace", "method")
}

func TestSplitEmptyProtocol(t *testing.T) {
	testSplit(t, "", "method")
}
