package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestRpcTags(t *testing.T) {
	logTags := make(CtxRpcTags)

	logTags["hello"] = "world"
	ctx, _ := NewContextWithLogTags(context.Background(), logTags)
	ctx, err := NewContextWithLogTags(ctx, logTags)
	require.EqualError(t, err, "Tag key already exists in context: hello")

	outTags, ok := RpcTagsFromContext(ctx)

	require.Equal(t, true, ok)
	require.Equal(t, logTags, outTags)
}
