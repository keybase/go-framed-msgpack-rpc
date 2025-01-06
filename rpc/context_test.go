package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestRpcTags(t *testing.T) {
	logTags := make(CtxRPCTags)

	logTags["hello"] = "world"
	logTags["foo"] = "bar"
	ctx := AddRPCTagsToContext(context.Background(), logTags)

	logTags2 := make(CtxRPCTags)
	logTags2["hello"] = "world2"
	ctx = AddRPCTagsToContext(ctx, logTags2)

	logTags, _ = TagsFromContext(ctx)
	require.Equal(t, "world2", logTags["hello"])
	require.Equal(t, "bar", logTags["foo"])

	outTags, ok := TagsFromContext(ctx)

	require.Equal(t, true, ok)
	require.Equal(t, logTags, outTags)
}
