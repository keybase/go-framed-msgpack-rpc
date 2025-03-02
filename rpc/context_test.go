package rpc

import (
	"context"
	"testing"

	"github.com/foks-proj/go-ctxlog"
	"github.com/stretchr/testify/require"
)

func TestRpcTags(t *testing.T) {
	logTags := make(ctxlog.CtxLogTags)

	logTags["hello"] = "world"
	logTags["foo"] = "bar"
	ctx := ctxlog.AddTagsToContext(context.Background(), logTags)

	logTags2 := make(ctxlog.CtxLogTags)
	logTags2["hello"] = "world2"
	ctx = ctxlog.AddTagsToContext(ctx, logTags2)

	logTags, _ = ctxlog.TagsFromContext(ctx)
	require.Equal(t, "world2", logTags["hello"])
	require.Equal(t, "bar", logTags["foo"])

	outTags, ok := ctxlog.TagsFromContext(ctx)

	require.Equal(t, true, ok)
	require.Equal(t, logTags, outTags)
}
