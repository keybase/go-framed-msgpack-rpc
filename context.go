package rpc

import (
	"golang.org/x/net/context"
)

// CtxRpcKey is a type defining the context key for the RPC context
type CtxRpcKey int

const (
	// CtxRpcTagsKey defines a context key that can hold a slice of context keys
	CtxRpcTagsKey CtxRpcKey = iota
)

type CtxRpcTags map[string]interface{}

// NewContext returns a new Context that carries adds the given log
// tag mappings (context key -> display string).
func AddRpcTagsToContext(ctx context.Context, logTagsToAdd CtxRpcTags) (context.Context, error) {
	currTags, ok := RpcTagsFromContext(ctx)
	if !ok {
		currTags = make(CtxRpcTags)
		ctx = context.WithValue(ctx, CtxRpcTagsKey, currTags)
	}
	for key, tag := range logTagsToAdd {
		currTags[key] = tag
	}
	return ctx, nil
}

// RpcTagsFromContext returns the tags being passed along with the given context.
func RpcTagsFromContext(ctx context.Context) (CtxRpcTags, bool) {
	logTags, ok := ctx.Value(CtxRpcTagsKey).(CtxRpcTags)
	return logTags, ok
}
