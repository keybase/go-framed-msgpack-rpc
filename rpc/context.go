package rpc

import "context"

// CtxRPCKey is a type defining the context key for the RPC context
type CtxRPCKey int

const (
	// CtxRPCTagsKey defines a context key that can hold a slice of context keys
	CtxRPCTagsKey CtxRPCKey = iota
)

type CtxRPCTags map[string]interface{}

// AddRPCTagsToContext adds the given log tag mappings (logTagsToAdd) to the
// given context, creating a new one if necessary. Returns the resulting
// context with the new log tag mappings.
func AddRPCTagsToContext(ctx context.Context, logTagsToAdd CtxRPCTags) context.Context {
	currTags, ok := TagsFromContext(ctx)
	if !ok {
		currTags = make(CtxRPCTags)
	}
	for key, tag := range logTagsToAdd {
		currTags[key] = tag
	}

	return context.WithValue(ctx, CtxRPCTagsKey, currTags)
}

// TagsFromContext returns the tags being passed along with the given context.
func TagsFromContext(ctx context.Context) (CtxRPCTags, bool) {
	logTags, ok := ctx.Value(CtxRPCTagsKey).(CtxRPCTags)
	if ok {
		ret := make(CtxRPCTags)
		for k, v := range logTags {
			ret[k] = v
		}
		return ret, true
	}
	return nil, false
}
