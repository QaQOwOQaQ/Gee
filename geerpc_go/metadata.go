package geerpc

import (
	"context"
	"strings"
)

const (
	MetadataTraceIDKey   = "trace_id"
	MetadataRequestIDKey = "request_id"
)

type metadataContextKey struct{}

func cloneMetadata(md map[string]string) map[string]string {
	if len(md) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(md))
	for key, value := range md {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		cloned[key] = value
	}
	if len(cloned) == 0 {
		return nil
	}
	return cloned
}

// WithMetadata 把轻量级字符串 metadata 注入 context，供客户端透传到服务端。
func WithMetadata(ctx context.Context, md map[string]string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	merged := MetadataFromContext(ctx)
	for key, value := range cloneMetadata(md) {
		merged[key] = value
	}
	if len(merged) == 0 {
		return ctx
	}
	return context.WithValue(ctx, metadataContextKey{}, merged)
}

// MetadataFromContext 读取 context 中的 metadata 副本。
func MetadataFromContext(ctx context.Context) map[string]string {
	if ctx == nil {
		return map[string]string{}
	}
	md, _ := ctx.Value(metadataContextKey{}).(map[string]string)
	if md == nil {
		return map[string]string{}
	}
	cloned := cloneMetadata(md)
	if cloned == nil {
		return map[string]string{}
	}
	return cloned
}

// WithTraceID 为当前请求补一个 trace_id。
func WithTraceID(ctx context.Context, traceID string) context.Context {
	traceID = strings.TrimSpace(traceID)
	if traceID == "" {
		if ctx == nil {
			return context.Background()
		}
		return ctx
	}
	return WithMetadata(ctx, map[string]string{
		MetadataTraceIDKey: traceID,
	})
}
