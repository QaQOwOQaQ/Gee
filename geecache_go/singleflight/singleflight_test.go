package singleflight

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestDo 测试 Do 方法的基本功能：
// 对给定 key 执行函数，返回值应与函数返回值一致，且不应有错误。
// TestDoContextCanceled 测试 DoContext 在等待已有请求时可以被 ctx 取消。
func TestDoContextCanceled(t *testing.T) {
	var g Group
	start := make(chan struct{})
	release := make(chan struct{})
	// 首个请求占住 key
	go func() {
		_, _ = g.Do("key", func() (interface{}, error) {
			close(start)
			<-release
			return "ok", nil
		})
	}()
	<-start

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := g.DoContext(ctx, "key", func() (interface{}, error) {
		return nil, errors.New("should not run")
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}

	close(release)
}
