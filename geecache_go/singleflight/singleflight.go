package singleflight

import (
	"context"
	"fmt"
	"sync"
)

// call 表示一次正在进行中或已完成的函数调用。
// 多个请求同一个 key 时，只有第一个请求会真正执行函数，
// 其余请求等待 done 信号后共享同一个结果。
type call struct {
	done chan struct{} // 调用完成信号
	val  interface{}   // 函数调用的返回值
	err  error         // 函数调用的错误信息
}

// Group 是 singleflight 的核心结构，管理一组正在进行的调用。
// 对于同一个 key 的并发请求，保证同一时刻只有一个函数在执行（请求合并）。
type Group struct {
	mu sync.Mutex       // 保护 m 的并发访问
	m  map[string]*call // key -> 正在进行的调用，懒初始化
}

// Do 的作用就是，针对相同的 key，无论 Do 被调用多少次，函数 fn 都只会被调用一次，
// 若有重复请求进来，重复请求会等待第一个请求完成后共享其结果（返回值或错误）
// 从而避免对同一资源（如数据库）的重复请求（防止缓存击穿）。
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	return g.DoContext(context.Background(), key, fn)
}

// DoContext 与 Do 类似，但等待已有 in-flight 请求时可被 ctx 取消。
// 若 key 对应请求已在执行，则等待其完成或 ctx 取消（先发生者先返回）。
func (g *Group) DoContext(ctx context.Context, key string, fn func() (interface{}, error)) (interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		// 该 key 已有请求在执行，等待其完成或 ctx 取消
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
			return c.val, c.err
		}
	}
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return nil, err
	}

	// 第一个请求：创建 call 并注册到 map
	c := &call{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()

	// 执行实际的数据加载函数，用 defer 保证 panic 时也能关闭 done channel，
	// 避免等待该 key 的 goroutine 永久阻塞。
	func() {
		defer func() {
			if r := recover(); r != nil {
				c.err = fmt.Errorf("singleflight: fn panicked: %v", r)
			}
			close(c.done) // 通知所有等待的请求：结果已就绪（或已 panic）
		}()
		c.val, c.err = fn()
	}()

	// 调用完成后从 map 中删除，允许后续请求重新发起加载
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}
