package geecache

import (
	"geecache/lru"
	"sync"
	"time"
)

// cache 是对 lru.Cache 的并发安全封装。
// 通过互斥锁保证多 goroutine 同时读写时的数据安全。
// 采用懒初始化：第一次添加数据时才创建底层 lru.Cache 实例。
type cache struct {
	mu                sync.Mutex     // 互斥锁，保护并发访问
	lru               *lru.Cache     // 底层 LRU 缓存实例（懒初始化）
	cacheBytes        int64          // 该缓存允许使用的最大内存字节数
	coldProtectWindow time.Duration  // 冷区保护窗口，传给底层 LRU
}

// add 线程安全地向缓存中添加一个键值对。
// ttl: 过期时长，0 表示永不过期。
// 若底层 lru.Cache 尚未初始化，则在此时按 cacheBytes 限制创建。
// 保护窗口设为 1 秒（参考 MySQL innodb_old_blocks_time 默认值），防止顺序扫描误升热区。
func (c *cache) add(key string, value ByteView, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, nil, c.coldProtectWindow)
	}
	c.lru.Add(key, value, ttl)
}

// get 线程安全地从缓存中查找 key 对应的值。
// 若缓存未初始化、key 不存在或已过期，返回零值和 false。
func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}

	if v, ok := c.lru.Get(key); ok {
		return v.(ByteView), ok
	}

	return
}

// remove 线程安全地主动删除指定 key 的缓存条目。
// 用于 Cache Aside 写路径：数据库更新后调用此方法使缓存失效。
func (c *cache) remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}
	c.lru.Remove(key)
}
