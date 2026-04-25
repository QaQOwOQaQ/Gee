package lru

import (
	"container/list"
	"time"
)

// Cache 是一个支持冷热分区的 LRU 缓存，不支持并发访问（非线程安全）。
//
// 冷热分区解决了标准 LRU 的两个经典问题：
//  1. 预读失效：批量顺序读取的数据只进冷区，不会污染热区的真实热点数据
//  2. 热点数据污染：只有被二次访问且在冷区停留超过保护窗口的数据才能进入热区
//
// 结构示意：
//
//	热区 warmList：[最近访问] → ... → [较旧]   （占总容量 63%）
//	冷区 coldList：[新插入]   → ... → [最旧]   （占总容量 37%）
//
// 淘汰顺序：优先淘汰冷区尾部，冷区为空时才淘汰热区尾部。
type Cache struct {
	maxBytes           int64         // 允许使用的最大内存字节数，0 表示不限制
	nbytes             int64         // 当前已使用的内存字节数
	warmBytes          int64         // 热区当前字节数
	warmList           *list.List    // 热区链表：存放被二次访问的热点数据
	coldList           *list.List    // 冷区链表：存放新插入的数据
	cache              map[string]*list.Element // 哈希表，key -> 链表节点，用于 O(1) 查找
	coldProtectWindow  time.Duration // 冷区保护窗口：数据在冷区停留超过此时长才允许升入热区，0 表示不保护
	// 可选的回调函数，当某条记录被淘汰时执行（例如用于通知或持久化）
	OnEvicted func(key string, value Value)
}

// entry 是存储在链表节点中的数据结构。
// inWarm 标记该条目当前所在的区域，用于 Get 时判断是否需要升区。
// expireAt 为零值时表示该条目永不过期。
// coldSince 记录数据进入冷区的时间，用于冷区保护窗口判断。
type entry struct {
	key        string
	value      Value
	expireAt   time.Time // 过期时间，零值表示不过期
	inWarm     bool      // true 表示在热区，false 表示在冷区
	coldSince  time.Time // 进入冷区的时间，用于保护窗口判断
}

// Value 是缓存值需要实现的接口，Len() 用于计算该值占用的字节数。
type Value interface {
	Len() int
}

// New 是 Cache 的构造函数。
// maxBytes: 最大内存限制（字节），0 表示不限制。
// onEvicted: 记录被淘汰时的回调函数，可传 nil。
// coldProtectWindow: 冷区保护窗口时长，数据在冷区停留超过此时长才允许升入热区。
//
//	参考 MySQL innodb_old_blocks_time 默认值 1s，传 0 表示不启用保护窗口。
func New(maxBytes int64, onEvicted func(string, Value), coldProtectWindow time.Duration) *Cache {
	return &Cache{
		maxBytes:          maxBytes,
		warmList:          list.New(),
		coldList:          list.New(),
		cache:             make(map[string]*list.Element),
		coldProtectWindow: coldProtectWindow,
		OnEvicted:         onEvicted,
	}
}

// Add 向缓存中添加或更新一个键值对。
// ttl: 过期时长，0 表示永不过期。
//
// 新 key：插入冷区头部，记录进入冷区的时间。
// 已存在的 key：更新值和过期时间；若在热区则移到热区头部，若在冷区则直接更新（不升区，等待下次 Get 升区）。
// 添加后若超出内存限制，则循环淘汰直到满足限制。
func (c *Cache) Add(key string, value Value, ttl time.Duration) {
	var expireAt time.Time
	if ttl > 0 {
		expireAt = time.Now().Add(ttl)
	}

	if ele, ok := c.cache[key]; ok {
		kv := ele.Value.(*entry)
		diff := int64(value.Len()) - int64(kv.value.Len())
		if kv.inWarm {
			c.warmList.MoveToFront(ele)
			c.warmBytes += diff
		} else {
			c.coldList.MoveToFront(ele)
		}
		c.nbytes += diff
		kv.value = value
		kv.expireAt = expireAt
	} else {
		// 新数据插入冷区头部，记录进入冷区的时间
		ele := c.coldList.PushFront(&entry{
			key:       key,
			value:     value,
			expireAt:  expireAt,
			inWarm:    false,
			coldSince: time.Now(),
		})
		c.cache[key] = ele
		c.nbytes += int64(len(key)) + int64(value.Len())
	}
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

// Get 查找并返回 key 对应的缓存值。
// 若该条目已过期，则惰性删除并返回未命中。
//
// 冷区命中时：
//   - 若在冷区停留时间超过 coldProtectWindow，升入热区头部
//   - 否则仅移到冷区头部，不升区（防止短时间内的偶发访问污染热区）
//
// 热区命中时：移到热区头部。
func (c *Cache) Get(key string) (value Value, ok bool) {
	ele, ok := c.cache[key]
	if !ok {
		return
	}
	kv := ele.Value.(*entry)

	// 惰性过期检测
	if !kv.expireAt.IsZero() && time.Now().After(kv.expireAt) {
		c.removeElement(ele)
		return nil, false
	}

	if kv.inWarm {
		// 热区命中：移到热区头部
		c.warmList.MoveToFront(ele)
	} else {
		// 冷区命中：检查是否超过保护窗口
		if c.coldProtectWindow == 0 || time.Since(kv.coldSince) > c.coldProtectWindow {
			// 超过保护窗口（或不启用保护），升入热区
			entryBytes := int64(len(kv.key)) + int64(kv.value.Len())
			c.coldList.Remove(ele)
			kv.inWarm = true
			newEle := c.warmList.PushFront(kv)
			c.cache[key] = newEle
			c.warmBytes += entryBytes
			// 升区后检查热区是否超出 63% 容量限制，超出则降级热区尾部到冷区
			c.rebalanceWarm()
		} else {
			// 未超过保护窗口：仅移到冷区头部，不升区
			c.coldList.MoveToFront(ele)
		}
	}
	return kv.value, true
}

// RemoveOldest 按优先级淘汰最久未使用的条目：优先淘汰冷区尾部，冷区为空时淘汰热区尾部。
func (c *Cache) RemoveOldest() {
	if c.coldList.Len() > 0 {
		c.removeElement(c.coldList.Back())
	} else if c.warmList.Len() > 0 {
		c.removeElement(c.warmList.Back())
	}
}

// removeElement 从所在链表和哈希表中删除节点，更新字节计数，并触发回调。
func (c *Cache) removeElement(ele *list.Element) {
	kv := ele.Value.(*entry)
	entryBytes := int64(len(kv.key)) + int64(kv.value.Len())
	if kv.inWarm {
		c.warmList.Remove(ele)
		c.warmBytes -= entryBytes
	} else {
		c.coldList.Remove(ele)
	}
	delete(c.cache, kv.key)
	c.nbytes -= entryBytes
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
}

// rebalanceWarm 检查热区是否超出 63% 容量限制。
// 超出时将热区尾部条目降级到冷区头部，直到满足限制。
func (c *Cache) rebalanceWarm() {
	if c.maxBytes == 0 {
		return // 不限制容量时无需平衡
	}
	warmLimit := c.maxBytes * 63 / 100
	for c.warmBytes > warmLimit && c.warmList.Len() > 0 {
		// 将热区尾部降级到冷区头部
		tail := c.warmList.Back()
		kv := tail.Value.(*entry)
		entryBytes := int64(len(kv.key)) + int64(kv.value.Len())
		c.warmList.Remove(tail)
		kv.inWarm = false
		kv.coldSince = time.Now()
		newEle := c.coldList.PushFront(kv)
		c.cache[kv.key] = newEle
		c.warmBytes -= entryBytes
	}
}

// Remove 主动删除指定 key 的缓存条目（Cache Aside 写路径：数据库更新后调用此方法）。
// 若 key 不存在则不做任何操作。
func (c *Cache) Remove(key string) {
	if ele, ok := c.cache[key]; ok {
		c.removeElement(ele)
	}
}

// Len 返回当前缓存中的条目总数（冷区 + 热区）。
func (c *Cache) Len() int {
	return c.coldList.Len() + c.warmList.Len()
}

// EntryInfo 是测试辅助结构，暴露条目的过期时间。
type EntryInfo struct {
	ExpireAt time.Time
}

// GetEntryForTest 返回指定 key 的条目信息，仅用于测试。
func (c *Cache) GetEntryForTest(key string) (EntryInfo, bool) {
	ele, ok := c.cache[key]
	if !ok {
		return EntryInfo{}, false
	}
	kv := ele.Value.(*entry)
	return EntryInfo{ExpireAt: kv.expireAt}, true
}
