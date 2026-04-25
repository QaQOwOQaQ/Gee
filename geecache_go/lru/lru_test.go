package lru

import (
	"reflect"
	"testing"
)

// String 是测试用的自定义类型，实现了 Value 接口。
type String string

// Len 返回字符串的字节长度，实现 lru.Value 接口。
func (d String) Len() int {
	return len(d)
}

// TestGet 测试缓存的基本读取功能：命中返回正确值，未命中返回 false。
func TestGet(t *testing.T) {
	lru := New(int64(0), nil, 0)
	lru.Add("key1", String("1234"), 0)
	if v, ok := lru.Get("key1"); !ok || string(v.(String)) != "1234" {
		t.Fatalf("cache hit key1=1234 failed")
	}
	if _, ok := lru.Get("key2"); ok {
		t.Fatalf("cache miss key2 failed")
	}
}

// TestRemoveoldest 测试 LRU 淘汰策略：
// 当缓存达到容量上限时，最久未使用的条目（key1）应被自动淘汰。
func TestRemoveoldest(t *testing.T) {
	k1, k2, k3 := "key1", "key2", "k3"
	v1, v2, v3 := "value1", "value2", "v3"
	cap := len(k1 + k2 + v1 + v2)
	lru := New(int64(cap), nil, 0)
	lru.Add(k1, String(v1), 0)
	lru.Add(k2, String(v2), 0)
	lru.Add(k3, String(v3), 0) // 添加 k3 触发淘汰，key1 应被移除

	if _, ok := lru.Get("key1"); ok || lru.Len() != 2 {
		t.Fatalf("Removeoldest key1 failed")
	}
}

// TestOnEvicted 测试淘汰回调函数：
// 被淘汰的 key 应按顺序触发回调，回调中记录被淘汰的 key 列表。
func TestOnEvicted(t *testing.T) {
	keys := make([]string, 0)
	callback := func(key string, value Value) {
		keys = append(keys, key)
	}
	lru := New(int64(10), callback, 0)
	lru.Add("key1", String("123456"), 0)
	lru.Add("k2", String("k2"), 0)
	lru.Add("k3", String("k3"), 0)
	lru.Add("k4", String("k4"), 0)

	expect := []string{"key1", "k2"}

	if !reflect.DeepEqual(expect, keys) {
		t.Fatalf("Call OnEvicted failed, expect keys equals to %s", expect)
	}
}

// TestAdd 测试更新已有 key 时字节计数是否正确更新：
// 同一个 key 更新值后，nbytes 应反映新值的大小而非累加。
func TestAdd(t *testing.T) {
	lru := New(int64(0), nil, 0)
	lru.Add("key", String("1"), 0)
	lru.Add("key", String("111"), 0) // 更新 key 的值

	if lru.nbytes != int64(len("key")+len("111")) {
		t.Fatal("expected 6 but got", lru.nbytes)
	}
}
