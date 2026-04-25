package geecache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// db 模拟数据库，存储测试用的 key-value 数据。
var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

var geecacheTestSeq uint64

func uniqueTestGroupName(prefix string) string {
	id := atomic.AddUint64(&geecacheTestSeq, 1)
	return fmt.Sprintf("%s_%d", prefix, id)
}

// TestGetter 测试 GetterFunc 类型是否正确实现了 Getter 接口。
// GetterFunc 是函数适配器，允许将普通函数直接用作 Getter。
func TestGetter(t *testing.T) {
	var f Getter = GetterFunc(func(key string) ([]byte, error) {
		return []byte(key), nil
	})

	expect := []byte("key")
	if v, _ := f.Get("key"); string(v) != string(expect) {
		t.Fatal("callback failed")
	}
}

// TestGet 测试 Group 的缓存读取流程：
// 1. 第一次 Get：缓存未命中，通过 Getter 从「数据库」加载并写入缓存。
// 2. 第二次 Get：缓存命中，不再调用 Getter（loadCounts 不增加）。
// 3. 查询不存在的 key：应返回错误。
func TestGet(t *testing.T) {
	loadCounts := make(map[string]int, len(db))
	gee := NewGroup(uniqueTestGroupName("scores"), 2<<10, GetterFunc(
		func(key string) ([]byte, error) {
			if v, ok := db[key]; ok {
				loadCounts[key]++ // 记录该 key 被从数据库加载的次数
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}), 0, 0)

	for k, v := range db {
		// 第一次查询：应从数据库加载
		if view, err := gee.Get(k); err != nil || view.String() != v {
			t.Fatal("failed to get value")
		}
		// 第二次查询：应命中缓存，loadCounts 不超过 1
		if _, err := gee.Get(k); err != nil || loadCounts[k] > 1 {
			t.Fatalf("cache %s miss", k)
		}
	}

	// 查询不存在的 key，应返回错误
	if view, err := gee.Get("unknown"); err == nil {
		t.Fatalf("the value of unknown should be empty, but %s got", view)
	}
}

// TestGetGroup 测试 NewGroup 和 GetGroup 的注册与查找功能：
// 已注册的 group 应能正确查找到，未注册的 group 应返回 nil。
func TestGetGroup(t *testing.T) {
	groupName := uniqueTestGroupName("scores")
	NewGroup(groupName, 2<<10, GetterFunc(
		func(key string) (bytes []byte, err error) { return }), 0, 0)
	if group := GetGroup(groupName); group == nil || group.name != groupName {
		t.Fatalf("group %s not exist", groupName)
	}

	// 查询不存在的 group，应返回 nil
	if group := GetGroup(groupName + "111"); group != nil {
		t.Fatalf("expect nil, but %s got", group.name)
	}
}

func TestDeleteReturnsErrorOnEmptyKey(t *testing.T) {
	g := NewGroup(uniqueTestGroupName("delete_empty"), 1<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("v"), nil
	}), 0, 0)
	if err := g.Delete(""); err == nil {
		t.Fatalf("expected error for empty key")
	}
}

func TestDeleteWriteInvalidationReloadsNewValue(t *testing.T) {
	groupName := uniqueTestGroupName("delete_reload")
	var mu sync.Mutex
	store := map[string]string{"Tom": "630"}
	loads := int32(0)
	g := NewGroup(groupName, 1<<20, GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt32(&loads, 1)
		mu.Lock()
		defer mu.Unlock()
		return []byte(store[key]), nil
	}), 0, 0)

	v, err := g.Get("Tom")
	if err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	if v.String() != "630" {
		t.Fatalf("want 630 got %s", v.String())
	}

	mu.Lock()
	store["Tom"] = "999"
	mu.Unlock()
	if err := g.Delete("Tom"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	v, err = g.Get("Tom")
	if err != nil {
		t.Fatalf("second get failed: %v", err)
	}
	if v.String() != "999" {
		t.Fatalf("want 999 got %s", v.String())
	}
	if atomic.LoadInt32(&loads) < 2 {
		t.Fatalf("expected reload after delete, loads=%d", loads)
	}
}

func TestDeletePreventsStaleLoadBackfill(t *testing.T) {
	groupName := uniqueTestGroupName("delete_race")
	loadStarted := make(chan struct{})
	releaseLoad := make(chan struct{})
	var callCount int32

	var mu sync.Mutex
	store := map[string]string{"Tom": "old"}
	g := NewGroup(groupName, 1<<20, GetterFunc(func(key string) ([]byte, error) {
		n := atomic.AddInt32(&callCount, 1)
		if n == 2 { // 只阻塞第二次加载
			loadStarted <- struct{}{}
			<-releaseLoad
		}
		mu.Lock()
		defer mu.Unlock()
		return []byte(store[key]), nil
	}), 0, 0)

	// 第一次加载不阻塞，缓存 old
	v, err := g.Get("Tom")
	if err != nil {
		t.Fatalf("initial get failed: %v", err)
	}
	if v.String() != "old" {
		t.Fatalf("want old got %s", v.String())
	}

	// 删除缓存，下次 Get 会重新进入加载路径
	if err := g.Delete("Tom"); err != nil {
		t.Fatalf("delete before race failed: %v", err)
	}

	done := make(chan struct{})
	go func() {
		_, _ = g.Get("Tom") // 第二次加载，会阻塞
		close(done)
	}()

	<-loadStarted // 确保旧 load 已经开始并阻塞

	mu.Lock()
	store["Tom"] = "new"
	mu.Unlock()

	if err := g.Delete("Tom"); err != nil {
		t.Fatalf("delete during load failed: %v", err)
	}

	close(releaseLoad) // 放行旧 load
	<-done

	v, err = g.Get("Tom")
	if err != nil {
		t.Fatalf("final get failed: %v", err)
	}
	if v.String() != "new" {
		t.Fatalf("stale backfill detected, want new got %s", v.String())
	}
}

func TestTTLExpiryReloadsValue(t *testing.T) {
	groupName := uniqueTestGroupName("ttl_reload")
	loads := int32(0)
	g := NewGroup(groupName, 1<<20, GetterFunc(func(key string) ([]byte, error) {
		n := atomic.AddInt32(&loads, 1)
		return []byte(fmt.Sprintf("v%d", n)), nil
	}), 30*time.Millisecond, 0)

	v1, err := g.Get("Tom")
	if err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	v2, err := g.Get("Tom")
	if err != nil {
		t.Fatalf("second get failed: %v", err)
	}
	if v1.String() == v2.String() {
		t.Fatalf("expected reload after ttl expiry, got same value %s", v1.String())
	}
}

func TestTTLJitterRange(t *testing.T) {
	groupName := uniqueTestGroupName("ttl_jitter")
	base := 80 * time.Millisecond
	jitter := 30 * time.Millisecond
	g := NewGroup(groupName, 1<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("v"), nil
	}), base, jitter)

	// 多次 delete + get 触发重新写入，统计过期时间范围
	for i := 0; i < 30; i++ {
		if err := g.Delete("Tom"); err != nil {
			t.Fatalf("delete failed: %v", err)
		}
		if _, err := g.Get("Tom"); err != nil {
			t.Fatalf("get failed: %v", err)
		}
		g.mainCache.mu.Lock()
		if g.mainCache.lru == nil {
			g.mainCache.mu.Unlock()
			t.Fatalf("lru should be initialized")
		}
		ele, ok := g.mainCache.lru.GetEntryForTest("Tom")
		g.mainCache.mu.Unlock()
		if !ok {
			t.Fatalf("entry should exist")
		}
		expireAt := ele.ExpireAt
		if expireAt.IsZero() {
			t.Fatalf("expected ttl entry")
		}
		delta := time.Until(expireAt)
		min := base - jitter - 10*time.Millisecond
		max := base + jitter + 10*time.Millisecond
		if delta < min || delta > max {
			t.Fatalf("ttl out of jitter range, got %v want [%v,%v]", delta, min, max)
		}
	}
}
