package geecache

// 缓存查询流程：
//
//	接收 key
//	  │
//	  ▼
//	本地缓存命中？──是──▶ 返回缓存值 ⑴
//	  │否
//	  ▼
//	有远程节点且 key 不归属自身？──是──▶ HTTP 请求远程节点 ──▶ 返回缓存值 ⑵
//	  │否                                   │失败
//	  ▼                                     ▼
//	调用 Getter 从数据源加载 ◀──────────────┘
//	  │
//	  ▼
//	写入本地缓存，返回缓存值 ⑶

import (
	"fmt"
	pb "geecache/geecachepb"
	"geecache/singleflight"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Group 是缓存的命名空间，也是 GeeCache 最核心的数据结构。
// 每个 Group 拥有唯一的名称、一个本地缓存、一个数据加载回调，以及分布式节点选择器。
// 不同业务数据（如用户信息、商品信息）可使用不同名称的 Group 隔离存储。
type Group struct {
	name       string                // 缓存命名空间的名称，全局唯一
	getter     Getter                // 缓存未命中时，用于从数据源加载数据的回调
	mainCache  cache                 // 本地并发安全的 LRU 缓存
	peers      PeerPicker            // 分布式节点选择器，用于将请求路由到正确的远程节点
	ttl        time.Duration         // 缓存条目的基础过期时长，0 表示永不过期
	ttlJitter  time.Duration         // TTL 随机抖动范围，实际 TTL 在 [ttl-jitter, ttl+jitter] 之间，防止缓存雪崩
	randMu     sync.Mutex            // 保护 randSrc 并发访问
	randSrc    *rand.Rand            // Group 独立随机源，避免全局 rand 锁竞争
	loadMu     sync.Mutex            // 保护 loadStates，并协调 Delete 与 populateCache
	loadStates map[string]*loadState // 仅在本地加载进行中时记录 key 状态，防止 stale load 回填
	// loader 使用 singleflight 确保同一个 key 的并发请求只触发一次数据加载
	loader *singleflight.Group
}

type loadState struct {
	generation uint64
	inFlight   int
}

// Getter 是数据源加载接口。
// 当缓存未命中时，GeeCache 会调用此接口从数据源（如数据库）加载数据。
type Getter interface {
	Get(key string) ([]byte, error)
}

// GetterFunc 是函数类型，实现了 Getter 接口。
// 允许直接将普通函数作为 Getter 使用（函数式适配器模式）。
type GetterFunc func(key string) ([]byte, error)

// Get 实现 Getter 接口，直接调用函数本身。
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

var (
	mu     sync.RWMutex              // 保护全局 groups map 的读写锁
	groups = make(map[string]*Group) // 全局 Group 注册表，按名称索引
)

// NewGroup 创建并注册一个新的缓存 Group。
// name: 命名空间名称（全局唯一）
// cacheBytes: 本地缓存最大内存限制（字节）
// getter: 缓存未命中时的数据加载回调，不能为 nil
// ttl: 缓存条目基础过期时长，0 表示永不过期
// ttlJitter: TTL 随机抖动范围，实际 TTL 在 [ttl-jitter, ttl+jitter] 之间，防止缓存雪崩；0 表示不抖动
func NewGroup(name string, cacheBytes int64, getter Getter, ttl time.Duration, ttlJitter time.Duration) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:       name,
		getter:     getter,
		mainCache:  cache{cacheBytes: cacheBytes, coldProtectWindow: time.Second},
		loader:     &singleflight.Group{},
		ttl:        ttl,
		ttlJitter:  ttlJitter,
		randSrc:    rand.New(rand.NewSource(time.Now().UnixNano())),
		loadStates: make(map[string]*loadState),
	}
	groups[name] = g
	return g
}

// SetColdProtectWindow 设置冷区保护窗口时长（覆盖默认值 1s）。
// 必须在 Group 首次写入缓存之前调用，否则不影响已创建的底层 LRU 实例。
// coldProtectWindow 参照 MySQL innodb_old_blocks_time，防止顺序扫描误将冷数据升入热区。
// 传入 0 表示关闭冷区保护。
func (g *Group) SetColdProtectWindow(d time.Duration) *Group {
	g.mainCache.mu.Lock()
	g.mainCache.coldProtectWindow = d
	g.mainCache.mu.Unlock()
	return g
}

// GetGroup 根据名称返回已注册的 Group，若不存在则返回 nil。
// 使用读锁以支持并发查询。
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// Get 是 Group 的核心查询方法，按以下顺序获取缓存值：
// 1. 查询本地缓存（命中直接返回）
// 2. 缓存未命中，调用 load 从远程节点或本地数据源加载
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}

	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] 缓存命中")
		return v, nil
	}

	return g.load(key)
}

// RegisterPeers 为 Group 注册分布式节点选择器。
// 只能调用一次，重复调用会 panic。
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

// load 负责在缓存未命中时加载数据，使用 singleflight 防止缓存击穿。
// 无论有多少个并发请求同时请求同一个 key，实际的数据加载函数只会执行一次。
// 加载优先级：远程节点 > 本地数据源。
func (g *Group) load(key string) (value ByteView, err error) {
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[GeeCache] 从远程节点获取失败", err)
			}
		}
		// 远程节点不存在或获取失败，回退到本地数据源
		return g.getLocally(key)
	})

	if err == nil {
		return viewi.(ByteView), nil
	}
	return
}

// populateCache 将从数据源加载的数据写入本地缓存，供后续请求直接命中。
// 实际 TTL = 基础 TTL + [-jitter, +jitter] 的随机偏移，防止大量 key 同时过期导致缓存雪崩。
// TTL 为 0 时表示永不过期，不施加抖动。
func (g *Group) populateCache(key string, value ByteView, expectedGeneration uint64) bool {
	ttl := g.ttl
	if ttl > 0 && g.ttlJitter > 0 {
		// 在 [-jitter, +jitter] 范围内随机偏移
		g.randMu.Lock()
		offset := time.Duration(g.randSrc.Int63n(int64(2*g.ttlJitter))) - g.ttlJitter
		g.randMu.Unlock()
		ttl += offset
		if ttl <= 0 {
			ttl = 1 // 至少保留 1ns，避免退化为永不过期
		}
	}

	g.loadMu.Lock()
	defer g.loadMu.Unlock()
	state, ok := g.loadStates[key]
	if !ok || state.generation != expectedGeneration {
		return false
	}
	g.mainCache.add(key, value, ttl)
	return true
}

func (g *Group) beginLocalLoad(key string) uint64 {
	g.loadMu.Lock()
	defer g.loadMu.Unlock()
	state := g.loadStates[key]
	if state == nil {
		state = &loadState{}
		g.loadStates[key] = state
	}
	state.inFlight++
	return state.generation
}

func (g *Group) endLocalLoad(key string) {
	g.loadMu.Lock()
	defer g.loadMu.Unlock()
	state := g.loadStates[key]
	if state == nil {
		return
	}
	state.inFlight--
	if state.inFlight == 0 {
		delete(g.loadStates, key)
	}
}

func (g *Group) invalidateLocal(key string) {
	g.loadMu.Lock()
	if state := g.loadStates[key]; state != nil {
		state.generation++
	}
	g.loadMu.Unlock()
	g.mainCache.remove(key)
}

// Delete 使指定 key 的缓存失效（Cache Aside 写路径）。
// 语义：
//  1. 始终先删除当前节点本地缓存；若该 key 有本地 in-flight load，则递增 generation 防止旧值回填。
//  2. 若 key owner 在远端，则请求 owner 节点失效；失败返回 error。
//  3. 若当前节点就是 owner，则本地失效后返回 nil。
func (g *Group) Delete(key string) error {
	if key == "" {
		return fmt.Errorf("key is required")
	}
	g.invalidateLocal(key)
	if g.peers != nil {
		if peer, ok := g.peers.PickPeer(key); ok {
			req := &pb.Request{Group: g.name, Key: key}
			if err := peer.Delete(req); err != nil {
				return fmt.Errorf("delete from owner peer failed: %w", err)
			}
		}
	}
	log.Printf("[GeeCache] 缓存失效 key=%s", key)
	return nil
}

// getLocally 通过 Getter 回调从本地数据源（如数据库）加载数据，
// 加载成功后在 generation 一致时写入本地缓存并返回。
func (g *Group) getLocally(key string) (ByteView, error) {
	expectedGeneration := g.beginLocalLoad(key)
	defer g.endLocalLoad(key)

	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes)}
	if !g.populateCache(key, value, expectedGeneration) {
		log.Printf("[GeeCache] 跳过旧值回填 key=%s", key)
	}
	return value, nil
}

// getFromPeer 通过 PeerGetter 接口从指定远程节点获取缓存数据。
// 使用 protobuf 编码请求和响应，提高序列化效率。
func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: res.Value}, nil
}
