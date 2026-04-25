package xclient

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"
	"sync"
)

// ConsistentHashDiscovery 实现一致性哈希负载均衡。
// 它将每个真实节点映射为多个虚拟节点分散在哈希环上，
// 请求按 key 哈希后顺时针找到第一个虚拟节点，即为目标节点。
// 虚拟节点越多，节点分布越均匀，但内存占用也越大。
type ConsistentHashDiscovery struct {
	mu       sync.RWMutex
	replicas int               // 每个真实节点对应的虚拟节点数量
	ring     []uint32          // 哈希环，存储所有虚拟节点的哈希值（有序）
	nodeMap  map[uint32]string // 虚拟节点哈希值 → 真实节点地址
}

// NewConsistentHashDiscovery 创建一致性哈希服务发现实例。
// replicas 建议设为 100~200，过小会导致分布不均匀。
func NewConsistentHashDiscovery(addrs []string, replicas int) *ConsistentHashDiscovery {
	if replicas <= 0 {
		replicas = 100
	}
	d := &ConsistentHashDiscovery{
		replicas: replicas,
		nodeMap:  make(map[uint32]string),
	}
	d.addNodes(addrs)
	return d
}

// hashKey 计算字符串的 SHA256 哈希，取前 4 字节转为 uint32。
func hashKey(key string) uint32 {
	sum := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(sum[:4])
}

// addNodes 把一批节点地址映射为虚拟节点，加入哈希环。
func (d *ConsistentHashDiscovery) addNodes(addrs []string) {
	for _, addr := range addrs {
		for i := 0; i < d.replicas; i++ {
			virtualKey := hashKey(addr + string(rune(i)))
			d.ring = append(d.ring, virtualKey)
			d.nodeMap[virtualKey] = addr
		}
	}
	sort.Slice(d.ring, func(i, j int) bool { return d.ring[i] < d.ring[j] })
}

func (d *ConsistentHashDiscovery) Refresh() error { return nil }

// Update 重建哈希环。
func (d *ConsistentHashDiscovery) Update(addrs []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ring = nil
	d.nodeMap = make(map[uint32]string)
	d.addNodes(addrs)
	return nil
}

// Get 根据 SelectMode 选择节点。
// 一致性哈希需要 key 来决定路由，直接调用 Get 时使用固定 key "default"。
// 如果需要按 key 路由，请直接调用 GetByKey。
func (d *ConsistentHashDiscovery) Get(_ SelectMode) (string, error) {
	return d.GetByKey("default")
}

// GetByKey 根据指定 key 在哈希环上顺时针查找最近的节点。
// key 应该使用稳定的业务标识，例如用户 ID、租户 ID、分片键；
// 如果传入每次都变化的请求 ID、时间戳或随机数，就无法得到稳定路由。
//
// 当节点集合发生变化时，一致性哈希会尽量只迁移少量 key，
// 但不会保证旧 key 100% 还落在同一节点上。
func (d *ConsistentHashDiscovery) GetByKey(key string) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.ring) == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	h := hashKey(key)
	// 二分查找第一个 >= h 的虚拟节点。
	idx := sort.Search(len(d.ring), func(i int) bool { return d.ring[i] >= h })
	// 如果超过环尾，回绕到第一个节点。
	if idx == len(d.ring) {
		idx = 0
	}
	return d.nodeMap[d.ring[idx]], nil
}

func (d *ConsistentHashDiscovery) getExcluding(_ SelectMode, exclude map[string]struct{}) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.ring) == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	start := sort.Search(len(d.ring), func(i int) bool { return d.ring[i] >= hashKey("default") })
	for i := 0; i < len(d.ring); i++ {
		idx := (start + i) % len(d.ring)
		addr := d.nodeMap[d.ring[idx]]
		if _, skipped := exclude[addr]; skipped {
			continue
		}
		return addr, nil
	}
	return "", errNoUntriedServers
}

// GetAll 返回所有真实节点地址（去重）。
func (d *ConsistentHashDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	seen := make(map[string]struct{})
	var addrs []string
	for _, addr := range d.nodeMap {
		if _, ok := seen[addr]; !ok {
			seen[addr] = struct{}{}
			addrs = append(addrs, addr)
		}
	}
	return addrs, nil
}
