package consistenthash

import (
	"hash/crc32"
)

const slotCount = 16384 // 哈希槽总数，与 Redis Cluster 保持一致

// Hash 是哈希函数类型，将字节数据映射为 uint32 整数。
// 默认使用 crc32.ChecksumIEEE，也可自定义（便于测试）。
type Hash func(data []byte) uint32

// Map 是基于固定槽位的分布式哈希实现，参考 Redis Cluster 的槽机制。
//
// 核心思路：
//   - 将 key 空间划分为固定的 16384 个槽（slot）
//   - 每个 key 通过 hash(key) % 16384 映射到一个槽
//   - 每个槽归属于某个节点，节点均分所有槽
//
// 注意：这不是传统的一致性哈希环 + 虚拟节点方案。
// 节点变更时会触发全量 rebalance（约 50% 槽迁移），而非仅迁移少量 key。
// 优势是 O(1) 查找和槽归属显式可控。
type Map struct {
	hash  Hash               // 哈希函数
	slots [slotCount]string  // 每个槽归属的节点名称，空字符串表示无节点
	nodes []string           // 当前所有节点列表，维护顺序用于均分槽
}

// New 创建一个槽机制哈希实例。
// fn: 自定义哈希函数，传 nil 则使用默认的 crc32.ChecksumIEEE。
func New(fn Hash) *Map {
	m := &Map{hash: fn}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add 将一批节点加入，并重新均分所有槽。
// 每次调用后所有节点重新均分 16384 个槽，保证分布尽量均匀。
func (m *Map) Add(peers ...string) {
	m.nodes = append(m.nodes, peers...)
	m.rebalance()
}

// Remove 将一批节点移除，并将其槽重新均分给剩余节点。
func (m *Map) Remove(peers ...string) {
	// 构建待删除节点的集合，便于 O(1) 查找
	removeSet := make(map[string]bool, len(peers))
	for _, p := range peers {
		removeSet[p] = true
	}
	// 原地过滤：保留不在 removeSet 中的节点，复用底层数组避免额外分配
	remaining := m.nodes[:0]
	for _, n := range m.nodes {
		if !removeSet[n] {
			remaining = append(remaining, n)
		}
	}
	// 将底层数组尾部被删除的元素置空，允许 GC 回收字符串
	for i := len(remaining); i < len(m.nodes); i++ {
		m.nodes[i] = ""
	}
	m.nodes = remaining
	m.rebalance() // 重新均分槽给剩余节点
}

// Get 根据 key 查找负责该 key 的节点，时间复杂度 O(1)。
// 算法：hash(key) % 16384 得到槽编号，直接查槽归属节点。
func (m *Map) Get(key string) string {
	if len(m.nodes) == 0 {
		return ""
	}
	slot := int(m.hash([]byte(key))) % slotCount
	return m.slots[slot]
}

// Slot 返回指定 key 对应的槽编号，便于调试和观察。
func (m *Map) Slot(key string) int {
	return int(m.hash([]byte(key))) % slotCount
}

// rebalance 将 16384 个槽均分给所有节点。
// 分配规则：槽 i 归属于 nodes[i*n/slotCount]，保证每个节点槽数相差不超过 1。
func (m *Map) rebalance() {
	n := len(m.nodes)
	if n == 0 {
		for i := range m.slots {
			m.slots[i] = ""
		}
		return
	}
	for i := 0; i < slotCount; i++ {
		m.slots[i] = m.nodes[i*n/slotCount]
	}
}
