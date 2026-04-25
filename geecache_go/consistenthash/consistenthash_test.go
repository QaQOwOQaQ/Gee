package consistenthash

import (
	"testing"
)

// TestSlotAssignment 测试固定槽机制下节点均匀分配槽的基本逻辑。
func TestSlotAssignment(t *testing.T) {
	m := New(nil)
	m.Add("node1", "node2", "node3")

	// 统计每个节点分到的槽数
	counts := make(map[string]int)
	for i := 0; i < slotCount; i++ {
		counts[m.slots[i]]++
	}

	// 3 个节点均分 16384 个槽，每个节点应分到约 5461 个槽，误差不超过 1
	for _, node := range []string{"node1", "node2", "node3"} {
		if counts[node] < 5461 || counts[node] > 5462 {
			t.Errorf("节点 %s 分配槽数 %d，期望 5461 或 5462", node, counts[node])
		}
	}
}

// TestGet 测试 key 路由到正确节点。
func TestGet(t *testing.T) {
	m := New(nil)
	m.Add("node1", "node2", "node3")

	// 同一个 key 多次查询应返回同一个节点
	node := m.Get("Tom")
	if node == "" {
		t.Fatal("Get 返回空节点")
	}
	for i := 0; i < 10; i++ {
		if m.Get("Tom") != node {
			t.Fatal("同一 key 多次查询结果不一致")
		}
	}
}

// TestAddNode 测试新增节点后槽全量 rebalance，约 50% 槽发生迁移。
func TestAddNode(t *testing.T) {
	m := New(nil)
	m.Add("node1", "node2")

	// 记录扩容前所有槽的归属
	before := [slotCount]string{}
	copy(before[:], m.slots[:])

	// 新增节点
	m.Add("node3")

	// 统计有多少槽发生了迁移
	migrated := 0
	for i := 0; i < slotCount; i++ {
		if before[i] != m.slots[i] {
			migrated++
		}
	}

	// 扩容后槽重新均分，迁移槽数约为 slotCount * (1 - 1/oldN) = 16384 * 1/2 ≈ 8192
	// 实际迁移 8191（因整除截断差 1）
	if migrated < 8000 || migrated > 8300 {
		t.Errorf("扩容后迁移槽数 %d，期望在 8000~8300 之间", migrated)
	}
}

// TestRemoveNode 测试移除节点后槽重新分配给剩余节点。
func TestRemoveNode(t *testing.T) {
	m := New(nil)
	m.Add("node1", "node2", "node3")
	m.Remove("node2")

	// node2 移除后，所有槽应只归属于 node1 或 node3
	for i := 0; i < slotCount; i++ {
		if m.slots[i] == "node2" {
			t.Fatalf("槽 %d 仍归属于已移除的 node2", i)
		}
	}

	// 剩余两个节点应均分槽
	counts := make(map[string]int)
	for i := 0; i < slotCount; i++ {
		counts[m.slots[i]]++
	}
	if counts["node1"] != 8192 || counts["node3"] != 8192 {
		t.Errorf("移除节点后槽分配不均：node1=%d, node3=%d", counts["node1"], counts["node3"])
	}
}
