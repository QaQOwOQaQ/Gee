package xclient

import (
	"errors"
	"sync"
)

// WeightedServer 表示一个带权重的服务节点。
// Weight 是配置的静态权重，currentWeight 是平滑加权轮询算法的动态权重。
type WeightedServer struct {
	Addr          string
	Weight        int
	currentWeight int
}

// WeightedDiscovery 实现加权轮询（Smooth Weighted Round Robin）。
// 算法每次选中 currentWeight 最大的节点，并对其减去总权重，
// 然后对所有节点的 currentWeight 加上各自的静态权重。
// 这样可以保证选中顺序平滑均匀，不会出现某个高权重节点连续被选中。
type WeightedDiscovery struct {
	mu      sync.Mutex
	servers []*WeightedServer
}

// NewWeightedDiscovery 创建一个加权轮询服务发现实例。
// 传入格式：[]WeightedServer{{Addr: "tcp@addr1", Weight: 2}, ...}
func NewWeightedDiscovery(servers []WeightedServer) *WeightedDiscovery {
	d := &WeightedDiscovery{}
	for i := range servers {
		if servers[i].Weight <= 0 {
			servers[i].Weight = 1
		}
		d.servers = append(d.servers, &servers[i])
	}
	return d
}

func (d *WeightedDiscovery) Refresh() error { return nil }

func (d *WeightedDiscovery) Update(addrs []string) error {
	return errors.New("rpc weighted discovery: use NewWeightedDiscovery to set weighted servers")
}

// Get 使用平滑加权轮询算法选择一个节点。
// 忽略传入的 mode 参数，始终使用加权轮询策略。
func (d *WeightedDiscovery) Get(_ SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.servers) == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	// 平滑加权轮询：每轮先对所有节点 currentWeight += weight，
	// 再选 currentWeight 最大的节点，并对其 currentWeight -= totalWeight。
	totalWeight := 0
	for _, s := range d.servers {
		totalWeight += s.Weight
		s.currentWeight += s.Weight
	}
	best := d.servers[0]
	for _, s := range d.servers[1:] {
		if s.currentWeight > best.currentWeight {
			best = s
		}
	}
	best.currentWeight -= totalWeight
	return best.Addr, nil
}

func (d *WeightedDiscovery) getExcluding(_ SelectMode, exclude map[string]struct{}) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	totalWeight := 0
	var best *WeightedServer
	for _, server := range d.servers {
		if _, skipped := exclude[server.Addr]; skipped {
			continue
		}
		totalWeight += server.Weight
		server.currentWeight += server.Weight
		if best == nil || server.currentWeight > best.currentWeight {
			best = server
		}
	}
	if best == nil {
		return "", errNoUntriedServers
	}
	best.currentWeight -= totalWeight
	return best.Addr, nil
}

// GetAll 返回所有节点的地址。
func (d *WeightedDiscovery) GetAll() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	addrs := make([]string, len(d.servers))
	for i, s := range d.servers {
		addrs[i] = s.Addr
	}
	return addrs, nil
}
