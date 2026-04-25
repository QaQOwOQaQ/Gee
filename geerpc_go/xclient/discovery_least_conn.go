package xclient

import (
	"errors"
	"sync"
)

// leastConnServer 记录单个节点的地址和当前活跃调用数。
type leastConnServer struct {
	addr  string
	conns int // 当前活跃调用数
}

// LeastConnDiscovery 实现最少连接数负载均衡。
// 每次调用前选择 conns 最小的节点，调用结束后调用方需调用 Done 归还计数。
type LeastConnDiscovery struct {
	mu      sync.Mutex
	servers []*leastConnServer
}

// NewLeastConnDiscovery 创建一个最少连接数服务发现实例。
func NewLeastConnDiscovery(addrs []string) *LeastConnDiscovery {
	d := &LeastConnDiscovery{}
	for _, addr := range addrs {
		d.servers = append(d.servers, &leastConnServer{addr: addr})
	}
	return d
}

func (d *LeastConnDiscovery) Refresh() error { return nil }

func (d *LeastConnDiscovery) Update(addrs []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	newServers := make([]*leastConnServer, 0, len(addrs))
	for _, addr := range addrs {
		newServers = append(newServers, &leastConnServer{addr: addr})
	}
	d.servers = newServers
	return nil
}

// Get 选择当前活跃连接数最少的节点，并将其计数加一。
// 调用方在请求完成后必须调用 Done(addr) 归还计数，否则计数会持续累积。
func (d *LeastConnDiscovery) Get(_ SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.servers) == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	best := d.servers[0]
	for _, s := range d.servers[1:] {
		if s.conns < best.conns {
			best = s
		}
	}
	best.conns++
	return best.addr, nil
}

func (d *LeastConnDiscovery) getExcluding(_ SelectMode, exclude map[string]struct{}) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var best *leastConnServer
	for _, server := range d.servers {
		if _, skipped := exclude[server.addr]; skipped {
			continue
		}
		if best == nil || server.conns < best.conns {
			best = server
		}
	}
	if best == nil {
		return "", errNoUntriedServers
	}
	best.conns++
	return best.addr, nil
}

// Done 在一次调用结束后，将对应节点的活跃连接数减一。
// 调用方在 Call 返回后必须调用此方法，无论调用成功还是失败。
func (d *LeastConnDiscovery) Done(addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range d.servers {
		if s.addr == addr {
			if s.conns > 0 {
				s.conns--
			}
			return
		}
	}
}

// GetAll 返回所有节点的地址。
func (d *LeastConnDiscovery) GetAll() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	addrs := make([]string, len(d.servers))
	for i, s := range d.servers {
		addrs[i] = s.addr
	}
	return addrs, nil
}
