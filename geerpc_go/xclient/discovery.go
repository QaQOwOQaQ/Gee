package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

var errNoUntriedServers = errors.New("rpc discovery: no untried servers available")

type SelectMode int

const (
	RandomSelect     SelectMode = iota // 随机选择一个服务实例
	RoundRobinSelect                   // 轮询选择一个服务实例
)

// Discovery 抽象服务发现能力
type Discovery interface {
	Refresh() error                      // 从远端注册中心刷新服务列表
	Update(servers []string) error       // 手动更新服务列表
	Get(mode SelectMode) (string, error) // 根据负载均衡策略，选择一个服务实例
	GetAll() ([]string, error)           // 返回所有的服务实例
}

// KeyedDiscovery 是 Discovery 的可选扩展。
// 当某类负载均衡需要按业务 key 做稳定路由时，
// XClient 可以通过它把相同 key 的请求路由到同一个节点。
type KeyedDiscovery interface {
	GetByKey(key string) (string, error)
}

// ReleasableDiscovery 是 Discovery 的可选扩展。
// 某些调度算法（例如最少连接数）在选中节点时会先增加活跃计数，
// 因此调用结束后需要通过 Done 归还这次占用。
type ReleasableDiscovery interface {
	Done(addr string)
}

var _ Discovery = (*MultiServersDiscovery)(nil)

// MultiServersDiscovery 适用于“服务地址已知”的场景
// 不需要注册中心，服务列表由手工维护的服务发现
type MultiServersDiscovery struct {
	r       *rand.Rand   // 产生随机数，用于随机负载均衡
	mu      sync.RWMutex // 保护以下字段
	servers []string
	index   int // 轮询模式下当前选择位置
}

// Refresh 对 MultiServersDiscovery 没有实际意义。
// 因为它不从远端拉取地址，所以这里直接返回 nil。
func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

// Update 动态替换当前维护的服务列表。
func (d *MultiServersDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	return nil
}

// Get 根据负载均衡模式选择一个服务地址。
func (d *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	switch mode {
	case RandomSelect:
		return d.servers[d.r.Intn(n)], nil
	case RoundRobinSelect:
		s := d.servers[d.index%n] // 服务列表可能会变化，因此这里对 n 取模保证安全
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

func (d *MultiServersDiscovery) getExcluding(mode SelectMode, exclude map[string]struct{}) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	switch mode {
	case RandomSelect:
		candidates := make([]string, 0, n)
		for _, server := range d.servers {
			if _, skipped := exclude[server]; skipped {
				continue
			}
			candidates = append(candidates, server)
		}
		if len(candidates) == 0 {
			return "", errNoUntriedServers
		}
		return candidates[d.r.Intn(len(candidates))], nil
	case RoundRobinSelect:
		start := d.index
		for i := 0; i < n; i++ {
			idx := (start + i) % n
			server := d.servers[idx]
			if _, skipped := exclude[server]; skipped {
				continue
			}
			d.index = (idx + 1) % n
			return server, nil
		}
		return "", errNoUntriedServers
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

// GetAll 返回当前全部服务地址的一个副本。
// 返回副本而不是原切片，可以避免调用方误改内部状态。
func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	servers := make([]string, len(d.servers), len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}

// NewMultiServerDiscovery 创建一个不依赖注册中心的服务发现实例。
func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		servers: servers,
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	d.index = d.r.Intn(math.MaxInt32 - 1)
	return d
}
