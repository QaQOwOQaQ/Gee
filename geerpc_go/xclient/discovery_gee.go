package xclient

import (
	"encoding/json"
	"fmt"
	"geerpc/registry"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// GeeRegistryDiscovery 是一个基于 GeeRegistry 的服务发现实现。
// 它在本地缓存一份服务列表，并按超时策略定期从注册中心刷新。
type GeeRegistryDiscovery struct {
	*MultiServersDiscovery
	registry   string
	timeout    time.Duration
	lastUpdate time.Time
	hash       *ConsistentHashDiscovery
	cfg        GeeRegistryDiscoveryConfig
	infos      []registry.ServerInfo
	weighted   *WeightedDiscovery
}

const defaultUpdateTimeout = time.Second * 10

// GeeRegistryDiscoveryConfig 描述注册中心发现时的过滤与加权配置。
type GeeRegistryDiscoveryConfig struct {
	Group              string
	Version            string
	UseRegistryWeights bool
}

func matchesRegistryFilter(info registry.ServerInfo, cfg GeeRegistryDiscoveryConfig) bool {
	if strings.TrimSpace(cfg.Group) != "" && info.Group != strings.TrimSpace(cfg.Group) {
		return false
	}
	if strings.TrimSpace(cfg.Version) != "" && info.Version != strings.TrimSpace(cfg.Version) {
		return false
	}
	return true
}

func filterRegistryInfos(infos []registry.ServerInfo, cfg GeeRegistryDiscoveryConfig) []registry.ServerInfo {
	filtered := make([]registry.ServerInfo, 0, len(infos))
	for _, info := range infos {
		if matchesRegistryFilter(info, cfg) {
			filtered = append(filtered, info)
		}
	}
	return filtered
}

func infosToAddrs(infos []registry.ServerInfo) []string {
	addrs := make([]string, 0, len(infos))
	for _, info := range infos {
		addrs = append(addrs, info.Addr)
	}
	return addrs
}

func infosToWeightedServers(infos []registry.ServerInfo) []WeightedServer {
	servers := make([]WeightedServer, 0, len(infos))
	for _, info := range infos {
		servers = append(servers, WeightedServer{
			Addr:   info.Addr,
			Weight: info.Weight,
		})
	}
	return servers
}

func defaultRegistryInfos(addrs []string) []registry.ServerInfo {
	infos := make([]registry.ServerInfo, 0, len(addrs))
	for _, addr := range addrs {
		infos = append(infos, registry.ServerInfo{
			Addr:    strings.TrimSpace(addr),
			Group:   registry.DefaultGroup,
			Version: registry.DefaultVersion,
			Weight:  registry.DefaultWeight,
		})
	}
	return infos
}

func (d *GeeRegistryDiscovery) applyServerInfosLocked(infos []registry.ServerInfo) {
	filtered := filterRegistryInfos(infos, d.cfg)
	d.infos = append([]registry.ServerInfo(nil), filtered...)
	d.servers = infosToAddrs(filtered)
	if d.hash != nil {
		_ = d.hash.Update(d.servers)
	}
	if d.cfg.UseRegistryWeights {
		d.weighted = NewWeightedDiscovery(infosToWeightedServers(filtered))
		return
	}
	d.weighted = nil
}

// Update 用新的服务列表覆盖本地缓存，并记录刷新时间。
func (d *GeeRegistryDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.applyServerInfosLocked(defaultRegistryInfos(servers))
	d.lastUpdate = time.Now()
	return nil
}

// Refresh 在缓存过期时，向注册中心拉取最新服务列表。
// 这样既避免每次调用都访问注册中心，又能较快感知实例变化。
func (d *GeeRegistryDiscovery) Refresh() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)
	resp, err := http.Get(d.registry)
	if err != nil {
		log.Println("rpc registry refresh err:", err)
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("rpc registry: unexpected status: %s", resp.Status)
		log.Println("rpc registry refresh err:", err)
		return err
	}
	var infos []registry.ServerInfo
	if raw := strings.TrimSpace(resp.Header.Get(registry.ServerInfosHeader)); raw != "" {
		if err := json.Unmarshal([]byte(raw), &infos); err != nil {
			log.Println("rpc registry refresh err:", err)
			return err
		}
	}
	if len(infos) == 0 {
		servers := strings.Split(resp.Header.Get(registry.ServersHeader), ",")
		for _, server := range servers {
			server = strings.TrimSpace(server)
			if server == "" {
				continue
			}
			infos = append(infos, registry.ServerInfo{
				Addr:    server,
				Group:   registry.DefaultGroup,
				Version: registry.DefaultVersion,
				Weight:  registry.DefaultWeight,
			})
		}
	}
	d.applyServerInfosLocked(infos)
	d.lastUpdate = time.Now()
	return nil
}

// Get 在选择实例前，先确保本地缓存是新鲜的。
func (d *GeeRegistryDiscovery) Get(mode SelectMode) (string, error) {
	if err := d.Refresh(); err != nil {
		return "", err
	}
	if d.weighted != nil {
		return d.weighted.Get(mode)
	}
	return d.MultiServersDiscovery.Get(mode)
}

func (d *GeeRegistryDiscovery) getExcluding(mode SelectMode, exclude map[string]struct{}) (string, error) {
	if err := d.Refresh(); err != nil {
		return "", err
	}
	if d.weighted != nil {
		return d.weighted.getExcluding(mode, exclude)
	}
	return d.MultiServersDiscovery.getExcluding(mode, exclude)
}

// GetAll 在返回全部实例前，先确保本地缓存是新鲜的。
func (d *GeeRegistryDiscovery) GetAll() ([]string, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll()
}

// GetAllServers 返回当前过滤后的实例快照。
func (d *GeeRegistryDiscovery) GetAllServers() ([]registry.ServerInfo, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	infos := make([]registry.ServerInfo, len(d.infos))
	copy(infos, d.infos)
	return infos, nil
}

// GetByKey 在返回节点前，先确保本地缓存和一致性哈希环是最新的。
func (d *GeeRegistryDiscovery) GetByKey(key string) (string, error) {
	if err := d.Refresh(); err != nil {
		return "", err
	}
	return d.hash.GetByKey(key)
}

// NewGeeRegistryDiscovery 创建一个基于 GeeRegistry 的服务发现实例。
func NewGeeRegistryDiscovery(registerAddr string, timeout time.Duration) *GeeRegistryDiscovery {
	return NewGeeRegistryDiscoveryWithConfig(registerAddr, timeout, nil)
}

// NewGeeRegistryDiscoveryWithConfig 创建一个带标签过滤与权重能力的注册中心发现实例。
func NewGeeRegistryDiscoveryWithConfig(registerAddr string, timeout time.Duration, cfg *GeeRegistryDiscoveryConfig) *GeeRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	normalized := GeeRegistryDiscoveryConfig{}
	if cfg != nil {
		normalized = *cfg
	}
	d := &GeeRegistryDiscovery{
		MultiServersDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry:              registerAddr,
		timeout:               timeout,
		hash:                  NewConsistentHashDiscovery(nil, 0),
		cfg:                   normalized,
	}
	return d
}
