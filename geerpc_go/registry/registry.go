package registry

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// GeeRegistry 是一个极简注册中心。
// 它承担两件事：
//  1. 记录服务实例地址，并通过心跳维持存活状态
//  2. 对外返回当前仍然可用的服务列表，并清理超时实例
//
// 这里的设计是教学导向，重点在于把注册中心最核心的职责讲清楚。
type GeeRegistry struct {
	timeout time.Duration
	mu      sync.Mutex // 保护以下共享状态
	servers map[string]*ServerItem
}

// HeartbeatController 管理一个服务实例到注册中心的保活生命周期。
// Stop 会停止后续心跳，并主动把该实例从注册中心摘除。
type HeartbeatController struct {
	registry string
	addr     string
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
	stopErr  error
}

// ServerOptions 描述实例注册到注册中心时携带的静态标签。
type ServerOptions struct {
	Group   string
	Version string
	Weight  int
}

// ServerInfo 是注册中心对外暴露的一条服务实例快照。
type ServerInfo struct {
	Addr    string `json:"addr"`
	Group   string `json:"group"`
	Version string `json:"version"`
	Weight  int    `json:"weight"`
}

// ServerItem 表示注册中心里的一条服务实例记录
type ServerItem struct {
	Info  ServerInfo // 在 timeout 窗口内没有收到该实例的心跳/注册刷新，就会被认定为超时
	start time.Time  // start 不是服务启动时间，而是最近一次收到注册/心跳的时间
}

const (
	defaultPath             = "/_geerpc_/registry"
	defaultTimeout          = time.Minute * 5
	heartbeatRequestTimeout = time.Second * 3
	DefaultGroup            = "default"
	DefaultVersion          = "v1"
	DefaultWeight           = 1
	ServersHeader           = "X-Geerpc-Servers"
	ServerInfosHeader       = "X-Geerpc-Server-Infos"
	ServerHeader            = "X-Geerpc-Server"
	GroupHeader             = "X-Geerpc-Group"
	VersionHeader           = "X-Geerpc-Version"
	WeightHeader            = "X-Geerpc-Weight"
)

// New 使用指定超时时间创建一个注册中心实例。
func New(timeout time.Duration) *GeeRegistry {
	return &GeeRegistry{
		servers: make(map[string]*ServerItem),
		timeout: timeout,
	}
}

var DefaultGeeRegister = New(defaultTimeout)

// putServer 添加服务实例，或者刷新已有实例的最近存活时间。
// 注册中心并不区分“首次注册”和“后续心跳”，统一都视为一次存活刷新。
func normalizeOptions(addr string, opts *ServerOptions) ServerInfo {
	info := ServerInfo{
		Addr:    addr,
		Group:   DefaultGroup,
		Version: DefaultVersion,
		Weight:  DefaultWeight,
	}
	if opts == nil {
		return info
	}
	if strings.TrimSpace(opts.Group) != "" {
		info.Group = strings.TrimSpace(opts.Group)
	}
	if strings.TrimSpace(opts.Version) != "" {
		info.Version = strings.TrimSpace(opts.Version)
	}
	if opts.Weight > 0 {
		info.Weight = opts.Weight
	}
	return info
}

func serverOptionsFromRequest(req *http.Request) *ServerOptions {
	opts := &ServerOptions{
		Group:   strings.TrimSpace(req.Header.Get(GroupHeader)),
		Version: strings.TrimSpace(req.Header.Get(VersionHeader)),
		Weight:  DefaultWeight,
	}
	if weight := strings.TrimSpace(req.Header.Get(WeightHeader)); weight != "" {
		if parsed, err := strconv.Atoi(weight); err == nil && parsed > 0 {
			opts.Weight = parsed
		}
	}
	return opts
}

func (r *GeeRegistry) putServer(addr string, opts *ServerOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	info := normalizeOptions(addr, opts)
	if s == nil {
		r.servers[addr] = &ServerItem{Info: info, start: time.Now()}
	} else {
		s.Info = info
		s.start = time.Now() // 已存在则刷新最后存活时间
	}
}

func (r *GeeRegistry) deleteServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.servers, addr)
}

// aliveServers 返回当前所有未过期的服务地址。
// 如果发现某个实例已经超时，会顺便把它从注册表中删除。
func (r *GeeRegistry) aliveServerInfos() []ServerInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []ServerInfo
	for addr, s := range r.servers {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, s.Info)
		} else {
			delete(r.servers, addr)
		}
	}
	sort.Slice(alive, func(i, j int) bool {
		return alive[i].Addr < alive[j].Addr
	})
	return alive
}

// aliveServers 返回当前所有未过期的服务地址。
func (r *GeeRegistry) aliveServers() []string {
	infos := r.aliveServerInfos()
	alive := make([]string, 0, len(infos))
	for _, info := range infos {
		alive = append(alive, info.Addr)
	}
	return alive
}

// 客户端或者服务器与注册中心间的通信通过 HTTP 协议进行，具体的：
//   - 服务端 ↔ 注册中心：用 HTTP POST 发心跳/注册
//   - 客户端（准确说是 discovery 组件）↔ 注册中心：用 HTTP GET 拉取服务列表

// ServeHTTP 把注册中心以 HTTP 形式暴露出来。
// 为了保持实现简单，所有有用信息都放在 Header 中：
// GET 通过 X-Geerpc-Servers 返回当前可用实例列表；
// POST 通过 X-Geerpc-Server 上报单个实例地址，用于注册或心跳。
func (r *GeeRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		infos := r.aliveServerInfos()
		addrs := make([]string, 0, len(infos))
		for _, info := range infos {
			addrs = append(addrs, info.Addr)
		}
		w.Header().Set(ServersHeader, strings.Join(addrs, ","))
		if encoded, err := json.Marshal(infos); err == nil {
			w.Header().Set(ServerInfosHeader, string(encoded))
		}
	case "POST":
		addr := req.Header.Get(ServerHeader)
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr, serverOptionsFromRequest(req))
	case "DELETE":
		addr := req.Header.Get(ServerHeader)
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.deleteServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// HandleHTTP 把注册中心挂到指定 HTTP 路径。
func (r *GeeRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registry path:", registryPath)
}

// HandleHTTP 是默认注册中心实例的快捷入口。
func HandleHTTP() {
	DefaultGeeRegister.HandleHTTP(defaultPath)
}

// Heartbeat 定期向注册中心发送心跳，并返回一个可停止的控制器。
// 这个辅助函数通常在服务启动时调用：先立即发送一次注册，再按周期持续保活。
// 与旧实现不同：即使某次发送失败，也不会停止后续心跳，而是记录日志后继续重试。
func Heartbeat(registry, addr string, duration time.Duration) *HeartbeatController {
	return HeartbeatWithOptions(registry, addr, duration, nil)
}

// HeartbeatWithOptions 定期向注册中心发送带标签的心跳。
func HeartbeatWithOptions(registry, addr string, duration time.Duration, opts *ServerOptions) *HeartbeatController {
	if duration == 0 {
		// 默认让心跳周期略小于过期时间，避免实例在正常运行时被误删。
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	controller := &HeartbeatController{
		registry: registry,
		addr:     addr,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	if err := sendHeartbeatWithOptions(registry, addr, opts); err != nil {
		log.Println("rpc server: initial heartbeat err:", err)
	}
	go func() {
		defer close(controller.done)
		t := time.NewTicker(duration)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if err := sendHeartbeatWithOptions(registry, addr, opts); err != nil {
					log.Println("rpc server: heart beat err:", err)
				}
			case <-controller.stop:
				return
			}
		}
	}()
	return controller
}

// Stop 停止后续心跳，并主动把实例从注册中心摘除。
func (c *HeartbeatController) Stop() error {
	if c == nil {
		return nil
	}
	c.once.Do(func() {
		close(c.stop)
		<-c.done
		c.stopErr = Unregister(c.registry, c.addr)
	})
	return c.stopErr
}

// Unregister 主动把实例从注册中心摘除，便于服务优雅下线时立刻对外不可见。
func Unregister(registry, addr string) error {
	httpClient := &http.Client{Timeout: heartbeatRequestTimeout}
	req, err := http.NewRequest("DELETE", registry, nil)
	if err != nil {
		return err
	}
	req.Header.Set(ServerHeader, addr)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected unregister status: %s", resp.Status)
	}
	return nil
}

// sendHeartbeat 向注册中心发送一次心跳请求。
// 它会设置请求超时、校验响应状态码，并确保响应体被关闭和回收，
// 避免长时间运行时连接/FD 资源泄漏。
func sendHeartbeat(registry, addr string) error {
	return sendHeartbeatWithOptions(registry, addr, nil)
}

func sendHeartbeatWithOptions(registry, addr string, opts *ServerOptions) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{Timeout: heartbeatRequestTimeout}
	req, err := http.NewRequest("POST", registry, nil)
	if err != nil {
		return err
	}
	req.Header.Set(ServerHeader, addr)
	info := normalizeOptions(addr, opts)
	req.Header.Set(GroupHeader, info.Group)
	req.Header.Set(VersionHeader, info.Version)
	req.Header.Set(WeightHeader, strconv.Itoa(info.Weight))
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected heartbeat status: %s", resp.Status)
	}
	return nil
}
