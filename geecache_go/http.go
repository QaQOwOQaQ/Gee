package geecache

import (
	"fmt"
	"geecache/consistenthash"
	pb "geecache/geecachepb"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

const (
	defaultBasePath = "/_geecache/" // HTTP 请求的默认路径前缀
)

// HTTPPool 实现了 PeerPicker 接口，是基于 HTTP 的分布式节点池。
// 既作为服务端（接收其他节点的缓存查询请求），
// 也作为客户端（向其他节点发起缓存查询请求）。
type HTTPPool struct {
	self        string                 // 当前节点的地址，例如 "http://10.0.0.1:8008"
	basePath    string                 // HTTP 请求路径前缀，默认为 "/_geecache/"
	mu          sync.RWMutex           // 保护 peers 和 httpGetters 的并发访问
	peers       *consistenthash.Map    // 槽机制哈希，用于根据 key 选择节点
	httpGetters map[string]*httpGetter // 每个远程节点对应一个 httpGetter 客户端
}

// NewHTTPPool 初始化一个 HTTP 节点池。
// self 为当前节点的 base URL，例如 "http://10.0.0.1:8008"。
func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
	}
}

// Log 以当前节点地址为前缀打印日志。
func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

// ServeHTTP 处理所有 HTTP 请求，是 HTTPPool 作为服务端的核心方法。
// 协议：
//   - GET /_geecache/?group=<group>&key=<key>：读取缓存
//   - DELETE /_geecache/?group=<group>&key=<key>：失效本地缓存
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		http.Error(w, "unexpected path: "+r.URL.Path, http.StatusNotFound)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	p.Log("%s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)

	groupName := r.URL.Query().Get("group")
	key := r.URL.Query().Get("key")
	if groupName == "" || key == "" {
		http.Error(w, "group and key are required", http.StatusBadRequest)
		return
	}

	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		view, err := group.Get(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// 将缓存值以 protobuf 格式序列化后写入响应体
		body, err := proto.Marshal(&pb.Response{Value: view.ByteSlice()})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(body)
	case http.MethodDelete:
		group.invalidateLocal(key)
		w.WriteHeader(http.StatusOK)
	}
}

// Set 更新节点池中的节点列表，并重建槽机制路由表和客户端映射。
// 每次调用都会完全重置节点列表。
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(nil)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{
			baseURL: peer + p.basePath,
			client:  &http.Client{Timeout: 5 * time.Second},
		}
	}
}

// PickPeer 根据 key 在槽机制路由表中选择对应的远程节点。
// 若选中的节点是当前节点自身，则返回 false（本地处理，不转发）。
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.peers == nil {
		return nil, false
	}
	peer := p.peers.Get(key)
	if peer == "" || peer == p.self {
		return nil, false
	}
	getter, ok := p.httpGetters[peer]
	if !ok {
		return nil, false
	}
	p.Log("选择节点 %s", peer)
	return getter, true
}

// 编译期断言：HTTPPool 必须实现 PeerPicker 接口
var _ PeerPicker = (*HTTPPool)(nil)

// httpGetter 是 HTTP 客户端，实现了 PeerGetter 接口。
// 每个远程节点对应一个 httpGetter 实例，负责向该节点发送缓存查询请求。
type httpGetter struct {
	baseURL string       // 目标节点的 base URL，例如 "http://10.0.0.2:8008/_geecache/"
	client  *http.Client // 带超时控制的 HTTP 客户端
}

// Get 向远程节点发送 HTTP GET 请求，获取指定 group/key 的缓存值。
// 请求和响应均使用 protobuf 格式，响应结果解码后写入 out。
func (h *httpGetter) Get(in *pb.Request, out *pb.Response) error {
	u, err := url.Parse(h.baseURL)
	if err != nil {
		return fmt.Errorf("解析 baseURL 失败: %v", err)
	}
	q := u.Query()
	q.Set("group", in.GetGroup())
	q.Set("key", in.GetKey())
	u.RawQuery = q.Encode()
	res, err := h.client.Get(u.String())
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("服务端返回错误状态: %v", res.Status)
	}

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("读取响应体失败: %v", err)
	}

	if err = proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("解码响应体失败: %v", err)
	}

	return nil
}

// Delete 向远程节点发送 HTTP DELETE 请求，失效指定 group/key 的缓存值。
func (h *httpGetter) Delete(in *pb.Request) error {
	u, err := url.Parse(h.baseURL)
	if err != nil {
		return fmt.Errorf("解析 baseURL 失败: %v", err)
	}
	q := u.Query()
	q.Set("group", in.GetGroup())
	q.Set("key", in.GetKey())
	u.RawQuery = q.Encode()
	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
	if err != nil {
		return fmt.Errorf("构造 DELETE 请求失败: %v", err)
	}
	res, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("服务端返回错误状态: %v", res.Status)
	}
	return nil
}

// 编译期断言：httpGetter 必须实现 PeerGetter 接口
var _ PeerGetter = (*httpGetter)(nil)
