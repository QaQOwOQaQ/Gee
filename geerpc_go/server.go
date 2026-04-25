package geerpc

// 服务端处理流程：
//
//	客户端建立连接
//	  │
//	  ▼
//	发送 Option(JSON 协商参数)
//	  │
//	  ▼
//	ServeConn 校验 MagicNumber / CodecType
//	  │
//	  ▼
//	serveCodec 循环读取请求
//	  │
//	  ├── readRequestHeader 读取 Header
//	  ├── findService 定位 Service.Method
//	  ├── 构造 argv / replyv
//	  └── ReadBody 读取请求参数
//	  │
//	  ▼
//	handleRequest 并发执行业务方法
//	  │
//	  ├── svc.call 反射调用真实方法
//	  ├── 支持 HandleTimeout 超时控制
//	  └── sendResponse 串行写回响应
//	  │
//	  ▼
//	客户端按 Seq 收到对应响应
//
// 这个文件只关心三件核心事情：
// 1. 如何把一条连接升级成 GeeRPC 协议连接
// 2. 如何把网络请求解析成某个 Service.Method 的一次调用
// 3. 如何在“请求可并发处理”和“响应必须串行写回”之间保持正确性

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"geerpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

// MagicNumber 是 GeeRPC 的协议魔数，用于在连接建立时快速识别合法客户端。
const MagicNumber = 0x3bef5c

// Option 是客户端和服务端在真正开始 RPC 读写之前的协商参数。
// 这一步故意放在连接建立后、消息收发前完成，目的有两个：
// 1. 让服务端快速识别“这是不是 GeeRPC 请求”
// 2. 让双方在同一条连接上约定后续使用哪种编解码器、采用什么超时策略
//
// 协商本身使用 JSON，是因为它只发生一次，重点是简单直观；
// 真正的高频 RPC 请求/响应则交给 codec 层处理。
type Option struct {
	MagicNumber    int           // 协议魔数，用于快速拒绝非 GeeRPC 连接
	CodecType      codec.Type    // 后续请求/响应采用的编解码协议
	ConnectTimeout time.Duration // 客户端建连与初始化超时时间，0 表示不限制
	HandleTimeout  time.Duration // 服务端处理单个请求的超时时间，0 表示不限制
}

// DefaultOption 是框架给出的默认协商参数。
// 默认使用 Gob 编解码，并限制连接建立最多等待 10 秒。
var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

// ServerConfig 描述服务端自身的运行时治理配置。
// 这些配置作用于服务端进程，不属于客户端与服务端的协议协商。
type ServerConfig struct {
	MaxConcurrentRequests int
}

// Server 表示一个 RPC 服务端实例。
// 它内部最重要的状态就是 serviceMap：
// key 是服务名（例如 Foo），value 是该服务的反射描述信息。
//
// 这里使用 sync.Map 的原因是：
// 1. 服务注册频率低，但请求读取服务信息频率高
// 2. 读多写少场景下，直接按服务名并发查询会更自然
type Server struct {
	serviceMap    sync.Map
	cfg           ServerConfig
	mu            sync.Mutex
	listeners     map[net.Listener]struct{}
	conns         map[io.ReadWriteCloser]*serverConnState
	activeReqs    int
	shutdownCond  *sync.Cond
	shuttingDown  bool
	overloaded    uint64
	lastTraceID   string
	lastRequestID string
}

// NewServer 创建一个空的 RPC 服务端。
// 空的意思是：连接处理能力已经具备，但还没有注册任何业务服务。
func NewServer() *Server {
	return NewServerWithConfig(nil)
}

func normalizeServerConfig(cfg *ServerConfig) ServerConfig {
	if cfg == nil {
		return ServerConfig{}
	}
	normalized := *cfg
	if normalized.MaxConcurrentRequests < 0 {
		normalized.MaxConcurrentRequests = 0
	}
	return normalized
}

// NewServerWithConfig 创建一个带运行时治理配置的 RPC 服务端。
func NewServerWithConfig(cfg *ServerConfig) *Server {
	server := &Server{
		cfg:       normalizeServerConfig(cfg),
		listeners: make(map[net.Listener]struct{}),
		conns:     make(map[io.ReadWriteCloser]*serverConnState),
	}
	server.shutdownCond = sync.NewCond(&server.mu)
	return server
}

// DefaultServer 是包级默认服务端。
// 这样调用方在简单场景下可以直接使用 geerpc.Register / geerpc.Accept，
// 不必手动维护一个 Server 实例。
var DefaultServer = NewServer()

// ErrServerClosed 表示服务端已经进入优雅停服流程，不再接收新的连接和请求。
var ErrServerClosed = errors.New("rpc server: server is shutting down")

// ErrServerOverloaded 表示服务端当前并发请求数已达到上限，拒绝继续接入新请求。
var ErrServerOverloaded = errors.New("rpc server: server is overloaded")

type serverConnState struct {
	active int
}

type closeReader interface {
	CloseRead() error
}

func (server *Server) isShuttingDown() bool {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.shuttingDown
}

func (server *Server) trackListener(lis net.Listener) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.shuttingDown {
		return ErrServerClosed
	}
	server.listeners[lis] = struct{}{}
	return nil
}

func (server *Server) untrackListener(lis net.Listener) {
	server.mu.Lock()
	defer server.mu.Unlock()
	delete(server.listeners, lis)
}

func (server *Server) trackConn(conn io.ReadWriteCloser) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.shuttingDown {
		return ErrServerClosed
	}
	server.conns[conn] = &serverConnState{}
	return nil
}

func (server *Server) untrackConn(conn io.ReadWriteCloser) {
	server.mu.Lock()
	defer server.mu.Unlock()
	delete(server.conns, conn)
}

func (server *Server) tryBeginRequest(conn io.ReadWriteCloser) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.shuttingDown {
		return ErrServerClosed
	}
	if server.cfg.MaxConcurrentRequests > 0 && server.activeReqs >= server.cfg.MaxConcurrentRequests {
		server.overloaded++
		return ErrServerOverloaded
	}
	if state := server.conns[conn]; state != nil {
		state.active++
	}
	server.activeReqs++
	return nil
}

func (server *Server) endRequest(conn io.ReadWriteCloser) {
	var shouldClose bool

	server.mu.Lock()
	if state := server.conns[conn]; state != nil {
		if state.active > 0 {
			state.active--
		}
		shouldClose = server.shuttingDown && state.active == 0
	}
	if server.activeReqs > 0 {
		server.activeReqs--
	}
	if server.activeReqs == 0 {
		server.shutdownCond.Broadcast()
	}
	server.mu.Unlock()

	if shouldClose {
		_ = conn.Close()
	}
}

func (server *Server) startShutdown() {
	server.mu.Lock()
	if server.shuttingDown {
		server.mu.Unlock()
		return
	}
	server.shuttingDown = true

	listeners := make([]net.Listener, 0, len(server.listeners))
	for lis := range server.listeners {
		listeners = append(listeners, lis)
	}

	idleConns := make([]io.ReadWriteCloser, 0, len(server.conns))
	activeConns := make([]io.ReadWriteCloser, 0, len(server.conns))
	for conn, state := range server.conns {
		if state.active == 0 {
			idleConns = append(idleConns, conn)
			continue
		}
		activeConns = append(activeConns, conn)
	}
	server.mu.Unlock()

	for _, lis := range listeners {
		_ = lis.Close()
	}
	for _, conn := range idleConns {
		_ = conn.Close()
	}
	for _, conn := range activeConns {
		if closer, ok := conn.(closeReader); ok {
			_ = closer.CloseRead()
		}
	}
}

func (server *Server) closeTrackedConns() {
	server.mu.Lock()
	conns := make([]io.ReadWriteCloser, 0, len(server.conns))
	for conn := range server.conns {
		conns = append(conns, conn)
	}
	server.mu.Unlock()

	for _, conn := range conns {
		_ = conn.Close()
	}
}

// Shutdown 让服务端进入优雅停服流程：
// 1. 立刻关闭 listener，拒绝新的连接
// 2. 关闭空闲连接，并对活跃连接停止继续读入新请求
// 3. 等待已经在执行中的请求完成，再关闭剩余连接
func (server *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	server.startShutdown()

	done := make(chan struct{})
	go func() {
		server.mu.Lock()
		for server.activeReqs > 0 {
			server.shutdownCond.Wait()
		}
		server.mu.Unlock()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		server.closeTrackedConns()
		return nil
	}
}

// ServeConn 在单个连接上提供 RPC 服务。
// 它是服务端协议入口：一旦某条连接被 Accept 进来，后续是否能进入 RPC 流程，
// 完全由这里决定。
//
// 处理顺序是：
// 1. 先用 JSON 解码出 Option
// 2. 校验 MagicNumber，确认这是 GeeRPC 客户端
// 3. 根据 CodecType 选出真正负责消息编解码的实现
// 4. 把连接交给 serveCodec，进入连续收发请求的阶段
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	if err := server.trackConn(conn); err != nil {
		_ = conn.Close()
		return
	}
	defer server.untrackConn(conn)
	defer func() { _ = conn.Close() }()

	// 第一步：协议协商。
	// 连接建立后首先读取 Option，使用 JSON 是因为这一步只发生一次，
	// 不追求高性能，优先简单可读。
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}

	// 第二步：合法性校验。
	// 魔数不匹配说明来的不是 GeeRPC 客户端，直接关闭连接。
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}

	// 第三步：选取编解码器。
	// 客户端在 Option 里指定了后续使用哪种 codec，
	// 这里根据类型找到对应的构造函数 f，再用 f(conn) 创建实际的 Codec 实例。
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}

	// 第四步：进入请求循环。
	// 协商完成后，这条连接的后续读写都由 serveCodec 接管。
	server.serveCodec(conn, f(conn), &opt)
}

// invalidRequest 是错误响应时返回的占位对象。
// 当请求头已经读出来，但请求体有问题、方法不存在或处理超时时，
// 服务端仍要返回一个结构完整的响应包，此时 body 并不重要，
// 只需要把错误信息写进 Header.Error 即可。
var invalidRequest = struct{}{}

// serveCodec 基于指定编解码器循环读取并处理请求。
// 这里有一个非常关键的并发策略：
// 1. 同一连接上的多个请求允许并发处理，提高吞吐量
// 2. 但多个响应写回时必须串行化，否则字节流会交叉，客户端无法正确解码
//
// 因此这里同时维护：
// - sending：控制“写响应”只能一个一个来
// - wg：保证连接关闭前，已经开始处理的请求都能收尾
func (server *Server) serveCodec(conn io.ReadWriteCloser, cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex) // 保证一次完整响应不会与其他响应交叉写入
	wg := new(sync.WaitGroup)  // 等待当前连接上的所有请求都处理完成
	for {
		if server.isShuttingDown() {
			break
		}
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break // 连请求头都读不出来，说明连接级错误，直接结束整个循环
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		if err := server.tryBeginRequest(conn); err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(conn, cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

// request 保存服务端处理一次请求所需的全部上下文。
// 这几个字段覆盖了“协议层 -> 反射层 -> 业务层”三个阶段：
// - h：协议层信息，决定要调哪个方法、响应该写给谁
// - argv / replyv：反射层真正要传给方法的参数与返回值对象
// - svc / mtype：方法定位结果，描述了该请求最终应该落到哪个业务方法上
type request struct {
	h            *codec.Header // 请求头，至少包含 ServiceMethod / Seq / Error
	ctx          context.Context
	argv, replyv reflect.Value // 业务方法的入参对象和返回对象
	mtype        *methodType   // 目标方法的反射描述
	svc          *service      // 目标服务的反射描述
}

func (server *Server) recordRequestMetadata(md map[string]string) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.lastTraceID = strings.TrimSpace(md[MetadataTraceIDKey])
	server.lastRequestID = strings.TrimSpace(md[MetadataRequestIDKey])
}

// readRequestHeader 只读取请求头，不读取请求体。
// 服务端必须先拿到 Header，才能知道：
// 1. 调用目标是谁（Service.Method）
// 2. 后续响应应该携带哪个 Seq
func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

// findService 根据 "Service.Method" 形式的方法名，定位到具体服务和方法。
// 这是协议世界和反射世界之间的桥：
// 客户端发送的是字符串名，服务端真正执行的是反射拿到的方法对象。
func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

// readRequest 读取并解析一条完整请求。
// 解析顺序是：
// 1. 先读 Header，确定目标方法
// 2. 根据方法签名构造 argv / replyv
// 3. 再把请求体解码进 argv
//
// 只有走完这三步，服务端才真正拥有一次可执行的“方法调用任务”。
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	server.recordRequestMetadata(h.Metadata)
	req := &request{h: h}
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.ctx = WithMetadata(context.Background(), h.Metadata)
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	// ReadBody 需要可写入的指针，因此当 argv 不是指针时，
	// 这里要临时取地址再交给 codec 解码。
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Pointer {
		argvi = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}
	return req, nil
}

// sendResponse 把响应写回客户端。
// 由于同一连接上的多个请求可能并发完成，这里必须在写出阶段加锁，
// 确保一次完整的“Header + Body”不会和另一条响应交错。
func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// handleRequest 执行单个请求，并支持可选的处理超时。
// 这里的结构看起来有点绕，但目的很明确：
// 1. 真正的业务方法调用要单独放进 goroutine
// 2. 外层才能同时“等调用完成”或“等超时发生”
//
// called 表示业务方法已经返回；
// sent 表示响应已经写回；
// 这样即使业务很快结束，也能确保同步路径里不会提前返回。
func (server *Server) handleRequest(conn io.ReadWriteCloser, cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	defer server.endRequest(conn)
	// called：业务方法已执行完毕（无论成功/失败）
	// sent：响应已写回连接
	// 两个信号分开，是为了在超时路径中也能等到 send 完成，避免并发写。
	called := make(chan struct{}, 1)
	sent := make(chan struct{}, 1)
	go func() {
		err := req.svc.call(req.mtype, req.ctx, req.argv, req.replyv)
		called <- struct{}{} // 通知外层：方法已返回，可以做超时判断了
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		// 不限时：等方法执行完，再等响应写完，然后返回。
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		// 超时：外层负责写一条错误响应。
		// goroutine 仍在运行，但它后续的 sendResponse 会被 sending 锁排队，
		// 不会与这里的错误响应交叉写入。
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		// 方法在超时前完成：等响应写完再退出，保持 wg 计数的准确性。
		<-sent
	}
}

// Accept 持续接收监听器上的新连接，并为每个连接启动 goroutine 处理。
// 连接级别并发发生在这里，请求级别并发发生在 serveCodec 里。
func (server *Server) Accept(lis net.Listener) {
	if err := server.trackListener(lis); err != nil {
		_ = lis.Close()
		return
	}
	defer server.untrackListener(lis)

	for {
		conn, err := lis.Accept()
		if err != nil {
			if server.isShuttingDown() || errors.Is(err, net.ErrClosed) {
				return
			}
			log.Println("rpc server: accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}

// Accept 是 DefaultServer.Accept 的快捷入口。
func Accept(lis net.Listener) { DefaultServer.Accept(lis) }

// Shutdown 是 DefaultServer.Shutdown 的快捷入口。
func Shutdown(ctx context.Context) error { return DefaultServer.Shutdown(ctx) }

// Register 把一个接收者对象注册到服务端。
// 只有满足 RPC 约束的方法才会被暴露给远程调用。
//
// 例如传入 &Foo{} 之后，客户端就可以通过 "Foo.Sum" 这种名称发起调用。
func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

// Register 是 DefaultServer.Register 的快捷入口。
func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

const (
	connected            = "200 Connected to Gee RPC" // HTTP CONNECT 握手成功后返回给客户端的状态行
	defaultRPCPath       = "/_geeprc_"                // 默认 RPC 隧道入口，建立后切换到底层 GeeRPC 协议
	defaultDebugPath     = "/debug/geerpc"            // 默认调试页面入口，用于查看已注册服务等信息
	defaultDebugJSONPath = "/debug/geerpc/stats"      // 默认 JSON 指标入口，便于脚本和监控直接抓取
)

// ServeHTTP 允许把 RPC 服务挂到 HTTP 服务上。
// 它采用 CONNECT 建立隧道，握手成功后不再继续处理普通 HTTP 语义，
// 而是直接把 hijack 出来的底层连接交给 ServeConn，切换到 GeeRPC 协议。
//
// 这么做的好处是：
// 1. 入口层还能复用 HTTP 服务器和端口
// 2. 连接建立后，后续仍然是高效的长连接 RPC 通信
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// 这里只接受 CONNECT。
	// 普通 GET/POST 仍然是 HTTP 语义，无法直接切换到长连接 RPC 字节流。
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}

	// Hijack 之后，HTTP 服务器不再接管这条连接；
	// 后续读写全部回到 GeeRPC 自己的协议栈中。
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	// 先回一条简短的 HTTP 成功响应，告诉客户端“隧道已经建立”，
	// 然后立刻复用这条底层连接进入 RPC 协议处理。
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.ServeConn(conn)
}

// HandleHTTP 把 RPC 处理器和调试页面注册到默认 HTTP mux。
// 这里只做“路由挂载”，并不会主动启动 HTTP 服务；
// 调用方仍然需要自行执行 http.Serve / http.ListenAndServe。
func (server *Server) HandleHTTP() {
	// 注册到默认 mux，方便调用方直接用标准库启动 HTTP 服务。
	http.Handle(defaultRPCPath, server)
	http.Handle(defaultDebugPath, debugHTTP{server})
	http.Handle(defaultDebugJSONPath, debugJSONHTTP{server})
	log.Println("rpc server debug path:", defaultDebugPath)
	log.Println("rpc server debug json path:", defaultDebugJSONPath)
}

// HandleHTTP 是 DefaultServer.HandleHTTP 的快捷入口。
func HandleHTTP() {
	DefaultServer.HandleHTTP()
}
