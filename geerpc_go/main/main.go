package main

import (
	"context"
	"fmt"
	"geerpc"
	"geerpc/registry"
	"geerpc/xclient"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Foo struct {
	ID      string
	Version string
}

type Args struct{ Num1, Num2 int }

// Sum 是一个普通的 RPC 方法，用来演示最基本的远程调用。
func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

// Sleep 会故意睡眠一段时间，用来演示超时和广播调用场景。
func (f Foo) Sleep(args Args, reply *int) error {
	time.Sleep(time.Second * time.Duration(args.Num1))
	*reply = args.Num1 + args.Num2
	return nil
}

// Panic 故意触发 panic，用来演示服务端的 panic 隔离能力。
func (f Foo) Panic(args Args, reply *int) error {
	panic(fmt.Sprintf("panic on %s with args=%+v", f.ID, args))
}

// Trace 演示请求 metadata 会自动透传到服务端 context。
func (f Foo) Trace(ctx context.Context, args Args, reply *string) error {
	traceID := geerpc.MetadataFromContext(ctx)[geerpc.MetadataTraceIDKey]
	log.Printf("trace on %s version=%s trace_id=%s", f.ID, f.Version, traceID)
	*reply = traceID
	return nil
}

type managedServer struct {
	addr      string
	server    *geerpc.Server
	listener  net.Listener
	heartbeat *registry.HeartbeatController
}

// Identity 返回当前处理请求的服务实例标识。
func (f Foo) Identity(args Args, reply *string) error {
	*reply = f.ID
	return nil
}

func buildFoo(rpcAddr string, opts *registry.ServerOptions) Foo {
	version := registry.DefaultVersion
	if opts != nil && opts.Version != "" {
		version = opts.Version
	}
	id := rpcAddr
	if version != "" {
		id = fmt.Sprintf("%s[%s]", rpcAddr, version)
	}
	return Foo{
		ID:      id,
		Version: version,
	}
}

// SleepIdentity 会先睡眠，再返回实例标识，用来演示最少连接数策略下的活跃调用分布。
func (f Foo) SleepIdentity(args Args, reply *string) error {
	time.Sleep(time.Second * time.Duration(args.Num1))
	*reply = f.ID
	return nil
}

// startRegistry 启动一个 HTTP 注册中心。
func startRegistry(wg *sync.WaitGroup) {
	l, _ := net.Listen("tcp", ":9999")
	registry.HandleHTTP()
	wg.Done()
	_ = http.Serve(l, nil)
}

// startServer 启动一个 RPC 服务实例，并把自己的地址通过心跳注册到注册中心。
func startServer(registryAddr string, addrCh chan<- string, wg *sync.WaitGroup) {
	startServerWithOptions(registryAddr, addrCh, wg, nil)
}

func startServerWithOptions(registryAddr string, addrCh chan<- string, wg *sync.WaitGroup, opts *registry.ServerOptions) {
	l, _ := net.Listen("tcp", ":0")
	rpcAddr := "tcp@" + l.Addr().String()
	foo := buildFoo(rpcAddr, opts)
	server := geerpc.NewServer()
	_ = server.Register(&foo)
	registry.HeartbeatWithOptions(registryAddr, rpcAddr, 0, opts)
	if addrCh != nil {
		addrCh <- rpcAddr
	}
	wg.Done()
	server.Accept(l)
}

func startConfiguredServer(cfg *geerpc.ServerConfig) (*geerpc.Server, string, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, "", err
	}
	rpcAddr := "tcp@" + l.Addr().String()
	foo := buildFoo(rpcAddr, nil)
	server := geerpc.NewServerWithConfig(cfg)
	if err := server.Register(&foo); err != nil {
		_ = l.Close()
		return nil, "", err
	}
	go server.Accept(l)
	return server, rpcAddr, nil
}

func startManagedServer(registryAddr string) (*managedServer, error) {
	return startManagedServerWithOptions(registryAddr, nil)
}

func startManagedServerWithOptions(registryAddr string, opts *registry.ServerOptions) (*managedServer, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	rpcAddr := "tcp@" + l.Addr().String()
	foo := buildFoo(rpcAddr, opts)
	server := geerpc.NewServer()
	if err := server.Register(&foo); err != nil {
		_ = l.Close()
		return nil, err
	}
	ms := &managedServer{
		addr:      rpcAddr,
		server:    server,
		listener:  l,
		heartbeat: registry.HeartbeatWithOptions(registryAddr, rpcAddr, 0, opts),
	}
	go server.Accept(l)
	return ms, nil
}

func (s *managedServer) shutdown(ctx context.Context) error {
	if s.heartbeat != nil {
		if err := s.heartbeat.Stop(); err != nil {
			return err
		}
	}
	return s.server.Shutdown(ctx)
}

// foo 是一个统一的演示辅助函数，用于发起普通调用或广播调用，并输出结果。
func foo(xc *xclient.XClient, ctx context.Context, typ, serviceMethod string, args *Args) {
	var reply int
	var err error
	switch typ {
	case "call":
		err = xc.Call(ctx, serviceMethod, args, &reply)
	case "broadcast":
		err = xc.Broadcast(ctx, serviceMethod, args, &reply)
	}
	if err != nil {
		log.Printf("%s %s error: %v", typ, serviceMethod, err)
	} else {
		log.Printf("%s %s success: %d + %d = %d", typ, serviceMethod, args.Num1, args.Num2, reply)
	}
}

// call 演示从注册中心发现服务后，按随机策略选择一个节点进行普通 RPC 调用。
func call(registry string) {
	d := xclient.NewGeeRegistryDiscovery(registry, 0)
	xc := xclient.NewXClient(d, xclient.RandomSelect, nil)
	defer func() { _ = xc.Close() }()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "call", "Foo.Sum", &Args{Num1: i, Num2: i * i})
		}(i)
	}
	wg.Wait()
}

// broadcast 演示把同一个请求广播到全部节点，并观察超时行为。
func broadcast(registry string) {
	d := xclient.NewGeeRegistryDiscovery(registry, 0)
	xc := xclient.NewXClient(d, xclient.RandomSelect, nil)
	defer func() { _ = xc.Close() }()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "broadcast", "Foo.Sum", &Args{Num1: i, Num2: i * i})
			// 这里故意给 Sleep 设置 2 秒超时，用来观察不同节点执行速度不同时的取消效果。
			ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
			foo(xc, ctx, "broadcast", "Foo.Sleep", &Args{Num1: i, Num2: i * i})
		}(i)
	}
	wg.Wait()
}

// callByKey 演示使用注册中心发现 + 一致性哈希按业务 key 做稳定路由。
// 这里会同时演示三类情况：
// 1. 稳定业务 key：相同租户/用户通常会落到同一节点
// 2. 不稳定 key：请求 ID 这类值每次都变，无法形成稳定路由
// 3. 空 key：会被框架直接拒绝，避免所有流量退化到同一路由
func callByKey(registry string) {
	d := xclient.NewGeeRegistryDiscovery(registry, 0)
	xc := xclient.NewXClient(d, xclient.RandomSelect, nil)
	defer func() { _ = xc.Close() }()

	log.Println("callByKey with stable business keys:")
	keys := []string{"tenant-a", "tenant-a", "tenant-b", "tenant-a", "tenant-b"}
	for _, key := range keys {
		var serverID string
		err := xc.CallByKey(context.Background(), key, "Foo.Identity", &Args{}, &serverID)
		if err != nil {
			log.Printf("callByKey %s error: %v", key, err)
			continue
		}
		log.Printf("callByKey %s -> %s", key, serverID)
	}

	log.Println("callByKey with request-style keys (not recommended):")
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("request-%d", i)
		var serverID string
		err := xc.CallByKey(context.Background(), key, "Foo.Identity", &Args{}, &serverID)
		if err != nil {
			log.Printf("callByKey %s error: %v", key, err)
			continue
		}
		log.Printf("callByKey %s -> %s", key, serverID)
	}

	var serverID string
	if err := xc.CallByKey(context.Background(), "   ", "Foo.Identity", &Args{}, &serverID); err != nil {
		log.Printf("callByKey with empty key rejected: %v", err)
	}
}

// weightedCall 演示对已知节点列表使用加权轮询。
func weightedCall(addrs []string) {
	if len(addrs) < 2 {
		log.Printf("weightedCall skipped: need at least 2 servers, got %d", len(addrs))
		return
	}
	d := xclient.NewWeightedDiscovery([]xclient.WeightedServer{
		{Addr: addrs[0], Weight: 2},
		{Addr: addrs[1], Weight: 1},
	})
	xc := xclient.NewXClient(d, xclient.RandomSelect, nil)
	defer func() { _ = xc.Close() }()

	for i := 0; i < 6; i++ {
		var serverID string
		err := xc.Call(context.Background(), "Foo.Identity", &Args{}, &serverID)
		if err != nil {
			log.Printf("weighted call %d error: %v", i, err)
			continue
		}
		log.Printf("weighted call %d -> %s", i, serverID)
	}
}

// leastConnCall 演示对已知节点列表使用最少连接数策略。
// 这里让每次调用都占住节点 1 秒，便于观察调度在并发场景下趋向于均衡。
func leastConnCall(addrs []string) {
	if len(addrs) == 0 {
		log.Printf("leastConnCall skipped: no servers")
		return
	}
	d := xclient.NewLeastConnDiscovery(addrs)
	xc := xclient.NewXClient(d, xclient.RandomSelect, nil)
	defer func() { _ = xc.Close() }()

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var serverID string
			err := xc.Call(context.Background(), "Foo.SleepIdentity", &Args{Num1: 1}, &serverID)
			if err != nil {
				log.Printf("least-conn call %d error: %v", i, err)
				return
			}
			log.Printf("least-conn call %d -> %s", i, serverID)
		}(i)
	}
	wg.Wait()
}

// failoverCall 演示客户端重试与故障转移。
// 这里故意把第一个节点设为无效地址，第二个节点设为真实服务，
// 这样能稳定复现“首次失败 -> 自动切到健康节点 -> 调用成功”的过程。
func failoverCall(addrs []string) {
	if len(addrs) == 0 {
		log.Printf("failoverCall skipped: no healthy servers")
		return
	}
	d := xclient.NewLeastConnDiscovery([]string{"tcp@127.0.0.1:1", addrs[0]})
	xc := xclient.NewXClientWithConfig(d, xclient.RoundRobinSelect, nil, &xclient.XClientConfig{
		RetryPolicy: xclient.RetryPolicy{
			MaxRetries: 1,
		},
	})
	defer func() { _ = xc.Close() }()

	var serverID string
	err := xc.Call(context.Background(), "Foo.Identity", &Args{}, &serverID)
	if err != nil {
		log.Printf("failover call error: %v", err)
		return
	}
	log.Printf("failover call success -> %s", serverID)
	log.Printf("failover stats: %+v", xc.Stats())
}

// circuitBreakerCall 演示节点熔断与隔离。
// 第一次调用会先撞到坏节点并触发 failover，同时把坏节点熔断；
// 紧接着的第二次调用会直接跳过该节点，命中健康实例。
func circuitBreakerCall(addrs []string) {
	if len(addrs) == 0 {
		log.Printf("circuitBreakerCall skipped: no healthy servers")
		return
	}
	d := xclient.NewLeastConnDiscovery([]string{"tcp@127.0.0.1:1", addrs[0]})
	xc := xclient.NewXClientWithConfig(d, xclient.RoundRobinSelect, nil, &xclient.XClientConfig{
		RetryPolicy: xclient.RetryPolicy{
			MaxRetries: 1,
		},
		CircuitBreakerPolicy: xclient.CircuitBreakerPolicy{
			FailureThreshold: 1,
			Cooldown:         200 * time.Millisecond,
		},
	})
	defer func() { _ = xc.Close() }()

	for i := 0; i < 2; i++ {
		var serverID string
		err := xc.Call(context.Background(), "Foo.Identity", &Args{}, &serverID)
		if err != nil {
			log.Printf("circuit-breaker call %d error: %v", i, err)
			continue
		}
		log.Printf("circuit-breaker call %d -> %s", i, serverID)
	}
	log.Printf("circuit-breaker stats: %+v", xc.Stats())
}

func startDemoRegistry() (string, func()) {
	r := registry.New(time.Minute)
	mux := http.NewServeMux()
	mux.Handle("/_geerpc_/registry", r)
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Printf("startDemoRegistry listen error: %v", err)
		return "", func() {}
	}
	go func() {
		_ = http.Serve(l, mux)
	}()
	return "http://" + l.Addr().String() + "/_geerpc_/registry", func() { _ = l.Close() }
}

// gracefulShutdownCall 演示服务节点优雅下线：
// 1. 节点先从注册中心摘除，避免新流量继续打进来
// 2. 已在执行中的请求继续完成
// 3. 下线完成后，服务发现中不再包含该节点
func gracefulShutdownCall() {
	registryAddr, stopRegistry := startDemoRegistry()
	if registryAddr == "" {
		return
	}
	defer stopRegistry()

	server, err := startManagedServer(registryAddr)
	if err != nil {
		log.Printf("graceful shutdown demo start server error: %v", err)
		return
	}

	time.Sleep(200 * time.Millisecond)

	d := xclient.NewGeeRegistryDiscovery(registryAddr, 50*time.Millisecond)
	xc := xclient.NewXClient(d, xclient.RandomSelect, nil)
	defer func() { _ = xc.Close() }()

	callDone := make(chan error, 1)
	go func() {
		var reply int
		err := xc.Call(context.Background(), "Foo.Sleep", &Args{Num1: 1, Num2: 99}, &reply)
		if err != nil {
			callDone <- err
			return
		}
		log.Printf("graceful shutdown in-flight call success: reply=%d", reply)
		callDone <- nil
	}()

	time.Sleep(100 * time.Millisecond)

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		shutdownDone <- server.shutdown(ctx)
	}()

	if err := <-callDone; err != nil {
		log.Printf("graceful shutdown in-flight call error: %v", err)
		return
	}
	if err := <-shutdownDone; err != nil {
		log.Printf("graceful shutdown error: %v", err)
		return
	}

	if err := d.Refresh(); err != nil {
		log.Printf("graceful shutdown refresh error: %v", err)
		return
	}
	addrs, err := d.GetAll()
	if err != nil {
		log.Printf("graceful shutdown getAll error: %v", err)
		return
	}
	log.Printf("graceful shutdown registry servers: %v", addrs)

	var reply int
	if err := xc.Call(context.Background(), "Foo.Sum", &Args{Num1: 1, Num2: 2}, &reply); err != nil {
		log.Printf("graceful shutdown follow-up call rejected: %v", err)
	}
}

// overloadProtectCall 演示服务端并发保护。
// 这里把服务端最大并发限制为 1，然后并发发起两个慢调用：
// 一个会正常执行，另一个会被服务端快速拒绝。
func overloadProtectCall() {
	server, rpcAddr, err := startConfiguredServer(&geerpc.ServerConfig{
		MaxConcurrentRequests: 1,
	})
	if err != nil {
		log.Printf("overloadProtectCall start server error: %v", err)
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	client, err := geerpc.XDial(rpcAddr)
	if err != nil {
		log.Printf("overloadProtectCall dial error: %v", err)
		return
	}
	defer func() { _ = client.Close() }()

	started := make(chan struct{}, 1)
	firstDone := make(chan error, 1)
	go func() {
		close(started)
		var reply int
		err := client.Call(context.Background(), "Foo.Sleep", &Args{Num1: 1, Num2: 10}, &reply)
		if err != nil {
			firstDone <- err
			return
		}
		log.Printf("overload first call success: reply=%d", reply)
		firstDone <- nil
	}()

	<-started
	time.Sleep(100 * time.Millisecond)

	var rejectedReply int
	err = client.Call(context.Background(), "Foo.Sleep", &Args{Num1: 1, Num2: 20}, &rejectedReply)
	if err != nil {
		log.Printf("overload second call rejected: %v", err)
	} else {
		log.Printf("overload second call unexpectedly succeeded: reply=%d", rejectedReply)
	}

	if err := <-firstDone; err != nil {
		log.Printf("overload first call error: %v", err)
		return
	}

	log.Printf("overload server stats: %+v", server.Stats())
}

// panicRecoverCall 演示服务端恢复业务 panic：
// 1. 第一次调用触发 panic，但进程不会被打挂
// 2. 后续正常调用仍然可以继续成功
// 3. 服务端统计里能看到 recovered panic 次数
func panicRecoverCall() {
	server, rpcAddr, err := startConfiguredServer(nil)
	if err != nil {
		log.Printf("panicRecoverCall start server error: %v", err)
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	client, err := geerpc.XDial(rpcAddr)
	if err != nil {
		log.Printf("panicRecoverCall dial error: %v", err)
		return
	}
	defer func() { _ = client.Close() }()

	var panicReply int
	err = client.Call(context.Background(), "Foo.Panic", &Args{Num1: 7, Num2: 8}, &panicReply)
	if err != nil {
		log.Printf("panic recover call returned expected error: %v", err)
	} else {
		log.Printf("panic recover call unexpectedly succeeded: %d", panicReply)
	}

	var sumReply int
	err = client.Call(context.Background(), "Foo.Sum", &Args{Num1: 3, Num2: 4}, &sumReply)
	if err != nil {
		log.Printf("panic recover follow-up sum error: %v", err)
		return
	}
	log.Printf("panic recover follow-up sum success: %d", sumReply)
	log.Printf("panic recover server stats: %+v", server.Stats())
}

// traceMetadataCall 演示客户端把 trace_id 放进 context 后，服务端 context 方法可以直接读取。
func traceMetadataCall() {
	server, rpcAddr, err := startConfiguredServer(nil)
	if err != nil {
		log.Printf("traceMetadataCall start server error: %v", err)
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	client, err := geerpc.XDial(rpcAddr)
	if err != nil {
		log.Printf("traceMetadataCall dial error: %v", err)
		return
	}
	defer func() { _ = client.Close() }()

	ctx := geerpc.WithTraceID(context.Background(), "trace-demo-1")
	ctx = geerpc.WithMetadata(ctx, map[string]string{
		geerpc.MetadataRequestIDKey: "req-demo-1",
	})

	var traceID string
	if err := client.Call(ctx, "Foo.Trace", &Args{}, &traceID); err != nil {
		log.Printf("trace metadata call error: %v", err)
		return
	}
	log.Printf("trace metadata call success: trace_id=%s", traceID)
	log.Printf("trace metadata server stats: %+v", server.Stats())
}

// idleCleanupCall 演示 XClient 会清理空闲连接，并在下次调用时自动重连。
func idleCleanupCall() {
	server, rpcAddr, err := startConfiguredServer(nil)
	if err != nil {
		log.Printf("idleCleanupCall start server error: %v", err)
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	d := xclient.NewMultiServerDiscovery([]string{rpcAddr})
	xc := xclient.NewXClientWithConfig(d, xclient.RandomSelect, nil, &xclient.XClientConfig{
		MaxIdleDuration: 150 * time.Millisecond,
		CleanupInterval: 50 * time.Millisecond,
	})
	defer func() { _ = xc.Close() }()

	var first string
	if err := xc.Call(context.Background(), "Foo.Identity", &Args{}, &first); err != nil {
		log.Printf("idle cleanup first call error: %v", err)
		return
	}
	log.Printf("idle cleanup first call -> %s", first)

	time.Sleep(300 * time.Millisecond)
	log.Printf("idle cleanup stats after eviction window: %+v", xc.Stats())

	var second string
	if err := xc.Call(context.Background(), "Foo.Identity", &Args{}, &second); err != nil {
		log.Printf("idle cleanup second call error: %v", err)
		return
	}
	log.Printf("idle cleanup second call -> %s", second)
	log.Printf("idle cleanup final stats: %+v", xc.Stats())
}

// versionRouteCall 演示注册中心标签过滤：客户端只发现 version=v2 的节点。
func versionRouteCall() {
	registryAddr, stopRegistry := startDemoRegistry()
	if registryAddr == "" {
		return
	}
	defer stopRegistry()

	serverV1, err := startManagedServerWithOptions(registryAddr, &registry.ServerOptions{Version: "v1", Weight: 1})
	if err != nil {
		log.Printf("version route start v1 error: %v", err)
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = serverV1.shutdown(ctx)
	}()

	serverV2, err := startManagedServerWithOptions(registryAddr, &registry.ServerOptions{Version: "v2", Weight: 2})
	if err != nil {
		log.Printf("version route start v2 error: %v", err)
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = serverV2.shutdown(ctx)
	}()

	time.Sleep(200 * time.Millisecond)

	d := xclient.NewGeeRegistryDiscoveryWithConfig(registryAddr, 50*time.Millisecond, &xclient.GeeRegistryDiscoveryConfig{
		Version:            "v2",
		UseRegistryWeights: true,
	})
	xc := xclient.NewXClient(d, xclient.RandomSelect, nil)
	defer func() { _ = xc.Close() }()

	for i := 0; i < 3; i++ {
		var serverID string
		if err := xc.Call(context.Background(), "Foo.Identity", &Args{}, &serverID); err != nil {
			log.Printf("version route call %d error: %v", i, err)
			continue
		}
		log.Printf("version route call %d -> %s", i, serverID)
	}
}

// main 按顺序启动注册中心、两个服务实例，然后演示普通调用、稳定路由、广播、
// 加权轮询和最少连接数调度。
func main() {
	log.SetFlags(0)
	registryAddr := "http://localhost:9999/_geerpc_/registry"
	var wg sync.WaitGroup
	wg.Add(1)
	go startRegistry(&wg)
	wg.Wait()

	time.Sleep(time.Second)
	addrCh := make(chan string, 2)
	wg.Add(2)
	go startServer(registryAddr, addrCh, &wg)
	go startServer(registryAddr, addrCh, &wg)
	wg.Wait()
	addrs := []string{<-addrCh, <-addrCh}

	time.Sleep(time.Second)
	call(registryAddr)
	callByKey(registryAddr)
	broadcast(registryAddr)
	weightedCall(addrs)
	leastConnCall(addrs)
	failoverCall(addrs)
	circuitBreakerCall(addrs)
	gracefulShutdownCall()
	overloadProtectCall()
	panicRecoverCall()
	traceMetadataCall()
	idleCleanupCall()
	versionRouteCall()
}
