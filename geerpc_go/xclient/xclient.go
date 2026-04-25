// XClient 本质上是一个连接池 + 负载均衡调度器，内部管理着多个 Client，每个 Client 对应一个服务节点地址。

package xclient

import (
	"context"
	"errors"
	"fmt"
	. "geerpc"
	"io"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// XClient 是对底层 Client 的进一步封装。
// 它不直接绑定单个服务地址，而是结合服务发现和负载均衡策略，
// 在多个服务节点之间自动选择、复用和管理连接。
type XClient struct {
	d                 Discovery     // 服务发现实例
	mode              SelectMode    // 负载均衡模式
	opt               *Option       // 协议选项
	cfg               XClientConfig // XClient 级治理配置
	mu                sync.Mutex    // 保护连接池
	pools             map[string]*connPool
	seenAddrs         map[string]struct{}
	circuits          map[string]*circuitBreaker
	callFunc          func(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error
	retriedCalls      uint64
	retryAttempts     uint64
	failoverSuccesses uint64
	failoverFailures  uint64
	circuitTrips      uint64
	halfOpenSuccesses uint64
	halfOpenFailures  uint64
	idleEvictions     uint64
	reconnects        uint64
	lastCleanupUnix   int64
	stopCleanup       chan struct{}
	cleanupDone       chan struct{}
	closeOnce         sync.Once
}

var _ io.Closer = (*XClient)(nil)

// ErrKeyRoutingUnsupported 表示当前服务发现实现不支持按 key 路由。
var ErrKeyRoutingUnsupported = errors.New("rpc discovery: keyed routing is not supported")

// ErrEmptyRouteKey 表示调用方没有提供合法的业务路由 key。
// 空 key 会把所有流量退化到同一条固定路由上，因此这里显式拒绝。
var ErrEmptyRouteKey = errors.New("rpc discovery: route key is empty")

// RetryPolicy 描述 XClient 层的重试策略。
// 这里只处理连接/节点级失败，不处理业务错误。
type RetryPolicy struct {
	MaxRetries int
	Backoff    time.Duration
}

// CircuitBreakerPolicy 描述按节点隔离的熔断策略。
// 它只统计连接/节点级失败，不统计业务错误。
type CircuitBreakerPolicy struct {
	FailureThreshold int
	Cooldown         time.Duration
}

// XClientConfig 描述 XClient 自身的治理配置。
// 这些配置不属于 RPC 协议协商范畴，因此不会放进 geerpc.Option。
type XClientConfig struct {
	RetryPolicy          RetryPolicy
	CircuitBreakerPolicy CircuitBreakerPolicy
	MaxIdleDuration      time.Duration
	CleanupInterval      time.Duration
	MaxConnsPerHost      int
}

// ClientConnStats 是单个底层连接的统计快照。
type ClientConnStats struct {
	Addr       string
	LastUsedAt time.Time
	Stats      ClientStats
}

// XClientStats 是 XClient 的连接池和调用统计快照。
type XClientStats struct {
	CachedConnections      int
	AvailableConnections   int
	ClosingConnections     int
	ShutdownConnections    int
	UnavailableConnections int
	PendingCalls           int
	StartedCalls           uint64
	CompletedCalls         uint64
	FailedCalls            uint64
	CanceledCalls          uint64
	WriteFailures          uint64
	ReceiveFailures        uint64
	RetriedCalls           uint64
	RetryAttempts          uint64
	FailoverSuccesses      uint64
	FailoverFailures       uint64
	OpenCircuits           int
	HalfOpenCircuits       int
	CircuitTrips           uint64
	HalfOpenProbeSuccesses uint64
	HalfOpenProbeFailures  uint64
	IdleEvictions          uint64
	Reconnects             uint64
	LastCleanupAt          time.Time
	Clients                []ClientConnStats
}

type clientEntry struct {
	client   *Client
	lastUsed time.Time
}

type circuitState string

const (
	circuitClosed   circuitState = "closed"
	circuitOpen     circuitState = "open"
	circuitHalfOpen circuitState = "half-open"
)

type circuitBreaker struct {
	state               circuitState
	consecutiveFailures int
	openUntil           time.Time
}

// NewXClient 创建一个带服务发现能力的客户端。
func NewXClient(d Discovery, mode SelectMode, opt *Option) *XClient {
	return NewXClientWithConfig(d, mode, opt, nil)
}

func normalizeConfig(cfg *XClientConfig) XClientConfig {
	if cfg == nil {
		return XClientConfig{}
	}
	normalized := *cfg
	if normalized.RetryPolicy.MaxRetries < 0 {
		normalized.RetryPolicy.MaxRetries = 0
	}
	if normalized.RetryPolicy.Backoff < 0 {
		normalized.RetryPolicy.Backoff = 0
	}
	if normalized.CircuitBreakerPolicy.FailureThreshold < 0 {
		normalized.CircuitBreakerPolicy.FailureThreshold = 0
	}
	if normalized.CircuitBreakerPolicy.Cooldown < 0 {
		normalized.CircuitBreakerPolicy.Cooldown = 0
	}
	if normalized.MaxIdleDuration < 0 {
		normalized.MaxIdleDuration = 0
	}
	if normalized.CleanupInterval < 0 {
		normalized.CleanupInterval = 0
	}
	if normalized.MaxIdleDuration > 0 && normalized.CleanupInterval == 0 {
		normalized.CleanupInterval = time.Second
	}
	return normalized
}

// NewXClientWithConfig 创建一个带服务发现和治理配置的客户端。
func NewXClientWithConfig(d Discovery, mode SelectMode, opt *Option, cfg *XClientConfig) *XClient {
	xc := &XClient{
		d:         d,
		mode:      mode,
		opt:       opt,
		cfg:       normalizeConfig(cfg),
		pools:     make(map[string]*connPool),
		seenAddrs: make(map[string]struct{}),
		circuits:  make(map[string]*circuitBreaker),
	}
	if xc.cfg.MaxIdleDuration > 0 && xc.cfg.CleanupInterval > 0 {
		xc.stopCleanup = make(chan struct{})
		xc.cleanupDone = make(chan struct{})
		go xc.cleanupLoop()
	}
	return xc
}

// Close 关闭所有缓存的底层 Client。
func (xc *XClient) Close() error {
	xc.closeOnce.Do(func() {
		if xc.stopCleanup != nil {
			close(xc.stopCleanup)
		}
	})
	if xc.cleanupDone != nil {
		<-xc.cleanupDone
	}
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for key, pool := range xc.pools {
		pool.Close()
		delete(xc.pools, key)
	}
	return nil
}

// Stats 返回 XClient 当前连接池和底层 Client 的统计快照。
func (xc *XClient) Stats() XClientStats {
	xc.mu.Lock()
	defer xc.mu.Unlock()

	stats := XClientStats{
		CachedConnections: len(xc.pools),
	}
	addrs := make([]string, 0, len(xc.pools))
	for addr := range xc.pools {
		addrs = append(addrs, addr)
	}
	sort.Strings(addrs)
	for _, addr := range addrs {
		pool := xc.pools[addr]
		pool.mu.Lock()
		for _, client := range pool.idle {
			clientStats := client.Stats()
			stats.Clients = append(stats.Clients, ClientConnStats{
				Addr:  addr,
				Stats: clientStats,
			})
			switch clientStats.State {
			case "closing":
				stats.ClosingConnections++
				stats.UnavailableConnections++
			case "shutdown":
				stats.ShutdownConnections++
				stats.UnavailableConnections++
			default:
				stats.AvailableConnections++
			}
			stats.PendingCalls += clientStats.PendingCalls
			stats.StartedCalls += clientStats.StartedCalls
			stats.CompletedCalls += clientStats.CompletedCalls
			stats.FailedCalls += clientStats.FailedCalls
			stats.CanceledCalls += clientStats.CanceledCalls
			stats.WriteFailures += clientStats.WriteFailures
			stats.ReceiveFailures += clientStats.ReceiveFailures
		}
		pool.mu.Unlock()
	}
	stats.RetriedCalls = atomic.LoadUint64(&xc.retriedCalls)
	stats.RetryAttempts = atomic.LoadUint64(&xc.retryAttempts)
	stats.FailoverSuccesses = atomic.LoadUint64(&xc.failoverSuccesses)
	stats.FailoverFailures = atomic.LoadUint64(&xc.failoverFailures)
	stats.CircuitTrips = atomic.LoadUint64(&xc.circuitTrips)
	stats.HalfOpenProbeSuccesses = atomic.LoadUint64(&xc.halfOpenSuccesses)
	stats.HalfOpenProbeFailures = atomic.LoadUint64(&xc.halfOpenFailures)
	stats.IdleEvictions = atomic.LoadUint64(&xc.idleEvictions)
	stats.Reconnects = atomic.LoadUint64(&xc.reconnects)
	stats.LastCleanupAt = snapshotTime(atomic.LoadInt64(&xc.lastCleanupUnix))
	for _, breaker := range xc.circuits {
		switch breaker.state {
		case circuitOpen:
			stats.OpenCircuits++
		case circuitHalfOpen:
			stats.HalfOpenCircuits++
		}
	}
	return stats
}

func snapshotTime(unixNano int64) time.Time {
	if unixNano == 0 {
		return time.Time{}
	}
	return time.Unix(0, unixNano).UTC()
}

func (xc *XClient) cleanupLoop() {
	defer close(xc.cleanupDone)
	ticker := time.NewTicker(xc.cfg.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			xc.cleanupConnections(time.Now())
		case <-xc.stopCleanup:
			return
		}
	}
}

func (xc *XClient) cleanupConnections(now time.Time) {
	if xc.cfg.MaxIdleDuration <= 0 {
		return
	}
	atomic.StoreInt64(&xc.lastCleanupUnix, now.UnixNano())
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for addr, pool := range xc.pools {
		pool.mu.Lock()
		idle := pool.active == 0 && len(pool.idle) > 0 && now.Sub(pool.lastUsed) >= xc.cfg.MaxIdleDuration
		openCircuit := pool.active == 0 && len(pool.idle) > 0 && xc.circuitOpenLocked(addr, now)
		pool.mu.Unlock()
		if !idle && !openCircuit {
			continue
		}
		pool.Close()
		delete(xc.pools, addr)
		if idle {
			atomic.AddUint64(&xc.idleEvictions, 1)
		}
	}
}

func (xc *XClient) circuitOpenLocked(addr string, now time.Time) bool {
	breaker := xc.circuits[addr]
	return breaker != nil && breaker.state == circuitOpen && now.Before(breaker.openUntil)
}

func (xc *XClient) getPool(rpcAddr string) *connPool {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	pool, ok := xc.pools[rpcAddr]
	if !ok {
		maxConns := xc.cfg.MaxConnsPerHost
		if maxConns <= 0 {
			maxConns = 1
		}
		if _, seen := xc.seenAddrs[rpcAddr]; seen {
			atomic.AddUint64(&xc.reconnects, 1)
		}
		xc.seenAddrs[rpcAddr] = struct{}{}
		pool = newConnPool(rpcAddr, xc.opt, maxConns, func(addr string, opt *Option) (*Client, error) {
			return XDial(addr, opt)
		})
		xc.pools[rpcAddr] = pool
	}
	return pool
}

// dial 从连接池中获取一条可用连接。
func (xc *XClient) dial(rpcAddr string) (*Client, error) {
	pool := xc.getPool(rpcAddr)
	return pool.Get()
}

// putClient 将连接归还到连接池。
func (xc *XClient) putClient(rpcAddr string, client *Client) {
	pool := xc.getPool(rpcAddr)
	pool.Put(client)
}

// forgetClient 关闭某个地址的整个连接池。
func (xc *XClient) forgetClient(rpcAddr string) {
	xc.mu.Lock()
	pool, ok := xc.pools[rpcAddr]
	if ok {
		delete(xc.pools, rpcAddr)
	}
	xc.mu.Unlock()
	if pool != nil {
		pool.Close()
	}
}

// call 在指定节点上发起一次 RPC 调用。
func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	if xc.callFunc != nil {
		return xc.callFunc(rpcAddr, ctx, serviceMethod, args, reply)
	}
	client, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}
	defer xc.putClient(rpcAddr, client)
	return client.Call(ctx, serviceMethod, args, reply)
}

// callWithRelease 在调用结束后按需归还 discovery 内部的占用状态。
// 这对最少连接数等“选中节点时先记账”的策略是必须的。
func (xc *XClient) callWithRelease(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}, release bool) error {
	if release {
		if d, ok := xc.d.(ReleasableDiscovery); ok {
			defer d.Done(rpcAddr)
		}
	}
	return xc.call(rpcAddr, ctx, serviceMethod, args, reply)
}

func wrapContextError(err error) error {
	if err == nil {
		return nil
	}
	return errors.New("rpc client: call failed: " + err.Error())
}

func (xc *XClient) circuitBreakerEnabled() bool {
	return xc.cfg.CircuitBreakerPolicy.FailureThreshold > 0 && xc.cfg.CircuitBreakerPolicy.Cooldown > 0
}

func (xc *XClient) acquireCircuit(addr string) bool {
	if !xc.circuitBreakerEnabled() {
		return true
	}
	now := time.Now()
	xc.mu.Lock()
	defer xc.mu.Unlock()
	breaker := xc.circuits[addr]
	if breaker == nil {
		return true
	}
	switch breaker.state {
	case circuitClosed:
		return true
	case circuitOpen:
		if now.Before(breaker.openUntil) {
			return false
		}
		breaker.state = circuitHalfOpen
		return true
	case circuitHalfOpen:
		return false
	default:
		return true
	}
}

func (xc *XClient) recordCircuitHealthy(addr string) {
	if !xc.circuitBreakerEnabled() {
		return
	}
	xc.mu.Lock()
	defer xc.mu.Unlock()
	breaker := xc.circuits[addr]
	if breaker == nil {
		return
	}
	if breaker.state == circuitHalfOpen {
		atomic.AddUint64(&xc.halfOpenSuccesses, 1)
	}
	breaker.state = circuitClosed
	breaker.consecutiveFailures = 0
	breaker.openUntil = time.Time{}
}

func (xc *XClient) recordCircuitFailure(addr string) {
	if !xc.circuitBreakerEnabled() {
		return
	}
	threshold := xc.cfg.CircuitBreakerPolicy.FailureThreshold
	cooldown := xc.cfg.CircuitBreakerPolicy.Cooldown
	xc.mu.Lock()
	defer xc.mu.Unlock()
	breaker := xc.circuits[addr]
	if breaker == nil {
		breaker = &circuitBreaker{state: circuitClosed}
		xc.circuits[addr] = breaker
	}
	trip := false
	switch breaker.state {
	case circuitHalfOpen:
		atomic.AddUint64(&xc.halfOpenFailures, 1)
		breaker.consecutiveFailures = threshold
		trip = true
	default:
		breaker.consecutiveFailures++
		if breaker.consecutiveFailures >= threshold {
			trip = true
		}
	}
	if trip {
		breaker.state = circuitOpen
		breaker.openUntil = time.Now().Add(cooldown)
		atomic.AddUint64(&xc.circuitTrips, 1)
	}
}

func isRetryableCallError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, ErrShutdown) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return !netErr.Timeout()
	}
	return false
}

func (xc *XClient) finishRetryCall(retryAttempts int, success bool) {
	if retryAttempts == 0 {
		return
	}
	atomic.AddUint64(&xc.retriedCalls, 1)
	atomic.AddUint64(&xc.retryAttempts, uint64(retryAttempts))
	if success {
		atomic.AddUint64(&xc.failoverSuccesses, 1)
		return
	}
	atomic.AddUint64(&xc.failoverFailures, 1)
}

func (xc *XClient) waitRetryBackoff(ctx context.Context) error {
	backoff := xc.cfg.RetryPolicy.Backoff
	if backoff <= 0 {
		return nil
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return wrapContextError(ctx.Err())
	}
}

func (xc *XClient) nextCallAddr(attempted map[string]struct{}) (string, error) {
	excluded := make(map[string]struct{}, len(attempted))
	for addr := range attempted {
		excluded[addr] = struct{}{}
	}
	if d, ok := xc.d.(interface {
		getExcluding(mode SelectMode, exclude map[string]struct{}) (string, error)
	}); ok {
		for {
			addr, err := d.getExcluding(xc.mode, excluded)
			if err != nil {
				return "", err
			}
			if xc.acquireCircuit(addr) {
				return addr, nil
			}
			if d, ok := xc.d.(ReleasableDiscovery); ok {
				d.Done(addr)
			}
			excluded[addr] = struct{}{}
		}
	}
	servers, err := xc.d.GetAll()
	if err != nil {
		return "", err
	}
	if len(servers) == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	for i := 0; i < len(servers); i++ {
		addr, err := xc.d.Get(xc.mode)
		if err != nil {
			return "", err
		}
		if _, seen := excluded[addr]; seen {
			if d, ok := xc.d.(ReleasableDiscovery); ok {
				d.Done(addr)
			}
			continue
		}
		if !xc.acquireCircuit(addr) {
			if d, ok := xc.d.(ReleasableDiscovery); ok {
				d.Done(addr)
			}
			excluded[addr] = struct{}{}
			continue
		}
		return addr, nil
	}
	return "", errors.New("rpc discovery: no untried servers available")
}

func (xc *XClient) callWithFailover(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	attempted := make(map[string]struct{})
	maxRetries := xc.cfg.RetryPolicy.MaxRetries
	retryAttempts := 0
	attempts := 0
	var lastErr error

	for {
		rpcAddr, err := xc.nextCallAddr(attempted)
		if err != nil {
			xc.finishRetryCall(retryAttempts, false)
			if lastErr != nil {
				return fmt.Errorf("rpc xclient: failover exhausted after %d attempts: last error: %v", attempts, lastErr)
			}
			return err
		}
		attempted[rpcAddr] = struct{}{}
		attempts++

		err = xc.callWithRelease(rpcAddr, ctx, serviceMethod, args, reply, true)
		if err == nil {
			xc.recordCircuitHealthy(rpcAddr)
			xc.finishRetryCall(retryAttempts, retryAttempts > 0)
			return nil
		}
		lastErr = err
		retryable := isRetryableCallError(ctx, err)
		if retryable {
			xc.recordCircuitFailure(rpcAddr)
		} else {
			xc.recordCircuitHealthy(rpcAddr)
		}
		if !retryable {
			xc.finishRetryCall(retryAttempts, false)
			return err
		}

		xc.forgetClient(rpcAddr)
		if retryAttempts >= maxRetries {
			xc.finishRetryCall(retryAttempts, false)
			return fmt.Errorf("rpc xclient: failover exhausted after %d attempts: last error: %v", attempts, lastErr)
		}
		if err := xc.d.Refresh(); err != nil {
			xc.finishRetryCall(retryAttempts, false)
			return fmt.Errorf("rpc xclient: failover refresh error after %d attempts: %w", attempts, err)
		}
		if err := xc.waitRetryBackoff(ctx); err != nil {
			xc.finishRetryCall(retryAttempts, false)
			return err
		}
		retryAttempts++
	}
}

// Call 根据负载均衡策略选择一个服务节点执行调用。
func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	if xc.cfg.RetryPolicy.MaxRetries > 0 || xc.circuitBreakerEnabled() {
		return xc.callWithFailover(ctx, serviceMethod, args, reply)
	}
	rpcAddr, err := xc.d.Get(xc.mode)
	if err != nil {
		return err
	}
	return xc.callWithRelease(rpcAddr, ctx, serviceMethod, args, reply, true)
}

// CallByKey 根据业务 key 选择一个稳定节点执行调用。
// key 应该是稳定的业务标识，例如用户 ID、租户 ID、分片键；
// 不应该使用时间戳、随机数这类每次都变化的值，否则会退化成随机分布。
//
// 注意：一致性哈希只能保证“同一 key 在当前节点集合下尽量稳定”。
// 当服务节点发生增删时，只有一部分 key 会被重新映射，而不是全部重排。
//
// 只有实现了 KeyedDiscovery 的 discovery 才支持该能力。
// 为了保住“稳定 key 尽量稳定落点”的语义，这里暂不自动接入故障转移。
func (xc *XClient) CallByKey(ctx context.Context, key, serviceMethod string, args, reply interface{}) error {
	if strings.TrimSpace(key) == "" {
		return ErrEmptyRouteKey
	}
	if err := xc.d.Refresh(); err != nil {
		return err
	}
	d, ok := xc.d.(KeyedDiscovery)
	if !ok {
		return ErrKeyRoutingUnsupported
	}
	rpcAddr, err := d.GetByKey(key)
	if err != nil {
		return err
	}
	return xc.callWithRelease(rpcAddr, ctx, serviceMethod, args, reply, true)
}

// Broadcast 把同一个请求广播到当前发现到的所有服务节点。
// 典型用途是：
// 1. 对所有节点执行相同操作
// 2. 容忍多个节点并发执行，但只取一个成功响应作为 reply
// 只要有一个节点返回错误，就取消其余未完成调用。
// 广播的失败语义比普通调用复杂得多，因此这里暂不自动接入故障转移。
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex // 保护错误值 e 和 replyDone
	var e error
	replyDone := reply == nil // reply 为 nil 时表示调用方不关心返回值
	ctx, cancel := context.WithCancel(ctx)
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			var clonedReply interface{}
			if reply != nil {
				// 每个节点使用独立的 reply 副本，避免并发写同一个对象。
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := xc.callWithRelease(rpcAddr, ctx, serviceMethod, args, clonedReply, false)
			mu.Lock()
			if err != nil && e == nil {
				e = err
				cancel() // 任一节点失败后，尽快取消其他未完成调用
			}
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return e
}
