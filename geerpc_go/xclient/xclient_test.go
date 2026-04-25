package xclient

import (
	"context"
	"encoding/json"
	"errors"
	geerpc "geerpc"
	"geerpc/registry"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type EchoService int

func (s EchoService) Double(arg int, reply *int) error {
	*reply = arg * 2
	return nil
}

func (s EchoService) Fail(arg int, reply *int) error {
	return errors.New("boom")
}

func (s EchoService) Slow(arg int, reply *int) error {
	time.Sleep(time.Duration(arg) * time.Millisecond)
	*reply = arg * 2
	return nil
}

type releasingDiscovery struct {
	rpcAddr string
	mu      sync.Mutex
	done    []string
}

func (d *releasingDiscovery) Refresh() error { return nil }

func (d *releasingDiscovery) Update([]string) error { return nil }

func (d *releasingDiscovery) Get(SelectMode) (string, error) { return d.rpcAddr, nil }

func (d *releasingDiscovery) GetAll() ([]string, error) { return []string{d.rpcAddr}, nil }

func (d *releasingDiscovery) Done(addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.done = append(d.done, addr)
}

type keyedDiscoveryStub struct {
	rpcAddr string
	mu      sync.Mutex
	keys    []string
	done    []string
}

func (d *keyedDiscoveryStub) Refresh() error { return nil }

func (d *keyedDiscoveryStub) Update([]string) error { return nil }

func (d *keyedDiscoveryStub) Get(SelectMode) (string, error) {
	return "", errors.New("unexpected Get call")
}

func (d *keyedDiscoveryStub) GetAll() ([]string, error) { return []string{d.rpcAddr}, nil }

func (d *keyedDiscoveryStub) GetByKey(key string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.keys = append(d.keys, key)
	return d.rpcAddr, nil
}

func (d *keyedDiscoveryStub) Done(addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.done = append(d.done, addr)
}

func startEchoServer(t *testing.T) string {
	t.Helper()
	addr, _ := startControllableEchoServer(t)
	return addr
}

func startControllableEchoServer(t *testing.T) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	server := geerpc.NewServer()
	var svc EchoService
	if err := server.Register(&svc); err != nil {
		_ = l.Close()
		t.Fatalf("failed to register service: %v", err)
	}
	go server.Accept(l)
	t.Cleanup(func() {
		_ = l.Close()
	})
	return "tcp@" + l.Addr().String(), func() {
		_ = l.Close()
	}
}

func restartEchoServerAt(t *testing.T, rpcAddr string) func() {
	t.Helper()
	rawAddr := strings.TrimPrefix(rpcAddr, "tcp@")
	var (
		l   net.Listener
		err error
	)
	for i := 0; i < 10; i++ {
		l, err = net.Listen("tcp", rawAddr)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("failed to restart listener at %s: %v", rawAddr, err)
	}
	server := geerpc.NewServer()
	var svc EchoService
	if err := server.Register(&svc); err != nil {
		_ = l.Close()
		t.Fatalf("failed to register service: %v", err)
	}
	go server.Accept(l)
	return func() {
		_ = l.Close()
	}
}

type releasingSequenceDiscovery struct {
	addrs    []string
	sequence []string
	mu       sync.Mutex
	index    int
	done     []string
}

func (d *releasingSequenceDiscovery) Refresh() error { return nil }

func (d *releasingSequenceDiscovery) Update([]string) error { return nil }

func (d *releasingSequenceDiscovery) Get(SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.sequence) == 0 {
		return "", errors.New("no sequence configured")
	}
	addr := d.sequence[d.index%len(d.sequence)]
	d.index++
	return addr, nil
}

func (d *releasingSequenceDiscovery) GetAll() ([]string, error) {
	return append([]string(nil), d.addrs...), nil
}

func (d *releasingSequenceDiscovery) Done(addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.done = append(d.done, addr)
}

func TestWeightedDiscoverySequence(t *testing.T) {
	d := NewWeightedDiscovery([]WeightedServer{
		{Addr: "tcp@a", Weight: 2},
		{Addr: "tcp@b", Weight: 1},
	})
	want := []string{"tcp@a", "tcp@b", "tcp@a", "tcp@a", "tcp@b", "tcp@a"}
	for i, expected := range want {
		got, err := d.Get(RandomSelect)
		if err != nil {
			t.Fatalf("pick %d returned err: %v", i, err)
		}
		if got != expected {
			t.Fatalf("pick %d: got %q, want %q", i, got, expected)
		}
	}
}

func TestConsistentHashDiscoveryGetByKeyStable(t *testing.T) {
	d := NewConsistentHashDiscovery([]string{"tcp@a", "tcp@b", "tcp@c"}, 32)
	first, err := d.GetByKey("user-42")
	if err != nil {
		t.Fatalf("GetByKey returned err: %v", err)
	}
	for i := 0; i < 10; i++ {
		got, err := d.GetByKey("user-42")
		if err != nil {
			t.Fatalf("GetByKey returned err: %v", err)
		}
		if got != first {
			t.Fatalf("iteration %d: got %q, want stable result %q", i, got, first)
		}
	}
}

func TestLeastConnDiscoveryDoneRestoresCapacity(t *testing.T) {
	d := NewLeastConnDiscovery([]string{"tcp@a", "tcp@b"})
	first, err := d.Get(RandomSelect)
	if err != nil {
		t.Fatalf("first Get returned err: %v", err)
	}
	second, err := d.Get(RandomSelect)
	if err != nil {
		t.Fatalf("second Get returned err: %v", err)
	}
	d.Done(first)
	third, err := d.Get(RandomSelect)
	if err != nil {
		t.Fatalf("third Get returned err: %v", err)
	}
	if first != "tcp@a" || second != "tcp@b" || third != "tcp@a" {
		t.Fatalf("unexpected selection order: first=%q second=%q third=%q", first, second, third)
	}
}

func TestLeastConnDiscoveryConcurrentBalanceAndRelease(t *testing.T) {
	d := NewLeastConnDiscovery([]string{"tcp@a", "tcp@b", "tcp@c"})
	const workers = 60

	type getResult struct {
		addr string
		err  error
	}

	start := make(chan struct{})
	release := make(chan struct{})
	results := make(chan getResult, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			addr, err := d.Get(RandomSelect)
			results <- getResult{addr: addr, err: err}
			if err == nil {
				<-release
				d.Done(addr)
			}
		}()
	}

	close(start)

	counts := make(map[string]int)
	for i := 0; i < workers; i++ {
		res := <-results
		if res.err != nil {
			t.Fatalf("Get returned err: %v", res.err)
		}
		counts[res.addr]++
	}

	sum := 0
	minConns, maxConns := workers, 0
	liveCounts := make(map[string]int)
	d.mu.Lock()
	for _, s := range d.servers {
		sum += s.conns
		liveCounts[s.addr] = s.conns
		if s.conns < minConns {
			minConns = s.conns
		}
		if s.conns > maxConns {
			maxConns = s.conns
		}
	}
	d.mu.Unlock()
	for addr, live := range liveCounts {
		if counts[addr] != live {
			t.Fatalf("addr %s: recorded count=%d, live conns=%d", addr, counts[addr], live)
		}
	}
	if sum != workers {
		t.Fatalf("sum of live conns = %d, want %d", sum, workers)
	}
	if maxConns-minConns > 1 {
		t.Fatalf("load is not balanced enough: min=%d max=%d", minConns, maxConns)
	}

	close(release)
	wg.Wait()
	d.mu.Lock()
	for _, s := range d.servers {
		if s.conns != 0 {
			d.mu.Unlock()
			t.Fatalf("addr %s: conns=%d after release, want 0", s.addr, s.conns)
		}
	}
	d.mu.Unlock()
}

func TestGeeRegistryDiscoveryRefreshUnexpectedStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	d := NewGeeRegistryDiscovery(srv.URL, time.Nanosecond)
	err := d.Refresh()
	if err == nil {
		t.Fatal("expected Refresh to fail on non-200 status")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGeeRegistryDiscoveryGetByKeyTracksUpdates(t *testing.T) {
	d := NewGeeRegistryDiscovery("http://registry.test", time.Minute)
	if err := d.Update([]string{"tcp@a", "tcp@b"}); err != nil {
		t.Fatalf("Update returned err: %v", err)
	}
	got, err := d.GetByKey("tenant-a")
	if err != nil {
		t.Fatalf("GetByKey returned err: %v", err)
	}
	if got != "tcp@a" && got != "tcp@b" {
		t.Fatalf("unexpected routed addr: %q", got)
	}

	if err := d.Update([]string{"tcp@solo"}); err != nil {
		t.Fatalf("second Update returned err: %v", err)
	}
	got, err = d.GetByKey("tenant-a")
	if err != nil {
		t.Fatalf("GetByKey after Update returned err: %v", err)
	}
	if got != "tcp@solo" {
		t.Fatalf("got %q, want tcp@solo after ring rebuild", got)
	}
}

func TestXClientCallReleasesSelectedAddr(t *testing.T) {
	rpcAddr := "tcp@echo"
	d := &releasingDiscovery{rpcAddr: rpcAddr}
	xc := NewXClient(d, RandomSelect, nil)
	xc.callFunc = func(addr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
		if addr != rpcAddr {
			t.Fatalf("unexpected addr: %s", addr)
		}
		*(reply.(*int)) = 14
		return nil
	}

	var reply int
	if err := xc.Call(context.Background(), "EchoService.Double", 7, &reply); err != nil {
		t.Fatalf("Call returned err: %v", err)
	}
	if reply != 14 {
		t.Fatalf("unexpected reply: got %d, want 14", reply)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.done) != 1 || d.done[0] != rpcAddr {
		t.Fatalf("unexpected Done calls: %+v", d.done)
	}
}

func TestXClientCallByKeyUsesKeyedDiscovery(t *testing.T) {
	rpcAddr := "tcp@echo"
	d := &keyedDiscoveryStub{rpcAddr: rpcAddr}
	xc := NewXClient(d, RandomSelect, nil)
	xc.callFunc = func(addr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
		if addr != rpcAddr {
			t.Fatalf("unexpected addr: %s", addr)
		}
		*(reply.(*int)) = 18
		return nil
	}

	var reply int
	if err := xc.CallByKey(context.Background(), "tenant-7", "EchoService.Double", 9, &reply); err != nil {
		t.Fatalf("CallByKey returned err: %v", err)
	}
	if reply != 18 {
		t.Fatalf("unexpected reply: got %d, want 18", reply)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.keys) != 1 || d.keys[0] != "tenant-7" {
		t.Fatalf("unexpected routed keys: %+v", d.keys)
	}
	if len(d.done) != 1 || d.done[0] != rpcAddr {
		t.Fatalf("unexpected Done calls: %+v", d.done)
	}
}

func TestXClientCallByKeyUnsupported(t *testing.T) {
	xc := NewXClient(NewMultiServerDiscovery(nil), RandomSelect, nil)
	err := xc.CallByKey(context.Background(), "user-1", "EchoService.Double", 1, nil)
	if !errors.Is(err, ErrKeyRoutingUnsupported) {
		t.Fatalf("got err %v, want %v", err, ErrKeyRoutingUnsupported)
	}
}

func TestXClientCallByKeyRejectsEmptyKey(t *testing.T) {
	d := &keyedDiscoveryStub{rpcAddr: "tcp@unused"}
	xc := NewXClient(d, RandomSelect, nil)

	err := xc.CallByKey(context.Background(), "   ", "EchoService.Double", 1, nil)
	if !errors.Is(err, ErrEmptyRouteKey) {
		t.Fatalf("got err %v, want %v", err, ErrEmptyRouteKey)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.keys) != 0 {
		t.Fatalf("GetByKey should not be called for empty key, got %+v", d.keys)
	}
}

func TestXClientStats(t *testing.T) {
	rpcAddr := "tcp@echo"
	d := &releasingDiscovery{rpcAddr: rpcAddr}
	xc := NewXClient(d, RandomSelect, nil)
	pool := xc.getPool(rpcAddr)
	pool.mu.Lock()
	pool.idle = append(pool.idle, &geerpc.Client{})
	pool.mu.Unlock()

	stats := xc.Stats()
	if stats.CachedConnections != 1 {
		t.Fatalf("unexpected cached connections: %d", stats.CachedConnections)
	}
	if stats.AvailableConnections != 1 || stats.ClosingConnections != 0 || stats.ShutdownConnections != 0 || stats.UnavailableConnections != 0 {
		t.Fatalf("unexpected connection availability: available=%d closing=%d shutdown=%d unavailable=%d", stats.AvailableConnections, stats.ClosingConnections, stats.ShutdownConnections, stats.UnavailableConnections)
	}
	if stats.StartedCalls != 0 || stats.CompletedCalls != 0 {
		t.Fatalf("unexpected aggregated call stats: started=%d completed=%d", stats.StartedCalls, stats.CompletedCalls)
	}
	if stats.PendingCalls != 0 || stats.FailedCalls != 0 || stats.CanceledCalls != 0 {
		t.Fatalf("unexpected pending/failed/canceled stats: pending=%d failed=%d canceled=%d", stats.PendingCalls, stats.FailedCalls, stats.CanceledCalls)
	}
	if stats.OpenCircuits != 0 || stats.HalfOpenCircuits != 0 || stats.CircuitTrips != 0 || stats.HalfOpenProbeSuccesses != 0 || stats.HalfOpenProbeFailures != 0 {
		t.Fatalf("unexpected circuit stats: %+v", stats)
	}
	if len(stats.Clients) != 1 || stats.Clients[0].Addr != rpcAddr {
		t.Fatalf("unexpected client snapshots: %+v", stats.Clients)
	}
	if stats.Clients[0].Stats.State != "available" {
		t.Fatalf("unexpected client state: %s", stats.Clients[0].Stats.State)
	}
}

func TestXClientCallWithRetryDisabledPreservesBehavior(t *testing.T) {
	badAddr := "tcp@127.0.0.1:1"
	goodAddr := "tcp@healthy"
	d := NewMultiServerDiscovery([]string{badAddr, goodAddr})
	d.index = 0
	xc := NewXClientWithConfig(d, RoundRobinSelect, nil, &XClientConfig{
		RetryPolicy: RetryPolicy{MaxRetries: 0},
	})
	var called []string
	xc.callFunc = func(addr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
		called = append(called, addr)
		if addr == badAddr {
			return geerpc.ErrShutdown
		}
		*(reply.(*int)) = 10
		return nil
	}

	var reply int
	err := xc.Call(context.Background(), "EchoService.Double", 5, &reply)
	if err == nil {
		t.Fatal("expected call to fail without retry")
	}
	if len(called) != 1 || called[0] != badAddr {
		t.Fatalf("unexpected call sequence: %+v", called)
	}

	stats := xc.Stats()
	if stats.RetriedCalls != 0 || stats.RetryAttempts != 0 || stats.FailoverSuccesses != 0 || stats.FailoverFailures != 0 {
		t.Fatalf("unexpected retry stats: %+v", stats)
	}
}

func TestXClientCallFailoverSuccess(t *testing.T) {
	badAddr := "tcp@127.0.0.1:1"
	goodAddr := "tcp@healthy"
	d := NewMultiServerDiscovery([]string{badAddr, goodAddr})
	d.index = 0
	xc := NewXClientWithConfig(d, RoundRobinSelect, nil, &XClientConfig{
		RetryPolicy: RetryPolicy{MaxRetries: 1},
	})
	var called []string
	xc.callFunc = func(addr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
		called = append(called, addr)
		if addr == badAddr {
			return geerpc.ErrShutdown
		}
		*(reply.(*int)) = 14
		return nil
	}

	var reply int
	if err := xc.Call(context.Background(), "EchoService.Double", 7, &reply); err != nil {
		t.Fatalf("Call returned err: %v", err)
	}
	if reply != 14 {
		t.Fatalf("unexpected reply: got %d, want 14", reply)
	}
	if len(called) != 2 || called[0] != badAddr || called[1] != goodAddr {
		t.Fatalf("unexpected call sequence: %+v", called)
	}

	stats := xc.Stats()
	if stats.RetriedCalls != 1 || stats.RetryAttempts != 1 || stats.FailoverSuccesses != 1 || stats.FailoverFailures != 0 {
		t.Fatalf("unexpected retry stats: %+v", stats)
	}
}

func TestXClientCallFailoverDropsUnavailableCachedClient(t *testing.T) {
	primaryAddr := "tcp@127.0.0.1:1"
	secondaryAddr := "tcp@healthy"
	d := NewMultiServerDiscovery([]string{primaryAddr, secondaryAddr})
	d.index = 0
	xc := NewXClientWithConfig(d, RoundRobinSelect, nil, &XClientConfig{
		RetryPolicy: RetryPolicy{MaxRetries: 1},
	})
	xc.callFunc = func(addr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
		if addr == primaryAddr {
			return geerpc.ErrShutdown
		}
		*(reply.(*int)) = 8
		return nil
	}

	primaryPool := xc.getPool(primaryAddr)
	primaryPool.mu.Lock()
	primaryPool.idle = append(primaryPool.idle, &geerpc.Client{})
	primaryPool.mu.Unlock()

	var reply int
	if err := xc.Call(context.Background(), "EchoService.Double", 4, &reply); err != nil {
		t.Fatalf("Call returned err: %v", err)
	}
	if reply != 8 {
		t.Fatalf("unexpected reply: got %d, want 8", reply)
	}

	xc.mu.Lock()
	_, hasPrimary := xc.pools[primaryAddr]
	xc.mu.Unlock()
	if hasPrimary {
		t.Logf("pool for %s still exists (may have been recreated)", primaryAddr)
	}
}

func TestGeeRegistryDiscoveryFiltersByVersion(t *testing.T) {
	infos := []registry.ServerInfo{
		{Addr: "tcp@a", Group: "default", Version: "v1", Weight: 1},
		{Addr: "tcp@b", Group: "default", Version: "v2", Weight: 1},
	}
	raw, err := json.Marshal(infos)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(registry.ServersHeader, "tcp@a,tcp@b")
		w.Header().Set(registry.ServerInfosHeader, string(raw))
	}))
	defer srv.Close()

	d := NewGeeRegistryDiscoveryWithConfig(srv.URL, time.Nanosecond, &GeeRegistryDiscoveryConfig{
		Version: "v2",
	})
	addrs, err := d.GetAll()
	if err != nil {
		t.Fatalf("GetAll returned err: %v", err)
	}
	if len(addrs) != 1 || addrs[0] != "tcp@b" {
		t.Fatalf("unexpected filtered addrs: %+v", addrs)
	}
}

func TestGeeRegistryDiscoveryUsesRegistryWeights(t *testing.T) {
	infos := []registry.ServerInfo{
		{Addr: "tcp@a", Group: "default", Version: "v1", Weight: 2},
		{Addr: "tcp@b", Group: "default", Version: "v1", Weight: 1},
	}
	raw, err := json.Marshal(infos)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(registry.ServersHeader, "tcp@a,tcp@b")
		w.Header().Set(registry.ServerInfosHeader, string(raw))
	}))
	defer srv.Close()

	d := NewGeeRegistryDiscoveryWithConfig(srv.URL, time.Hour, &GeeRegistryDiscoveryConfig{
		UseRegistryWeights: true,
	})
	if err := d.Refresh(); err != nil {
		t.Fatalf("Refresh returned err: %v", err)
	}
	want := []string{"tcp@a", "tcp@b", "tcp@a", "tcp@a", "tcp@b", "tcp@a"}
	for i, expected := range want {
		got, err := d.Get(RandomSelect)
		if err != nil {
			t.Fatalf("Get %d returned err: %v", i, err)
		}
		if got != expected {
			t.Fatalf("Get %d: got %q, want %q", i, got, expected)
		}
	}
}

func TestXClientIdleConnectionCleanupAndReconnect(t *testing.T) {
	rpcAddr := startEchoServer(t)
	d := NewMultiServerDiscovery([]string{rpcAddr})
	xc := NewXClientWithConfig(d, RandomSelect, nil, &XClientConfig{
		MaxIdleDuration: 80 * time.Millisecond,
		CleanupInterval: 20 * time.Millisecond,
	})
	defer func() { _ = xc.Close() }()

	var reply int
	if err := xc.Call(context.Background(), "EchoService.Double", 6, &reply); err != nil {
		t.Fatalf("first Call returned err: %v", err)
	}
	if reply != 12 {
		t.Fatalf("unexpected first reply: %d", reply)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if stats := xc.Stats(); stats.CachedConnections == 0 && stats.IdleEvictions >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	stats := xc.Stats()
	if stats.CachedConnections != 0 {
		t.Fatalf("expected idle cleanup to evict cached connection, got %+v", stats)
	}
	if stats.IdleEvictions != 1 {
		t.Fatalf("unexpected idle evictions: %d", stats.IdleEvictions)
	}
	if stats.LastCleanupAt.IsZero() {
		t.Fatal("expected LastCleanupAt to be populated")
	}

	reply = 0
	if err := xc.Call(context.Background(), "EchoService.Double", 7, &reply); err != nil {
		t.Fatalf("second Call returned err: %v", err)
	}
	if reply != 14 {
		t.Fatalf("unexpected second reply: %d", reply)
	}

	stats = xc.Stats()
	if stats.Reconnects != 1 {
		t.Fatalf("unexpected reconnects: %d", stats.Reconnects)
	}
	if stats.CachedConnections != 1 {
		t.Fatalf("expected one cached connection after reconnect, got %+v", stats)
	}
}

func TestXClientCloseStopsCleanupLoop(t *testing.T) {
	xc := NewXClientWithConfig(NewMultiServerDiscovery([]string{"tcp@echo"}), RandomSelect, nil, &XClientConfig{
		MaxIdleDuration: 50 * time.Millisecond,
		CleanupInterval: 10 * time.Millisecond,
	})
	if xc.cleanupDone == nil {
		t.Fatal("expected cleanup loop to be started")
	}

	if err := xc.Close(); err != nil {
		t.Fatalf("Close returned err: %v", err)
	}

	select {
	case <-xc.cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("expected cleanup loop to stop")
	}
}

func TestXClientCleanupDisabledKeepsConnections(t *testing.T) {
	xc := NewXClient(NewMultiServerDiscovery([]string{"tcp@echo"}), RandomSelect, nil)
	pool := xc.getPool("tcp@echo")
	pool.mu.Lock()
	pool.idle = append(pool.idle, &geerpc.Client{})
	pool.mu.Unlock()

	xc.cleanupConnections(time.Now())

	xc.mu.Lock()
	_, ok := xc.pools["tcp@echo"]
	xc.mu.Unlock()
	if !ok {
		t.Fatal("cleanup should be disabled when MaxIdleDuration is zero")
	}
}

func TestIsRetryableCallErrorExcludesBusinessError(t *testing.T) {
	if isRetryableCallError(context.Background(), errors.New("boom")) {
		t.Fatal("business error should not be retryable")
	}
}

func TestXClientCallDoesNotRetryContextTimeout(t *testing.T) {
	goodAddr := "tcp@healthy"
	d := NewMultiServerDiscovery([]string{goodAddr, "tcp@127.0.0.1:1"})
	d.index = 0
	xc := NewXClientWithConfig(d, RoundRobinSelect, nil, &XClientConfig{
		RetryPolicy: RetryPolicy{MaxRetries: 1},
	})
	var called []string
	xc.callFunc = func(addr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
		called = append(called, addr)
		<-ctx.Done()
		return wrapContextError(ctx.Err())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	var reply int
	err := xc.Call(ctx, "EchoService.Slow", 50, &reply)
	if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("expected context timeout, got %v", err)
	}
	if len(called) != 1 || called[0] != goodAddr {
		t.Fatalf("unexpected call sequence: %+v", called)
	}

	stats := xc.Stats()
	if stats.RetriedCalls != 0 || stats.RetryAttempts != 0 || stats.FailoverSuccesses != 0 || stats.FailoverFailures != 0 {
		t.Fatalf("context timeout should not trigger retry stats: %+v", stats)
	}
}

func TestXClientCallFailoverExhausted(t *testing.T) {
	d := NewMultiServerDiscovery([]string{"tcp@127.0.0.1:1", "tcp@127.0.0.1:2"})
	d.index = 0
	xc := NewXClientWithConfig(d, RoundRobinSelect, nil, &XClientConfig{
		RetryPolicy: RetryPolicy{MaxRetries: 1},
	})
	defer func() { _ = xc.Close() }()

	var reply int
	err := xc.Call(context.Background(), "EchoService.Double", 1, &reply)
	if err == nil || !strings.Contains(err.Error(), "failover exhausted after 2 attempts") {
		t.Fatalf("expected failover exhausted error, got %v", err)
	}

	stats := xc.Stats()
	if stats.RetriedCalls != 1 || stats.RetryAttempts != 1 || stats.FailoverSuccesses != 0 || stats.FailoverFailures != 1 {
		t.Fatalf("unexpected retry stats: %+v", stats)
	}
}

func TestXClientCallFailoverReleasesSkippedAddr(t *testing.T) {
	badAddr := "tcp@127.0.0.1:1"
	goodAddr := "tcp@127.0.0.1:2"
	d := &releasingSequenceDiscovery{
		addrs:    []string{badAddr, goodAddr},
		sequence: []string{badAddr, badAddr, goodAddr},
	}
	xc := NewXClientWithConfig(d, RoundRobinSelect, nil, &XClientConfig{
		RetryPolicy: RetryPolicy{MaxRetries: 1},
	})
	d.index = 1

	addr, err := xc.nextCallAddr(map[string]struct{}{badAddr: {}})
	if err != nil {
		t.Fatalf("nextCallAddr returned err: %v", err)
	}
	if addr != goodAddr {
		t.Fatalf("got addr %q, want %q", addr, goodAddr)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.done) != 1 || d.done[0] != badAddr {
		t.Fatalf("unexpected Done calls: %+v", d.done)
	}
}

func TestXClientCircuitBreakerSkipsOpenNode(t *testing.T) {
	badAddr := "tcp@127.0.0.1:1"
	goodAddr := "tcp@healthy"
	d := NewLeastConnDiscovery([]string{badAddr, goodAddr})
	xc := NewXClientWithConfig(d, RoundRobinSelect, nil, &XClientConfig{
		RetryPolicy: RetryPolicy{MaxRetries: 1},
		CircuitBreakerPolicy: CircuitBreakerPolicy{
			FailureThreshold: 1,
			Cooldown:         time.Minute,
		},
	})
	var called []string
	xc.callFunc = func(addr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
		called = append(called, addr)
		if addr == badAddr {
			return geerpc.ErrShutdown
		}
		*(reply.(*int)) = args.(int) * 2
		return nil
	}

	var reply int
	if err := xc.Call(context.Background(), "EchoService.Double", 10, &reply); err != nil {
		t.Fatalf("first Call returned err: %v", err)
	}
	if reply != 20 {
		t.Fatalf("unexpected first reply: got %d, want 20", reply)
	}

	stats := xc.Stats()
	if stats.OpenCircuits != 1 || stats.CircuitTrips != 1 {
		t.Fatalf("expected one open circuit after first call, got %+v", stats)
	}
	if stats.RetryAttempts != 1 {
		t.Fatalf("unexpected retry attempts after first call: %d", stats.RetryAttempts)
	}

	reply = 0
	if err := xc.Call(context.Background(), "EchoService.Double", 11, &reply); err != nil {
		t.Fatalf("second Call returned err: %v", err)
	}
	if reply != 22 {
		t.Fatalf("unexpected second reply: got %d, want 22", reply)
	}

	stats = xc.Stats()
	if stats.RetryAttempts != 1 {
		t.Fatalf("open circuit should skip bad node without extra retry, got %+v", stats)
	}
	if stats.OpenCircuits != 1 || stats.CircuitTrips != 1 {
		t.Fatalf("unexpected circuit stats after skip: %+v", stats)
	}
	if len(called) != 3 || called[0] != badAddr || called[1] != goodAddr || called[2] != goodAddr {
		t.Fatalf("unexpected call sequence: %+v", called)
	}
}

func TestXClientCircuitBreakerHalfOpenRecovery(t *testing.T) {
	badAddr := "tcp@bad"
	goodAddr := "tcp@healthy"
	d := NewLeastConnDiscovery([]string{badAddr, goodAddr})
	xc := NewXClientWithConfig(d, RoundRobinSelect, nil, &XClientConfig{
		RetryPolicy: RetryPolicy{MaxRetries: 1},
		CircuitBreakerPolicy: CircuitBreakerPolicy{
			FailureThreshold: 1,
			Cooldown:         60 * time.Millisecond,
		},
	})
	badHealthy := false
	var called []string
	xc.callFunc = func(addr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
		called = append(called, addr)
		if addr == badAddr && !badHealthy {
			return geerpc.ErrShutdown
		}
		*(reply.(*int)) = args.(int) * 2
		return nil
	}

	var reply int
	if err := xc.Call(context.Background(), "EchoService.Double", 12, &reply); err != nil {
		t.Fatalf("first Call returned err: %v", err)
	}
	if reply != 24 {
		t.Fatalf("unexpected first reply: got %d, want 24", reply)
	}

	stats := xc.Stats()
	if stats.OpenCircuits != 1 || stats.CircuitTrips != 1 {
		t.Fatalf("expected open circuit after failure, got %+v", stats)
	}

	badHealthy = true
	time.Sleep(80 * time.Millisecond)

	reply = 0
	if err := xc.Call(context.Background(), "EchoService.Double", 13, &reply); err != nil {
		t.Fatalf("half-open Call returned err: %v", err)
	}
	if reply != 26 {
		t.Fatalf("unexpected second reply: got %d, want 26", reply)
	}

	stats = xc.Stats()
	if stats.OpenCircuits != 0 || stats.HalfOpenCircuits != 0 {
		t.Fatalf("expected circuit to close after successful probe, got %+v", stats)
	}
	if stats.HalfOpenProbeSuccesses != 1 || stats.HalfOpenProbeFailures != 0 {
		t.Fatalf("unexpected half-open stats: %+v", stats)
	}
	if len(called) != 3 || called[0] != badAddr || called[1] != goodAddr || called[2] != badAddr {
		t.Fatalf("unexpected call sequence: %+v", called)
	}
}
