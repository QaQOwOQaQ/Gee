package geerpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

type Foo int

type Args struct{ Num1, Num2 int }

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func (f Foo) Fail(args Args, reply *int) error {
	return errors.New("boom")
}

func (f Foo) Panic(args Args, reply *int) error {
	panic("boom panic")
}

func (f Foo) Sleep(args Args, reply *int) error {
	time.Sleep(10 * time.Millisecond)
	*reply = args.Num1 + args.Num2
	return nil
}

type MetaService int

func (m MetaService) Capture(ctx context.Context, arg string, reply *map[string]string) error {
	metadata := MetadataFromContext(ctx)
	metadata["arg"] = arg
	*reply = metadata
	return nil
}

func (m MetaService) Empty(ctx context.Context, arg string, reply *string) error {
	metadata := MetadataFromContext(ctx)
	if len(metadata) != 0 {
		return fmt.Errorf("unexpected metadata: %+v", metadata)
	}
	*reply = arg
	return nil
}

// sum 是未导出方法，用来验证它不会被注册成 RPC 方法。
func (f Foo) sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

// _assert 是测试辅助函数，条件不满足时直接 panic。
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func bucketCountSum(buckets []LatencyBucketStats) uint64 {
	var total uint64
	for _, bucket := range buckets {
		total += bucket.Count
	}
	return total
}

func TestNewService(t *testing.T) {
	var foo Foo
	s := newService(&foo)
	_assert(len(s.method) == 4, "wrong service Method, expect 4, but got %d", len(s.method))
	mType := s.method["Sum"]
	_assert(mType != nil, "wrong Method, Sum shouldn't nil")
}

func TestMethodType_Call(t *testing.T) {
	var foo Foo
	s := newService(&foo)
	mType := s.method["Sum"]

	argv := mType.newArgv()
	replyv := mType.newReplyv()
	argv.Set(reflect.ValueOf(Args{Num1: 1, Num2: 3}))
	err := s.call(mType, context.Background(), argv, replyv)
	_assert(err == nil && *replyv.Interface().(*int) == 4 && mType.NumCalls() == 1, "failed to call Foo.Sum")
}

func TestMethodType_Stats(t *testing.T) {
	var foo Foo
	s := newService(&foo)

	fail := s.method["Fail"]
	failArgv := fail.newArgv()
	failReplyv := fail.newReplyv()
	failArgv.Set(reflect.ValueOf(Args{}))
	err := s.call(fail, context.Background(), failArgv, failReplyv)
	_assert(err != nil, "expected Foo.Fail to return error")
	failStats := fail.Stats("Fail")
	_assert(failStats.Calls == 1, "unexpected fail calls: %d", failStats.Calls)
	_assert(failStats.Errors == 1, "unexpected fail errors: %d", failStats.Errors)
	_assert(failStats.Panics == 0, "unexpected fail panics: %d", failStats.Panics)
	_assert(failStats.Active == 0, "unexpected fail active calls: %d", failStats.Active)

	panicMethod := s.method["Panic"]
	panicArgv := panicMethod.newArgv()
	panicReplyv := panicMethod.newReplyv()
	panicArgv.Set(reflect.ValueOf(Args{}))
	err = s.call(panicMethod, context.Background(), panicArgv, panicReplyv)
	_assert(err != nil && strings.Contains(err.Error(), "panic recovered"), "expected Foo.Panic to be recovered")
	panicStats := panicMethod.Stats("Panic")
	_assert(panicStats.Calls == 1, "unexpected panic calls: %d", panicStats.Calls)
	_assert(panicStats.Errors == 1, "unexpected panic errors: %d", panicStats.Errors)
	_assert(panicStats.Panics == 1, "unexpected panic recoveries: %d", panicStats.Panics)
	_assert(panicStats.Active == 0, "unexpected panic active calls: %d", panicStats.Active)

	sleep := s.method["Sleep"]
	sleepArgv := sleep.newArgv()
	sleepReplyv := sleep.newReplyv()
	sleepArgv.Set(reflect.ValueOf(Args{Num1: 1, Num2: 2}))
	err = s.call(sleep, context.Background(), sleepArgv, sleepReplyv)
	_assert(err == nil, "expected Foo.Sleep to succeed")
	sleepStats := sleep.Stats("Sleep")
	_assert(sleepStats.Calls == 1, "unexpected sleep calls: %d", sleepStats.Calls)
	_assert(sleepStats.Errors == 0, "unexpected sleep errors: %d", sleepStats.Errors)
	_assert(sleepStats.Panics == 0, "unexpected sleep panics: %d", sleepStats.Panics)
	_assert(sleepStats.Active == 0, "unexpected sleep active calls: %d", sleepStats.Active)
	_assert(sleepStats.TotalLatency > 0, "expected total latency > 0")
	_assert(sleepStats.AvgLatency > 0, "expected avg latency > 0")
	_assert(bucketCountSum(sleepStats.LatencyBuckets) == sleepStats.Calls, "unexpected latency bucket total: %d", bucketCountSum(sleepStats.LatencyBuckets))
	_assert(strings.Contains(sleepStats.LatencySummary, "<="), "expected latency summary to include bucket labels")
}

func TestServerStats(t *testing.T) {
	server := NewServer()
	var foo Foo
	if err := server.Register(&foo); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	svci, _ := server.serviceMap.Load("Foo")
	svc := svci.(*service)

	sum := svc.method["Sum"]
	sumArgv := sum.newArgv()
	sumReplyv := sum.newReplyv()
	sumArgv.Set(reflect.ValueOf(Args{Num1: 1, Num2: 2}))
	_ = svc.call(sum, context.Background(), sumArgv, sumReplyv)

	fail := svc.method["Fail"]
	failArgv := fail.newArgv()
	failReplyv := fail.newReplyv()
	failArgv.Set(reflect.ValueOf(Args{}))
	_ = svc.call(fail, context.Background(), failArgv, failReplyv)

	panicMethod := svc.method["Panic"]
	panicArgv := panicMethod.newArgv()
	panicReplyv := panicMethod.newReplyv()
	panicArgv.Set(reflect.ValueOf(Args{}))
	_ = svc.call(panicMethod, context.Background(), panicArgv, panicReplyv)

	stats := server.Stats()
	_assert(stats.Calls == 3, "unexpected server calls: %d", stats.Calls)
	_assert(stats.Errors == 2, "unexpected server errors: %d", stats.Errors)
	_assert(stats.Panics == 1, "unexpected server panics: %d", stats.Panics)
	_assert(stats.Active == 0, "unexpected server active calls: %d", stats.Active)
	_assert(stats.MaxConcurrentRequests == 0, "unexpected max concurrent requests: %d", stats.MaxConcurrentRequests)
	_assert(stats.OverloadRejections == 0, "unexpected overload rejections: %d", stats.OverloadRejections)
	_assert(len(stats.Services) == 1, "unexpected service count: %d", len(stats.Services))
	_assert(stats.Services[0].Name == "Foo", "unexpected service name: %s", stats.Services[0].Name)
	_assert(stats.Services[0].Calls == 3, "unexpected service calls: %d", stats.Services[0].Calls)
	_assert(stats.Services[0].Errors == 2, "unexpected service errors: %d", stats.Services[0].Errors)
	_assert(stats.Services[0].Panics == 1, "unexpected service panics: %d", stats.Services[0].Panics)
	_assert(bucketCountSum(stats.LatencyBuckets) == stats.Calls, "unexpected server latency bucket total: %d", bucketCountSum(stats.LatencyBuckets))
	_assert(bucketCountSum(stats.Services[0].LatencyBuckets) == stats.Services[0].Calls, "unexpected service latency bucket total: %d", bucketCountSum(stats.Services[0].LatencyBuckets))
}

func TestDebugHTTPIncludesMetrics(t *testing.T) {
	server := NewServer()
	var foo Foo
	if err := server.Register(&foo); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	svci, _ := server.serviceMap.Load("Foo")
	svc := svci.(*service)
	sum := svc.method["Sum"]
	argv := sum.newArgv()
	replyv := sum.newReplyv()
	argv.Set(reflect.ValueOf(Args{Num1: 1, Num2: 2}))
	_ = svc.call(sum, context.Background(), argv, replyv)

	req := httptest.NewRequest("GET", defaultDebugPath, nil)
	rec := httptest.NewRecorder()
	debugHTTP{server}.ServeHTTP(rec, req)

	body := rec.Body.String()
	_assert(strings.Contains(body, "Server Calls"), "debug page should include server summary")
	_assert(strings.Contains(body, "Panics"), "debug page should include panic metrics")
	_assert(strings.Contains(body, "Max Concurrent"), "debug page should include max concurrency")
	_assert(strings.Contains(body, "Overload Rejections"), "debug page should include overload metrics")
	_assert(strings.Contains(body, "Errors"), "debug page should include error column")
	_assert(strings.Contains(body, "Avg Latency"), "debug page should include latency column")
	_assert(strings.Contains(body, "Latency Buckets"), "debug page should include latency distribution")
	_assert(strings.Contains(body, defaultDebugJSONPath), "debug page should include json stats link")
	_assert(strings.Contains(body, "Foo"), "debug page should include service name")
	_assert(strings.Contains(body, "Sum"), "debug page should include method name")
}

func TestDebugJSONHTTPIncludesMetrics(t *testing.T) {
	server := NewServer()
	var foo Foo
	if err := server.Register(&foo); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	svci, _ := server.serviceMap.Load("Foo")
	svc := svci.(*service)
	sum := svc.method["Sum"]
	argv := sum.newArgv()
	replyv := sum.newReplyv()
	argv.Set(reflect.ValueOf(Args{Num1: 2, Num2: 3}))
	_ = svc.call(sum, context.Background(), argv, replyv)

	req := httptest.NewRequest("GET", defaultDebugJSONPath, nil)
	rec := httptest.NewRecorder()
	debugJSONHTTP{server}.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("unexpected content type: %s", got)
	}

	var stats ServerStats
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	_assert(stats.Calls == 1, "unexpected server calls: %d", stats.Calls)
	_assert(stats.Panics == 0, "unexpected server panics: %d", stats.Panics)
	_assert(len(stats.Services) == 1, "unexpected service count: %d", len(stats.Services))
	_assert(stats.Services[0].Name == "Foo", "unexpected service name: %s", stats.Services[0].Name)
	found := false
	for _, method := range stats.Services[0].Methods {
		if method.Name == "Sum" {
			found = true
			break
		}
	}
	_assert(found, "expected json stats to include Sum method")
}

func TestServerRecoversFromHandlerPanic(t *testing.T) {
	server := NewServer()
	var foo Foo
	if err := server.Register(&foo); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	serverConn, clientConn := net.Pipe()
	go server.ServeConn(serverConn)

	client, err := NewClient(clientConn, DefaultOption)
	if err != nil {
		_ = clientConn.Close()
		t.Fatalf("NewClient failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var panicReply int
	err = client.Call(ctx, "Foo.Panic", Args{}, &panicReply)
	if err == nil || !strings.Contains(err.Error(), "panic recovered") {
		t.Fatalf("expected recovered panic error, got %v", err)
	}

	var sumReply int
	if err := client.Call(ctx, "Foo.Sum", Args{Num1: 2, Num2: 3}, &sumReply); err != nil {
		t.Fatalf("Sum after panic returned err: %v", err)
	}
	if sumReply != 5 {
		t.Fatalf("unexpected sum reply after panic: %d", sumReply)
	}

	stats := server.Stats()
	if stats.Panics != 1 {
		t.Fatalf("unexpected server panic count: %d", stats.Panics)
	}
	if len(stats.Services) != 1 || stats.Services[0].Panics != 1 {
		t.Fatalf("unexpected service panic stats: %+v", stats.Services)
	}
}

func TestServerPropagatesMetadataToContextMethod(t *testing.T) {
	server := NewServer()
	var svc MetaService
	if err := server.Register(&svc); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	serverConn, clientConn := net.Pipe()
	go server.ServeConn(serverConn)

	client, err := NewClient(clientConn, DefaultOption)
	if err != nil {
		_ = clientConn.Close()
		t.Fatalf("NewClient failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := WithTraceID(context.Background(), "trace-123")
	ctx = WithMetadata(ctx, map[string]string{
		MetadataRequestIDKey: "req-456",
		"tenant":             "acme",
	})

	var reply map[string]string
	if err := client.Call(ctx, "MetaService.Capture", "hello", &reply); err != nil {
		t.Fatalf("Call returned err: %v", err)
	}
	if reply[MetadataTraceIDKey] != "trace-123" {
		t.Fatalf("unexpected trace id: %q", reply[MetadataTraceIDKey])
	}
	if reply[MetadataRequestIDKey] != "req-456" {
		t.Fatalf("unexpected request id: %q", reply[MetadataRequestIDKey])
	}
	if reply["tenant"] != "acme" || reply["arg"] != "hello" {
		t.Fatalf("unexpected metadata payload: %+v", reply)
	}

	stats := server.Stats()
	if stats.LastTraceID != "trace-123" || stats.LastRequestID != "req-456" {
		t.Fatalf("unexpected last request identifiers: %+v", stats)
	}
}

func TestContextMethodWithoutMetadata(t *testing.T) {
	server := NewServer()
	var svc MetaService
	if err := server.Register(&svc); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	serverConn, clientConn := net.Pipe()
	go server.ServeConn(serverConn)

	client, err := NewClient(clientConn, DefaultOption)
	if err != nil {
		_ = clientConn.Close()
		t.Fatalf("NewClient failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	var reply string
	if err := client.Call(context.Background(), "MetaService.Empty", "plain", &reply); err != nil {
		t.Fatalf("Call returned err: %v", err)
	}
	if reply != "plain" {
		t.Fatalf("unexpected reply: %q", reply)
	}
}
