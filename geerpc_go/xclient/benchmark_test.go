package xclient

import (
	"context"
	geerpc "geerpc"
	"net"
	"testing"
	"time"
)

func startBenchmarkXClient(b *testing.B) (*XClient, func()) {
	b.Helper()

	server := geerpc.NewServer()
	var svc EchoService
	if err := server.Register(&svc); err != nil {
		b.Fatalf("Register failed: %v", err)
	}
	serverConn, clientConn := net.Pipe()
	go server.ServeConn(serverConn)

	client, err := geerpc.NewClient(clientConn, geerpc.DefaultOption)
	if err != nil {
		_ = clientConn.Close()
		b.Fatalf("NewClient failed: %v", err)
	}

	rpcAddr := "pipe@echo"
	discovery := &releasingDiscovery{rpcAddr: rpcAddr}
	xc := NewXClient(discovery, RandomSelect, nil)
	xc.callFunc = func(addr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
		if addr != rpcAddr {
			b.Fatalf("unexpected addr: %s", addr)
		}
		return client.Call(ctx, serviceMethod, args, reply)
	}

	cleanup := func() {
		_ = client.Close()
		_ = xc.Close()
	}
	return xc, cleanup
}

func benchmarkXClientQPS(b *testing.B, elapsed time.Duration) {
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "qps")
}

func BenchmarkXClientCall(b *testing.B) {
	xc, cleanup := startBenchmarkXClient(b)
	defer cleanup()

	var warmup int
	if err := xc.Call(context.Background(), "EchoService.Double", 1, &warmup); err != nil {
		b.Fatalf("warmup call failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		var reply int
		if err := xc.Call(context.Background(), "EchoService.Double", i, &reply); err != nil {
			b.Fatalf("Call failed: %v", err)
		}
		if reply != i*2 {
			b.Fatalf("unexpected reply: got %d, want %d", reply, i*2)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	benchmarkXClientQPS(b, elapsed)
}

func BenchmarkXClientCallParallel(b *testing.B) {
	xc, cleanup := startBenchmarkXClient(b)
	defer cleanup()

	var warmup int
	if err := xc.Call(context.Background(), "EchoService.Double", 1, &warmup); err != nil {
		b.Fatalf("warmup call failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			var reply int
			if err := xc.Call(context.Background(), "EchoService.Double", i, &reply); err != nil {
				b.Fatalf("Call failed: %v", err)
			}
			if reply != i*2 {
				b.Fatalf("unexpected reply: got %d, want %d", reply, i*2)
			}
			i++
		}
	})
	elapsed := time.Since(start)
	b.StopTimer()
	benchmarkXClientQPS(b, elapsed)
}
