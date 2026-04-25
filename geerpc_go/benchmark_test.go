package geerpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type BenchmarkService int

func (s BenchmarkService) Double(arg int, reply *int) error {
	*reply = arg * 2
	return nil
}

func startBenchmarkClient(b *testing.B, opt *Option) (*Client, func()) {
	b.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Listen failed: %v", err)
	}

	server := NewServer()
	var svc BenchmarkService
	if err := server.Register(&svc); err != nil {
		_ = l.Close()
		b.Fatalf("Register failed: %v", err)
	}
	go server.Accept(l)

	client, err := Dial("tcp", l.Addr().String(), opt)
	if err != nil {
		_ = l.Close()
		b.Fatalf("Dial failed: %v", err)
	}

	cleanup := func() {
		_ = client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		_ = l.Close()
	}
	return client, cleanup
}

func benchmarkQPS(b *testing.B, elapsed time.Duration) {
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "qps")
}

func BenchmarkClientCall(b *testing.B) {
	client, cleanup := startBenchmarkClient(b, nil)
	defer cleanup()

	var warmup int
	if err := client.Call(context.Background(), "BenchmarkService.Double", 1, &warmup); err != nil {
		b.Fatalf("warmup call failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		var reply int
		if err := client.Call(context.Background(), "BenchmarkService.Double", i, &reply); err != nil {
			b.Fatalf("Call failed: %v", err)
		}
		if reply != i*2 {
			b.Fatalf("unexpected reply: got %d, want %d", reply, i*2)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	benchmarkQPS(b, elapsed)
}

func BenchmarkClientCallWithMetadata(b *testing.B) {
	client, cleanup := startBenchmarkClient(b, nil)
	defer cleanup()

	ctx := WithTraceID(context.Background(), "bench-trace")
	ctx = WithMetadata(ctx, map[string]string{
		MetadataRequestIDKey: "bench-request",
		"tenant":             "bench",
	})

	var warmup int
	if err := client.Call(ctx, "BenchmarkService.Double", 1, &warmup); err != nil {
		b.Fatalf("warmup call failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		var reply int
		if err := client.Call(ctx, "BenchmarkService.Double", i, &reply); err != nil {
			b.Fatalf("Call failed: %v", err)
		}
		if reply != i*2 {
			b.Fatalf("unexpected reply: got %d, want %d", reply, i*2)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	benchmarkQPS(b, elapsed)
}

func BenchmarkClientCallConcurrency(b *testing.B) {
	const totalRequests = 50000
	for _, concurrency := range []int{1, 10, 50, 100, 200, 500, 1000} {
		b.Run(fmt.Sprintf("goroutines-%d", concurrency), func(b *testing.B) {
			client, cleanup := startBenchmarkClient(b, nil)
			defer cleanup()

			var warmup int
			if err := client.Call(context.Background(), "BenchmarkService.Double", 1, &warmup); err != nil {
				b.Fatalf("warmup call failed: %v", err)
			}

			perGoroutine := totalRequests / concurrency
			if perGoroutine < 1 {
				perGoroutine = 1
			}

			b.ResetTimer()
			start := time.Now()

			var wg sync.WaitGroup
			var ops atomic.Int64

			for g := 0; g < concurrency; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < perGoroutine; i++ {
						var reply int
						if err := client.Call(context.Background(), "BenchmarkService.Double", i, &reply); err != nil {
							return
						}
						ops.Add(1)
					}
				}()
			}
			wg.Wait()

			elapsed := time.Since(start)
			b.StopTimer()
			totalOps := ops.Load()
			qps := float64(totalOps) / elapsed.Seconds()
			b.ReportMetric(qps, "qps")
			b.ReportMetric(float64(concurrency), "goroutines")
			b.Logf("concurrency=%d  ops=%d  elapsed=%v  qps=%.0f", concurrency, totalOps, elapsed, qps)
		})
	}
}

func BenchmarkClientCallParallel(b *testing.B) {
	client, cleanup := startBenchmarkClient(b, nil)
	defer cleanup()

	var warmup int
	if err := client.Call(context.Background(), "BenchmarkService.Double", 1, &warmup); err != nil {
		b.Fatalf("warmup call failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			var reply int
			if err := client.Call(context.Background(), "BenchmarkService.Double", i, &reply); err != nil {
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
	benchmarkQPS(b, elapsed)
}
