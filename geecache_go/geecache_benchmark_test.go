package geecache

import (
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var benchmarkSeq uint64

func uniqueBenchmarkGroupName(prefix string) string {
	id := atomic.AddUint64(&benchmarkSeq, 1)
	return fmt.Sprintf("%s_%d", prefix, id)
}

func muteBenchLogs() func() {
	oldWriter := log.Writer()
	log.SetOutput(io.Discard)
	return func() {
		log.SetOutput(oldWriter)
	}
}

func BenchmarkGroupGetCacheHit(b *testing.B) {
	restore := muteBenchLogs()
	defer restore()

	g := NewGroup(uniqueBenchmarkGroupName("bench_hit"), 1<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("value"), nil
	}), 0, 0)

	if _, err := g.Get("Tom"); err != nil {
		b.Fatalf("warmup get failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.Get("Tom"); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

func BenchmarkGroupGetParallelCacheHit(b *testing.B) {
	restore := muteBenchLogs()
	defer restore()

	g := NewGroup(uniqueBenchmarkGroupName("bench_parallel_hit"), 1<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("value"), nil
	}), 0, 0)

	if _, err := g.Get("Tom"); err != nil {
		b.Fatalf("warmup get failed: %v", err)
	}

	var failed atomic.Bool
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if failed.Load() {
				return
			}
			if _, err := g.Get("Tom"); err != nil {
				failed.Store(true)
				return
			}
		}
	})

	if failed.Load() {
		b.Fatal("parallel get failed")
	}
}

func BenchmarkGroupGetStampedeSingleflight(b *testing.B) {
	restore := muteBenchLogs()
	defer restore()

	const fanout = 64
	var loads int64

	g := NewGroup(uniqueBenchmarkGroupName("bench_stampede_sf"), 1<<20, GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt64(&loads, 1)
		time.Sleep(200 * time.Microsecond)
		return []byte("value"), nil
	}), 0, 0)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k-%d", i)

		var wg sync.WaitGroup
		wg.Add(fanout)

		var once sync.Once
		var firstErr error

		for j := 0; j < fanout; j++ {
			go func(k string) {
				defer wg.Done()
				if _, err := g.Get(k); err != nil {
					once.Do(func() { firstErr = err })
				}
			}(key)
		}

		wg.Wait()
		if firstErr != nil {
			b.Fatalf("stampede get failed: %v", firstErr)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(atomic.LoadInt64(&loads))/float64(b.N), "loads/op")
	b.ReportMetric(float64(fanout), "reqs/op")
}

func BenchmarkStampedeWithoutCoalescing(b *testing.B) {
	const fanout = 64
	var loads int64

	getter := GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt64(&loads, 1)
		time.Sleep(200 * time.Microsecond)
		return []byte("value"), nil
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k-%d", i)

		var wg sync.WaitGroup
		wg.Add(fanout)

		var once sync.Once
		var firstErr error

		for j := 0; j < fanout; j++ {
			go func(k string) {
				defer wg.Done()
				if _, err := getter.Get(k); err != nil {
					once.Do(func() { firstErr = err })
				}
			}(key)
		}

		wg.Wait()
		if firstErr != nil {
			b.Fatalf("baseline stampede load failed: %v", firstErr)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(atomic.LoadInt64(&loads))/float64(b.N), "loads/op")
	b.ReportMetric(float64(fanout), "reqs/op")
}
