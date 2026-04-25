package geerpc

// 服务注册与反射调用流程：
//
//	业务对象（如 &Foo{}）
//	  │
//	  ▼
//	newService 提取服务名与类型信息
//	  │
//	  ▼
//	registerMethods 扫描可导出方法
//	  │
//	  ├── 校验方法签名是否满足 RPC 约束
//	  ├── 记录参数类型 / 返回值类型
//	  └── 放入 service.method 映射
//	  │
//	  ▼
//	服务端收到请求后，通过 Service.Method 找到 methodType
//	  │
//	  ▼
//	newArgv / newReplyv 构造调用参数
//	  │
//	  ▼
//	call 使用反射执行真实业务方法
//
// 这个文件是整个 RPC 框架的“反射桥接层”：
// 客户端世界里只有字符串方法名和字节流参数，
// 到了这里才被还原成真实的 Go 方法调用。

import (
	"context"
	"fmt"
	"go/ast"
	"log"
	"reflect"
	rdebug "runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var latencyBucketBounds = [...]time.Duration{
	time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
}

var latencyBucketLabels = [...]string{
	"<=1ms",
	"<=5ms",
	"<=10ms",
	"<=50ms",
	"<=100ms",
	"<=500ms",
	"<=1s",
	">1s",
}

const latencyBucketCount = len(latencyBucketLabels)

var contextType = reflect.TypeOf((*context.Context)(nil)).Elem()

// methodType 描述一个可被 RPC 暴露的方法。
// 它缓存了方法本身以及参数、返回值类型，并统计被调用次数。
//
// 之所以单独抽出 methodType，而不是每次临时从 reflect.Method 现算，
// 是因为：
// 1. 服务注册时一次性扫描、校验方法签名更高效
// 2. 请求处理阶段只需直接取出缓存好的元信息
// 3. 还能顺手保存统计信息，供调试页展示
type methodType struct {
	method         reflect.Method
	HasContext     bool
	ArgType        reflect.Type
	ReplyType      reflect.Type
	numCalls       uint64
	numErrors      uint64
	numPanics      uint64
	activeCalls    int64
	totalLatencyNs uint64
	latencyBuckets [latencyBucketCount]uint64
}

// NumCalls 返回该方法累计被调用的次数。
func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

// NumErrors 返回该方法累计返回错误的次数。
func (m *methodType) NumErrors() uint64 {
	return atomic.LoadUint64(&m.numErrors)
}

// NumPanics 返回该方法累计恢复的 panic 次数。
func (m *methodType) NumPanics() uint64 {
	return atomic.LoadUint64(&m.numPanics)
}

// ActiveCalls 返回该方法当前仍在执行中的调用数。
func (m *methodType) ActiveCalls() int64 {
	return atomic.LoadInt64(&m.activeCalls)
}

// TotalLatency 返回该方法累计耗时。
func (m *methodType) TotalLatency() time.Duration {
	return time.Duration(atomic.LoadUint64(&m.totalLatencyNs))
}

// AvgLatency 返回该方法平均耗时。
func (m *methodType) AvgLatency() time.Duration {
	calls := m.NumCalls()
	if calls == 0 {
		return 0
	}
	return time.Duration(atomic.LoadUint64(&m.totalLatencyNs) / calls)
}

// MethodStats 是方法级的运行时指标快照。
type MethodStats struct {
	Name           string
	ArgType        string
	ReplyType      string
	Calls          uint64
	Errors         uint64
	Panics         uint64
	Active         int64
	TotalLatency   time.Duration
	AvgLatency     time.Duration
	LatencyBuckets []LatencyBucketStats
	LatencySummary string
}

// LatencyBucketStats 表示一个延迟桶的统计结果。
type LatencyBucketStats struct {
	Label string
	Count uint64
}

// ServiceStats 是服务级的运行时指标快照。
type ServiceStats struct {
	Name           string
	Methods        []MethodStats
	Calls          uint64
	Errors         uint64
	Panics         uint64
	Active         int64
	TotalLatency   time.Duration
	AvgLatency     time.Duration
	LatencyBuckets []LatencyBucketStats
	LatencySummary string
}

// ServerStats 是整个 RPC 服务端的运行时指标快照。
type ServerStats struct {
	Services              []ServiceStats
	Calls                 uint64
	Errors                uint64
	Panics                uint64
	Active                int64
	ActiveConnections     int
	TrackedListeners      int
	ShuttingDown          bool
	MaxConcurrentRequests int
	OverloadRejections    uint64
	LastTraceID           string
	LastRequestID         string
	TotalLatency          time.Duration
	AvgLatency            time.Duration
	LatencyBuckets        []LatencyBucketStats
	LatencySummary        string
}

func latencyBucketIndex(elapsed time.Duration) int {
	for i, bound := range latencyBucketBounds {
		if elapsed <= bound {
			return i
		}
	}
	return latencyBucketCount - 1
}

func latencyBucketStatsFromCounts(counts [latencyBucketCount]uint64) ([]LatencyBucketStats, string) {
	stats := make([]LatencyBucketStats, 0, latencyBucketCount)
	parts := make([]string, 0, latencyBucketCount)
	for i, label := range latencyBucketLabels {
		stats = append(stats, LatencyBucketStats{
			Label: label,
			Count: counts[i],
		})
		parts = append(parts, label+":"+fmtUint(counts[i]))
	}
	return stats, strings.Join(parts, ", ")
}

func fmtUint(v uint64) string {
	return strconv.FormatUint(v, 10)
}

// Stats 返回该方法的指标快照。
func (m *methodType) Stats(name string) MethodStats {
	calls := m.NumCalls()
	totalLatency := m.TotalLatency()
	var counts [latencyBucketCount]uint64
	for i := range counts {
		counts[i] = atomic.LoadUint64(&m.latencyBuckets[i])
	}
	latencyBuckets, latencySummary := latencyBucketStatsFromCounts(counts)
	return MethodStats{
		Name:           name,
		ArgType:        m.ArgType.String(),
		ReplyType:      m.ReplyType.String(),
		Calls:          calls,
		Errors:         m.NumErrors(),
		Panics:         m.NumPanics(),
		Active:         m.ActiveCalls(),
		TotalLatency:   totalLatency,
		AvgLatency:     m.AvgLatency(),
		LatencyBuckets: latencyBuckets,
		LatencySummary: latencySummary,
	}
}

// newArgv 创建请求参数对象。
// 参数既可能是值类型，也可能是指针类型，因此这里要区分处理：
// - 如果方法参数本身是指针，就创建 Elem 对应对象的指针
// - 如果方法参数是值类型，就直接创建值对象
func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

// newReplyv 创建响应对象。
// 按照 GeeRPC 的约定，reply 参数必须是指针；
// 如果底层元素是 map 或 slice，这里还会额外初始化为空值，
// 避免业务方法里还要先判空再写入。
func (m *methodType) newReplyv() reflect.Value {
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

// service 表示一个被注册到 RPC 服务端的接收者实例。
// 它把一个普通 Go 对象包装成“可被远程调用的服务”：
// - name：客户端调用时使用的服务名
// - rcvr：真正的接收者对象
// - method：所有已通过校验的 RPC 方法集合
type service struct {
	name   string
	typ    reflect.Type
	rcvr   reflect.Value
	method map[string]*methodType
}

// newService 根据业务对象构造服务描述。
// 服务名默认取接收者类型名，并要求该类型名必须是导出的，
// 否则外部调用方从语义上就不应该能访问它。
func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

// registerMethods 扫描并注册符合 RPC 规范的方法。
// 合法方法需要满足：
// 1. 方法签名形如 func (t *T) Method(arg T1, reply *T2) error
// 2. 只有两个业务参数和一个返回值
// 3. 返回值必须是 error
// 4. 参数和返回参数类型必须是导出类型或内建类型
//
// 这些约束的目的，是让框架能够：
// - 自动构造参数对象
// - 安全进行编解码
// - 统一处理业务错误
func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		if (mType.NumIn() != 3 && mType.NumIn() != 4) || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argTypeIndex := 1
		replyTypeIndex := 2
		hasContext := false
		if mType.NumIn() == 4 {
			if !mType.In(1).Implements(contextType) {
				continue
			}
			hasContext = true
			argTypeIndex = 2
			replyTypeIndex = 3
		}
		argType, replyType := mType.In(argTypeIndex), mType.In(replyTypeIndex)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:     method,
			HasContext: hasContext,
			ArgType:    argType,
			ReplyType:  replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}
}

// call 通过反射执行目标方法，并更新调用次数、错误数、活跃调用数和耗时统计。
// 到了这里，argv / replyv 都已经准备完毕，反射层的工作只剩两步：
// 1. 调用真正的方法
// 2. 把 error 结果按统一规则返回给上层
func (s *service) call(m *methodType, ctx context.Context, argv, replyv reflect.Value) (err error) {
	start := time.Now()
	atomic.AddUint64(&m.numCalls, 1)
	atomic.AddInt64(&m.activeCalls, 1)
	defer func() {
		if recovered := recover(); recovered != nil {
			atomic.AddUint64(&m.numPanics, 1)
			log.Printf("rpc server: panic recovered in %s.%s: %v\n%s", s.name, m.method.Name, recovered, rdebug.Stack())
			err = fmt.Errorf("rpc server: panic recovered in %s.%s: %v", s.name, m.method.Name, recovered)
		}
		elapsed := time.Since(start)
		atomic.AddInt64(&m.activeCalls, -1)
		atomic.AddUint64(&m.totalLatencyNs, uint64(elapsed))
		atomic.AddUint64(&m.latencyBuckets[latencyBucketIndex(elapsed)], 1)
		if err != nil {
			atomic.AddUint64(&m.numErrors, 1)
		}
	}()
	f := m.method.Func
	callArgs := []reflect.Value{s.rcvr}
	if m.HasContext {
		if ctx == nil {
			ctx = context.Background()
		}
		callArgs = append(callArgs, reflect.ValueOf(ctx))
	}
	callArgs = append(callArgs, argv, replyv)
	returnValues := f.Call(callArgs)
	if errInter := returnValues[0].Interface(); errInter != nil {
		err = errInter.(error)
	}
	return
}

// isExportedOrBuiltinType 判断一个类型是否适合作为 RPC 暴露类型。
// 只有导出类型或内建类型才适合作为跨包远程调用的参数/返回值。
func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

// Stats 返回当前服务端的运行时指标快照。
func (server *Server) Stats() ServerStats {
	stats := ServerStats{}
	server.mu.Lock()
	stats.ActiveConnections = len(server.conns)
	stats.TrackedListeners = len(server.listeners)
	stats.ShuttingDown = server.shuttingDown
	stats.MaxConcurrentRequests = server.cfg.MaxConcurrentRequests
	stats.OverloadRejections = server.overloaded
	stats.LastTraceID = server.lastTraceID
	stats.LastRequestID = server.lastRequestID
	server.mu.Unlock()
	var serverLatencyCounts [latencyBucketCount]uint64
	server.serviceMap.Range(func(namei, svci interface{}) bool {
		svc := svci.(*service)
		serviceStats := ServiceStats{Name: namei.(string)}
		var serviceLatencyCounts [latencyBucketCount]uint64
		methodNames := make([]string, 0, len(svc.method))
		for name := range svc.method {
			methodNames = append(methodNames, name)
		}
		sort.Strings(methodNames)
		for _, name := range methodNames {
			methodStats := svc.method[name].Stats(name)
			serviceStats.Methods = append(serviceStats.Methods, methodStats)
			serviceStats.Calls += methodStats.Calls
			serviceStats.Errors += methodStats.Errors
			serviceStats.Panics += methodStats.Panics
			serviceStats.Active += methodStats.Active
			serviceStats.TotalLatency += methodStats.TotalLatency
			for i, bucket := range methodStats.LatencyBuckets {
				serviceLatencyCounts[i] += bucket.Count
			}
		}
		if serviceStats.Calls > 0 {
			serviceStats.AvgLatency = time.Duration(int64(serviceStats.TotalLatency) / int64(serviceStats.Calls))
		}
		serviceStats.LatencyBuckets, serviceStats.LatencySummary = latencyBucketStatsFromCounts(serviceLatencyCounts)
		stats.Services = append(stats.Services, serviceStats)
		stats.Calls += serviceStats.Calls
		stats.Errors += serviceStats.Errors
		stats.Panics += serviceStats.Panics
		stats.Active += serviceStats.Active
		stats.TotalLatency += serviceStats.TotalLatency
		for i := range serviceLatencyCounts {
			serverLatencyCounts[i] += serviceLatencyCounts[i]
		}
		return true
	})
	sort.Slice(stats.Services, func(i, j int) bool {
		return stats.Services[i].Name < stats.Services[j].Name
	})
	if stats.Calls > 0 {
		stats.AvgLatency = time.Duration(int64(stats.TotalLatency) / int64(stats.Calls))
	}
	stats.LatencyBuckets, stats.LatencySummary = latencyBucketStatsFromCounts(serverLatencyCounts)
	return stats
}
