// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package geerpc

// client.go 实现 GeeRPC 客户端的核心逻辑。
//
// 【核心挑战】
// 如何在单条 TCP 连接上支持多个 goroutine 并发发起 RPC 调用？
// 需要同时解决：
// 1. 多个请求同时写入 → 字节流会交叉损坏
// 2. 响应返回顺序不确定 → 需要路由到正确的调用方
// 3. 超时/取消/异常 → 需要正确清理挂起状态
//
// 【解决方案：多路复用】
// - 发送侧：sending 锁串行化，保证 Header+Body 原子写出
// - 接收侧：Seq 序号 + pending 映射表，精确路由响应
// - 异常处理：terminateCalls 统一清理所有挂起请求
//
// 【调用流程】
//	Call/Go → registerCall(分配Seq) → send(串行写出)
//	→ 服务端处理 → receive协程(后台读取) → 路由到Call.Done → 返回结果

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"geerpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Call 表示一次正在进行中的 RPC 调用。
// 它把业务语义（方法名、参数、响应）和协议语义（序号、完成通知）绑定在一起。
type Call struct {
	Seq           uint64      // 请求唯一序号，由客户端分配，用于将响应路由回本次调用
	ServiceMethod string      // 目标方法，格式为 "<service>.<method>"
	Args          interface{} // 请求参数
	Reply         interface{} // 响应结果的承载对象，通常是指针
	Metadata      map[string]string
	Error         error      // 调用过程中出现的错误
	Done          chan *Call // 调用结束时把自身投入通道，通知等待方（支持同步和异步两种用法）
}

// done 通知调用方：这次 RPC 已经结束，可以读取 Error 和 Reply 了。
func (call *Call) done() {
	call.Done <- call
}

// Client 表示一个 RPC 客户端
// 这个结构体最重要的目标，是把“单条连接”包装成“支持多 goroutine 并发调用”的客户端
// 同一个客户端可以关联多个尚未完成的调用：这意味着该客户端的网络通信是非阻塞的
// 并且一个客户端可以被多个 goroutine 同时使用：这意味着该 Client 对象内部已经完美处理了并发冲突
type Client struct {
	cc               codec.Codec      // 负责编解码和读写底层连接
	opt              *Option          // 客户端与服务器的协商配置
	sending          sync.Mutex       // 控制请求写出串行化，防止 Header/Body 交叉，保护 header
	header           codec.Header     // 复用的请求头对象，减少每次发送时的临时分配
	mu               sync.Mutex       // 保护以下状态字段
	seq              uint64           // 为每个新调用分配唯一请求号，用于响应匹配
	pending          map[uint64]*Call // 所有已经发出但尚未收到响应的调用，key 为请求序号
	closing          bool             // 用户主动关闭了客户端
	shutdown         bool             // 底层连接或服务端异常，客户端已不可继续使用
	startedCalls     uint64
	completedCalls   uint64
	failedCalls      uint64
	canceledCalls    uint64
	writeFailures    uint64
	receiveFailures  uint64
	maxPendingCalls  uint64
	createdAtUnix    int64
	lastStartedUnix  int64
	lastDoneUnix     int64
	lastFailedUnix   int64
	lastCanceledUnix int64
	lastErrorUnix    int64
	lastError        atomic.Value
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// ClientStats 是客户端的运行时统计快照。
type ClientStats struct {
	StartedCalls    uint64
	CompletedCalls  uint64
	FailedCalls     uint64
	CanceledCalls   uint64
	WriteFailures   uint64
	ReceiveFailures uint64
	PendingCalls    int
	MaxPendingCalls uint64
	Closing         bool
	Shutdown        bool
	Available       bool
	State           string
	CreatedAt       time.Time
	LastStartedAt   time.Time
	LastCompletedAt time.Time
	LastFailedAt    time.Time
	LastCanceledAt  time.Time
	LastError       string
	LastErrorAt     time.Time
}

func snapshotTime(unixNano int64) time.Time {
	if unixNano == 0 {
		return time.Time{}
	}
	return time.Unix(0, unixNano).UTC()
}

func clientState(closing, shutdown bool) string {
	switch {
	case closing:
		return "closing"
	case shutdown:
		return "shutdown"
	default:
		return "available"
	}
}

func (client *Client) recordLastError(err error) {
	if err == nil {
		return
	}
	client.lastError.Store(err.Error())
	atomic.StoreInt64(&client.lastErrorUnix, time.Now().UnixNano())
}

func (client *Client) updateMaxPendingCalls(pending int) {
	for {
		current := atomic.LoadUint64(&client.maxPendingCalls)
		if uint64(pending) <= current {
			return
		}
		if atomic.CompareAndSwapUint64(&client.maxPendingCalls, current, uint64(pending)) {
			return
		}
	}
}

func (client *Client) recordStartedCall() {
	atomic.AddUint64(&client.startedCalls, 1)
	atomic.StoreInt64(&client.lastStartedUnix, time.Now().UnixNano())
}

func (client *Client) recordCompletedCall() {
	atomic.AddUint64(&client.completedCalls, 1)
	atomic.StoreInt64(&client.lastDoneUnix, time.Now().UnixNano())
}

func (client *Client) recordFailedCall() {
	atomic.AddUint64(&client.failedCalls, 1)
	atomic.StoreInt64(&client.lastFailedUnix, time.Now().UnixNano())
}

func (client *Client) recordCanceledCall() {
	atomic.AddUint64(&client.canceledCalls, 1)
	atomic.StoreInt64(&client.lastCanceledUnix, time.Now().UnixNano())
}

// Stats 返回客户端当前的运行时统计快照。
func (client *Client) Stats() ClientStats {
	client.mu.Lock()
	defer client.mu.Unlock()
	stats := ClientStats{
		StartedCalls:    atomic.LoadUint64(&client.startedCalls),
		CompletedCalls:  atomic.LoadUint64(&client.completedCalls),
		FailedCalls:     atomic.LoadUint64(&client.failedCalls),
		CanceledCalls:   atomic.LoadUint64(&client.canceledCalls),
		WriteFailures:   atomic.LoadUint64(&client.writeFailures),
		ReceiveFailures: atomic.LoadUint64(&client.receiveFailures),
		PendingCalls:    len(client.pending),
		MaxPendingCalls: atomic.LoadUint64(&client.maxPendingCalls),
		Closing:         client.closing,
		Shutdown:        client.shutdown,
		CreatedAt:       snapshotTime(atomic.LoadInt64(&client.createdAtUnix)),
		LastStartedAt:   snapshotTime(atomic.LoadInt64(&client.lastStartedUnix)),
		LastCompletedAt: snapshotTime(atomic.LoadInt64(&client.lastDoneUnix)),
		LastFailedAt:    snapshotTime(atomic.LoadInt64(&client.lastFailedUnix)),
		LastCanceledAt:  snapshotTime(atomic.LoadInt64(&client.lastCanceledUnix)),
		LastErrorAt:     snapshotTime(atomic.LoadInt64(&client.lastErrorUnix)),
	}
	stats.State = clientState(stats.Closing, stats.Shutdown)
	if lastError := client.lastError.Load(); lastError != nil {
		stats.LastError = lastError.(string)
	}
	stats.Available = !stats.Closing && !stats.Shutdown
	return stats
}

// Close 主动关闭客户端连接。
// 一旦进入 closing 状态，就不再接受新请求；
// 底层连接关闭后，receive 协程最终会把剩余挂起请求统一唤醒并置错。
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	if client.cc == nil {
		return nil
	}
	return client.cc.Close()
}

// IsAvailable 返回客户端当前是否还能继续发起调用。
// 对上层连接池来说，这是判断缓存连接能否复用的关键依据。
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// registerCall 为新请求分配序号，并放入 pending 表等待响应。
// 这是“发送请求之前”的必要动作，因为：
// 即使服务端返回极快，receive 也必须能立刻按 Seq 找到对应调用。
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.updateMaxPendingCalls(len(client.pending))
	client.seq++
	return call.Seq, nil
}

// removeCall 根据请求序号移除一个挂起调用。
// 它会在三种场景下发生：
// 1. 正常收到响应
// 2. 调用方 context 超时/取消
// 3. 写请求失败，或连接级错误导致调用无法继续
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// terminateCalls 在连接级错误发生时终止所有挂起请求。
// 一旦读响应失败，意味着这条连接上的未完成请求都已经失去继续成功的可能，
// 因此必须统一标记失败并通知等待方，避免永久阻塞。
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		client.recordFailedCall()
		client.recordLastError(err)
		call.done()
	}
}

// send 负责编码并发送一次请求。
// 发送流程必须整体串行化，因为一次 RPC 消息是连续写出的 “Header + Body”，
// 如果两个 goroutine 同时写同一条连接，客户端和服务端两边都会把流读坏。
func (client *Client) send(call *Call) {
	client.recordStartedCall()
	// 先锁住写路径，保证本次请求从 header 到 body 是一个原子写出片段。
	client.sending.Lock()
	defer client.sending.Unlock()

	// 再把调用登记到 pending，确保响应回来时能按 Seq 找到它。
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		client.recordFailedCall()
		client.recordLastError(err)
		call.done()
		return
	}

	// 组装本次请求头。
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""
	client.header.Metadata = cloneMetadata(call.Metadata)

	// 编码并发送请求体。
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		atomic.AddUint64(&client.writeFailures, 1)
		call := client.removeCall(seq)
		// 写出失败时尝试从 pending 取回该 Call 并通知错误。
		// 若 call 已为 nil，说明 receive 协程恰好在写出失败的瞬间收到了响应并先行移除，
		// 此时无需重复通知。
		if call != nil {
			call.Error = err
			client.recordFailedCall()
			client.recordLastError(err)
			call.done()
		}
	}
}

// receive 持续读取服务端响应，并根据 Seq 分发给对应的挂起调用。
// 接收到的响应有三种情况：
//  1. call 不存在，可能是请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了
//  2. call 存在，但服务端处理出错，即 h.Error 不为空。
//  3. call 存在，服务端处理正常，那么需要从 body 中读取 Reply 的值。
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			// 响应到达时已找不到对应调用，可能是：
			// 	1. 调用方因超时/取消已主动移除了该 Call
			// 	2. 写请求阶段失败，该 Call 已被提前清理
			// 即便没有等待者，也必须把 body 读走，否则流会错位导致后续消息解码失败。
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			// 服务端明确返回了业务错误。
			// body 此时没有有效内容，但仍需读走以保持流同步。
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			client.recordFailedCall()
			client.recordLastError(call.Error)
			call.done()
		default:
			// 正常响应：把 body 解码到调用方预留的 Reply 对象中。
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
				client.recordFailedCall()
				client.recordLastError(call.Error)
			} else {
				client.recordCompletedCall()
			}
			call.done()
		}
	}
	// 一旦这个循环退出，通常意味着连接已经不可用，因此最后一定会触发 terminateCalls
	if err != nil && err != io.EOF {
		atomic.AddUint64(&client.receiveFailures, 1)
		client.recordLastError(err)
	}
	client.terminateCalls(err)
}

// Go 发起一次异步调用。
// 它不会等待结果返回，而是立刻把一个 Call 交给调用方，
// 后续由调用方自己从 Done 通道中取结果。
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// Call 发起一次同步调用。
// 它本质上是对 Go 的同步封装：
// - 内部仍然先走异步发送
// - 然后在当前 goroutine 中等待 Done 或 context 取消
func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	if ctx == nil {
		ctx = context.Background()
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Metadata:      MetadataFromContext(ctx),
		Done:          make(chan *Call, 1),
	}
	client.send(call)
	select {
	case <-ctx.Done():
		if client.removeCall(call.Seq) != nil {
			client.recordCanceledCall()
			client.recordLastError(ctx.Err())
		}
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
}

// parseOptions 解析用户传入的可选配置。
// 当前实现只允许传入 0 个或 1 个 Option：
//   - 不传或传 nil：使用默认配置
//   - 传 1 个：在默认值基础上补齐必要字段
func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// NewClient 基于已建立的网络连接创建 RPC 客户端。
// 这里做的事情只有两步：
//  1. 先向服务端发送 Option，完成协议协商
//  2. 再创建真正负责收发消息的 Client
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

// newClientCodec 用指定的编解码器包装连接，并启动后台收包协程。
// 一个 Client 一创建就立刻具备“边发边收”的能力。
func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1, // Seq 从 1 开始，0 预留为无效值
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	atomic.StoreInt64(&client.createdAtUnix, time.Now().UnixNano())
	go client.receive()
	return client
}

// clientResult 是异步建连结果的载体。
// dialTimeout 会把“网络连接建立”与“Client 初始化”放进 goroutine，
// 然后通过它把结果传回主流程。
type clientResult struct {
	client *Client
	err    error
}

// newClientFunc 抽象“如何从一个 net.Conn 构造出 Client”。
// 这样 dialTimeout 就能统一复用到：
// - 普通 TCP/Unix 直连
// - HTTP CONNECT 隧道连接
// 不必重复写一份超时控制逻辑。
type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

// dialTimeout 负责“带超时地建立一个 Client”。
// 它控制了两段耗时：
//  1. net.DialTimeout 控制底层连接建立
//  2. 后续 Client 初始化也必须在 ConnectTimeout 内完成
func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult)
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

// Dial 使用指定网络协议和地址直连 RPC 服务端。
func Dial(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}

// NewHTTPClient 通过 HTTP CONNECT 隧道创建一个 RPC Client。
// 前半段走 HTTP 握手，后半段复用建立好的底层连接切换到 GeeRPC 协议。
func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))

	// 只有拿到成功的 HTTP 响应，才能说明隧道建立完成。
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

// DialHTTP 连接到基于 HTTP 暴露的 RPC 服务端。
func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

// XDial 根据统一地址格式自动选择建连方式。
// rpcAddr 约定为 "protocol@addr"，例如：
//
//	http@10.0.0.1:7001
//	tcp@10.0.0.1:9999
//	unix@/tmp/geerpc.sock
func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		// tcp、unix 或其他 net.Dial 支持的协议都走这里。
		return Dial(protocol, addr, opts...)
	}
}
