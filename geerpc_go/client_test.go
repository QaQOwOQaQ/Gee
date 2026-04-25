package geerpc

import (
	"context"
	"net"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

type Bar int

func (b Bar) Echo(argv int, reply *int) error {
	*reply = argv
	return nil
}

// Timeout 故意休眠 2 秒，用于测试客户端超时和服务端处理超时。
func (b Bar) Timeout(argv int, reply *int) error {
	time.Sleep(time.Second * 2)
	return nil
}

// startPipeServer 启动一个基于 net.Pipe 的测试 RPC 服务端，并返回客户端实例。
func startPipeServer(t *testing.T, opt *Option) (*Client, func()) {
	t.Helper()
	server := NewServer()
	var b Bar
	_ = server.Register(&b)
	parsedOpt, err := parseOptions(opt)
	if err != nil {
		t.Fatalf("parseOptions failed: %v", err)
	}
	serverConn, clientConn := net.Pipe()
	go server.ServeConn(serverConn)
	client, err := NewClient(clientConn, parsedOpt)
	if err != nil {
		_ = clientConn.Close()
		t.Fatalf("NewClient failed: %v", err)
	}
	return client, func() {
		_ = client.Close()
	}
}

func TestClient_dialTimeout(t *testing.T) {
	t.Parallel()
	l, _ := net.Listen("tcp", ":0")

	f := func(conn net.Conn, opt *Option) (client *Client, err error) {
		_ = conn.Close()
		time.Sleep(time.Second * 2)
		return nil, nil
	}
	t.Run("timeout", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", l.Addr().String(), &Option{ConnectTimeout: time.Second})
		_assert(err != nil && strings.Contains(err.Error(), "connect timeout"), "expect a timeout error")
	})
	t.Run("0", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", l.Addr().String(), &Option{ConnectTimeout: 0})
		_assert(err == nil, "0 means no limit")
	})
}

func TestClient_Call(t *testing.T) {
	client, stop := startPipeServer(t, nil)
	defer stop()
	t.Run("client timeout", func(t *testing.T) {
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		var reply int
		err := client.Call(ctx, "Bar.Timeout", 1, &reply)
		_assert(err != nil && strings.Contains(err.Error(), ctx.Err().Error()), "expect a timeout error")
	})
	t.Run("server handle timeout", func(t *testing.T) {
		client, stop := startPipeServer(t, &Option{
			HandleTimeout: time.Second,
		})
		defer stop()
		var reply int
		err := client.Call(context.Background(), "Bar.Timeout", 1, &reply)
		_assert(err != nil && strings.Contains(err.Error(), "handle timeout"), "expect a timeout error")
	})
}

func TestClient_Stats(t *testing.T) {
	client, stop := startPipeServer(t, nil)
	defer stop()

	var reply int
	err := client.Call(context.Background(), "Bar.Echo", 7, &reply)
	_assert(err == nil && reply == 7, "expect a successful echo call")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = client.Call(ctx, "Bar.Timeout", 1, &reply)
	_assert(err != nil && strings.Contains(err.Error(), ctx.Err().Error()), "expect a timeout error")

	stats := client.Stats()
	_assert(stats.StartedCalls == 2, "unexpected started calls: %d", stats.StartedCalls)
	_assert(stats.CompletedCalls == 1, "unexpected completed calls: %d", stats.CompletedCalls)
	_assert(stats.CanceledCalls == 1, "unexpected canceled calls: %d", stats.CanceledCalls)
	_assert(stats.FailedCalls == 0, "unexpected failed calls: %d", stats.FailedCalls)
	_assert(stats.PendingCalls == 0, "unexpected pending calls: %d", stats.PendingCalls)
	_assert(stats.MaxPendingCalls >= 1, "unexpected max pending calls: %d", stats.MaxPendingCalls)
	_assert(stats.Available, "client should still be available")
	_assert(stats.State == "available", "unexpected client state: %s", stats.State)
	_assert(!stats.CreatedAt.IsZero(), "expected CreatedAt to be populated")
	_assert(!stats.LastStartedAt.IsZero(), "expected LastStartedAt to be populated")
	_assert(!stats.LastCompletedAt.IsZero(), "expected LastCompletedAt to be populated")
	_assert(!stats.LastCanceledAt.IsZero(), "expected LastCanceledAt to be populated")
	_assert(stats.LastFailedAt.IsZero(), "expected LastFailedAt to be zero")
	_assert(strings.Contains(stats.LastError, context.DeadlineExceeded.Error()), "unexpected last error: %q", stats.LastError)
	_assert(!stats.LastErrorAt.IsZero(), "expected LastErrorAt to be populated")
}

func TestXDial(t *testing.T) {
	if runtime.GOOS == "linux" {
		addr := t.TempDir() + "/geerpc.sock"
		l, err := net.Listen("unix", addr)
		if err != nil {
			t.Fatal("failed to listen unix socket")
		}
		defer func() {
			_ = l.Close()
			_ = os.Remove(addr)
		}()
		ch := make(chan struct{})
		go func() {
			server := NewServer()
			var b Bar
			_ = server.Register(&b)
			ch <- struct{}{}
			server.Accept(l)
		}()
		<-ch
		client, err := XDial("unix@" + addr)
		_assert(err == nil, "failed to connect unix socket")
		if client != nil {
			_ = client.Close()
		}
	}
}
