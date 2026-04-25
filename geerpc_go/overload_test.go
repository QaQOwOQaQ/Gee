package geerpc

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func TestServerOverloadRejectsExcessRequests(t *testing.T) {
	server := NewServerWithConfig(&ServerConfig{
		MaxConcurrentRequests: 1,
	})
	svc := &ShutdownService{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	if err := server.Register(svc); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	defer func() { _ = l.Close() }()
	go server.Accept(l)

	client, err := Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	var warmup int
	if err := client.Call(context.Background(), "ShutdownService.Ping", 1, &warmup); err != nil {
		t.Fatalf("warmup call failed: %v", err)
	}
	if warmup != 1 {
		t.Fatalf("unexpected warmup reply: %d", warmup)
	}

	firstDone := make(chan error, 1)
	go func() {
		var reply int
		err := client.Call(context.Background(), "ShutdownService.Block", 1, &reply)
		if err == nil && reply != 1 {
			err = ErrServerOverloaded
		}
		firstDone <- err
	}()

	<-svc.started

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var reply int
	err = client.Call(ctx, "ShutdownService.Block", 2, &reply)
	if err == nil || !strings.Contains(err.Error(), ErrServerOverloaded.Error()) {
		t.Fatalf("expected overload error, got %v", err)
	}

	close(svc.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first in-flight call returned err: %v", err)
	}

	stats := server.Stats()
	if stats.MaxConcurrentRequests != 1 {
		t.Fatalf("unexpected max concurrent requests: %d", stats.MaxConcurrentRequests)
	}
	if stats.OverloadRejections != 1 {
		t.Fatalf("unexpected overload rejections: %d", stats.OverloadRejections)
	}
}
