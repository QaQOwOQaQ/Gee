package geerpc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

type ShutdownService struct {
	started chan struct{}
	release chan struct{}
}

func (s *ShutdownService) Ping(arg int, reply *int) error {
	*reply = arg
	return nil
}

func (s *ShutdownService) Block(arg int, reply *int) error {
	select {
	case <-s.started:
	default:
		close(s.started)
	}
	<-s.release
	*reply = arg
	return nil
}

func TestServerShutdownWaitsForInflightRequests(t *testing.T) {
	server := NewServer()
	svc := &ShutdownService{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	if err := server.Register(svc); err != nil {
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

	var warmup int
	if err := client.Call(context.Background(), "ShutdownService.Ping", 1, &warmup); err != nil {
		t.Fatalf("warmup call failed: %v", err)
	}
	if warmup != 1 {
		t.Fatalf("unexpected warmup reply: %d", warmup)
	}

	callDone := make(chan error, 1)
	go func() {
		var reply int
		err := client.Call(context.Background(), "ShutdownService.Block", 7, &reply)
		if err == nil && reply != 7 {
			err = errors.New("unexpected reply")
		}
		callDone <- err
	}()

	<-svc.started

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownDone <- server.Shutdown(ctx)
	}()

	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before in-flight request completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	stats := server.Stats()
	if !stats.ShuttingDown {
		t.Fatalf("expected server to be in shutting down state, got %+v", stats)
	}

	close(svc.release)

	if err := <-callDone; err != nil {
		t.Fatalf("in-flight call returned err: %v", err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown returned err: %v", err)
	}

	var reply int
	err = client.Call(context.Background(), "ShutdownService.Block", 9, &reply)
	if err == nil {
		t.Fatal("expected follow-up call to fail after shutdown")
	}
}
