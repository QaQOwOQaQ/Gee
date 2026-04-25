package xclient

import (
	"errors"
	. "geerpc"
	"sync"
	"time"
)

var (
	errPoolClosed = errors.New("rpc pool: pool is closed")
	errPoolFull   = errors.New("rpc pool: connection pool is full")
)

type dialFunc func(addr string, opt *Option) (*Client, error)

type connPool struct {
	mu        sync.Mutex
	addr      string
	opt       *Option
	dial      dialFunc
	idle      []*Client
	active    int
	maxSize   int
	waitQueue []chan *Client
	closed    bool
	lastUsed  time.Time
}

func newConnPool(addr string, opt *Option, maxSize int, dial dialFunc) *connPool {
	if maxSize <= 0 {
		maxSize = 1
	}
	return &connPool{
		addr:    addr,
		opt:     opt,
		dial:    dial,
		maxSize: maxSize,
	}
}

func (p *connPool) Get() (*Client, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errPoolClosed
	}

	// 优先从空闲列表取
	for len(p.idle) > 0 {
		client := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		if client.IsAvailable() {
			p.active++
			p.lastUsed = time.Now()
			p.mu.Unlock()
			return client, nil
		}
		_ = client.Close()
	}
	// 未达上限，新建连接
	if p.active < p.maxSize {
		p.active++
		p.mu.Unlock()
		client, err := p.dial(p.addr, p.opt)
		if err != nil {
			p.mu.Lock()
			p.active--
			p.maybeWakeWaiterLocked()
			p.mu.Unlock()
			return nil, err
		}
		return client, nil
	}

	// 达到上限，排队等待
	ch := make(chan *Client, 1)
	p.waitQueue = append(p.waitQueue, ch)
	p.mu.Unlock()

	client, ok := <-ch
	if !ok {
		return nil, errPoolClosed
	}
	return client, nil
}

func (p *connPool) Put(client *Client) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		_ = client.Close()
		return
	}

	if !client.IsAvailable() {
		_ = client.Close()
		p.active--
		p.maybeWakeWaiterLocked()
		return
	}

	// 有等待者，直接交给等待者
	if len(p.waitQueue) > 0 {
		ch := p.waitQueue[0]
		p.waitQueue = p.waitQueue[1:]
		ch <- client
		return
	}

	// 放回空闲列表
	p.active--
	p.lastUsed = time.Now()
	p.idle = append(p.idle, client)
}

func (p *connPool) maybeWakeWaiterLocked() {
	for len(p.waitQueue) > 0 {
		ch := p.waitQueue[0]
		p.waitQueue = p.waitQueue[1:]
		p.active++
		// 唤醒等待者，让它自己去新建连接
		go func() {
			client, err := p.dial(p.addr, p.opt)
			if err != nil {
				p.mu.Lock()
				p.active--
				p.maybeWakeWaiterLocked()
				p.mu.Unlock()
				close(ch)
				return
			}
			ch <- client
		}()
		return
	}
}

func (p *connPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle) + p.active
}

func (p *connPool) IdleCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle)
}

func (p *connPool) ActiveCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active
}

func (p *connPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	for _, client := range p.idle {
		_ = client.Close()
	}
	p.idle = nil
	for _, ch := range p.waitQueue {
		close(ch)
	}
	p.waitQueue = nil
}
