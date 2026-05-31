package gpool

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
)

var (
	ErrPoolClosed    = errors.New("gpool: pool is closed")
	ErrFactoryFailed = errors.New("gpool: factory failed")
)

type Config[T any] struct {
	MinIdle             int
	MaxIdle             int
	MaxTotal            int
	MaxLifetime         time.Duration
	IdleTimeout         time.Duration
	HealthCheckInterval time.Duration
	BackgroundInterval  time.Duration
	Factory             func(ctx context.Context) (T, error)
	HealthCheck         func(ctx context.Context, conn T) bool
}

type Pool[T any] struct {
	mu     sync.Mutex
	closed bool
	idle   []*conn[T]
	total  int
	config Config[T]
	stopCh chan struct{}
	conns  map[uintptr]*conn[T]
	wait   chan struct{}
}

func New[T any](config Config[T]) (*Pool[T], error) {
	if config.Factory == nil {
		return nil, errors.New("gpool: Factory is required")
	}
	if config.MaxTotal == 0 {
		config.MaxTotal = 100
	}
	if config.BackgroundInterval == 0 {
		config.BackgroundInterval = config.HealthCheckInterval
	}
	if config.BackgroundInterval == 0 {
		config.BackgroundInterval = 30 * time.Second
	}

	p := &Pool[T]{
		config: config,
		stopCh: make(chan struct{}),
		conns:  make(map[uintptr]*conn[T]),
		wait:   make(chan struct{}),
	}
	go p.backgroundLoop()
	return p, nil
}

func (p *Pool[T]) backgroundLoop() {
	ticker := time.NewTicker(p.config.BackgroundInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.maintenance()
		}
	}
}

func (p *Pool[T]) maintenance() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	now := time.Now()

	// Evict expired and unhealthy idle connections
	valid := p.idle[:0]
	for _, c := range p.idle {
		if p.config.MaxLifetime > 0 && now.Sub(c.createdAt) > p.config.MaxLifetime {
			p.deleteConn(c)
			continue
		}
		if p.config.IdleTimeout > 0 && now.Sub(c.lastUsedAt) > p.config.IdleTimeout {
			p.deleteConn(c)
			continue
		}
		if p.config.HealthCheck != nil && p.config.HealthCheckInterval > 0 && now.Sub(c.lastChecked) >= p.config.HealthCheckInterval {
			p.mu.Unlock()
			ok := p.config.HealthCheck(context.Background(), c.value)
			p.mu.Lock()
			if !ok {
				p.deleteConn(c)
				continue
			}
			c.lastChecked = now
		}
		valid = append(valid, c)
	}
	p.idle = valid

	// Replenish to MinIdle
	for len(p.idle) < p.config.MinIdle && (p.config.MaxTotal == 0 || p.total < p.config.MaxTotal) {
		p.total++
		p.mu.Unlock()
		c, err := p.createConn(context.Background())
		p.mu.Lock()
		if err != nil {
			p.total--
			break
		}
		p.idle = append(p.idle, c)
	}

	// Trim to MaxIdle
	if p.config.MaxIdle > 0 && len(p.idle) > p.config.MaxIdle {
		for len(p.idle) > p.config.MaxIdle {
			c := p.idle[0]
			p.idle = p.idle[1:]
			p.deleteConn(c)
		}
	}
}

func (p *Pool[T]) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.closed = true
	p.idle = nil
	p.conns = nil
	p.wakeWaiters()
	close(p.stopCh)
}

// valueID returns a stable identifier for a value to track its conn[T] wrapper.
// Works for pointer and interface types (the common case for connections).
func valueID[T any](v T) uintptr {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		return rv.Pointer()
	}
	return 0
}

func (p *Pool[T]) getFromIdle(ctx context.Context) *conn[T] {
	for len(p.idle) > 0 {
		c := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		c.checkQueued.Store(false)

		if p.config.MaxLifetime > 0 && time.Since(c.createdAt) > p.config.MaxLifetime {
			p.deleteConn(c)
			continue
		}
		if p.config.IdleTimeout > 0 && time.Since(c.lastUsedAt) > p.config.IdleTimeout {
			p.deleteConn(c)
			continue
		}

		if p.config.HealthCheck != nil {
			p.mu.Unlock()
			ok := p.config.HealthCheck(ctx, c.value)
			p.mu.Lock()
			if !ok {
				p.deleteConn(c)
				continue
			}
			c.lastChecked = time.Now()
		}

		c.lastUsedAt = time.Now()
		return c
	}
	return nil
}

func (p *Pool[T]) createConn(ctx context.Context) (*conn[T], error) {
	v, err := p.config.Factory(ctx)
	if err != nil {
		return nil, err
	}
	c := &conn[T]{
		value:      v,
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
	}
	if p.config.HealthCheck != nil {
		c.lastChecked = time.Now()
	}
	p.conns[valueID(v)] = c
	return c, nil
}

func (p *Pool[T]) deleteConn(c *conn[T]) {
	delete(p.conns, valueID(c.value))
	p.total--
}

func (p *Pool[T]) Put(value T) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	id := valueID(value)
	c := p.conns[id]
	if c == nil {
		c = &conn[T]{
			value:      value,
			createdAt:  time.Now(),
			lastUsedAt: time.Now(),
		}
		p.conns[id] = c
	}

	c.lastUsedAt = time.Now()
	p.idle = append(p.idle, c)
	p.wakeWaiters()

	if p.config.MaxIdle > 0 && len(p.idle) > p.config.MaxIdle {
		for len(p.idle) > p.config.MaxIdle {
			c := p.idle[0]
			p.idle = p.idle[1:]
			p.deleteConn(c)
		}
	}

	if p.config.HealthCheck != nil {
		if c.checkQueued.CompareAndSwap(false, true) {
			go p.runAsyncHealthCheck(c)
		}
	}
}

func (p *Pool[T]) runAsyncHealthCheck(c *conn[T]) {
	p.mu.Lock()
	if time.Since(c.lastChecked) < p.config.HealthCheckInterval {
		c.checkQueued.Store(false)
		p.mu.Unlock()
		return
	}
	// Skip if connection is no longer idle (already borrowed)
	if !p.isIdle(c) {
		c.checkQueued.Store(false)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	ok := p.config.HealthCheck(context.Background(), c.value)

	p.mu.Lock()
	defer p.mu.Unlock()
	if !ok && p.isIdle(c) {
		p.removeFromIdle(c)
		p.deleteConn(c)
	}
	c.checkQueued.Store(false)
}

func (p *Pool[T]) isIdle(c *conn[T]) bool {
	for _, ic := range p.idle {
		if ic == c {
			return true
		}
	}
	return false
}

func (p *Pool[T]) removeFromIdle(c *conn[T]) {
	for i, ic := range p.idle {
		if ic == c {
			p.idle = append(p.idle[:i], p.idle[i+1:]...)
			break
		}
	}
}

func (p *Pool[T]) wakeWaiters() {
	select {
	case <-p.wait:
	default:
		close(p.wait)
		p.wait = make(chan struct{})
	}
}

func (p *Pool[T]) Get(ctx context.Context) (T, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		var zero T
		return zero, ErrPoolClosed
	}

	for {
		if c := p.getFromIdle(ctx); c != nil {
			return c.value, nil
		}

		if p.config.MaxTotal == 0 || p.total < p.config.MaxTotal {
			p.total++
			p.mu.Unlock()
			c, err := p.createConn(ctx)
			p.mu.Lock()
			if err != nil {
				p.total--
				var zero T
				return zero, err
			}
			return c.value, nil
		}

		// Create a new wait channel for this round
		wait := p.wait
		p.mu.Unlock()

		select {
		case <-wait:
			p.mu.Lock()
			continue
		case <-ctx.Done():
			p.mu.Lock()
			close(p.wait)
			p.wait = make(chan struct{})
			var zero T
			return zero, ctx.Err()
		}
	}
}
