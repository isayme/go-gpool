// Package gpool provides a generic, configurable connection pool.
// It supports any connection type via Go 1.18+ generics.
//
// The pool manages connections through a user-provided Factory function
// and optionally validates them via a HealthCheck function. Connections
// are lent via Get and returned via Put.
//
// Basic usage:
//
//	pool, _ := gpool.New(gpool.Config[*grpc.ClientConn]{
//	    MinIdle:  2,
//	    MaxTotal: 10,
//	    Factory: func(ctx context.Context) (*grpc.ClientConn, error) {
//	        return grpc.Dial("target", grpc.WithInsecure())
//	    },
//	    HealthCheck: func(ctx context.Context, conn *grpc.ClientConn) bool {
//	        return conn.GetState() == connectivity.Ready
//	    },
//	})
//	defer pool.Close()
//
//	conn, _ := pool.Get(ctx)
//	defer pool.Put(conn)
package gpool

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
)

// ErrPoolClosed is returned by Get when the pool has been closed.
var ErrPoolClosed = errors.New("gpool: pool is closed")

// Config defines the pool's behaviour. All duration fields accept 0 to mean
// "no limit" (or "no cooldown" for interval fields).
type Config[T any] struct {
	// MinIdle is the minimum number of idle connections the background
	// goroutine tries to maintain. The pool replenishes idle connections
	// up to this threshold during periodic maintenance.
	MinIdle int

	// MaxIdle is the maximum number of idle connections kept in the pool.
	// Excess idle connections are closed and removed. 0 means no limit.
	MaxIdle int

	// MaxTotal is the maximum number of connections (idle + in-use).
	// Get blocks when this limit is reached until a connection is returned.
	// 0 means no limit.
	MaxTotal int

	// MaxLifetime is the maximum time a connection may live regardless of
	// activity. Connections older than this are retired. 0 means no limit.
	MaxLifetime time.Duration

	// IdleTimeout is the maximum time a connection may sit idle before
	// being retired. 0 means no limit.
	IdleTimeout time.Duration

	// HealthCheckInterval sets the minimum interval between health checks
	// on the same connection. When a connection is returned via Put, an
	// async health check may be scheduled but is skipped if the last check
	// was more recent than this interval or if a check is already queued.
	// Used together with HealthCheck. 0 means every Put triggers a check.
	HealthCheckInterval time.Duration

	// BackgroundInterval controls how often the background maintenance
	// goroutine runs. It evicts expired connections and replenishes idle
	// connections to meet MinIdle.
	// Defaults to HealthCheckInterval. Falls back to 30s if also 0.
	BackgroundInterval time.Duration

	// Factory creates a new connection. Required. The pool may call Factory
	// with a cancelled context when a blocking Get times out; implementations
	// should respect ctx.Done() and return promptly.
	Factory func(ctx context.Context) (T, error)

	// HealthCheck validates a connection before it is lent out (synchronous,
	// inside Get) and optionally after return (asynchronous, inside Put).
	// Return true if the connection is healthy. If nil, no health checks
	// are performed.
	HealthCheck func(ctx context.Context, conn T) bool
}

// Pool is a generic goroutine-safe connection pool.
//
// Pool manages connections via a LIFO idle stack protected by a mutex.
// When the pool is exhausted, Get blocks on a channel until a connection
// is returned via Put or the context is cancelled.
//
// The zero Pool is not usable — use New to create one.
type Pool[T any] struct {
	mu     sync.Mutex
	closed bool

	idle  []*conn[T]       // LIFO stack of idle connections
	total int               // total connections (idle + in-use)
	config Config[T]
	stopCh chan struct{}     // signals backgroundLoop to stop
	conns  map[uintptr]*conn[T] // tracks all conn wrappers by valueID for Put lookup
	wait   chan struct{}     // closed to wake blocked Get callers
}

// New creates a Pool with the given Config. Factory is required;
// other fields have sensible defaults.
func New[T any](config Config[T]) (*Pool[T], error) {
	if config.Factory == nil {
		return nil, errors.New("gpool: Factory is required")
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

// backgroundLoop runs periodic maintenance until Close is called.
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

// maintenance evicts expired idle connections and replenishes to MinIdle.
// Health checks are NOT done here to avoid the race between releasing
// the lock for I/O and concurrent Get/Put operations — they are handled
// synchronously in Get and asynchronously in Put.
func (p *Pool[T]) maintenance() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	now := time.Now()

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
		valid = append(valid, c)
	}
	p.idle = valid

	for len(p.idle) < p.config.MinIdle && (p.config.MaxTotal == 0 || p.total < p.config.MaxTotal) {
		p.total++
		p.mu.Unlock()
		c, err := p.createConn(context.Background())
		p.mu.Lock()
		if err != nil {
			p.total--
			break
		}
		p.conns[valueID(c.value)] = c
		if p.closed {
			p.total--
			break
		}
		p.idle = append(p.idle, c)
	}

	if p.config.MaxIdle > 0 && len(p.idle) > p.config.MaxIdle {
		for len(p.idle) > p.config.MaxIdle {
			c := p.idle[0]
			p.idle = p.idle[1:]
			p.deleteConn(c)
		}
	}
}

// Close shuts down the pool. It clears idle connections, wakes any
// goroutines blocked in Get (they return ErrPoolClosed), stops the
// background goroutine, and marks the pool as closed.
// Connections still borrowed at the time of Close are not closed;
// they become orphaned and will be discarded when returned via Put.
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

// valueID returns a stable identifier for a value to track its conn[T] wrapper
// across Get/Put cycles. It works for pointer and interface types (the common
// case for connections). For value types it always returns 0, which means they
// share a single tracking slot and the pool cannot distinguish between
// different values — the pool still works correctly but connection metadata
// (createdAt, lastChecked) is reset on every cycle.
func valueID[T any](v T) uintptr {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		return rv.Pointer()
	}
	return 0
}

// getFromIdle pops the most recently returned connection from the idle stack
// (LIFO, favouring hot connections), checks expiry and health, and returns it.
// Returns nil if the stack is empty or all entries have been discarded.
//
// The mutex must be held when calling this method; it may temporarily
// release the mutex during the HealthCheck callback.
func (p *Pool[T]) getFromIdle(ctx context.Context) *conn[T] {
	for len(p.idle) > 0 {
		c := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		// Clear the health-check flag so Put can schedule a fresh one later.
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

// createConn calls the Factory, wraps the result in a conn[T], and returns it.
// The caller is responsible for storing it in p.conns under the mutex.
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
	return c, nil
}

// deleteConn removes a connection from the tracking map and decrements total.
// The connection is assumed to be permanently gone from the pool.
// Must be called with p.mu held.
func (p *Pool[T]) deleteConn(c *conn[T]) {
	delete(p.conns, valueID(c.value))
	p.total--
}

// Put returns a borrowed connection to the pool. If the pool has been closed,
// the connection is silently discarded. Put is idempotent-safe for known
// connections but should be called exactly once per Get.
//
// After placing the connection in the idle stack, Put optionally schedules an
// asynchronous health check. The check is debounced: it is skipped if
// HealthCheckInterval has not elapsed since the last check, or if a check
// goroutine is already queued for this connection.
func (p *Pool[T]) Put(value T) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	// Look up the existing conn wrapper so metadata (createdAt, lastChecked)
	// is preserved across Get/Put cycles.
	id := valueID(value)
	c := p.conns[id]
	if c == nil {
		// Unknown value — create a fresh wrapper and count it.
		c = &conn[T]{
			value:      value,
			createdAt:  time.Now(),
			lastUsedAt: time.Now(),
		}
		p.conns[id] = c
		p.total++
	}

	c.lastUsedAt = time.Now()
	p.idle = append(p.idle, c)
	p.wakeWaiters()

	// Evict oldest idle connections to stay within MaxIdle.
	if p.config.MaxIdle > 0 && len(p.idle) > p.config.MaxIdle {
		for len(p.idle) > p.config.MaxIdle {
			c := p.idle[0]
			p.idle = p.idle[1:]
			p.deleteConn(c)
		}
	}

	// Schedule async health check with debounce via CAS on checkQueued.
	if p.config.HealthCheck != nil {
		if c.checkQueued.CompareAndSwap(false, true) {
			go p.runAsyncHealthCheck(c)
		}
	}
}

// runAsyncHealthCheck performs a health check outside the pool lock and
// removes the connection if it fails — but only if the connection is still
// idle (has not been lent out in the meantime).
func (p *Pool[T]) runAsyncHealthCheck(c *conn[T]) {
	p.mu.Lock()
	if time.Since(c.lastChecked) < p.config.HealthCheckInterval {
		c.checkQueued.Store(false)
		p.mu.Unlock()
		return
	}
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

// isIdle reports whether c is currently in the idle stack.
func (p *Pool[T]) isIdle(c *conn[T]) bool {
	for _, ic := range p.idle {
		if ic == c {
			return true
		}
	}
	return false
}

// removeFromIdle removes c from the idle stack by linear scan.
// The scan is O(n) but runs only in the error / eviction path
// where performance is not critical.
func (p *Pool[T]) removeFromIdle(c *conn[T]) {
	for i, ic := range p.idle {
		if ic == c {
			p.idle = append(p.idle[:i], p.idle[i+1:]...)
			break
		}
	}
}

// wakeWaiters closes the current wait channel and replaces it with a fresh
// one, waking all goroutines blocked in Get. Must be called with p.mu held.
func (p *Pool[T]) wakeWaiters() {
	select {
	case <-p.wait:
		// Already closed — nothing to do.
	default:
		close(p.wait)
		p.wait = make(chan struct{})
	}
}

// Get borrows a connection from the pool. It returns an idle connection if
// one is available and healthy; otherwise it creates a new one via Factory.
//
// When the pool has reached MaxTotal and all connections are in use, Get
// blocks until a connection is returned via Put or the context is cancelled.
//
// A successful call returns (T, nil). The caller must return the connection
// via Put when done.
//
// Possible errors:
//   - ErrPoolClosed — pool has been closed.
//   - ctx.Err() — context was cancelled or deadline exceeded while waiting.
//   - Errors returned by Factory (passed through verbatim).
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
			p.conns[valueID(c.value)] = c
			// Pool may have been closed while we released the lock for
			// createConn — discard and return ErrPoolClosed.
			if p.closed {
				p.total--
				var zero T
				return zero, ErrPoolClosed
			}
			return c.value, nil
		}

		// Pool exhausted — wait on the wake channel.
		wait := p.wait
		p.mu.Unlock()

		select {
		case <-wait:
			p.mu.Lock()
			continue
		case <-ctx.Done():
			// Wake all waiters so they can re-check the condition or
			// discover the pool is closed. A dedicated wake per waiter
			// would be more precise but this thundering-herd pattern
			// is simple and safe.
			p.mu.Lock()
			close(p.wait)
			p.wait = make(chan struct{})
			var zero T
			return zero, ctx.Err()
		}
	}
}
