package gpool

import (
	"context"
	"errors"
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
	cond   *sync.Cond
	closed bool
	idle   []*conn[T]
	total  int
	config Config[T]
	stopCh chan struct{}
	conns  map[uintptr]*conn[T]
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
	}
	p.cond = sync.NewCond(&p.mu)
	go p.backgroundLoop()
	return p, nil
}

func (p *Pool[T]) backgroundLoop() {
	// Will be implemented in a later task
}
