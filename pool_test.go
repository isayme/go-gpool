package gpool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type testConn struct {
	id int
}

func TestBasicGetPut(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxTotal: 5,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	conn, err := pool.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if conn == nil {
		t.Fatal("expected non-nil connection")
	}
	pool.Put(conn)
}

func TestGetReturnsErrorOnClosedPool(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxTotal: 5,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()

	_, err = pool.Get(context.Background())
	if err != ErrPoolClosed {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}
}

func TestPutOnClosedPoolIsSilent(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxTotal: 5,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	conn, _ := pool.Get(context.Background())
	pool.Close()
	pool.Put(conn)
}

func TestGetContextCancellation(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxTotal: 1,
		Factory: func(ctx context.Context) (*testConn, error) {
			time.Sleep(50 * time.Millisecond)
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	conn, _ := pool.Get(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err = pool.Get(ctx)
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}

	pool.Put(conn)
}

func TestFactoryError(t *testing.T) {
	factoryErr := errors.New("factory failed")
	pool, err := New(Config[*testConn]{
		MaxTotal: 5,
		Factory: func(ctx context.Context) (*testConn, error) {
			return nil, factoryErr
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	_, err = pool.Get(context.Background())
	if err != factoryErr {
		t.Fatalf("expected factoryErr, got %v", err)
	}
}

func TestConcurrentGetPut(t *testing.T) {
	counter := 0
	var mu sync.Mutex

	pool, err := New(Config[*testConn]{
		MinIdle:  2,
		MaxIdle:  5,
		MaxTotal: 10,
		Factory: func(ctx context.Context) (*testConn, error) {
			mu.Lock()
			counter++
			id := counter
			mu.Unlock()
			return &testConn{id: id}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				conn, err := pool.Get(context.Background())
				if err != nil {
					t.Log(err)
					return
				}
				time.Sleep(time.Millisecond)
				pool.Put(conn)
			}
		}()
	}
	wg.Wait()
}

func TestHealthCheckDiscardsBadConnections(t *testing.T) {
	counter := 0
	var mu sync.Mutex

	pool, err := New(Config[*testConn]{
		MaxTotal: 5,
		Factory: func(ctx context.Context) (*testConn, error) {
			mu.Lock()
			counter++
			id := counter
			mu.Unlock()
			return &testConn{id: id}, nil
		},
		HealthCheck: func(ctx context.Context, conn *testConn) bool {
			return conn.id != 1
		},
		HealthCheckInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	conn, _ := pool.Get(context.Background())
	conn.id = 1
	pool.Put(conn)

	// The health check runs asynchronously and removes the bad connection.
	time.Sleep(50 * time.Millisecond)

	conn2, _ := pool.Get(context.Background())
	if conn2.id == 1 {
		t.Fatal("expected a new connection, got the bad one")
	}
}

func TestMaxLifetime(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxTotal:    5,
		MaxLifetime: 50 * time.Millisecond,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	conn, _ := pool.Get(context.Background())
	pool.Put(conn)

	time.Sleep(100 * time.Millisecond)

	conn2, _ := pool.Get(context.Background())
	if conn == conn2 {
		t.Fatal("expected a different connection due to MaxLifetime")
	}
	pool.Put(conn2)
}

func TestIdleTimeout(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxTotal:    5,
		IdleTimeout: 50 * time.Millisecond,
		BackgroundInterval: 20 * time.Millisecond,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	conn, _ := pool.Get(context.Background())
	pool.Put(conn)

	time.Sleep(100 * time.Millisecond)

	conn2, _ := pool.Get(context.Background())
	if conn == conn2 {
		t.Fatal("expected a different connection due to IdleTimeout")
	}
	pool.Put(conn2)
}

func TestMaxIdleTrim(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxIdle:  2,
		MaxTotal: 10,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	var conns []*testConn
	for i := 0; i < 5; i++ {
		conn, _ := pool.Get(context.Background())
		conns = append(conns, conn)
	}
	for _, c := range conns {
		pool.Put(c)
	}

	// Pool should have at most 2 idle connections (MaxIdle=2).
	pool.mu.Lock()
	idleCount := len(pool.idle)
	pool.mu.Unlock()
	if idleCount > 2 {
		t.Fatalf("expected at most 2 idle connections, got %d", idleCount)
	}
}

func TestMinIdleReplenish(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MinIdle:  3,
		MaxTotal: 10,
		BackgroundInterval: 20 * time.Millisecond,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Wait for maintenance to replenish to MinIdle.
	time.Sleep(100 * time.Millisecond)

	pool.mu.Lock()
	idleCount := len(pool.idle)
	pool.mu.Unlock()
	if idleCount < 3 {
		t.Fatalf("expected at least 3 idle connections, got %d", idleCount)
	}
}

func TestGetBlockAndWake(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxTotal: 1,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Take the only connection.
	conn, _ := pool.Get(context.Background())

	// Try to Get in background — it will block.
	done := make(chan *testConn, 1)
	go func() {
		c, err := pool.Get(context.Background())
		if err == nil {
			done <- c
		}
	}()

	time.Sleep(50 * time.Millisecond)

	// Return the connection — should wake the blocked Get.
	pool.Put(conn)

	select {
	case c := <-done:
		if c != conn {
			t.Fatal("expected the returned connection")
		}
		defer pool.Put(c)
	case <-time.After(time.Second):
		t.Fatal("blocked Get was not woken")
	}
}

func TestPutUnknownConn(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxTotal: 5,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Put a value that was never borrowed from the pool.
	pool.Put(&testConn{id: 99})

	conn, _ := pool.Get(context.Background())
	if conn.id != 99 {
		t.Fatal("expected the returned connection with id=99")
	}
	pool.Put(conn)
}

func TestNewFactoryRequired(t *testing.T) {
	_, err := New(Config[*testConn]{})
	if err == nil {
		t.Fatal("expected error when Factory is nil")
	}
}

func TestNewBackgroundIntervalDefaults(t *testing.T) {
	// When BackgroundInterval is 0 and HealthCheckInterval is also 0,
	// it should default to 30s.
	pool, err := New(Config[*testConn]{
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if pool.config.BackgroundInterval != 30*time.Second {
		t.Fatalf("expected 30s, got %s", pool.config.BackgroundInterval)
	}
	pool.Close()
}

func TestValueIDWithValueType(t *testing.T) {
	// valueID returns 0 for non-pointer types.
	var x int = 42
	id := valueID(x)
	if id != 0 {
		t.Fatalf("expected 0 for value type, got %d", id)
	}
}

func TestAsyncHealthCheckOnBorrowedConn(t *testing.T) {
	var mu sync.Mutex
	checked := 0

	pool, err := New(Config[*testConn]{
		MaxTotal: 5,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
		HealthCheck: func(ctx context.Context, conn *testConn) bool {
			mu.Lock()
			checked++
			mu.Unlock()
			return true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Get a connection and Put it back (schedules async check).
	conn, _ := pool.Get(context.Background())
	pool.Put(conn)

	// Immediately borrow it again before the async check runs.
	conn2, _ := pool.Get(context.Background())
	if conn != conn2 {
		t.Fatal("expected the same connection")
	}

	// Let the async check goroutine run — it should find conn not idle and skip.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if checked > 1 {
		t.Fatalf("expected at most 1 health check, got %d (async check should have been skipped)", checked)
	}
	mu.Unlock()

	pool.Put(conn2)
}

func TestMaintenanceMaxLifetime(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxTotal:           5,
		MaxLifetime:        50 * time.Millisecond,
		BackgroundInterval: 30 * time.Millisecond,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Create an idle connection.
	conn, _ := pool.Get(context.Background())
	pool.Put(conn)
	time.Sleep(100 * time.Millisecond)

	// Maintenance should have evicted the expired connection.
	pool.mu.Lock()
	idleCount := len(pool.idle)
	pool.mu.Unlock()
	if idleCount > 0 {
		t.Fatal("expected all idle connections to be evicted by MaxLifetime")
	}
}

func TestMaintenanceReplenishFactoryFail(t *testing.T) {
	failCount := 0
	pool, err := New(Config[*testConn]{
		MinIdle:            2,
		MaxTotal:           5,
		BackgroundInterval: 20 * time.Millisecond,
		Factory: func(ctx context.Context) (*testConn, error) {
			failCount++
			return nil, errors.New("factory failed")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Wait for maintenance to attempt replenish — Factory will fail.
	time.Sleep(100 * time.Millisecond)

	pool.mu.Lock()
	idleCount := len(pool.idle)
	total := pool.total
	pool.mu.Unlock()
	if idleCount != 0 {
		t.Fatalf("expected 0 idle, got %d", idleCount)
	}
	if total != 0 {
		t.Fatalf("expected total=0 after failed replenish, got %d", total)
	}
	if failCount == 0 {
		t.Fatal("expected Factory to be called")
	}
}

func TestMaintenanceTrimMaxIdle(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxIdle:            2,
		MaxTotal:           10,
		BackgroundInterval: 20 * time.Millisecond,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Create and return more connections than MaxIdle.
	var conns []*testConn
	for i := 0; i < 5; i++ {
		c, _ := pool.Get(context.Background())
		conns = append(conns, c)
	}
	for _, c := range conns {
		pool.Put(c)
	}

	// Put doesn't always trim below MaxIdle — it trims to MaxIdle.
	// Wait for maintenance to also trim.
	time.Sleep(100 * time.Millisecond)

	pool.mu.Lock()
	idleCount := len(pool.idle)
	pool.mu.Unlock()
	if idleCount > 2 {
		t.Fatalf("expected at most 2 idle, got %d", idleCount)
	}
}

func TestGetIdleTimeoutExpired(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxTotal:   5,
		IdleTimeout: 30 * time.Millisecond,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	conn, _ := pool.Get(context.Background())
	pool.Put(conn)

	time.Sleep(100 * time.Millisecond)

	// Get should discard the expired idle connection and create a new one.
	conn2, _ := pool.Get(context.Background())
	if conn == conn2 {
		t.Fatal("expected a different connection due to IdleTimeout")
	}
	pool.Put(conn2)
}

func TestWakeWaitersAlreadyClosed(t *testing.T) {
	// Directly test the already-closed path in wakeWaiters.
	p := &Pool[*testConn]{
		wait: make(chan struct{}),
	}
	// First call closes the channel.
	p.wakeWaiters()
	// Second call hits the closed case.
	p.wakeWaiters()
}

func TestGetClosedDuringCreateConn(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxTotal: 5,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	conn, err := pool.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pool.Put(conn)
	pool.Close()

	// Pool is closed — Get should return ErrPoolClosed.
	_, err = pool.Get(context.Background())
	if err != ErrPoolClosed {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}
}

func TestGetBlockedAndClosed(t *testing.T) {
	pool, err := New(Config[*testConn]{
		MaxTotal: 1,
		Factory: func(ctx context.Context) (*testConn, error) {
			return &testConn{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	conn, _ := pool.Get(context.Background())

	errCh := make(chan error, 1)
	go func() {
		_, err := pool.Get(context.Background())
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	pool.Close()

	select {
	case err := <-errCh:
		if err != ErrPoolClosed {
			t.Fatalf("expected ErrPoolClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Get was not woken by Close")
	}

	// conn is now orphaned — Put after Close should be silent.
	pool.Put(conn)
}
