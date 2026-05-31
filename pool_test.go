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
