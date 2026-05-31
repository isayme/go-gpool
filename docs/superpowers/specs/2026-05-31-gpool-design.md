# gpool: Generic Connection Pool for Go

## Overview

gpool is a generic connection pool library for Go. It uses Go 1.18+ generics to provide a type-safe,
configurable pool for any kind of connection (database, gRPC, HTTP, etc.).

## Goals

- Generic: works with any connection type `T`
- Configurable: min/max capacity, lifetime, idle timeout, health check
- Thread-safe: safe for concurrent use by multiple goroutines
- Blocking: `Get` blocks when pool is exhausted, with context support for timeout/cancellation
- Minimal dependencies: standard library only

## Non-Goals

- Connection lifecycle management (creation, reconnection) — caller provides Factory
- Protocol-specific logic (PING, auth, etc.) — caller provides HealthCheck

## Configuration

```go
type Config[T any] struct {
    // MinIdle is the minimum number of idle connections to maintain.
    // The background goroutine replenishes idle connections to meet this threshold.
    MinIdle int

    // MaxIdle is the maximum number of idle connections in the pool.
    // Idle connections beyond this limit are closed and removed.
    // 0 means no limit.
    MaxIdle int

    // MaxTotal is the maximum total number of connections (idle + in-use).
    // Get blocks when this limit is reached, waiting for a connection to be returned.
    // 0 means no limit.
    MaxTotal int

    // MaxLifetime is the maximum amount of time a connection may be reused.
    // Connections older than this are closed and replaced.
    // 0 means no limit.
    MaxLifetime time.Duration

    // IdleTimeout is the maximum amount of time a connection may be idle.
    // Idle connections exceeding this duration are closed and removed.
    // 0 means no limit.
    IdleTimeout time.Duration

    // HealthCheckInterval is the minimum interval between health checks for a connection.
    // When a connection is returned, an async health check is scheduled but skipped if
    // the last check was less than HealthCheckInterval ago, or if a check is already queued.
    // 0 means check every time.
    HealthCheckInterval time.Duration

    // BackgroundInterval is the interval between background maintenance runs.
    // During each run, the pool evicts expired connections, performs health checks,
    // and replenishes idle connections to meet MinIdle.
    // Defaults to HealthCheckInterval if not set.
    BackgroundInterval time.Duration

    // Factory creates a new connection. Called when the pool needs to grow.
    // If the provided ctx is canceled or exceeds a deadline, Factory should
    // return immediately. The pool passes through the error returned by Factory.
    Factory func(ctx context.Context) (T, error)

    // HealthCheck verifies a connection is still healthy.
    // Called before lending a connection (sync) and asynchronously after return.
    // Return true if healthy, false to discard and replace the connection.
    HealthCheck func(ctx context.Context, conn T) bool
}
```

## Internal Types

```go
type conn[T any] struct {
    value       T
    createdAt   time.Time
    lastUsedAt  time.Time
    lastChecked time.Time
    checkQueued atomic.Bool
}
```

Each connection carries metadata for lifetime management and health check debouncing.

## Pool Structure

```go
type Pool[T any] struct {
    mu      sync.Mutex
    cond    *sync.Cond
    closed  bool
    idle    []*conn[T]
    total   int
    config  Config[T]
    stopCh  chan struct{}
}
```

- `idle`: LIFO stack of idle connections
- `total`: current total connections (idle + in-use), guarded by `mu`
- `cond`: notifies blocked `Get` callers when a connection is returned
- `stopCh`: signals the background goroutine to stop

## Core Operations

### Get(ctx)

1. Lock `mu`
2. If `closed`, unlock and return `ErrPoolClosed`
3. Loop:
   a. Pop from `idle` (LIFO)
   b. If no idle connection:
      - If `total < MaxTotal`: unlock, call `Factory(ctx)`, lock, increment `total` on success
      - If `total >= MaxTotal`: `cond.Wait()` (blocks until Put signals or ctx cancels)
   c. Clear `checkQueued` flag on the connection
   d. Check `MaxLifetime` / `IdleTimeout`: if expired, close and discard, continue loop
   e. Run `HealthCheck(ctx)`: if fails, close and discard, continue loop
   f. Update `lastUsedAt`, unlock, return connection

### Put(conn)

1. Lock `mu`
2. If `closed`: unlock, close connection silently, return nil
3. Push connection onto `idle` stack
4. Update `lastUsedAt`
5. Schedule async health check with debounce:
   - If `time.Since(lastChecked) < HealthCheckInterval`: skip
   - If `checkQueued` is true: skip
   - Otherwise: set `checkQueued`, launch goroutine
6. Signal `cond` to wake one waiting `Get`
7. Check `MaxIdle`: if `len(idle) > MaxIdle`, close and remove one
8. Unlock

### Close()

1. Lock `mu`, set `closed = true`
2. Close all idle connections
3. Unlock
4. Broadcast `cond` (wake all blocked Get callers, they return ErrPoolClosed)
5. Close `stopCh` to stop the background goroutine

## Health Check Debounce

The debounce prevents redundant health checks on frequently-used connections:

| Event | Action |
|-------|--------|
| Put (async schedule) | Skip if `lastChecked` within `HealthCheckInterval` OR `checkQueued` is already set |
| Get (borrow) | Clear `checkQueued` flag — the queued goroutine will find `checkQueued == false` and skip |
| Background scan | Only check idle connections where `lastChecked` is outside `HealthCheckInterval` |

## Background Goroutine

A single background goroutine runs on `Pool` creation and stops on `Close()`. It:

1. Sleeps for a configurable interval (e.g., `HealthCheckInterval`)
2. Locks `mu`, iterates `idle`:
   - Close and remove connections exceeding `MaxLifetime`
   - Close and remove connections exceeding `IdleTimeout`
   - Run `HealthCheck` on connections overdue for a check
3. If `len(idle) < MinIdle` and `total < MaxTotal`, create new connections via `Factory`
4. If `len(idle) > MaxIdle`, close and remove excess connections
5. Unlock, repeat

## Errors

```go
var (
    ErrPoolClosed    = errors.New("gpool: pool is closed")
    ErrFactoryFailed = errors.New("gpool: factory failed")
)
```

`Get` returns `context.DeadlineExceeded` / `context.Canceled` when the context expires
while waiting.

## Thread Safety

- All shared state (`idle`, `total`, `closed`) is protected by `sync.Mutex`
- `sync.Cond` provides blocking semantics for pool exhaustion
- `atomic.Bool` on `conn.checkQueued` avoids locking in the hot path
- Background goroutine is the only concurrent reader of idle connections during scans

## Testing Strategy

- Unit tests with a mock connection type
- Concurrent tests to verify thread safety
- Context cancellation/deadline tests
- Health check debounce tests
- Idle timeout / max lifetime expiry tests
- Pool exhaustion blocking tests
- Close-with-active-connections tests


