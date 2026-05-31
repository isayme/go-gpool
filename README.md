# gpool

[![Go Reference](https://pkg.go.dev/badge/github.com/isayme/go-gpool.svg)](https://pkg.go.dev/github.com/isayme/go-gpool)

Generic connection pool for Go. Uses Go 1.18+ generics for type-safe connection management.

## Features

- Type-safe via generics — works with any connection type `T`
- Configurable min/max idle, max total, max lifetime, idle timeout
- Blocking `Get` with context support for timeout/cancellation
- Async health check with debounce
- Background maintenance — evicts expired connections, replenishes min idle
- Zero external dependencies — standard library only

## Usage

```go
package main

import (
    "context"
    "time"

    "github.com/isayme/go-gpool"
    "google.golang.org/grpc"
    "google.golang.org/grpc/connectivity"
)

func main() {
    pool, err := gpool.New(gpool.Config[*grpc.ClientConn]{
        MinIdle:             2,
        MaxIdle:             5,
        MaxTotal:            10,
        MaxLifetime:         30 * time.Minute,
        IdleTimeout:         5 * time.Minute,
        HealthCheckInterval: 10 * time.Second,
        Factory: func(ctx context.Context) (*grpc.ClientConn, error) {
            return grpc.Dial("target:443", grpc.WithInsecure())
        },
        HealthCheck: func(ctx context.Context, conn *grpc.ClientConn) bool {
            return conn.GetState() != connectivity.TransientFailure
        },
    })
    if err != nil {
        panic(err)
    }
    defer pool.Close()

    conn, err := pool.Get(context.Background())
    if err != nil {
        panic(err)
    }
    defer pool.Put(conn)

    // use conn ...
}
```

## Config

| Field | Description | Default |
|-------|-------------|---------|
| MinIdle | Minimum idle connections to maintain | 0 |
| MaxIdle | Maximum idle connections (0 = no limit) | 0 |
| MaxTotal | Maximum total connections (0 = no limit) | 0 |
| MaxLifetime | Max connection lifetime (0 = no limit) | 0 |
| IdleTimeout | Max idle time (0 = no limit) | 0 |
| HealthCheckInterval | Min interval between health checks (0 = check every time) | 0 |
| BackgroundInterval | Interval between maintenance runs | HealthCheckInterval or 30s |
| Factory | Connection factory (required) | — |
| HealthCheck | Connection health check (optional) | nil |

## License

MIT
