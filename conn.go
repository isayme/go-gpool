package gpool

import (
	"sync/atomic"
	"time"
)

// conn wraps a pooled value with metadata for lifecycle management.
// It tracks creation time, last use time, last health check time,
// and whether a health check goroutine is already queued for this connection.
type conn[T any] struct {
	value       T
	createdAt   time.Time
	lastUsedAt  time.Time
	lastChecked time.Time

	// checkQueued is CAS'd to ensure at most one async health check
	// goroutine runs for this connection at any time.
	checkQueued atomic.Bool
}
