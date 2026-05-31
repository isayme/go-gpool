package gpool

import (
	"sync/atomic"
	"time"
)

type conn[T any] struct {
	value       T
	createdAt   time.Time
	lastUsedAt  time.Time
	lastChecked time.Time
	checkQueued atomic.Bool
}
