package table

import (
	"context"
	"log/slog"
	"time"
)

// Config holds configuration for table locker.
type Config struct {
	TableName         string
	LedgerTableName   string
	LockID            int64
	InstanceID        string
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	LockTimeout       ProbeConfig
	UnlockTimeout     ProbeConfig

	// Optional logger for lock operations
	Logger *slog.Logger

	// Optional custom retry policy for database errors
	RetryPolicy RetryPolicyFunc

	// Optional handler invoked once when a heartbeat is rejected because the lock was taken over
	// by a newer generation (fencing). It runs on the heartbeat goroutine and must return quickly;
	// typical use is to cancel the work performed under the lock to prevent split-brain writes.
	LockLostHandler func(context.Context, LockLostInfo)
}

// ProbeConfig holds retry configuration.
type ProbeConfig struct {
	IntervalDuration time.Duration
	FailureThreshold uint64
}

// LockLostInfo describes a fencing event delivered to LockLostHandler.
type LockLostInfo struct {
	LockID       int64
	InstanceID   string
	Epoch        int64
	CurrentOwner string
	CurrentEpoch int64
	Reason       string
}
