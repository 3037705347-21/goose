package lock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/pressly/goose/v3/lock/internal/table"
)

const (
	// DefaultLockTableName is the default name of the lock table.
	DefaultLockTableName = "goose_lock"
)

// TableLockerOption is used to configure a table-based locker.
type TableLockerOption interface {
	apply(*table.Config) error
}

// WithTableName sets the name of the lock table.
func WithTableName(tableName string) TableLockerOption {
	return tableLockerConfigFunc(func(c *table.Config) error {
		if tableName == "" {
			return errors.New("lock table name must not be empty")
		}
		c.TableName = tableName
		return nil
	})
}

// WithLedgerTableName sets the name of the append-only ledger table in which lock takeovers and
// denied renewals are recorded.
func WithLedgerTableName(tableName string) TableLockerOption {
	return tableLockerConfigFunc(func(c *table.Config) error {
		if tableName == "" {
			return errors.New("ledger table name must not be empty")
		}
		c.LedgerTableName = tableName
		return nil
	})
}

// WithInstanceID pins the locker's instance identity. By default a unique identity is generated.
// Pin the same identity used by a reconciler so that the locker adopts locks the reconciler took
// over without starting a new fencing generation.
func WithInstanceID(instanceID string) TableLockerOption {
	return tableLockerConfigFunc(func(c *table.Config) error {
		if instanceID == "" {
			return errors.New("instance ID must not be empty")
		}
		c.InstanceID = instanceID
		return nil
	})
}

// WithLockLostHandler sets a handler invoked once when a heartbeat is rejected because a newer
// generation took over the lock (fencing). The handler runs on the heartbeat goroutine and must
// return quickly; use it to cancel the work performed under the lock (for example by cancelling a
// context) to prevent split-brain writes.
func WithLockLostHandler(handler func(context.Context, LockLostInfo)) TableLockerOption {
	return tableLockerConfigFunc(func(c *table.Config) error {
		if handler == nil {
			return errors.New("lock lost handler must not be nil")
		}
		c.LockLostHandler = handler
		return nil
	})
}

// WithTableLockID sets the lock ID to use for this locker instance. Different lock IDs allow for
// multiple independent locks in the same table.
func WithTableLockID(lockID int64) TableLockerOption {
	return tableLockerConfigFunc(func(c *table.Config) error {
		if lockID <= 0 {
			return fmt.Errorf("lock ID must be greater than zero: %d", lockID)
		}
		c.LockID = lockID
		return nil
	})
}

// WithTableLeaseDuration sets how long a lock lease lasts. The lock will expire after this duration
// if not renewed by heartbeat.
func WithTableLeaseDuration(duration time.Duration) TableLockerOption {
	return tableLockerConfigFunc(func(c *table.Config) error {
		if duration <= 0 {
			return errors.New("lease duration must be positive")
		}
		c.LeaseDuration = duration
		return nil
	})
}

// WithTableHeartbeatInterval sets how often to send heartbeat updates to renew the lease. This
// should be significantly smaller than the lease duration.
func WithTableHeartbeatInterval(interval time.Duration) TableLockerOption {
	return tableLockerConfigFunc(func(c *table.Config) error {
		if interval <= 0 {
			return errors.New("heartbeat interval must be positive")
		}
		c.HeartbeatInterval = interval
		return nil
	})
}

// WithTableLockTimeout configures how long to retry acquiring a lock and how often to retry.
func WithTableLockTimeout(intervalDuration time.Duration, failureThreshold uint64) TableLockerOption {
	return tableLockerConfigFunc(func(c *table.Config) error {
		if intervalDuration <= 0 {
			return errors.New("lock timeout interval duration must be positive")
		}
		if failureThreshold == 0 {
			return errors.New("lock timeout failure threshold must be positive")
		}
		c.LockTimeout = table.ProbeConfig{
			IntervalDuration: intervalDuration,
			FailureThreshold: failureThreshold,
		}
		return nil
	})
}

// WithTableUnlockTimeout configures how long to retry releasing a lock and how often to retry.
func WithTableUnlockTimeout(intervalDuration time.Duration, failureThreshold uint64) TableLockerOption {
	return tableLockerConfigFunc(func(c *table.Config) error {
		if intervalDuration <= 0 {
			return errors.New("unlock timeout interval duration must be positive")
		}
		if failureThreshold == 0 {
			return errors.New("unlock timeout failure threshold must be positive")
		}
		c.UnlockTimeout = table.ProbeConfig{
			IntervalDuration: intervalDuration,
			FailureThreshold: failureThreshold,
		}
		return nil
	})
}

// WithTableLogger sets an optional logger for lock operations. If not provided, lock operations
// will use a default logger that only logs errors to stderr.
func WithTableLogger(logger *slog.Logger) TableLockerOption {
	return tableLockerConfigFunc(func(c *table.Config) error {
		c.Logger = logger
		return nil
	})
}

// WithTableRetryPolicy sets an optional callback to classify database errors during table lock
// operations.
//
// The provided function is invoked whenever a database operation fails. This includes Lock(),
// Unlock(), and heartbeat/lease update operations.
//
// If the function returns true, the operation is retried according to the configured retry/backoff
// policy.
//
// If it returns false, the operation fails immediately, bypassing any retries.
//
// This allows clients to implement custom logic for transient errors, driver-specific errors, or
// application-specific failure handling.
func WithTableRetryPolicy(retryPolicy func(error) bool) TableLockerOption {
	return tableLockerConfigFunc(func(c *table.Config) error {
		c.RetryPolicy = retryPolicy
		return nil
	})
}

type tableLockerConfigFunc func(*table.Config) error

func (f tableLockerConfigFunc) apply(cfg *table.Config) error {
	return f(cfg)
}
