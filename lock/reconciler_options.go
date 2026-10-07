package lock

import (
	"errors"
	"log/slog"
	"time"

	"github.com/pressly/goose/v3/lock/internal/reconcile"
)

// ReconcilerOption configures a table-lock reconciler.
type ReconcilerOption interface {
	applyReconciler(*reconcile.Config) error
}

// WithReconcileTableName sets the name of the lock table the reconciler reads.
func WithReconcileTableName(tableName string) ReconcilerOption {
	return reconcilerConfigFunc(func(c *reconcile.Config) error {
		if tableName == "" {
			return errors.New("lock table name must not be empty")
		}
		c.LockTableName = tableName
		return nil
	})
}

// WithReconcileLedgerTableName sets the name of the append-only ledger table in which takeovers
// and denied renewals are recorded.
func WithReconcileLedgerTableName(tableName string) ReconcilerOption {
	return reconcilerConfigFunc(func(c *reconcile.Config) error {
		if tableName == "" {
			return errors.New("ledger table name must not be empty")
		}
		c.LedgerTableName = tableName
		return nil
	})
}

// WithReconcileLockIDs restricts reconciliation to the given lock IDs. By default every actively
// locked row is reconciled.
func WithReconcileLockIDs(lockIDs ...int64) ReconcilerOption {
	return reconcilerConfigFunc(func(c *reconcile.Config) error {
		for _, id := range lockIDs {
			if id <= 0 {
				return errors.New("reconcile lock IDs must be greater than zero")
			}
		}
		c.LockIDs = append(c.LockIDs, lockIDs...)
		return nil
	})
}

// WithReconcileInstanceID pins the identity under which expired locks are taken over. Use the same
// identity on the locker that subsequently adopts the taken-over locks.
func WithReconcileInstanceID(instanceID string) ReconcilerOption {
	return reconcilerConfigFunc(func(c *reconcile.Config) error {
		if instanceID == "" {
			return errors.New("instance ID must not be empty")
		}
		c.InstanceID = instanceID
		return nil
	})
}

// WithReconcileLeaseDuration sets the fresh lease granted to every lock taken over.
func WithReconcileLeaseDuration(duration time.Duration) ReconcilerOption {
	return reconcilerConfigFunc(func(c *reconcile.Config) error {
		if duration <= 0 {
			return errors.New("lease duration must be positive")
		}
		c.LeaseDuration = duration
		return nil
	})
}

// WithReconcileExpiryGrace requires a lease to have been expired for at least the given duration
// before its lock may be taken over. All time is measured against the database server clock.
func WithReconcileExpiryGrace(grace time.Duration) ReconcilerOption {
	return reconcilerConfigFunc(func(c *reconcile.Config) error {
		if grace < 0 {
			return errors.New("expiry grace must not be negative")
		}
		c.ExpiryGrace = grace
		return nil
	})
}

// WithReconcileLogger sets an optional logger for reconciliation runs. If not provided, runs log at
// error level to stderr.
func WithReconcileLogger(logger *slog.Logger) ReconcilerOption {
	return reconcilerConfigFunc(func(c *reconcile.Config) error {
		c.Logger = logger
		return nil
	})
}

type reconcilerConfigFunc func(*reconcile.Config) error

func (f reconcilerConfigFunc) applyReconciler(cfg *reconcile.Config) error {
	return f(cfg)
}
