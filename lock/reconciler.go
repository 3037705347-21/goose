package lock

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3/lock/internal/reconcile"
	"github.com/pressly/goose/v3/lock/internal/store"
	"github.com/pressly/goose/v3/lock/internal/table"
)

const (
	// DefaultLockLedgerTableName is the default name of the lock ledger (audit) table.
	DefaultLockLedgerTableName = "goose_lock_ledger"
)

// Sentinel errors for table-lock ownership.
var (
	// ErrLockLost is returned by lease renewal or release when the lock is no longer owned by the
	// calling instance: a newer generation took it over and the caller has been fenced off. A
	// denied renewal is also recorded in the ledger table.
	ErrLockLost = store.ErrLockLost
	// ErrAmbiguousLockState is returned by a reconciliation run that found at least one lock in an
	// ambiguous state. The whole run is aborted and no lock is taken over or modified.
	ErrAmbiguousLockState = store.ErrAmbiguousLockState
)

// Reconciler performs lock ownership reconciliation.
type Reconciler interface {
	// Reconcile evaluates all in-scope actively locked rows, takes over locks whose leases have
	// unambiguously expired and records the takeovers in the ledger. Healthy locks are left
	// untouched; if any lock is ambiguous the run aborts without modifying any lock.
	Reconcile(ctx context.Context, db *sql.DB) (*ReconcileReport, error)
	// InstanceID returns the identity under which expired locks are taken over.
	InstanceID() string
}

// Public type aliases for reconciliation inputs, outputs and errors.
type (
	// ReconcileReport is the outcome of a reconciliation run.
	ReconcileReport = reconcile.Report
	// TakeoverRecord describes one lock takeover recorded in the ledger.
	TakeoverRecord = store.TakeoverRecord
	// LockSnapshot is a healthy lock observed during reconciliation and left untouched.
	LockSnapshot = store.ActiveLock
	// AmbiguousLock describes a lock that could not be classified safely.
	AmbiguousLock = store.AmbiguousLock
	// AmbiguousStateError wraps ErrAmbiguousLockState with the offending locks. Use errors.As to
	// extract the details.
	AmbiguousStateError = store.AmbiguousStateError
	// LockLostError wraps ErrLockLost with the current owner details. Use errors.As to extract the
	// details.
	LockLostError = store.LockLostError
	// LockLostInfo is delivered to the optional lock-lost handler when a heartbeat is fenced.
	LockLostInfo = table.LockLostInfo
)
