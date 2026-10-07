package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Ledger event types recorded in the lock ledger table.
const (
	// EventTakeover records ownership of a lock moving to a new generation: a lock whose lease had
	// expired (or that was left unlocked by a dead holder) was taken over by another instance.
	EventTakeover = "takeover"
	// EventRenewalDenied records a heartbeat/lease renewal attempt from an instance that no longer
	// owns the lock: a newer generation holds it and the stale instance has been fenced off.
	EventRenewalDenied = "renewal_denied"
)

// Ledger event reasons.
const (
	ReasonLeaseExpired = "lease_expired"
	ReasonFenced       = "fenced"
)

var (
	// ErrLockLost is returned when an operation is rejected because the lock is no longer owned by
	// this instance. The lock was either released or taken over by another (newer) generation, and
	// the caller must stop acting as the lock owner (fencing).
	ErrLockLost = errors.New("lock no longer held by this instance")
	// ErrAmbiguousLockState is returned by a reconciliation run that found at least one lock in an
	// ambiguous state. The entire run is aborted and no lock is modified or taken over.
	ErrAmbiguousLockState = errors.New("ambiguous lock state; reconciliation aborted")
)

// LockStore defines the interface for storing and managing database locks.
type LockStore interface {
	// CreateLockTable creates the lock table if it doesn't exist. Implementations should ensure
	// that this operation is idempotent.
	CreateLockTable(ctx context.Context, db *sql.DB) error
	// EnsureSchema makes sure the lock table, the ledger table and all columns the current version
	// relies on exist. It is safe to call repeatedly and upgrades tables created by older versions.
	EnsureSchema(ctx context.Context, db *sql.DB) error
	// TableExists checks if the lock table exists.
	TableExists(ctx context.Context, db *sql.DB) (bool, error)
	// AcquireLock attempts to acquire a lock.
	AcquireLock(ctx context.Context, db *sql.DB, params AcquireLockParams) (*AcquireLockResult, error)
	// ReleaseLock releases a lock held by the current instance. The release is fenced: it only
	// succeeds while the instance still owns the exact generation (epoch) it acquired.
	ReleaseLock(ctx context.Context, db *sql.DB, params ReleaseLockParams) (*ReleaseLockResult, error)
	// UpdateLease updates the lease expiration time for a lock (heartbeat). The renewal is fenced:
	// if another generation has taken over the lock, the renewal is rejected (ErrLockLost) and a
	// renewal_denied event is written to the ledger.
	UpdateLease(ctx context.Context, db *sql.DB, params UpdateLeaseParams) (*UpdateLeaseResult, error)
	// CheckLockStatus checks the current status of a lock.
	CheckLockStatus(ctx context.Context, db *sql.DB, lockID int64) (*LockStatus, error)
	// CleanupStaleLocks marks locks whose leases have expired as unlocked using server time.
	// Returns the list of lock IDs that were cleaned up, if any. The previous owner and epoch are
	// preserved on the row so that a subsequent takeover can be recorded in the ledger.
	CleanupStaleLocks(ctx context.Context, db *sql.DB) ([]int64, error)
	// ReconcileLocks evaluates every actively locked row in scope and takes over locks whose
	// leases have unambiguously expired. The operation is all-or-nothing within a single
	// transaction: if any scoped lock is in an ambiguous state, the transaction is rolled back and
	// ErrAmbiguousLockState is returned without touching any lock.
	ReconcileLocks(ctx context.Context, db *sql.DB, params ReconcileLocksParams) (*ReconcileLocksResult, error)
}

// LockStatus represents the current status of a lock.
type LockStatus struct {
	Locked         bool
	LockedBy       *string
	LeaseExpiresAt *time.Time
	UpdatedAt      *time.Time
	Epoch          int64
}

// AcquireLockParams contains the parameters for acquiring a lock.
type AcquireLockParams struct {
	LockID        int64
	LockedBy      string
	LeaseDuration time.Duration
}

// ReleaseLockParams contains the parameters for releasing a lock.
type ReleaseLockParams struct {
	LockID   int64
	LockedBy string
	// Epoch is the fencing generation the instance believes it owns.
	Epoch int64
}

// UpdateLeaseParams contains the parameters for renewing a lock lease.
type UpdateLeaseParams struct {
	LockID        int64
	LockedBy      string
	Epoch         int64
	LeaseDuration time.Duration
}

// ReconcileLocksParams contains the parameters for a reconciliation run.
type ReconcileLocksParams struct {
	// NewOwner is the instance that takes over expired locks.
	NewOwner string
	// LockIDs optionally restricts the run to the given lock IDs. An empty slice reconciles all
	// actively locked rows.
	LockIDs []int64
	// LeaseDuration is the fresh lease granted to taken-over locks.
	LeaseDuration time.Duration
	// ExpiryGrace requires a lease to have been expired for at least this long before the lock may
	// be taken over. All time is measured against the database server clock.
	ExpiryGrace time.Duration
	// RunID identifies the reconciliation run in the ledger.
	RunID string
}

// AcquireLockResult contains the result of a lock acquisition attempt.
type AcquireLockResult struct {
	LockedBy       string
	LeaseExpiresAt time.Time
	// Epoch is the fencing generation of the acquired lock. It must be presented on subsequent
	// lease renewals and on release.
	Epoch int64
}

// ReleaseLockResult contains the result of a lock release.
type ReleaseLockResult struct {
	LockID int64
}

// UpdateLeaseResult contains the result of a lease update.
type UpdateLeaseResult struct {
	LeaseExpiresAt time.Time
	Epoch          int64
}

// ActiveLock is a point-in-time view of an actively locked row.
type ActiveLock struct {
	LockID         int64
	LockedBy       string
	Epoch          int64
	LeaseExpiresAt time.Time
}

// TakeoverRecord describes a single lock takeover recorded in the ledger.
type TakeoverRecord struct {
	LockID int64
	// OwnerFrom is the previous owner. It may be empty for rows left without an owner.
	OwnerFrom string
	// OwnerTo is the instance that took over the lock.
	OwnerTo string
	// PreviousEpoch is the fencing generation that died. Epoch in the ledger row.
	PreviousEpoch int64
	// NewEpoch is the fencing generation started by the takeover.
	NewEpoch int64
	// PreviousLeaseExpiresAt is the lease expiration observed at takeover time.
	PreviousLeaseExpiresAt time.Time
	// TakenOverAt is the server time at which the takeover was committed.
	TakenOverAt time.Time
	// Reason is the ledger reason, e.g. lease_expired.
	Reason string
}

// AmbiguousLock describes a lock that could not be classified safely during reconciliation.
type AmbiguousLock struct {
	LockID int64
	Reason string
}

// ReconcileLocksResult contains the outcome of a reconciliation run.
type ReconcileLocksResult struct {
	// ServerTime is the database server time used for classification.
	ServerTime time.Time
	// TakenOver is the list of locks taken over by the run.
	TakenOver []TakeoverRecord
	// Alive is the snapshot of locks that were healthy (lease still valid) and left untouched.
	Alive []ActiveLock
}

// LockLostError wraps ErrLockLost with details about who currently owns the lock.
type LockLostError struct {
	LockID int64
	// ClaimedBy is the instance whose operation was rejected.
	ClaimedBy string
	// ClaimedEpoch is the fencing generation presented by the rejected instance.
	ClaimedEpoch int64
	// CurrentOwner is the owner recorded on the lock row at rejection time, if any.
	CurrentOwner string
	// CurrentEpoch is the generation recorded on the lock row at rejection time.
	CurrentEpoch int64
	// Reason is lease_expired or fenced.
	Reason string
}

func (e *LockLostError) Error() string {
	if e.CurrentOwner == "" {
		return fmt.Sprintf("lock %d: owner %q epoch %d no longer holds the lock",
			e.LockID, e.ClaimedBy, e.ClaimedEpoch)
	}
	return fmt.Sprintf("lock %d: owner %q epoch %d was fenced; current owner is %q epoch %d",
		e.LockID, e.ClaimedBy, e.ClaimedEpoch, e.CurrentOwner, e.CurrentEpoch)
}

func (e *LockLostError) Is(target error) bool { return target == ErrLockLost }

// AmbiguousStateError wraps ErrAmbiguousLockState with the list of locks that could not be
// classified.
type AmbiguousStateError struct {
	Locks []AmbiguousLock
}

func (e *AmbiguousStateError) Error() string {
	return fmt.Sprintf("ambiguous state for %d lock(s); no locks were modified", len(e.Locks))
}

func (e *AmbiguousStateError) Is(target error) bool { return target == ErrAmbiguousLockState }
