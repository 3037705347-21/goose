package table

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/pressly/goose/v3/lock/internal/instance"
	"github.com/pressly/goose/v3/lock/internal/store"
	"github.com/sethvargo/go-retry"
)

// RetryPolicyFunc inspects an error and returns whether the caller should retry the operation. This
// allows for database-specific error handling without hardcoding driver-specific logic.
type RetryPolicyFunc func(err error) bool

// Locker implements table-based locking for databases. This implementation is safe for concurrent
// use by multiple goroutines.
type Locker struct {
	store             store.LockStore
	tableName         string
	lockID            int64
	instanceID        string
	leaseDuration     time.Duration
	heartbeatInterval time.Duration
	retryLock         retry.Backoff
	retryUnlock       retry.Backoff
	logger            *slog.Logger
	retryPolicy       RetryPolicyFunc
	lockLostHandler   func(context.Context, LockLostInfo)

	// Application-level coordination
	mu sync.Mutex

	// epoch is the fencing generation returned by the successful acquisition. Guarded by mu.
	epoch int64

	// Heartbeat management
	heartbeatCancel context.CancelFunc
	heartbeatDone   chan struct{}
}

// New creates a new table-based locker.
func New(lockStore store.LockStore, cfg Config) *Locker {
	// Use the configured instance identifier or generate one.
	instanceID := cfg.InstanceID
	if instanceID == "" {
		instanceID = instance.NewID()
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	}

	return &Locker{
		store:             lockStore,
		tableName:         cfg.TableName,
		lockID:            cfg.LockID,
		instanceID:        instanceID,
		leaseDuration:     cfg.LeaseDuration,
		heartbeatInterval: cfg.HeartbeatInterval,
		logger:            logger,
		retryPolicy:       cfg.RetryPolicy,
		lockLostHandler:   cfg.LockLostHandler,
		retryLock: retry.WithMaxRetries(
			cfg.LockTimeout.FailureThreshold,
			// Add +/- 25% jitter to reduce thundering herd
			retry.WithJitterPercent(25, retry.NewConstant(cfg.LockTimeout.IntervalDuration)),
		),
		retryUnlock: retry.WithMaxRetries(
			cfg.UnlockTimeout.FailureThreshold,
			// Add +/- 25% jitter to reduce thundering herd
			retry.WithJitterPercent(25, retry.NewConstant(cfg.UnlockTimeout.IntervalDuration)),
		),
	}
}

// InstanceID returns the identity of this locker. It can be pinned on a reconciler so that the
// locker adopts locks the reconciler takes over without starting a new fencing generation.
func (l *Locker) InstanceID() string {
	return l.instanceID
}

// Epoch returns the fencing generation held by the locker. It returns 0 before Lock succeeds.
func (l *Locker) Epoch() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.epoch
}

// Lock acquires the database lock. This method is safe for concurrent use - the mutex is held until
// Unlock() is called. Only one goroutine can hold the lock at a time across the entire lifecycle.
func (l *Locker) Lock(ctx context.Context, db *sql.DB) error {
	l.mu.Lock()
	// NOTE: mutex is NOT defer unlocked here, it remains held until Unlock() is called explicitly
	// or a specific error occurs below!

	// Ensure the lock table, ledger table and fencing columns exist
	if err := l.store.EnsureSchema(ctx, db); err != nil {
		l.mu.Unlock()
		return fmt.Errorf("ensure lock schema: %w", err)
	}

	var acquired *store.AcquireLockResult
	err := retry.Do(ctx, l.retryLock, func(ctx context.Context) error {
		result, err := l.store.AcquireLock(ctx, db, store.AcquireLockParams{
			LockID:        l.lockID,
			LockedBy:      l.instanceID,
			LeaseDuration: l.leaseDuration,
		})
		if err != nil {
			// Clean up any stale locks before retrying
			if _, cleanupErr := l.store.CleanupStaleLocks(ctx, db); cleanupErr != nil {
				l.logger.WarnContext(ctx, "failed to cleanup stale locks",
					slog.Int64("lock_id", l.lockID),
					slog.Any("error", cleanupErr),
				)
				// Continue with retry, cleanup failure shouldn't block acquisition attempts
			}
			if l.shouldRetry(err) {
				return retry.RetryableError(fmt.Errorf("acquire retryable lock: %w", err))
			}
			return fmt.Errorf("acquire lock: %w", err)
		}
		acquired = result
		return nil
	})
	if err != nil {
		l.mu.Unlock()
		l.logger.WarnContext(ctx, "failed to acquire lock after retries",
			slog.Int64("lock_id", l.lockID),
			slog.String("instance_id", l.instanceID),
			slog.Any("error", err),
		)
		return fmt.Errorf("acquire lock %d after retries: %w", l.lockID, err)
	}

	l.epoch = acquired.Epoch
	l.logger.DebugContext(ctx, "successfully acquired lock",
		slog.Int64("lock_id", l.lockID),
		slog.String("instance_id", l.instanceID),
		slog.Int64("epoch", l.epoch),
		slog.Duration("lease_duration", l.leaseDuration),
	)
	// Start heartbeat to maintain the lease
	l.startHeartbeat(ctx, db)

	// Mutex remains held - will be released in Unlock()
	return nil
}

// Unlock releases the database lock. This method must be called exactly once after a successful
// Lock() call.
func (l *Locker) Unlock(ctx context.Context, db *sql.DB) error {
	// NOTE: The mutex was acquired in Lock() and is still held
	defer l.mu.Unlock()

	// Use a context that can't be cancelled to ensure we always attempt to unlock even if the
	// caller's context is cancelled. The call can control the retry behavior via the configured
	// timeouts.
	ctx = context.WithoutCancel(ctx)

	// Stop heartbeat first
	l.stopHeartbeat()

	err := retry.Do(ctx, l.retryUnlock, func(ctx context.Context) error {
		_, err := l.store.ReleaseLock(ctx, db, store.ReleaseLockParams{
			LockID:   l.lockID,
			LockedBy: l.instanceID,
			Epoch:    l.epoch,
		})
		if err != nil {
			// A fencing rejection is terminal: the lock already belongs to a newer generation, so
			// retrying would only risk disturbing it.
			if errors.Is(err, store.ErrLockLost) {
				return fmt.Errorf("release lock fenced: %w", err)
			}
			if l.shouldRetry(err) {
				return retry.RetryableError(fmt.Errorf("release retryable lock: %w", err))
			}
			return fmt.Errorf("release lock: %w", err)
		}
		return nil
	})
	if err != nil {
		l.logger.WarnContext(ctx, "failed to release lock",
			slog.Int64("lock_id", l.lockID),
			slog.String("instance_id", l.instanceID),
			slog.Int64("epoch", l.epoch),
			slog.Any("error", err),
		)
		return err
	}

	l.logger.DebugContext(ctx, "successfully released lock",
		slog.Int64("lock_id", l.lockID),
		slog.String("instance_id", l.instanceID),
		slog.Int64("epoch", l.epoch),
	)
	return nil
}

// startHeartbeat starts the heartbeat goroutine (called from within Lock with mutex held)
func (l *Locker) startHeartbeat(parentCtx context.Context, db *sql.DB) {
	// If there's already a heartbeat running, stop it first
	l.stopHeartbeat()

	// Create a new context for the heartbeat
	ctx, cancel := context.WithCancel(parentCtx)
	l.heartbeatCancel = cancel
	l.heartbeatDone = make(chan struct{})

	go func() {
		defer close(l.heartbeatDone)
		ticker := time.NewTicker(l.heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				result, err := l.store.UpdateLease(ctx, db, store.UpdateLeaseParams{
					LockID:        l.lockID,
					LockedBy:      l.instanceID,
					Epoch:         l.epoch,
					LeaseDuration: l.leaseDuration,
				})
				if err != nil {
					// Fencing: a newer generation took over the lock. The lease renewal is
					// rejected and recorded by the store; this instance must stop acting as owner.
					var lostErr *store.LockLostError
					if errors.As(err, &lostErr) {
						l.logger.ErrorContext(ctx, "lock lost to a newer generation; stopping heartbeat",
							slog.Int64("lock_id", l.lockID),
							slog.String("instance_id", l.instanceID),
							slog.Int64("epoch", l.epoch),
							slog.String("current_owner", lostErr.CurrentOwner),
							slog.Int64("current_epoch", lostErr.CurrentEpoch),
							slog.Any("error", err),
						)
						l.notifyLockLost(ctx, LockLostInfo{
							LockID:       l.lockID,
							InstanceID:   l.instanceID,
							Epoch:        l.epoch,
							CurrentOwner: lostErr.CurrentOwner,
							CurrentEpoch: lostErr.CurrentEpoch,
							Reason:       lostErr.Reason,
						})
						return
					}
					// TODO(mf): should we add a retry policy here?
					l.logger.WarnContext(ctx, "heartbeat failed to update lease",
						slog.Int64("lock_id", l.lockID),
						slog.String("instance_id", l.instanceID),
						slog.Int64("epoch", l.epoch),
						slog.Any("error", err),
					)
					continue
				}
				l.logger.DebugContext(ctx, "heartbeat updated lease",
					slog.Int64("lock_id", l.lockID),
					slog.String("instance_id", l.instanceID),
					slog.Int64("epoch", l.epoch),
					slog.Time("lease_expires_at", result.LeaseExpiresAt),
				)
			}
		}
	}()
}

// notifyLockLost invokes the optional lock-lost handler. Handlers must be non-blocking.
func (l *Locker) notifyLockLost(ctx context.Context, info LockLostInfo) {
	if l.lockLostHandler == nil {
		return
	}
	defer func() {
		_ = recover() // handlers must not crash the heartbeat goroutine
	}()
	l.lockLostHandler(ctx, info)
}

// stopHeartbeat stops the heartbeat goroutine (called from within Unlock with mutex held).
func (l *Locker) stopHeartbeat() {
	if l.heartbeatCancel != nil {
		l.heartbeatCancel()
		<-l.heartbeatDone
		l.heartbeatCancel = nil
		l.heartbeatDone = nil
	}
}

// shouldRetry determines whether an error is retryable based on the configured retry policy. If no
// retry policy is configured, it defaults to always retrying.
func (l *Locker) shouldRetry(err error) bool {
	if l.retryPolicy != nil {
		return l.retryPolicy(err)
	}
	return true
}
