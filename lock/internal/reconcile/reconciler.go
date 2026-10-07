// Package reconcile implements ownership reconciliation for table-based locks.
//
// A reconciliation run evaluates every actively locked row against the database server clock and
// takes over locks whose leases have unambiguously expired, recording each takeover in an
// append-only ledger. Healthy locks are never touched; ambiguous rows abort the entire run.
package reconcile

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/pressly/goose/v3/lock/internal/instance"
	"github.com/pressly/goose/v3/lock/internal/store"
)

// Config holds configuration for a Reconciler.
type Config struct {
	// LockTableName is the table holding lock rows.
	LockTableName string
	// LedgerTableName is the append-only takeover/denial audit table.
	LedgerTableName string
	// LockIDs optionally restricts reconciliation to the given lock IDs. Empty reconciles all
	// actively locked rows.
	LockIDs []int64
	// InstanceID is the identity that takes over expired locks. When empty a unique identity is
	// generated.
	InstanceID string
	// LeaseDuration is the fresh lease granted to each taken-over lock.
	LeaseDuration time.Duration
	// ExpiryGrace requires a lease to have been expired for at least this long before takeover.
	ExpiryGrace time.Duration
	// Logger receives run diagnostics. When nil an error-level stderr logger is used.
	Logger *slog.Logger
}

// Report is the outcome of a reconciliation run.
type Report struct {
	// RunID identifies the run; it is stamped on every takeover ledger row written by this run.
	RunID string
	// CheckedAt is the database server time used for classification.
	CheckedAt time.Time
	// TakenOver lists the locks this run took over.
	TakenOver []store.TakeoverRecord
	// Alive lists healthy locks that were observed and left untouched.
	Alive []store.ActiveLock
}

// Reconciler runs lock ownership reconciliation against a LockStore.
type Reconciler struct {
	store  store.LockStore
	config Config
	logger *slog.Logger
}

// New creates a Reconciler. An empty InstanceID in cfg is replaced with a generated identity.
func New(lockStore store.LockStore, cfg Config) *Reconciler {
	if cfg.InstanceID == "" {
		cfg.InstanceID = instance.NewID()
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	}
	return &Reconciler{
		store:  lockStore,
		config: cfg,
		logger: logger,
	}
}

// InstanceID returns the identity under which this reconciler takes over locks. Callers can pin a
// table locker to the same identity so that the locker adopts taken-over locks without losing a
// generation.
func (r *Reconciler) InstanceID() string {
	return r.config.InstanceID
}

// Reconcile runs a single, all-or-nothing reconciliation pass.
//
// Healthy locks are left untouched, expired locks are atomically taken over and recorded in the
// ledger. If any in-scope lock is in an ambiguous state, the whole run aborts with
// store.ErrAmbiguousLockState and no lock is modified.
//
// The operation is safe to rerun: reruns converge to the current ownership state without
// duplicating ledger entries for locks that are already healthy.
func (r *Reconciler) Reconcile(ctx context.Context, db *sql.DB) (*Report, error) {
	if err := r.store.EnsureSchema(ctx, db); err != nil {
		return nil, fmt.Errorf("ensure lock schema: %w", err)
	}

	runID := instance.RandomHex(8)

	result, err := r.store.ReconcileLocks(ctx, db, store.ReconcileLocksParams{
		NewOwner:      r.config.InstanceID,
		LockIDs:       r.config.LockIDs,
		LeaseDuration: r.config.LeaseDuration,
		ExpiryGrace:   r.config.ExpiryGrace,
		RunID:         runID,
	})
	if err != nil {
		r.logger.WarnContext(ctx, "lock reconciliation aborted",
			slog.String("run_id", runID),
			slog.String("instance_id", r.config.InstanceID),
			slog.Any("error", err),
		)
		return nil, err
	}

	for _, rec := range result.TakenOver {
		r.logger.InfoContext(ctx, "took over expired lock",
			slog.String("run_id", runID),
			slog.Int64("lock_id", rec.LockID),
			slog.String("owner_from", rec.OwnerFrom),
			slog.String("owner_to", rec.OwnerTo),
			slog.Int64("previous_epoch", rec.PreviousEpoch),
			slog.Int64("new_epoch", rec.NewEpoch),
		)
	}
	r.logger.DebugContext(ctx, "lock reconciliation complete",
		slog.String("run_id", runID),
		slog.Int("taken_over", len(result.TakenOver)),
		slog.Int("alive", len(result.Alive)),
	)

	return &Report{
		RunID:     runID,
		CheckedAt: result.ServerTime,
		TakenOver: result.TakenOver,
		Alive:     result.Alive,
	}, nil
}
