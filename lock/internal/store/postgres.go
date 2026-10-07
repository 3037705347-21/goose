package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/multierr"
)

// NewPostgres creates a new Postgres-based [LockStore].
func NewPostgres(lockTableName, ledgerTableName string) (LockStore, error) {
	if lockTableName == "" {
		return nil, errors.New("lock table name must not be empty")
	}
	if ledgerTableName == "" {
		return nil, errors.New("ledger table name must not be empty")
	}
	return &postgresStore{
		tableName: lockTableName,
		ledger:    ledgerTableName,
	}, nil
}

var _ LockStore = (*postgresStore)(nil)

type postgresStore struct {
	tableName string
	ledger    string
}

func (s *postgresStore) TableExists(
	ctx context.Context,
	db *sql.DB,
) (bool, error) {
	return s.tableExistsNamed(ctx, db, s.tableName)
}

func (s *postgresStore) tableExistsNamed(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var query string
	schemaName, tableName := parseTableIdentifier(name)
	if schemaName != "" {
		q := `SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE schemaname = '%s' AND tablename = '%s' )`
		query = fmt.Sprintf(q, schemaName, tableName)
	} else {
		q := `SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE (current_schema() IS NULL OR schemaname = current_schema()) AND tablename = '%s' )`
		query = fmt.Sprintf(q, tableName)
	}

	var exists bool
	if err := db.QueryRowContext(ctx, query).Scan(
		&exists,
	); err != nil {
		return false, fmt.Errorf("table exists: %w", err)
	}
	return exists, nil
}

func (s *postgresStore) CreateLockTable(
	ctx context.Context,
	db *sql.DB,
) error {
	exists, err := s.TableExists(ctx, db)
	if err != nil {
		return fmt.Errorf("check lock table existence: %w", err)
	}
	if exists {
		return nil
	}

	query := fmt.Sprintf(`CREATE TABLE %s (
		lock_id bigint NOT NULL PRIMARY KEY,
		locked boolean NOT NULL DEFAULT false,
		locked_at timestamptz NULL,
		locked_by text NULL,
		lease_expires_at timestamptz NULL,
		updated_at timestamptz NULL,
		epoch bigint NOT NULL DEFAULT 1
	)`, s.tableName)
	if _, err := db.ExecContext(ctx, query); err != nil {
		// Double-check if another process created it concurrently
		if exists, checkErr := s.TableExists(ctx, db); checkErr == nil && exists {
			// Another process created it, that's fine!
			return nil
		}
		return fmt.Errorf("create lock table %q: %w", s.tableName, err)
	}
	return nil
}

// EnsureSchema creates missing tables/columns. It is safe to run repeatedly, even concurrently from
// multiple processes, and upgrades tables created by older goose versions.
func (s *postgresStore) EnsureSchema(ctx context.Context, db *sql.DB) error {
	if err := s.CreateLockTable(ctx, db); err != nil {
		return err
	}
	if err := s.createLedgerTable(ctx, db); err != nil {
		return err
	}
	// Postgres supports IF NOT EXISTS on ADD COLUMN, but two concurrent ADD COLUMN statements can
	// still race; re-check the column when that happens.
	alterQ := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS epoch bigint NOT NULL DEFAULT 1`, s.tableName)
	if _, err := db.ExecContext(ctx, alterQ); err != nil {
		if exists, _ := s.columnExists(ctx, db, s.tableName, "epoch"); exists {
			return nil
		}
		return fmt.Errorf("ensure epoch column on %q: %w", s.tableName, err)
	}
	return nil
}

func (s *postgresStore) createLedgerTable(ctx context.Context, db *sql.DB) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		event_id bigserial PRIMARY KEY,
		lock_id bigint NOT NULL,
		epoch bigint NOT NULL,
		event_type text NOT NULL,
		owner_from text NULL,
		owner_to text NULL,
		reason text NOT NULL,
		lease_expires_at timestamptz NULL,
		run_id text NULL,
		created_at timestamptz NOT NULL DEFAULT now()
	)`, s.ledger)
	if _, err := db.ExecContext(ctx, query); err != nil {
		// CREATE TABLE IF NOT EXISTS is not concurrency-safe in Postgres: a concurrent creator may
		// surface a duplicate_table error. Treat it as success when the table now exists.
		if exists, _ := s.tableExistsNamed(ctx, db, s.ledger); exists {
			return nil
		}
		return fmt.Errorf("create ledger table %q: %w", s.ledger, err)
	}
	indexQ := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (lock_id, epoch)`,
		ledgerIndexName(s.ledger), s.ledger)
	if _, err := db.ExecContext(ctx, indexQ); err != nil {
		var exists bool
		iq := `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)`
		if checkErr := db.QueryRowContext(ctx, iq, ledgerIndexName(s.ledger)).Scan(&exists); checkErr != nil || !exists {
			return fmt.Errorf("create ledger index on %q: %w", s.ledger, err)
		}
	}
	return nil
}

func (s *postgresStore) columnExists(ctx context.Context, db *sql.DB, tableName, columnName string) (bool, error) {
	schemaName, baseName := parseTableIdentifier(tableName)
	q := `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = COALESCE(NULLIF($1, ''), current_schema())
			AND table_name = $2 AND column_name = $3)`
	var exists bool
	if err := db.QueryRowContext(ctx, q, schemaName, baseName, columnName).Scan(&exists); err != nil {
		return false, fmt.Errorf("column exists: %w", err)
	}
	return exists, nil
}

func (s *postgresStore) AcquireLock(
	ctx context.Context,
	db *sql.DB,
	params AcquireLockParams,
) (_ *AcquireLockResult, retErr error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("acquire lock %d: begin tx: %w", params.LockID, err)
	}
	defer func() {
		if retErr != nil {
			retErr = multierr.Append(retErr, tx.Rollback())
		}
	}()

	var now time.Time
	if err := tx.QueryRowContext(ctx, `SELECT now()`).Scan(&now); err != nil {
		return nil, fmt.Errorf("acquire lock %d: server time: %w", params.LockID, err)
	}

	// Insert a brand-new row at generation 1. If the row already exists this is a no-op and the
	// SELECT ... FOR UPDATE below decides what happens.
	insertQ := fmt.Sprintf(`INSERT INTO %s (lock_id, locked, locked_at, locked_by, lease_expires_at, updated_at, epoch)
		VALUES ($1, true, now(), $2, now() + $3::interval, now(), 1)
		ON CONFLICT (lock_id) DO NOTHING`, s.tableName)
	if _, err := tx.ExecContext(ctx, insertQ,
		params.LockID,
		params.LockedBy,
		formatDurationAsInterval(params.LeaseDuration),
	); err != nil {
		return nil, fmt.Errorf("acquire lock %d: %w", params.LockID, err)
	}

	row, err := s.selectLockForUpdate(ctx, tx, params.LockID)
	if err != nil {
		return nil, fmt.Errorf("acquire lock %d: %w", params.LockID, err)
	}

	// Reentrant acquisition: the same instance still holds a valid lease at the same generation.
	// Nothing is written and the epoch is preserved, which also lets a locker co-identified with a
	// reconciler adopt locks the reconciler just took over.
	if row.Locked && row.LockedBy.Valid && row.LockedBy.String == params.LockedBy &&
		row.LeaseExpiresAt.Valid && !row.LeaseExpiresAt.Time.Before(now) {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("acquire lock %d: commit: %w", params.LockID, err)
		}
		return &AcquireLockResult{
			LockedBy:       params.LockedBy,
			LeaseExpiresAt: row.LeaseExpiresAt.Time,
			Epoch:          row.Epoch.Int64,
		}, nil
	}

	// Someone else holds a live lease.
	if row.Locked && row.LockedBy.Valid && row.LockedBy.String != params.LockedBy &&
		row.LeaseExpiresAt.Valid && !row.LeaseExpiresAt.Time.Before(now) {
		return nil, fmt.Errorf("acquire lock %d: already held by another instance", params.LockID)
	}

	// Unlocked row or expired lease: start a new fencing generation.
	updateQ := fmt.Sprintf(`UPDATE %s SET
		locked = true,
		locked_at = now(),
		locked_by = $2,
		lease_expires_at = now() + $3::interval,
		updated_at = now(),
		epoch = epoch + 1
	WHERE lock_id = $1
	RETURNING lease_expires_at, epoch`, s.tableName)

	var leaseExpiresAt time.Time
	var newEpoch int64
	if err := tx.QueryRowContext(ctx, updateQ,
		params.LockID,
		params.LockedBy,
		formatDurationAsInterval(params.LeaseDuration),
	).Scan(&leaseExpiresAt, &newEpoch); err != nil {
		return nil, fmt.Errorf("acquire lock %d: take over: %w", params.LockID, err)
	}

	// Record the ownership change when a previous, different owner is known.
	if row.LockedBy.Valid && row.LockedBy.String != "" && row.LockedBy.String != params.LockedBy {
		ev := ledgerEvent{
			LockID:         params.LockID,
			Epoch:          row.Epoch.Int64,
			EventType:      EventTakeover,
			OwnerFrom:      row.LockedBy,
			OwnerTo:        nullString(params.LockedBy),
			Reason:         ReasonLeaseExpired,
			LeaseExpiresAt: row.LeaseExpiresAt,
		}
		if err := s.insertLedgerEvent(ctx, tx, ev); err != nil {
			return nil, fmt.Errorf("acquire lock %d: record takeover: %w", params.LockID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("acquire lock %d: commit: %w", params.LockID, err)
	}
	return &AcquireLockResult{
		LockedBy:       params.LockedBy,
		LeaseExpiresAt: leaseExpiresAt,
		Epoch:          newEpoch,
	}, nil
}

func (s *postgresStore) ReleaseLock(
	ctx context.Context,
	db *sql.DB,
	params ReleaseLockParams,
) (_ *ReleaseLockResult, retErr error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("release lock %d: begin tx: %w", params.LockID, err)
	}
	committed := false
	defer func() {
		if retErr != nil && !committed {
			retErr = multierr.Append(retErr, tx.Rollback())
		}
	}()

	// Release only while we still own the exact generation we acquired. Epoch is intentionally
	// preserved: it is the base for the next generation and a tombstone for the ledger.
	query := fmt.Sprintf(`UPDATE %s SET
		locked = false,
		locked_at = NULL,
		locked_by = NULL,
		lease_expires_at = NULL,
		updated_at = now()
	WHERE lock_id = $1 AND locked_by = $2 AND locked = true AND epoch = $3
	RETURNING lock_id`, s.tableName)

	var returnedLockID int64
	err = tx.QueryRowContext(ctx, query,
		params.LockID,
		params.LockedBy,
		params.Epoch,
	).Scan(&returnedLockID)
	switch {
	case err == nil:
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("release lock %d: commit: %w", params.LockID, err)
		}
		committed = true
		return &ReleaseLockResult{LockID: returnedLockID}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("release lock %d: %w", params.LockID, err)
	}

	// No rows: distinguish fencing (a newer generation owns the lock) from ordinary "not held".
	lost, err := s.detectFencing(ctx, tx, params.LockID, params.LockedBy, params.Epoch)
	if err != nil {
		return nil, fmt.Errorf("release lock %d: %w", params.LockID, err)
	}
	if lost != nil {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("release lock %d: commit: %w", params.LockID, err)
		}
		committed = true
		return nil, lost
	}
	return nil, fmt.Errorf("release lock %d: not held by this instance", params.LockID)
}

func (s *postgresStore) UpdateLease(
	ctx context.Context,
	db *sql.DB,
	params UpdateLeaseParams,
) (_ *UpdateLeaseResult, retErr error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to update lease for lock %d: begin tx: %w", params.LockID, err)
	}
	committed := false
	defer func() {
		if retErr != nil && !committed {
			retErr = multierr.Append(retErr, tx.Rollback())
		}
	}()

	// Renew only while we still own the exact generation we acquired.
	query := fmt.Sprintf(`UPDATE %s SET
		lease_expires_at = now() + $1::interval,
		updated_at = now()
	WHERE lock_id = $2 AND locked_by = $3 AND locked = true AND epoch = $4
	RETURNING lease_expires_at, epoch`, s.tableName)

	var leaseExpiresAt time.Time
	var epoch int64
	err = tx.QueryRowContext(ctx, query,
		formatDurationAsInterval(params.LeaseDuration),
		params.LockID,
		params.LockedBy,
		params.Epoch,
	).Scan(&leaseExpiresAt, &epoch)
	switch {
	case err == nil:
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to update lease for lock %d: commit: %w", params.LockID, err)
		}
		committed = true
		return &UpdateLeaseResult{LeaseExpiresAt: leaseExpiresAt, Epoch: epoch}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("failed to update lease for lock %d: %w", params.LockID, err)
	}

	// The renewal was rejected. If a newer generation currently owns the lock, the stale instance
	// has been fenced: record the denied renewal before returning ErrLockLost.
	lost, err := s.detectFencing(ctx, tx, params.LockID, params.LockedBy, params.Epoch)
	if err != nil {
		return nil, fmt.Errorf("failed to update lease for lock %d: %w", params.LockID, err)
	}
	if lost == nil {
		return nil, fmt.Errorf("failed to update lease for lock %d: not held by this instance", params.LockID)
	}
	lost.Reason = ReasonFenced
	ev := ledgerEvent{
		LockID:    params.LockID,
		Epoch:     params.Epoch,
		EventType: EventRenewalDenied,
		OwnerFrom: nullString(params.LockedBy),
		OwnerTo:   nullString(lost.CurrentOwner),
		Reason:    ReasonFenced,
	}
	if ledgerErr := s.insertLedgerEvent(ctx, tx, ev); ledgerErr != nil {
		// The renewal stays rejected even if the audit write fails; surface both to the caller.
		if commitErr := tx.Commit(); commitErr != nil {
			return nil, fmt.Errorf("failed to update lease for lock %d: record denied renewal: %w (commit: %v)",
				params.LockID, ledgerErr, commitErr)
		}
		committed = true
		return nil, multierr.Append(lost, fmt.Errorf("record denied renewal: %w", ledgerErr))
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to update lease for lock %d: commit: %w", params.LockID, err)
	}
	committed = true
	return nil, lost
}

func (s *postgresStore) CheckLockStatus(
	ctx context.Context,
	db *sql.DB,
	lockID int64,
) (*LockStatus, error) {
	query := fmt.Sprintf(`SELECT locked, locked_by, lease_expires_at, updated_at, epoch FROM %s WHERE lock_id = $1`, s.tableName)
	var status LockStatus

	err := db.QueryRowContext(ctx, query,
		lockID,
	).Scan(
		&status.Locked,
		&status.LockedBy,
		&status.LeaseExpiresAt,
		&status.UpdatedAt,
		&status.Epoch,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("lock %d not found", lockID)
		}
		return nil, fmt.Errorf("check lock status for %d: %w", lockID, err)
	}

	return &status, nil
}

func (s *postgresStore) CleanupStaleLocks(ctx context.Context, db *sql.DB) (_ []int64, retErr error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("cleanup stale locks: begin tx: %w", err)
	}
	defer func() {
		if retErr != nil {
			retErr = multierr.Append(retErr, tx.Rollback())
		}
	}()

	// Identity (locked_by) and epoch are deliberately preserved as a tombstone, so a later
	// takeover can credit the previous owner in the ledger.
	selectQ := fmt.Sprintf(`SELECT lock_id FROM %s WHERE locked = true AND lease_expires_at < now() FOR UPDATE`, s.tableName)
	rows, err := tx.QueryContext(ctx, selectQ)
	if err != nil {
		return nil, fmt.Errorf("cleanup stale locks: %w", err)
	}
	var stale []int64
	for rows.Next() {
		var lockID int64
		if err := rows.Scan(&lockID); err != nil {
			return nil, multierr.Append(fmt.Errorf("scan cleaned lock ID: %w", err), rows.Close())
		}
		stale = append(stale, lockID)
	}
	if err := rows.Err(); err != nil {
		return nil, multierr.Append(fmt.Errorf("iterate over cleaned locks: %w", err), rows.Close())
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close cleaned locks rows: %w", err)
	}

	if len(stale) > 0 {
		updateQ := fmt.Sprintf(`UPDATE %s SET
			locked = false,
			locked_at = NULL,
			lease_expires_at = NULL,
			updated_at = now()
		WHERE lock_id IN (%s)`, s.tableName, pgPlaceholders(len(stale), 1))
		args := make([]any, len(stale))
		for i, id := range stale {
			args[i] = id
		}
		if _, err := tx.ExecContext(ctx, updateQ, args...); err != nil {
			return nil, fmt.Errorf("cleanup stale locks: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("cleanup stale locks: commit: %w", err)
	}
	return stale, nil
}

func (s *postgresStore) ReconcileLocks(
	ctx context.Context,
	db *sql.DB,
	params ReconcileLocksParams,
) (_ *ReconcileLocksResult, retErr error) {
	if params.NewOwner == "" {
		return nil, errors.New("reconcile: new owner must not be empty")
	}
	if params.LeaseDuration <= 0 {
		return nil, fmt.Errorf("reconcile: lease duration must be positive, got %s", params.LeaseDuration)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("reconcile locks: begin tx: %w", err)
	}
	defer func() {
		if retErr != nil {
			retErr = multierr.Append(retErr, tx.Rollback())
		}
	}()

	var now time.Time
	if err := tx.QueryRowContext(ctx, `SELECT now()`).Scan(&now); err != nil {
		return nil, fmt.Errorf("reconcile locks: server time: %w", err)
	}

	// Snapshot and lock every in-scope active row. All time comparisons use the single server
	// timestamp above.
	selectQ := fmt.Sprintf(`SELECT lock_id, locked, locked_by, epoch, lease_expires_at FROM %s
		WHERE locked = true`, s.tableName)
	var args []any
	if len(params.LockIDs) > 0 {
		selectQ += fmt.Sprintf(` AND lock_id IN (%s)`, pgPlaceholders(len(params.LockIDs), 1))
		for _, id := range params.LockIDs {
			args = append(args, id)
		}
	}
	selectQ += ` ORDER BY lock_id FOR UPDATE`

	rows, err := tx.QueryContext(ctx, selectQ, args...)
	if err != nil {
		return nil, fmt.Errorf("reconcile locks: snapshot: %w", err)
	}
	var active []scannedLock
	for rows.Next() {
		var r scannedLock
		if err := rows.Scan(&r.LockID, &r.Locked, &r.LockedBy, &r.Epoch, &r.LeaseExpiresAt); err != nil {
			return nil, multierr.Append(fmt.Errorf("reconcile locks: scan: %w", err), rows.Close())
		}
		active = append(active, r)
	}
	if err := rows.Err(); err != nil {
		return nil, multierr.Append(fmt.Errorf("reconcile locks: iterate: %w", err), rows.Close())
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("reconcile locks: close snapshot: %w", err)
	}

	alive, expired, ambiguous := classifyLocks(active, now, params.ExpiryGrace)
	if len(ambiguous) > 0 {
		// All-or-nothing: leave every lock untouched, including the clearly expired ones.
		return nil, &AmbiguousStateError{Locks: ambiguous}
	}

	takenOver := make([]TakeoverRecord, 0, len(expired))
	for _, row := range expired {
		updateQ := fmt.Sprintf(`UPDATE %s SET
			locked = true,
			locked_at = $2::timestamptz,
			locked_by = $3,
			lease_expires_at = $2::timestamptz + $4::interval,
			updated_at = $2::timestamptz,
			epoch = epoch + 1
		WHERE lock_id = $1 AND epoch = $5
		RETURNING epoch`, s.tableName)

		var newEpoch int64
		if err := tx.QueryRowContext(ctx, updateQ,
			row.LockID,
			now,
			params.NewOwner,
			formatDurationAsInterval(params.LeaseDuration),
			row.Epoch.Int64,
		).Scan(&newEpoch); err != nil {
			return nil, fmt.Errorf("reconcile locks: take over lock %d: %w", row.LockID, err)
		}

		ev := ledgerEvent{
			LockID:         row.LockID,
			Epoch:          row.Epoch.Int64,
			EventType:      EventTakeover,
			OwnerFrom:      row.LockedBy,
			OwnerTo:        nullString(params.NewOwner),
			Reason:         ReasonLeaseExpired,
			LeaseExpiresAt: row.LeaseExpiresAt,
			RunID:          nullString(params.RunID),
		}
		if err := s.insertLedgerEvent(ctx, tx, ev); err != nil {
			return nil, fmt.Errorf("reconcile locks: record takeover for %d: %w", row.LockID, err)
		}
		takenOver = append(takenOver, newTakeoverRecord(row, params.NewOwner, newEpoch, now))
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("reconcile locks: commit: %w", err)
	}
	return &ReconcileLocksResult{
		ServerTime: now,
		TakenOver:  takenOver,
		Alive:      alive,
	}, nil
}

// selectLockForUpdate reads a single lock row under a row lock.
func (s *postgresStore) selectLockForUpdate(ctx context.Context, tx *sql.Tx, lockID int64) (scannedLock, error) {
	q := fmt.Sprintf(`SELECT lock_id, locked, locked_by, epoch, lease_expires_at FROM %s
		WHERE lock_id = $1 FOR UPDATE`, s.tableName)
	var r scannedLock
	err := tx.QueryRowContext(ctx, q, lockID).Scan(&r.LockID, &r.Locked, &r.LockedBy, &r.Epoch, &r.LeaseExpiresAt)
	return r, err
}

// detectFencing reports a LockLostError when the lock row exists, is locked, and no longer belongs
// to the claiming instance (or generation). It must be called inside the caller's transaction.
func (s *postgresStore) detectFencing(
	ctx context.Context,
	tx *sql.Tx,
	lockID int64,
	claimedBy string,
	claimedEpoch int64,
) (*LockLostError, error) {
	var (
		locked bool
		owner  sql.NullString
		epoch  sql.NullInt64
	)
	q := fmt.Sprintf(`SELECT locked, locked_by, epoch FROM %s WHERE lock_id = $1`, s.tableName)
	err := tx.QueryRowContext(ctx, q, lockID).Scan(&locked, &owner, &epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, nil
	}
	if owner.Valid && owner.String == claimedBy && epoch.Int64 == claimedEpoch {
		// Row still appears ours: not a fencing situation, an unexpected no-op update.
		return nil, nil
	}
	return &LockLostError{
		LockID:       lockID,
		ClaimedBy:    claimedBy,
		ClaimedEpoch: claimedEpoch,
		CurrentOwner: owner.String,
		CurrentEpoch: epoch.Int64,
	}, nil
}

type pgExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (s *postgresStore) insertLedgerEvent(ctx context.Context, e pgExecer, ev ledgerEvent) error {
	q := fmt.Sprintf(`INSERT INTO %s
		(lock_id, epoch, event_type, owner_from, owner_to, reason, lease_expires_at, run_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, s.ledger)
	if _, err := e.ExecContext(ctx, q,
		ev.LockID,
		ev.Epoch,
		ev.EventType,
		ev.OwnerFrom,
		ev.OwnerTo,
		ev.Reason,
		ev.LeaseExpiresAt,
		ev.RunID,
	); err != nil {
		return fmt.Errorf("insert ledger event %q: %w", ev.EventType, err)
	}
	return nil
}

// pgPlaceholders builds a Postgres placeholder list like "$1,$2,$3" starting at start.
func pgPlaceholders(n, start int) string {
	parts := make([]string, n)
	for i := range n {
		parts[i] = fmt.Sprintf("$%d", start+i)
	}
	return strings.Join(parts, ",")
}

// formatDurationAsInterval converts a time.Duration to PostgreSQL interval format
func formatDurationAsInterval(d time.Duration) string {
	return fmt.Sprintf("%d seconds", int(d.Seconds()))
}

func parseTableIdentifier(name string) (schema, table string) {
	schema, table, found := strings.Cut(name, ".")
	if !found {
		return "", name
	}
	return schema, table
}
