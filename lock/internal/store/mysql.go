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

// NewMySQL creates a new MySQL-based [LockStore].
func NewMySQL(lockTableName, ledgerTableName string) (LockStore, error) {
	if lockTableName == "" {
		return nil, errors.New("lock table name must not be empty")
	}
	if ledgerTableName == "" {
		return nil, errors.New("ledger table name must not be empty")
	}
	return &mysqlStore{
		tableName: lockTableName,
		ledger:    ledgerTableName,
	}, nil
}

var _ LockStore = (*mysqlStore)(nil)

type mysqlStore struct {
	tableName string
	ledger    string
}

func (s *mysqlStore) TableExists(
	ctx context.Context,
	db *sql.DB,
) (bool, error) {
	return s.tableExistsNamed(ctx, db, s.tableName)
}

func (s *mysqlStore) tableExistsNamed(
	ctx context.Context,
	db *sql.DB,
	name string,
) (bool, error) {
	schemaName, tableName := parseTableIdentifier(name)

	var query string
	var args []any
	if schemaName != "" {
		query = `SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ? )`
		args = []any{schemaName, tableName}
	} else {
		query = `SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ? )`
		args = []any{tableName}
	}

	var exists bool
	if err := db.QueryRowContext(ctx, query, args...).Scan(
		&exists,
	); err != nil {
		return false, fmt.Errorf("table exists: %w", err)
	}
	return exists, nil
}

func (s *mysqlStore) CreateLockTable(
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
		lock_id BIGINT NOT NULL PRIMARY KEY,
		locked BOOLEAN NOT NULL DEFAULT 0,
		locked_at DATETIME(6) NULL,
		locked_by TEXT NULL,
		lease_expires_at DATETIME(6) NULL,
		updated_at DATETIME(6) NULL,
		epoch BIGINT NOT NULL DEFAULT 1
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
func (s *mysqlStore) EnsureSchema(ctx context.Context, db *sql.DB) error {
	if err := s.CreateLockTable(ctx, db); err != nil {
		return err
	}
	if err := s.createLedgerTable(ctx, db); err != nil {
		return err
	}

	// MySQL (unlike MariaDB) does not support ADD COLUMN IF NOT EXISTS: probe information_schema.
	exists, err := s.columnExists(ctx, db, s.tableName, "epoch")
	if err != nil {
		return fmt.Errorf("ensure epoch column on %q: %w", s.tableName, err)
	}
	if !exists {
		alterQ := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN epoch BIGINT NOT NULL DEFAULT 1`, s.tableName)
		if _, err := db.ExecContext(ctx, alterQ); err != nil {
			// A concurrent process may have added the column first.
			if recheck, _ := s.columnExists(ctx, db, s.tableName, "epoch"); recheck {
				return nil
			}
			return fmt.Errorf("ensure epoch column on %q: %w", s.tableName, err)
		}
	}
	return nil
}

func (s *mysqlStore) createLedgerTable(ctx context.Context, db *sql.DB) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		event_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		lock_id BIGINT NOT NULL,
		epoch BIGINT NOT NULL,
		event_type VARCHAR(32) NOT NULL,
		owner_from TEXT NULL,
		owner_to TEXT NULL,
		reason VARCHAR(64) NOT NULL,
		lease_expires_at DATETIME(6) NULL,
		run_id TEXT NULL,
		created_at DATETIME(6) NOT NULL DEFAULT now(6),
		KEY %s (lock_id, epoch)
	)`, s.ledger, ledgerIndexName(s.ledger))
	if _, err := db.ExecContext(ctx, query); err != nil {
		// Concurrent CREATE TABLE IF NOT EXISTS from another process; re-check existence.
		if exists, _ := s.tableExistsNamed(ctx, db, s.ledger); exists {
			return nil
		}
		return fmt.Errorf("create ledger table %q: %w", s.ledger, err)
	}
	return nil
}

func (s *mysqlStore) columnExists(ctx context.Context, db *sql.DB, name, columnName string) (bool, error) {
	schemaName, tableName := parseTableIdentifier(name)
	q := `SELECT COUNT(1) FROM information_schema.COLUMNS
		WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE()) AND table_name = ? AND column_name = ?`
	var count int
	if err := db.QueryRowContext(ctx, q, schemaName, tableName, columnName).Scan(&count); err != nil {
		return false, fmt.Errorf("column exists: %w", err)
	}
	return count > 0, nil
}

func (s *mysqlStore) AcquireLock(
	ctx context.Context,
	db *sql.DB,
	params AcquireLockParams,
) (_ *AcquireLockResult, retErr error) {
	leaseSeconds := int(params.LeaseDuration.Seconds())

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
	if err := tx.QueryRowContext(ctx, `SELECT now(6)`).Scan(&now); err != nil {
		return nil, fmt.Errorf("acquire lock %d: server time: %w", params.LockID, err)
	}

	// First, try to insert a brand-new row at generation 1. If a row already exists for this
	// lock_id, INSERT IGNORE silently no-ops and the SELECT ... FOR UPDATE below decides.
	insertQ := fmt.Sprintf(`INSERT IGNORE INTO %s (lock_id, locked, locked_at, locked_by, lease_expires_at, updated_at, epoch)
		VALUES (?, true, now(6), ?, DATE_ADD(now(6), INTERVAL ? SECOND), now(6), 1)`, s.tableName)
	if _, err := tx.ExecContext(ctx, insertQ, params.LockID, params.LockedBy, leaseSeconds); err != nil {
		return nil, fmt.Errorf("acquire lock %d: %w", params.LockID, err)
	}

	row, err := s.selectLockForUpdate(ctx, tx, params.LockID)
	if err != nil {
		return nil, fmt.Errorf("acquire lock %d: %w", params.LockID, err)
	}

	// Reentrant acquisition: the same instance still holds a valid lease at the same generation.
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
		locked_at = ?,
		locked_by = ?,
		lease_expires_at = DATE_ADD(?, INTERVAL ? SECOND),
		updated_at = ?,
		epoch = epoch + 1
	WHERE lock_id = ? AND epoch = ?`, s.tableName)
	res, err := tx.ExecContext(ctx, updateQ,
		now, params.LockedBy, now, leaseSeconds, now,
		params.LockID, row.Epoch.Int64,
	)
	if err != nil {
		return nil, fmt.Errorf("acquire lock %d: take over: %w", params.LockID, err)
	}
	if affected, err := res.RowsAffected(); err != nil {
		return nil, fmt.Errorf("acquire lock %d: rows affected: %w", params.LockID, err)
	} else if affected != 1 {
		return nil, fmt.Errorf("acquire lock %d: concurrent ownership change", params.LockID)
	}

	// Read the resulting row back (MySQL has no RETURNING clause).
	selectQ := fmt.Sprintf(`SELECT locked_by, lease_expires_at, epoch FROM %s WHERE lock_id = ?`, s.tableName)
	var returnedLockedBy string
	var leaseExpiresAt time.Time
	var newEpoch int64
	if err := tx.QueryRowContext(ctx, selectQ, params.LockID).Scan(
		&returnedLockedBy, &leaseExpiresAt, &newEpoch,
	); err != nil {
		return nil, fmt.Errorf("acquire lock %d: %w", params.LockID, err)
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

	if returnedLockedBy != params.LockedBy {
		return nil, fmt.Errorf("acquire lock %d: acquired by %s instead of %s", params.LockID, returnedLockedBy, params.LockedBy)
	}
	return &AcquireLockResult{
		LockedBy:       returnedLockedBy,
		LeaseExpiresAt: leaseExpiresAt,
		Epoch:          newEpoch,
	}, nil
}

func (s *mysqlStore) ReleaseLock(
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

	// Release only while we still own the exact generation we acquired.
	updateQ := fmt.Sprintf(`UPDATE %s SET
		locked = false,
		locked_at = NULL,
		locked_by = NULL,
		lease_expires_at = NULL,
		updated_at = now(6)
	WHERE lock_id = ? AND locked_by = ? AND locked = true AND epoch = ?`, s.tableName)
	res, err := tx.ExecContext(ctx, updateQ, params.LockID, params.LockedBy, params.Epoch)
	if err != nil {
		return nil, fmt.Errorf("release lock %d: %w", params.LockID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("release lock %d: rows affected: %w", params.LockID, err)
	}
	if affected == 1 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("release lock %d: commit: %w", params.LockID, err)
		}
		committed = true
		return &ReleaseLockResult{LockID: params.LockID}, nil
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

func (s *mysqlStore) UpdateLease(
	ctx context.Context,
	db *sql.DB,
	params UpdateLeaseParams,
) (_ *UpdateLeaseResult, retErr error) {
	leaseSeconds := int(params.LeaseDuration.Seconds())

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
	updateQ := fmt.Sprintf(`UPDATE %s SET
		lease_expires_at = DATE_ADD(now(6), INTERVAL ? SECOND),
		updated_at = now(6)
	WHERE lock_id = ? AND locked_by = ? AND locked = true AND epoch = ?`, s.tableName)
	res, err := tx.ExecContext(ctx, updateQ, leaseSeconds, params.LockID, params.LockedBy, params.Epoch)
	if err != nil {
		return nil, fmt.Errorf("failed to update lease for lock %d: %w", params.LockID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to update lease for lock %d: rows affected: %w", params.LockID, err)
	}
	if affected == 1 {
		selectQ := fmt.Sprintf(`SELECT lease_expires_at, epoch FROM %s WHERE lock_id = ?`, s.tableName)
		var leaseExpiresAt time.Time
		var epoch int64
		if err := tx.QueryRowContext(ctx, selectQ, params.LockID).Scan(&leaseExpiresAt, &epoch); err != nil {
			return nil, fmt.Errorf("failed to update lease for lock %d: %w", params.LockID, err)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to update lease for lock %d: commit: %w", params.LockID, err)
		}
		committed = true
		return &UpdateLeaseResult{LeaseExpiresAt: leaseExpiresAt, Epoch: epoch}, nil
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

func (s *mysqlStore) CheckLockStatus(
	ctx context.Context,
	db *sql.DB,
	lockID int64,
) (*LockStatus, error) {
	query := fmt.Sprintf(`SELECT locked, locked_by, lease_expires_at, updated_at, epoch FROM %s WHERE lock_id = ?`, s.tableName)
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

func (s *mysqlStore) CleanupStaleLocks(ctx context.Context, db *sql.DB) (_ []int64, retErr error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("cleanup stale locks: begin tx: %w", err)
	}
	defer func() {
		if retErr != nil {
			retErr = multierr.Append(retErr, tx.Rollback())
		}
	}()

	// MySQL has no RETURNING on UPDATE; lock rows for update, collect their IDs, then clear them.
	// Identity (locked_by) and epoch are deliberately preserved as a tombstone, so a later takeover
	// can credit the previous owner in the ledger.
	selectQ := fmt.Sprintf(`SELECT lock_id FROM %s WHERE locked = true AND lease_expires_at < now(6) FOR UPDATE`, s.tableName)
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
		placeholders := myPlaceholders(len(stale))
		updateQ := fmt.Sprintf(`UPDATE %s SET
			locked = false,
			locked_at = NULL,
			lease_expires_at = NULL,
			updated_at = now(6)
		WHERE lock_id IN (%s)`, s.tableName, placeholders)
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

func (s *mysqlStore) ReconcileLocks(
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
	leaseSeconds := int(params.LeaseDuration.Seconds())

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
	if err := tx.QueryRowContext(ctx, `SELECT now(6)`).Scan(&now); err != nil {
		return nil, fmt.Errorf("reconcile locks: server time: %w", err)
	}

	// Snapshot and lock every in-scope active row. All time comparisons use the single server
	// timestamp above.
	selectQ := fmt.Sprintf(`SELECT lock_id, locked, locked_by, epoch, lease_expires_at FROM %s
		WHERE locked = true`, s.tableName)
	var args []any
	if len(params.LockIDs) > 0 {
		selectQ += ` AND lock_id IN (` + myPlaceholders(len(params.LockIDs)) + `)`
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
		newEpoch := row.Epoch.Int64 + 1
		updateQ := fmt.Sprintf(`UPDATE %s SET
			locked = true,
			locked_at = ?,
			locked_by = ?,
			lease_expires_at = DATE_ADD(?, INTERVAL ? SECOND),
			updated_at = ?,
			epoch = epoch + 1
		WHERE lock_id = ? AND epoch = ?`, s.tableName)
		res, err := tx.ExecContext(ctx, updateQ,
			now, params.NewOwner, now, leaseSeconds, now,
			row.LockID, row.Epoch.Int64,
		)
		if err != nil {
			return nil, fmt.Errorf("reconcile locks: take over lock %d: %w", row.LockID, err)
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, fmt.Errorf("reconcile locks: take over lock %d: rows affected: %w", row.LockID, err)
		} else if affected != 1 {
			return nil, fmt.Errorf("reconcile locks: take over lock %d: concurrent ownership change", row.LockID)
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
func (s *mysqlStore) selectLockForUpdate(ctx context.Context, tx *sql.Tx, lockID int64) (scannedLock, error) {
	q := fmt.Sprintf(`SELECT lock_id, locked, locked_by, epoch, lease_expires_at FROM %s
		WHERE lock_id = ? FOR UPDATE`, s.tableName)
	var r scannedLock
	err := tx.QueryRowContext(ctx, q, lockID).Scan(&r.LockID, &r.Locked, &r.LockedBy, &r.Epoch, &r.LeaseExpiresAt)
	return r, err
}

// detectFencing reports a LockLostError when the lock row exists, is locked, and no longer belongs
// to the claiming instance (or generation). It must be called inside the caller's transaction.
func (s *mysqlStore) detectFencing(
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
	q := fmt.Sprintf(`SELECT locked, locked_by, epoch FROM %s WHERE lock_id = ?`, s.tableName)
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

type myExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (s *mysqlStore) insertLedgerEvent(ctx context.Context, e myExecer, ev ledgerEvent) error {
	q := fmt.Sprintf(`INSERT INTO %s
		(lock_id, epoch, event_type, owner_from, owner_to, reason, lease_expires_at, run_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, s.ledger)
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

// myPlaceholders builds a MySQL placeholder list like "?,?,?".
func myPlaceholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}
