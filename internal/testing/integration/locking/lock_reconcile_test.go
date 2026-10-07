package locking_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pressly/goose/v3/internal/testing/testdb"
	"github.com/pressly/goose/v3/lock"
	"github.com/stretchr/testify/require"
)

var kitCounter int64

// lockKit is a dialect-neutral playground with a unique lock/ledger table pair per scenario.
type lockKit struct {
	t              *testing.T
	db             *sql.DB
	isPG           bool
	table          string
	ledger         string
	makeLocker     func(...lock.TableLockerOption) (lock.Locker, error)
	makeReconciler func(...lock.ReconcilerOption) (lock.Reconciler, error)
}

func newLockKit(
	t *testing.T,
	db *sql.DB,
	isPG bool,
	makeLocker func(...lock.TableLockerOption) (lock.Locker, error),
	makeReconciler func(...lock.ReconcilerOption) (lock.Reconciler, error),
) *lockKit {
	t.Helper()
	n := atomic.AddInt64(&kitCounter, 1)
	k := &lockKit{
		t:              t,
		db:             db,
		isPG:           isPG,
		table:          fmt.Sprintf("goose_lock_rc_%d", n),
		ledger:         fmt.Sprintf("goose_lock_ledger_rc_%d", n),
		makeLocker:     makeLocker,
		makeReconciler: makeReconciler,
	}
	// Create the schema by running an empty reconciliation.
	r := k.Reconciler()
	_, err := r.Reconcile(context.Background(), db)
	require.NoError(t, err)
	return k
}

func (k *lockKit) ph(n int) string {
	if k.isPG {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

func (k *lockKit) agoClause(d time.Duration) string {
	secs := int(d.Seconds())
	if k.isPG {
		return fmt.Sprintf("now() - interval '%d second'", secs)
	}
	return fmt.Sprintf("now(6) - INTERVAL %d SECOND", secs)
}

func (k *lockKit) futureClause(d time.Duration) string {
	secs := int(d.Seconds())
	if k.isPG {
		return fmt.Sprintf("now() + interval '%d second'", secs)
	}
	return fmt.Sprintf("now(6) + INTERVAL %d SECOND", secs)
}

func (k *lockKit) nowClause() string {
	if k.isPG {
		return "now()"
	}
	return "now(6)"
}

// Locker builds a locker bound to this kit's tables.
func (k *lockKit) Locker(opts ...lock.TableLockerOption) lock.Locker {
	all := append([]lock.TableLockerOption{
		lock.WithTableName(k.table),
		lock.WithLedgerTableName(k.ledger),
	}, opts...)
	l, err := k.makeLocker(all...)
	require.NoError(k.t, err)
	return l
}

// Reconciler builds a reconciler bound to this kit's tables.
func (k *lockKit) Reconciler(opts ...lock.ReconcilerOption) lock.Reconciler {
	all := append([]lock.ReconcilerOption{
		lock.WithReconcileTableName(k.table),
		lock.WithReconcileLedgerTableName(k.ledger),
	}, opts...)
	r, err := k.makeReconciler(all...)
	require.NoError(k.t, err)
	return r
}

// InsertLocked writes an actively locked row whose lease expired ago.
func (k *lockKit) InsertLocked(id int64, owner string, epoch int64, ago time.Duration) {
	q := fmt.Sprintf(`INSERT INTO %s (lock_id, locked, locked_at, locked_by, lease_expires_at, updated_at, epoch)
		VALUES (%s, true, %s, %s, %s, %s, %s)`,
		k.table, k.ph(1), k.agoClause(ago), k.ph(2), k.agoClause(ago), k.agoClause(ago), k.ph(3))
	_, err := k.db.Exec(q, id, owner, epoch)
	require.NoError(k.t, err)
}

// InsertAmbiguous writes a locked row missing owner and lease: reconciliation cannot classify it.
func (k *lockKit) InsertAmbiguous(id int64) {
	q := fmt.Sprintf(`INSERT INTO %s (lock_id, locked, locked_at, locked_by, lease_expires_at, updated_at, epoch)
		VALUES (%s, true, %s, NULL, NULL, %s, 1)`, k.table, k.ph(1), k.nowClause(), k.nowClause())
	_, err := k.db.Exec(q, id)
	require.NoError(k.t, err)
}

// SimulateTakeover flips the owner/generation directly, as a prior reconciliation would have done.
func (k *lockKit) SimulateTakeover(id int64, oldOwner, newOwner string, oldEpoch, newEpoch int64) {
	// MySQL binds placeholders positionally; Postgres by $n number. The SET clause appears before
	// the WHERE clause, so the argument order differs between dialects.
	var updateQ string
	var updateArgs []any
	if k.isPG {
		updateQ = fmt.Sprintf(`UPDATE %s SET locked = true, locked_by = %s, epoch = %s,
			lease_expires_at = %s, updated_at = %s WHERE lock_id = %s`,
			k.table, k.ph(2), k.ph(3), k.futureClause(30*time.Second), k.nowClause(), k.ph(1))
		updateArgs = []any{id, newOwner, newEpoch}
	} else {
		updateQ = fmt.Sprintf(`UPDATE %s SET locked = true, locked_by = %s, epoch = %s,
			lease_expires_at = %s, updated_at = %s WHERE lock_id = %s`,
			k.table, k.ph(1), k.ph(2), k.futureClause(30*time.Second), k.nowClause(), k.ph(3))
		updateArgs = []any{newOwner, newEpoch, id}
	}
	_, err := k.db.Exec(updateQ, updateArgs...)
	require.NoError(k.t, err)

	insertQ := fmt.Sprintf(`INSERT INTO %s (lock_id, epoch, event_type, owner_from, owner_to, reason, lease_expires_at, run_id)
		VALUES (%s, %s, 'takeover', %s, %s, 'lease_expired', NULL, NULL)`,
		k.ledger, k.ph(1), k.ph(2), k.ph(3), k.ph(4))
	_, err = k.db.Exec(insertQ, id, oldEpoch, oldOwner, newOwner)
	require.NoError(k.t, err)
}

type lockRow struct {
	locked   bool
	owner    string
	epoch    int64
	hasLease bool
	lease    time.Time
}

func (k *lockKit) GetLockRow(id int64) lockRow {
	q := fmt.Sprintf(`SELECT locked, locked_by, epoch, lease_expires_at FROM %s WHERE lock_id = %s`, k.table, k.ph(1))
	var (
		r lockRow

		owner sql.NullString
		lease sql.NullTime
	)
	err := k.db.QueryRow(q, id).Scan(&r.locked, &owner, &r.epoch, &lease)
	require.NoError(k.t, err)
	r.owner = owner.String
	r.hasLease = lease.Valid
	r.lease = lease.Time
	return r
}

type ledgerEntry struct {
	epoch     int64
	eventType string
	ownerFrom string
	ownerTo   string
	reason    string
	runID     string
}

func (k *lockKit) GetLedger(id int64) []ledgerEntry {
	q := fmt.Sprintf(`SELECT epoch, event_type, owner_from, owner_to, reason, run_id FROM %s
		WHERE lock_id = %s ORDER BY event_id`, k.ledger, k.ph(1))
	rows, err := k.db.Query(q, id)
	require.NoError(k.t, err)
	defer rows.Close()

	var out []ledgerEntry
	for rows.Next() {
		var (
			e               ledgerEntry
			from, to, runID sql.NullString
		)
		require.NoError(k.t, rows.Scan(&e.epoch, &e.eventType, &from, &to, &e.reason, &runID))
		e.ownerFrom, e.ownerTo, e.runID = from.String, to.String, runID.String
		out = append(out, e)
	}
	require.NoError(k.t, rows.Err())
	return out
}

func (k *lockKit) CountLedger(id int64, eventType string) int {
	q := fmt.Sprintf(`SELECT count(*) FROM %s WHERE lock_id = %s AND event_type = %s`,
		k.ledger, k.ph(1), k.ph(2))
	var n int
	require.NoError(k.t, k.db.QueryRow(q, id, eventType).Scan(&n))
	return n
}

func findAlive(report *lock.ReconcileReport, lockID int64) (lock.LockSnapshot, bool) {
	for _, l := range report.Alive {
		if l.LockID == lockID {
			return l, true
		}
	}
	return lock.LockSnapshot{}, false
}

func runReconcileScenarios(
	t *testing.T,
	newKit func(t *testing.T) *lockKit,
) {
	ctx := context.Background()

	t.Run("takeover_with_ledger", func(t *testing.T) {
		k := newKit(t)
		k.InsertLocked(1001, "dead-instance", 1, 60*time.Second)

		r := k.Reconciler(
			lock.WithReconcileLockIDs(1001),
			lock.WithReconcileLeaseDuration(30*time.Second),
		)
		report, err := r.Reconcile(ctx, k.db)
		require.NoError(t, err)

		require.Len(t, report.TakenOver, 1)
		rec := report.TakenOver[0]
		require.Equal(t, int64(1001), rec.LockID)
		require.Equal(t, "dead-instance", rec.OwnerFrom)
		require.Equal(t, r.InstanceID(), rec.OwnerTo)
		require.Equal(t, int64(1), rec.PreviousEpoch)
		require.Equal(t, int64(2), rec.NewEpoch)
		require.Equal(t, "lease_expired", rec.Reason)

		row := k.GetLockRow(1001)
		require.True(t, row.locked)
		require.Equal(t, r.InstanceID(), row.owner)
		require.Equal(t, int64(2), row.epoch)
		require.True(t, row.hasLease)
		require.True(t, row.lease.After(time.Now().Add(20*time.Second)))

		entries := k.GetLedger(1001)
		require.Len(t, entries, 1)
		require.Equal(t, ledgerEntry{
			epoch:     1,
			eventType: "takeover",
			ownerFrom: "dead-instance",
			ownerTo:   r.InstanceID(),
			reason:    "lease_expired",
			runID:     report.RunID,
		}, entries[0])
	})

	t.Run("alive_locks_untouched", func(t *testing.T) {
		k := newKit(t)

		// A healthy, actively heartbeating lock must never be touched.
		lost := make(chan lock.LockLostInfo, 1)
		lockerA := k.Locker(
			lock.WithTableLockID(1002),
			lock.WithInstanceID("alive-instance-A"),
			lock.WithTableLeaseDuration(30*time.Second),
			lock.WithTableHeartbeatInterval(100*time.Millisecond),
			lock.WithLockLostHandler(func(_ context.Context, info lock.LockLostInfo) { lost <- info }),
		)
		require.NoError(t, lockerA.Lock(ctx, k.db))
		t.Cleanup(func() { _ = lockerA.Unlock(ctx, k.db) })

		before := k.GetLockRow(1002)
		require.Equal(t, "alive-instance-A", before.owner)

		// A second, expired lock in the same table must be taken over.
		k.InsertLocked(1003, "dead-instance", 1, 60*time.Second)

		r := k.Reconciler(lock.WithReconcileLeaseDuration(60 * time.Second))
		report, err := r.Reconcile(ctx, k.db)
		require.NoError(t, err)

		require.Len(t, report.TakenOver, 1)
		require.Equal(t, int64(1003), report.TakenOver[0].LockID)

		alive, ok := findAlive(report, 1002)
		require.True(t, ok)
		require.Equal(t, "alive-instance-A", alive.LockedBy)
		require.Equal(t, int64(1), alive.Epoch)

		after := k.GetLockRow(1002)
		require.Equal(t, before.owner, after.owner)
		require.Equal(t, before.epoch, after.epoch)
		// The alive row must not have been rewritten at all.
		require.True(t, before.lease.Equal(after.lease), "alive lease changed: before=%s after=%s", before.lease, after.lease)
		require.Zero(t, k.CountLedger(1002, "takeover"))
		require.Zero(t, k.CountLedger(1002, "renewal_denied"))

		// And the healthy locker must not have been fenced.
		select {
		case info := <-lost:
			t.Fatalf("alive locker was fenced: %+v", info)
		case <-time.After(150 * time.Millisecond):
		}
	})

	t.Run("ambiguous_aborts_entire_run", func(t *testing.T) {
		k := newKit(t)
		k.InsertLocked(1004, "dead-instance", 1, 60*time.Second) // clearly expired
		k.InsertAmbiguous(1005)                                  // cannot classify

		expiredBefore := k.GetLockRow(1004)

		r := k.Reconciler(lock.WithReconcileLeaseDuration(60 * time.Second))
		_, err := r.Reconcile(ctx, k.db)
		require.Error(t, err)
		require.ErrorIs(t, err, lock.ErrAmbiguousLockState)

		var ambErr *lock.AmbiguousStateError
		require.ErrorAs(t, err, &ambErr)
		var found bool
		for _, l := range ambErr.Locks {
			if l.LockID == 1005 {
				found = true
				require.NotEmpty(t, l.Reason)
			}
		}
		require.True(t, found, "ambiguous lock 1005 not reported: %+v", ambErr.Locks)

		// All-or-nothing: the clearly expired lock is also left untouched.
		expiredAfter := k.GetLockRow(1004)
		require.Equal(t, expiredBefore.owner, expiredAfter.owner)
		require.Equal(t, expiredBefore.epoch, expiredAfter.epoch)
		require.True(t, expiredBefore.lease.Equal(expiredAfter.lease))
		require.True(t, expiredAfter.locked)

		// Nothing was written to the ledger.
		require.Empty(t, k.GetLedger(1004))
		require.Empty(t, k.GetLedger(1005))
	})

	t.Run("stale_renewal_rejected_and_recorded", func(t *testing.T) {
		k := newKit(t)

		lost := make(chan lock.LockLostInfo, 1)
		lockerA := k.Locker(
			lock.WithTableLockID(1006),
			lock.WithInstanceID("old-instance-A"),
			lock.WithTableLeaseDuration(30*time.Second),
			lock.WithTableHeartbeatInterval(50*time.Millisecond),
			lock.WithLockLostHandler(func(_ context.Context, info lock.LockLostInfo) {
				select {
				case lost <- info:
				default:
				}
			}),
		)
		require.NoError(t, lockerA.Lock(ctx, k.db))

		// Simulate reconciliation taking the lock away from A: generation 1 -> 2, new owner.
		k.SimulateTakeover(1006, "old-instance-A", "new-owner-B", 1, 2)

		var info lock.LockLostInfo
		select {
		case info = <-lost:
		case <-time.After(3 * time.Second):
			t.Fatal("lock lost handler was not invoked after takeover")
		}
		require.Equal(t, int64(1006), info.LockID)
		require.Equal(t, "old-instance-A", info.InstanceID)
		require.Equal(t, int64(1), info.Epoch)
		require.Equal(t, "new-owner-B", info.CurrentOwner)
		require.Equal(t, int64(2), info.CurrentEpoch)
		require.Equal(t, "fenced", info.Reason)

		// The heartbeat loop must stop: no matter how long we wait, exactly one denial is logged.
		time.Sleep(250 * time.Millisecond)
		require.Equal(t, 1, k.CountLedger(1006, "renewal_denied"))

		entries := k.GetLedger(1006)
		var denied *ledgerEntry
		for i := range entries {
			if entries[i].eventType == "renewal_denied" {
				denied = &entries[i]
			}
		}
		require.NotNil(t, denied)
		require.Equal(t, int64(1), denied.epoch)
		require.Equal(t, "old-instance-A", denied.ownerFrom)
		require.Equal(t, "new-owner-B", denied.ownerTo)
		require.Equal(t, "fenced", denied.reason)

		// A stale Unlock must not disturb the new owner's lock.
		err := lockerA.Unlock(ctx, k.db)
		require.Error(t, err)
		require.ErrorIs(t, err, lock.ErrLockLost)

		row := k.GetLockRow(1006)
		require.True(t, row.locked)
		require.Equal(t, "new-owner-B", row.owner)
		require.Equal(t, int64(2), row.epoch)
	})

	t.Run("reconciler_then_same_identity_locker", func(t *testing.T) {
		k := newKit(t)
		k.InsertLocked(1007, "dead-instance", 1, 60*time.Second)

		r := k.Reconciler(
			lock.WithReconcileLockIDs(1007),
			lock.WithReconcileLeaseDuration(60*time.Second),
		)
		report, err := r.Reconcile(ctx, k.db)
		require.NoError(t, err)
		require.Len(t, report.TakenOver, 1)

		// A locker pinned to the reconciler identity adopts the lock reentrantly: no new
		// generation, no new ledger entry.
		locker := k.Locker(
			lock.WithTableLockID(1007),
			lock.WithInstanceID(r.InstanceID()),
			lock.WithTableLeaseDuration(60*time.Second),
			lock.WithTableHeartbeatInterval(1*time.Second),
		)
		require.NoError(t, locker.Lock(ctx, k.db))

		row := k.GetLockRow(1007)
		require.True(t, row.locked)
		require.Equal(t, r.InstanceID(), row.owner)
		require.Equal(t, int64(2), row.epoch)
		require.Len(t, k.GetLedger(1007), 1)

		require.NoError(t, locker.Unlock(ctx, k.db))
		row = k.GetLockRow(1007)
		require.False(t, row.locked)
	})

	t.Run("rerun_converges", func(t *testing.T) {
		k := newKit(t)
		k.InsertLocked(1008, "dead-instance", 1, 60*time.Second)

		r := k.Reconciler(
			lock.WithReconcileLockIDs(1008),
			lock.WithReconcileLeaseDuration(600*time.Second),
		)
		report1, err := r.Reconcile(ctx, k.db)
		require.NoError(t, err)
		require.Len(t, report1.TakenOver, 1)
		leaseAfterFirst := k.GetLockRow(1008).lease

		// Interrupted rerun with the same reconciler: nothing more to take over, no duplicate
		// ledger rows, no row rewrites.
		report2, err := r.Reconcile(ctx, k.db)
		require.NoError(t, err)
		require.Empty(t, report2.TakenOver)
		alive, ok := findAlive(report2, 1008)
		require.True(t, ok)
		require.Equal(t, r.InstanceID(), alive.LockedBy)

		require.Len(t, k.GetLedger(1008), 1)
		leaseAfterSecond := k.GetLockRow(1008).lease
		require.True(t, leaseAfterFirst.Equal(leaseAfterSecond))

		// A third run converges identically.
		report3, err := r.Reconcile(ctx, k.db)
		require.NoError(t, err)
		require.Empty(t, report3.TakenOver)
		require.Len(t, k.GetLedger(1008), 1)
	})

	t.Run("expiry_grace", func(t *testing.T) {
		k := newKit(t)
		k.InsertLocked(1009, "dead-instance", 1, 5*time.Second)

		// Within the grace window the lock is treated as alive.
		r := k.Reconciler(
			lock.WithReconcileLockIDs(1009),
			lock.WithReconcileLeaseDuration(60*time.Second),
			lock.WithReconcileExpiryGrace(30*time.Second),
		)
		report, err := r.Reconcile(ctx, k.db)
		require.NoError(t, err)
		require.Empty(t, report.TakenOver)
		_, ok := findAlive(report, 1009)
		require.True(t, ok)
		require.Empty(t, k.GetLedger(1009))

		// A run with a grace shorter than the expiry age takes over.
		r2 := k.Reconciler(
			lock.WithReconcileLockIDs(1009),
			lock.WithReconcileLeaseDuration(60*time.Second),
			lock.WithReconcileExpiryGrace(1*time.Second),
		)
		report, err = r2.Reconcile(ctx, k.db)
		require.NoError(t, err)
		require.Len(t, report.TakenOver, 1)
		require.Len(t, k.GetLedger(1009), 1)
	})
}

func TestPostgresLockReconciliation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	db, cleanup, err := testdb.NewPostgres()
	require.NoError(t, err)
	defer cleanup()

	newKit := func(t *testing.T) *lockKit {
		return newLockKit(t, db, true,
			func(o ...lock.TableLockerOption) (lock.Locker, error) {
				return lock.NewPostgresTableLocker(o...)
			},
			func(o ...lock.ReconcilerOption) (lock.Reconciler, error) {
				return lock.NewPostgresTableLockReconciler(o...)
			},
		)
	}
	runReconcileScenarios(t, newKit)
}

func TestMySQLLockReconciliation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	db, cleanup, err := testdb.NewMariaDB()
	require.NoError(t, err)
	defer cleanup()

	newKit := func(t *testing.T) *lockKit {
		return newLockKit(t, db, false,
			func(o ...lock.TableLockerOption) (lock.Locker, error) {
				return lock.NewMySQLTableLocker(o...)
			},
			func(o ...lock.ReconcilerOption) (lock.Reconciler, error) {
				return lock.NewMySQLTableLockReconciler(o...)
			},
		)
	}
	runReconcileScenarios(t, newKit)
}
