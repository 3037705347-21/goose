package lock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTableLockerOptions(t *testing.T) {
	// Test that options are applied correctly
	locker, err := NewPostgresTableLocker(
		WithTableName("custom_locks"),
		WithTableLockID(999),
		WithTableLeaseDuration(10*time.Second),
		WithTableHeartbeatInterval(3*time.Second),
	)
	require.NoError(t, err)
	require.NotNil(t, locker)
	// Test invalid lease duration
	_, err = NewPostgresTableLocker(WithTableLeaseDuration(-1 * time.Second))
	require.Error(t, err)
	// Test invalid heartbeat interval
	_, err = NewPostgresTableLocker(WithTableHeartbeatInterval(0))
	require.Error(t, err)
	// Test empty table name
	_, err = NewPostgresTableLocker(WithTableName(""))
	require.Error(t, err)
	// Test invalid lock ID
	_, err = NewPostgresTableLocker(WithTableLockID(0))
	require.Error(t, err)
	// Test invalid lock timeout interval duration
	_, err = NewPostgresTableLocker(WithTableLockTimeout(0, 10))
	require.Error(t, err)
	// Test invalid lock timeout failure threshold
	_, err = NewPostgresTableLocker(WithTableLockTimeout(5*time.Second, 0))
	require.Error(t, err)
	// Test invalid unlock timeout interval duration
	_, err = NewPostgresTableLocker(WithTableUnlockTimeout(0, 10))
	require.Error(t, err)
	// Test invalid unlock timeout failure threshold
	_, err = NewPostgresTableLocker(WithTableUnlockTimeout(5*time.Second, 0))
	require.Error(t, err)
	// Test empty ledger table name
	_, err = NewPostgresTableLocker(WithLedgerTableName(""))
	require.Error(t, err)
	// Test empty instance ID
	_, err = NewPostgresTableLocker(WithInstanceID(""))
	require.Error(t, err)
	// Test nil lock lost handler
	_, err = NewPostgresTableLocker(WithLockLostHandler(nil))
	require.Error(t, err)
	// Test valid new options
	handler := func(context.Context, LockLostInfo) {}
	locker, err = NewPostgresTableLocker(
		WithLedgerTableName("custom_ledger"),
		WithInstanceID("pinned-instance"),
		WithLockLostHandler(handler),
	)
	require.NoError(t, err)
	require.NotNil(t, locker)
}

func TestMySQLTableLockerOptions(t *testing.T) {
	// Test that options are applied correctly
	locker, err := NewMySQLTableLocker(
		WithTableName("custom_locks"),
		WithTableLockID(999),
		WithTableLeaseDuration(10*time.Second),
		WithTableHeartbeatInterval(3*time.Second),
	)
	require.NoError(t, err)
	require.NotNil(t, locker)
	// Test invalid lease duration
	_, err = NewMySQLTableLocker(WithTableLeaseDuration(-1 * time.Second))
	require.Error(t, err)
	// Test invalid heartbeat interval
	_, err = NewMySQLTableLocker(WithTableHeartbeatInterval(0))
	require.Error(t, err)
	// Test empty table name
	_, err = NewMySQLTableLocker(WithTableName(""))
	require.Error(t, err)
	// Test invalid lock ID
	_, err = NewMySQLTableLocker(WithTableLockID(0))
	require.Error(t, err)
	// Test invalid lock timeout interval duration
	_, err = NewMySQLTableLocker(WithTableLockTimeout(0, 10))
	require.Error(t, err)
	// Test invalid lock timeout failure threshold
	_, err = NewMySQLTableLocker(WithTableLockTimeout(5*time.Second, 0))
	require.Error(t, err)
	// Test invalid unlock timeout interval duration
	_, err = NewMySQLTableLocker(WithTableUnlockTimeout(0, 10))
	require.Error(t, err)
	// Test invalid unlock timeout failure threshold
	_, err = NewMySQLTableLocker(WithTableUnlockTimeout(5*time.Second, 0))
	require.Error(t, err)
	// Test empty ledger table name
	_, err = NewMySQLTableLocker(WithLedgerTableName(""))
	require.Error(t, err)
	// Test empty instance ID
	_, err = NewMySQLTableLocker(WithInstanceID(""))
	require.Error(t, err)
	// Test nil lock lost handler
	_, err = NewMySQLTableLocker(WithLockLostHandler(nil))
	require.Error(t, err)
}

func TestReconcilerOptions(t *testing.T) {
	r, err := NewPostgresTableLockReconciler(
		WithReconcileTableName("custom_locks"),
		WithReconcileLedgerTableName("custom_ledger"),
		WithReconcileLockIDs(1, 2, 3),
		WithReconcileInstanceID("pinned-reconciler"),
		WithReconcileLeaseDuration(10*time.Second),
		WithReconcileExpiryGrace(2*time.Second),
	)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, "pinned-reconciler", r.InstanceID())

	mr, err := NewMySQLTableLockReconciler(
		WithReconcileInstanceID("mysql-reconciler"),
	)
	require.NoError(t, err)
	require.Equal(t, "mysql-reconciler", mr.InstanceID())

	// A generated instance ID is used when none is pinned.
	generated, err := NewPostgresTableLockReconciler()
	require.NoError(t, err)
	require.NotEmpty(t, generated.InstanceID())

	// Invalid options.
	for _, opt := range []ReconcilerOption{
		WithReconcileTableName(""),
		WithReconcileLedgerTableName(""),
		WithReconcileLockIDs(0),
		WithReconcileInstanceID(""),
		WithReconcileLeaseDuration(-1 * time.Second),
		WithReconcileExpiryGrace(-1 * time.Second),
	} {
		_, err := NewPostgresTableLockReconciler(opt)
		require.Error(t, err)
	}
}
