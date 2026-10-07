package store

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func str(v string) sql.NullString   { return sql.NullString{String: v, Valid: true} }
func integer(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }
func at(t time.Time) sql.NullTime   { return sql.NullTime{Time: t, Valid: true} }

func TestClassifyLocks(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

	rows := []scannedLock{
		// 1. Healthy lease, owned by someone else: alive, must not be touched.
		{LockID: 1, LockedBy: str("alive-other"), Epoch: integer(3), LeaseExpiresAt: at(now.Add(10 * time.Second))},
		// 2. Healthy lease, owned by the reconciler itself: still alive.
		{LockID: 2, LockedBy: str("me"), Epoch: integer(1), LeaseExpiresAt: at(now)},
		// 3. Expired well beyond grace: takeover.
		{LockID: 3, LockedBy: str("dead-instance"), Epoch: integer(1), LeaseExpiresAt: at(now.Add(-30 * time.Second))},
		// 4. Expired exactly at the grace boundary (equal): not strictly older, stays alive.
		{LockID: 4, LockedBy: str("boundary"), Epoch: integer(2), LeaseExpiresAt: at(now.Add(-5 * time.Second))},
		// 5. Ambiguous: locked row without owner.
		{LockID: 5, LockedBy: sql.NullString{}, Epoch: integer(1), LeaseExpiresAt: at(now.Add(-30 * time.Second))},
		// 6. Ambiguous: locked row without lease.
		{LockID: 6, LockedBy: str("some-instance"), Epoch: integer(1), LeaseExpiresAt: sql.NullTime{}},
		// 7. Ambiguous: invalid epoch.
		{LockID: 7, LockedBy: str("some-instance"), Epoch: sql.NullInt64{}, LeaseExpiresAt: at(now.Add(-30 * time.Second))},
	}

	alive, takeovers, ambiguous := classifyLocks(rows, now, 5*time.Second)

	require.Equal(t, []int64{1, 2, 4}, []int64{alive[0].LockID, alive[1].LockID, alive[2].LockID})
	require.Equal(t, "alive-other", alive[0].LockedBy)
	require.Equal(t, int64(3), alive[0].Epoch)

	require.Len(t, takeovers, 1)
	require.Equal(t, int64(3), takeovers[0].LockID)

	require.Len(t, ambiguous, 3)
	require.Equal(t, int64(5), ambiguous[0].LockID)
	require.Contains(t, ambiguous[0].Reason, "locked_by")
	require.Equal(t, int64(6), ambiguous[1].LockID)
	require.Contains(t, ambiguous[1].Reason, "lease_expires_at")
	require.Equal(t, int64(7), ambiguous[2].LockID)
	require.Contains(t, ambiguous[2].Reason, "epoch")

	// Zero grace: the boundary lock also becomes a takeover candidate. Ambiguous rows stay
	// ambiguous regardless of grace.
	alive2, takeovers2, ambiguous2 := classifyLocks(rows, now, 0)
	require.Len(t, ambiguous2, 3)
	require.Equal(t, []int64{5, 6, 7}, []int64{ambiguous2[0].LockID, ambiguous2[1].LockID, ambiguous2[2].LockID})
	require.Len(t, takeovers2, 2)
	require.Equal(t, []int64{3, 4}, []int64{takeovers2[0].LockID, takeovers2[1].LockID})
	require.Len(t, alive2, 2)
}

func TestClassifyLocksZeroGraceAndExpiredSelf(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

	rows := []scannedLock{
		// Expired even if the owner is the would-be new owner: a fresh generation is still needed.
		{LockID: 10, LockedBy: str("me"), Epoch: integer(4), LeaseExpiresAt: at(now.Add(-1 * time.Nanosecond))},
	}
	alive, takeovers, ambiguous := classifyLocks(rows, now, 0)
	require.Empty(t, alive)
	require.Empty(t, ambiguous)
	require.Len(t, takeovers, 1)

	rec := newTakeoverRecord(takeovers[0], "me", 5, now)
	require.Equal(t, int64(10), rec.LockID)
	require.Equal(t, "me", rec.OwnerFrom)
	require.Equal(t, "me", rec.OwnerTo)
	require.Equal(t, int64(4), rec.PreviousEpoch)
	require.Equal(t, int64(5), rec.NewEpoch)
	require.Equal(t, ReasonLeaseExpired, rec.Reason)
	require.Equal(t, now, rec.TakenOverAt)
}

func TestClassifyLocksEmpty(t *testing.T) {
	alive, takeovers, ambiguous := classifyLocks(nil, time.Now(), time.Second)
	require.Empty(t, alive)
	require.Empty(t, takeovers)
	require.Empty(t, ambiguous)
}
