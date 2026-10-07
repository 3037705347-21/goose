package store

import (
	"database/sql"
	"fmt"
	"time"
)

// scannedLock is a lock row read under SELECT ... FOR UPDATE. Nullable columns are kept nullable
// so that inconsistent rows can be detected instead of silently coerced.
type scannedLock struct {
	LockID         int64
	Locked         bool
	LockedBy       sql.NullString
	Epoch          sql.NullInt64
	LeaseExpiresAt sql.NullTime
}

type decisionKind int

const (
	decisionAlive decisionKind = iota
	decisionTakeover
	decisionAmbiguous
)

type decision struct {
	lock   scannedLock
	kind   decisionKind
	reason string
}

// classifyLocks partitions actively locked rows into three groups using a single, caller-provided
// database server timestamp:
//
//   - alive: lease is still valid (within the optional expiry grace); these rows are never
//     touched, regardless of who owns them;
//   - takeover: lease has unambiguously expired; the row may be taken over by newOwner;
//   - ambiguous: the row is internally inconsistent (missing owner, lease or valid epoch). Any
//     ambiguous row aborts the entire reconciliation run.
//
// A lock owned by newOwner itself is taken over again when its lease is expired: this starts a
// fresh fencing generation and grants a fresh lease.
func classifyLocks(
	rows []scannedLock,
	now time.Time,
	expiryGrace time.Duration,
) (alive []ActiveLock, takeovers []scannedLock, ambiguous []AmbiguousLock) {
	// A lock is considered expired only once its lease expiration is strictly older than the
	// grace-adjusted cutoff.
	cutoff := now.Add(-expiryGrace)
	for _, row := range rows {
		switch kind, reason := decide(row, cutoff); kind {
		case decisionAlive:
			alive = append(alive, ActiveLock{
				LockID:         row.LockID,
				LockedBy:       row.LockedBy.String,
				Epoch:          row.Epoch.Int64,
				LeaseExpiresAt: row.LeaseExpiresAt.Time,
			})
		case decisionTakeover:
			takeovers = append(takeovers, row)
		case decisionAmbiguous:
			ambiguous = append(ambiguous, AmbiguousLock{LockID: row.LockID, Reason: reason})
		}
	}
	return alive, takeovers, ambiguous
}

func decide(row scannedLock, cutoff time.Time) (decisionKind, string) {
	switch {
	case !row.LockedBy.Valid || row.LockedBy.String == "":
		return decisionAmbiguous, "locked row has no locked_by"
	case !row.LeaseExpiresAt.Valid:
		return decisionAmbiguous, "locked row has no lease_expires_at"
	case !row.Epoch.Valid || row.Epoch.Int64 <= 0:
		return decisionAmbiguous, fmt.Sprintf("locked row has invalid epoch: %d", row.Epoch.Int64)
	case row.LeaseExpiresAt.Time.Before(cutoff):
		return decisionTakeover, ""
	default:
		return decisionAlive, ""
	}
}

// newTakeoverRecord builds the ledger record for taking over row at time now.
func newTakeoverRecord(row scannedLock, newOwner string, newEpoch int64, now time.Time) TakeoverRecord {
	return TakeoverRecord{
		LockID:                 row.LockID,
		OwnerFrom:              row.LockedBy.String,
		OwnerTo:                newOwner,
		PreviousEpoch:          row.Epoch.Int64,
		NewEpoch:               newEpoch,
		PreviousLeaseExpiresAt: row.LeaseExpiresAt.Time,
		TakenOverAt:            now,
		Reason:                 ReasonLeaseExpired,
	}
}
