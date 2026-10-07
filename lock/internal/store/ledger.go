package store

import (
	"database/sql"
	"strings"
)

// ledgerEvent is an append-only audit row in the lock ledger table.
type ledgerEvent struct {
	LockID         int64
	Epoch          int64
	EventType      string
	OwnerFrom      sql.NullString
	OwnerTo        sql.NullString
	Reason         string
	LeaseExpiresAt sql.NullTime
	RunID          sql.NullString
}

func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

// ledgerIndexName derives a database-agnostic, length-safe index name from the ledger table name.
func ledgerIndexName(ledgerTable string) string {
	name := strings.NewReplacer(".", "_", "-", "_").Replace(ledgerTable)
	if len(name) > 40 {
		name = name[:40]
	}
	return "idx_" + name + "_lock_epoch"
}
