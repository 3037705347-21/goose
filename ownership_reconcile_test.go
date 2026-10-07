package goose_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	goose "github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/internal/ownership"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// The scripted DOWN sections deliberately contain statements that MUST NOT run when ownership
// reconciliation is enabled. Executing them verbatim would either fail or delete more than the
// migration owns.
const (
	migrationV1 = `-- +goose Up
CREATE TABLE users (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL
);
CREATE INDEX idx_users_name ON users (name);

-- +goose Down
DROP TABLE goose_scripted_down_must_not_run;
`
	migrationV2 = `-- +goose Up
ALTER TABLE users ADD COLUMN email TEXT;
CREATE INDEX idx_users_email ON users (email);

-- +goose Down
ALTER TABLE users DROP COLUMN email;
DROP INDEX idx_users_email;
`
	migrationV3 = `-- +goose Up
ALTER TABLE users ADD COLUMN nickname VARCHAR(100);

-- +goose Down
ALTER TABLE users DROP COLUMN nickname;
`
)

func writeOwnershipMigrations(t *testing.T, dir string) {
	t.Helper()
	files := map[string]string{
		"00001_users.sql":    migrationV1,
		"00002_email.sql":    migrationV2,
		"00003_nickname.sql": migrationV3,
	}
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644))
	}
}

func ownTableExists(t *testing.T, db *sql.DB, table string) bool {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&n))
	return n == 1
}

func ownColumnExists(t *testing.T, db *sql.DB, table, column string) bool {
	t.Helper()
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var (
			cid     int
			name    string
			typ     string
			notNull int
			dflt    sql.NullString
			pk      int
		)
		require.NoError(t, rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk))
		if name == column {
			return true
		}
	}
	return false
}

func ownIndexExists(t *testing.T, db *sql.DB, index string) bool {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type='index' AND name=?`, index).Scan(&n))
	return n == 1
}

func ownDriftError(t *testing.T, err error) *ownership.DriftError {
	t.Helper()
	var drift *ownership.DriftError
	require.Error(t, err)
	require.True(t, errors.As(err, &drift), "expected ownership.DriftError, got %v", err)
	return drift
}

// Full lifecycle: claims on UP, external columns/indexes survive rollback, renamed columns block
// until restored, tables with external children are retained, and scripted DOWN never runs.
func TestOwnershipReconciliation_Lifecycle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeOwnershipMigrations(t, dir)

	db, err := sql.Open("sqlite", filepath.Join(dir, "app.db"))
	require.NoError(t, err)
	defer db.Close()

	p, err := goose.NewProvider(goose.DialectSQLite3, db, os.DirFS(dir),
		goose.WithOwnershipReconciliation(true),
		goose.WithVerbose(testing.Verbose()),
	)
	require.NoError(t, err)

	_, err = p.Up(ctx)
	require.NoError(t, err)
	v, err := p.GetDBVersion(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 3, v)
	require.True(t, ownTableExists(t, db, ownership.DefaultLedgerTable))

	// Someone adds a column and an index by hand, outside goose.
	_, err = db.Exec(`ALTER TABLE users ADD COLUMN manual_col TEXT`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE INDEX idx_users_manual ON users (manual_col)`)
	require.NoError(t, err)

	// Roll back v3 (nickname column) normally.
	res, err := p.Down(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 3, res.Source.Version)
	assert.False(t, ownColumnExists(t, db, "users", "nickname"))
	assert.True(t, ownColumnExists(t, db, "users", "manual_col"), "external column must survive")
	assert.True(t, ownIndexExists(t, db, "idx_users_manual"), "external index must survive")

	// Roll back v2: only ledger-owned objects (email column + its index) go.
	_, err = p.Down(ctx)
	require.NoError(t, err)
	v, err = p.GetDBVersion(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, v)
	assert.False(t, ownColumnExists(t, db, "users", "email"))
	assert.False(t, ownIndexExists(t, db, "idx_users_email"))
	assert.True(t, ownColumnExists(t, db, "users", "manual_col"))
	assert.True(t, ownIndexExists(t, db, "idx_users_manual"))
	assert.True(t, ownTableExists(t, db, "users"))

	// Re-apply v3, then rename the owned column by hand: rollback must block and leave it.
	_, err = p.Up(ctx)
	require.NoError(t, err)
	_, err = db.Exec(`ALTER TABLE users RENAME COLUMN nickname TO nickname2`)
	require.NoError(t, err)
	_, err = p.Down(ctx)
	derr := ownDriftError(t, err)
	require.Len(t, derr.Report.Drifts, 1)
	assert.Equal(t, ownership.SkipRenamed, derr.Report.Drifts[0].Reason)
	v, err = p.GetDBVersion(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 3, v, "version must stay applied while drift is unresolved")
	assert.True(t, ownColumnExists(t, db, "users", "nickname2"))

	// Restore the original name: the re-run converges.
	_, err = db.Exec(`ALTER TABLE users RENAME COLUMN nickname2 TO nickname`)
	require.NoError(t, err)
	_, err = p.Down(ctx)
	require.NoError(t, err)
	v, _ = p.GetDBVersion(ctx)
	assert.EqualValues(t, 2, v)
	assert.False(t, ownColumnExists(t, db, "users", "nickname"))

	// Re-applied alongside v3, v2 now rolls back normally too; external objects still survive.
	_, err = p.Down(ctx)
	require.NoError(t, err)
	v, _ = p.GetDBVersion(ctx)
	assert.EqualValues(t, 1, v)
	assert.False(t, ownColumnExists(t, db, "users", "email"))
	assert.True(t, ownColumnExists(t, db, "users", "manual_col"))
	assert.True(t, ownIndexExists(t, db, "idx_users_manual"))

	// Rolling back v1 would destroy the hand-made children: the table and its owned subgraph are
	// retained and the rollback blocks.
	_, err = p.Down(ctx)
	derr = ownDriftError(t, err)
	for _, d := range derr.Report.Drifts {
		assert.Equal(t, ownership.SkipHasDependents, d.Reason)
	}
	assert.True(t, ownTableExists(t, db, "users"))
	v, _ = p.GetDBVersion(ctx)
	assert.EqualValues(t, 1, v)

	// Operator removes the external objects; now v1 rollback converges and the scripted DOWN
	// (which would fail if it ran) is never executed.
	_, err = db.Exec(`DROP INDEX idx_users_manual`)
	require.NoError(t, err)
	_, err = db.Exec(`ALTER TABLE users DROP COLUMN manual_col`)
	require.NoError(t, err)
	_, err = p.Down(ctx)
	require.NoError(t, err)
	assert.False(t, ownTableExists(t, db, "users"))
	v, _ = p.GetDBVersion(ctx)
	assert.EqualValues(t, 0, v)
}

// An interrupted rollback (object already dropped, ledger row orphaned) must converge on re-run.
func TestOwnershipReconciliation_InterruptedRerunConverges(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeOwnershipMigrations(t, dir)

	db, err := sql.Open("sqlite", filepath.Join(dir, "app.db"))
	require.NoError(t, err)
	defer db.Close()

	p, err := goose.NewProvider(goose.DialectSQLite3, db, os.DirFS(dir),
		goose.WithOwnershipReconciliation(true),
		goose.WithVerbose(testing.Verbose()),
	)
	require.NoError(t, err)
	_, err = p.Up(ctx)
	require.NoError(t, err)

	// Simulate a crash after the index drop but before the column drop / version removal.
	_, err = db.Exec(`DROP INDEX idx_users_email`)
	require.NoError(t, err)

	_, err = p.DownTo(ctx, 0)
	require.NoError(t, err)
	v, err := p.GetDBVersion(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 0, v)
	assert.False(t, ownTableExists(t, db, "users"))
	assert.True(t, ownTableExists(t, db, ownership.DefaultLedgerTable), "ledger bookkeeping stays")
}
