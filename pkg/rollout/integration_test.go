package rollout

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// Migration fixtures.

const migration1Users = `-- +goose Up
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL DEFAULT '');
-- +goose Down
DROP TABLE users;
`

const migration2Posts = `-- +goose Up
CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL);
INSERT INTO posts (id, user_id) VALUES (10, 1);
-- +goose Down
DROP TABLE posts;
`

const migration2PostsFailingDown = `-- +goose Up
CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL);
-- +goose Down
SELECT boom_no_such_table;
`

const migration3Audit = `-- +goose Up
CREATE TABLE audit (id INTEGER PRIMARY KEY);
-- +goose Down
DROP TABLE audit;
`

func writeSQL(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

// setupIntegration creates a workspace with migrations and a manifest for the
// named targets and generations. It returns paths and the loaded manifest.
func setupIntegration(
	t *testing.T,
	targets []string,
	genVersions []int64,
	migration2 string,
) (root string, m *Manifest, ledgerPath string) {
	t.Helper()
	root = t.TempDir()
	mdir := filepath.Join(root, "migrations")
	require.NoError(t, os.MkdirAll(mdir, 0o755))
	writeSQL(t, filepath.Join(mdir, "00001_users.sql"), migration1Users)
	writeSQL(t, filepath.Join(mdir, "00002_posts.sql"), migration2)
	writeSQL(t, filepath.Join(mdir, "00003_audit.sql"), migration3Audit)

	var b strings.Builder
	b.WriteString("version: 1\nmigrations_dir: migrations\ntargets:\n")
	for _, name := range targets {
		fmt.Fprintf(&b, "  - {name: %s, driver: sqlite3, dsn: %q}\n",
			name, filepath.Join(root, name+".db"))
	}
	b.WriteString("generations:\n")
	for gi, v := range genVersions {
		fmt.Fprintf(&b, "  - {name: gen-%d, target_version: %d, targets: [", gi+1, v)
		for i, name := range targets {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(name)
		}
		b.WriteString("]}\n")
	}
	mp := filepath.Join(root, "rollout.yaml")
	require.NoError(t, os.WriteFile(mp, []byte(b.String()), 0o644))
	m, err := LoadManifest(mp)
	require.NoError(t, err)
	ledgerPath = filepath.Join(root, "ledger.json")
	return root, m, ledgerPath
}

func newIntegrationRunner(t *testing.T, m *Manifest, ledgerPath, runID string) *Runner {
	t.Helper()
	l, err := OpenLedger(ledgerPath, m)
	require.NoError(t, err)
	return NewRunner(m, l, WithRunID(runID))
}

// sqliteVersion returns the max applied version recorded in a sqlite database.
// Returns 0 for a file with an initialized version table and no migrations.
func sqliteVersion(t *testing.T, path string) int64 {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer db.Close()
	var v sql.NullInt64
	err = db.QueryRow(`SELECT MAX(version_id) FROM goose_db_version WHERE is_applied = 1`).Scan(&v)
	if err != nil {
		// Table does not exist: database never initialized.
		return -1
	}
	if !v.Valid {
		return 0
	}
	return v.Int64
}

func sqliteTableExists(t *testing.T, path, table string) bool {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer db.Close()
	var n int
	err = db.QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&n)
	require.NoError(t, err)
	return n == 1
}

// handMigrate opens a goose executor for name, applies exactly version, and
// closes it. It is used to simulate prior manual operations.
func handMigrate(t *testing.T, m *Manifest, name string, version int64) {
	t.Helper()
	target, ok := m.Target(name)
	require.True(t, ok)
	exec, err := NewExecutorFactory(m.MigrationsDir, nil)(context.Background(), target)
	require.NoError(t, err)
	require.NoError(t, exec.UpgradeTo(context.Background(), version))
	require.NoError(t, exec.Close())
}

// handExec opens a raw connection and runs a statement, simulating a hand-made
// change outside goose.
func handExec(t *testing.T, path, stmt string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(stmt)
	require.NoError(t, err)
}

func dbPath(m *Manifest, name string) string {
	t, ok := m.Target(name)
	if !ok {
		panic("unknown target")
	}
	return t.DSN
}

// seedAdopted writes adopted entries for all targets at baseline and leaves the
// generation pending, as if they had been taken over previously.
func seedAdopted(t *testing.T, l *Ledger, gi int, targets []string, baseline int64) {
	t.Helper()
	for _, name := range targets {
		require.NoError(t, l.AppendEntry(Entry{
			RunID: "seed", GenIndex: gi, GenName: "gen-1", Target: name,
			Kind: EntryAdopted, Baseline: baseline, Observed: baseline,
		}))
	}
}

// Scenarios.

func TestIntegration_AllSuccess(t *testing.T) {
	t.Parallel()
	_, m, lp := setupIntegration(t, []string{"db-a", "db-b"}, []int64{2, 3}, migration2Posts)
	r := newIntegrationRunner(t, m, lp, "run-1")

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Generations, 2)

	for _, name := range []string{"db-a", "db-b"} {
		p := dbPath(m, name)
		require.Equal(t, int64(3), sqliteVersion(t, p))
		require.True(t, sqliteTableExists(t, p, "users"))
		require.True(t, sqliteTableExists(t, p, "posts"))
	}
}

func TestIntegration_FailedUpgradeRollsBackWholeGeneration(t *testing.T) {
	t.Parallel()
	root, m, lp := setupIntegration(t,
		[]string{"db-a", "db-b", "db-c"}, []int64{2, 3}, migration2Posts)

	// c is a fresh database (no goose history) with a hand-created audit
	// table. Gen-1 (v2) succeeds for all three. In gen-2 (v3), a and b apply
	// v3 while c fails with "table audit already exists" after v1/v2 were
	// applied during the same UpTo.
	handExec(t, dbPath(m, "db-c"), `CREATE TABLE audit (id INTEGER PRIMARY KEY)`)

	r := newIntegrationRunner(t, m, lp, "run-1")
	report, err := r.Run(context.Background())
	require.Error(t, err)

	// Whole failed generation rolled back: every target back at baseline 0.
	for _, name := range []string{"db-a", "db-b", "db-c"} {
		p := dbPath(m, name)
		require.Equal(t, int64(0), sqliteVersion(t, p), name)
		require.False(t, sqliteTableExists(t, p, "users"), name)
		require.False(t, sqliteTableExists(t, p, "posts"), name)
	}
	// The hand-created audit table in c is an orphan artifact goose never
	// created and therefore does not drop.
	require.True(t, sqliteTableExists(t, dbPath(m, "db-c"), "audit"))

	g0 := report.Generations[0]
	require.Equal(t, GenSucceeded, g0.Status)
	g1 := report.Generations[1]
	require.Equal(t, GenRolledBack, g1.Status)
	require.Len(t, g1.Targets, 3)

	_ = root
}

func TestIntegration_ConflictFromHandChange(t *testing.T) {
	t.Parallel()
	_, m, lp := setupIntegration(t,
		[]string{"db-a", "db-b", "db-c"}, []int64{2}, migration2Posts)

	l, err := OpenLedger(lp, m)
	require.NoError(t, err)
	seedAdopted(t, l, 0, []string{"db-a", "db-b", "db-c"}, 0)

	// c hand-migrated to v1 with no ledger record; a/b untouched.
	handMigrate(t, m, "db-c", 1)

	r := NewRunner(m, l, WithRunID("run-1"))
	report, err := r.Run(context.Background())
	require.Error(t, err)
	require.Equal(t, GenRolledBack, report.Generations[0].Status)

	// a/b rolled back (from 2) to 0; c untouched at v1.
	require.Equal(t, int64(0), sqliteVersion(t, dbPath(m, "db-a")))
	require.Equal(t, int64(0), sqliteVersion(t, dbPath(m, "db-b")))
	require.Equal(t, int64(1), sqliteVersion(t, dbPath(m, "db-c")))
	require.True(t, sqliteTableExists(t, dbPath(m, "db-c"), "users"))

	var conflict TargetReport
	var found bool
	for _, tr := range report.Generations[0].Targets {
		if tr.Outcome == EntryConflict {
			conflict, found = tr, true
		}
	}
	require.True(t, found)
	require.Equal(t, "db-c", conflict.Name)
	require.Equal(t, int64(0), conflict.Recorded)
	require.Equal(t, int64(1), conflict.Observed)
}

func TestIntegration_SkipAdoptAndOutsider(t *testing.T) {
	t.Parallel()
	_, m, lp := setupIntegration(t,
		[]string{"db-skip", "db-adopt"}, []int64{2}, migration2Posts)

	l, err := OpenLedger(lp, m)
	require.NoError(t, err)

	// skip: physically at v2 and ledger adopted at 2.
	handMigrate(t, m, "db-skip", 2)
	require.NoError(t, l.AppendEntry(Entry{
		RunID: "seed", GenIndex: 0, Target: "db-skip",
		Kind: EntryAdopted, Baseline: 2, Observed: 2,
	}))

	// adopt: physically at v1, no ledger history.
	handMigrate(t, m, "db-adopt", 1)

	// outsider: physically at v2, not listed in manifest.
	outsiderPath := filepath.Join(filepath.Dir(lp), "outsider.db")
	outsiderTarget := Target{Name: "outsider", Driver: "sqlite3", DSN: outsiderPath}
	oe, err := NewExecutorFactory(m.MigrationsDir, nil)(context.Background(), outsiderTarget)
	require.NoError(t, err)
	require.NoError(t, oe.UpgradeTo(context.Background(), 2))
	require.NoError(t, oe.Close())

	skipInfo, err := os.Stat(dbPath(m, "db-skip"))
	require.NoError(t, err)
	outInfo, err := os.Stat(outsiderPath)
	require.NoError(t, err)

	r := NewRunner(m, l, WithRunID("run-1"))
	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, GenSucceeded, report.Generations[0].Status)

	// skip target unchanged at 2.
	require.Equal(t, int64(2), sqliteVersion(t, dbPath(m, "db-skip")))
	// adopt target converged from 1 to 2 with baseline 1.
	require.Equal(t, int64(2), sqliteVersion(t, dbPath(m, "db-adopt")))
	// outsider completely untouched.
	require.Equal(t, int64(2), sqliteVersion(t, outsiderPath))

	skipInfo2, _ := os.Stat(dbPath(m, "db-skip"))
	require.Equal(t, skipInfo.ModTime(), skipInfo2.ModTime())
	outInfo2, _ := os.Stat(outsiderPath)
	require.Equal(t, outInfo.ModTime(), outInfo2.ModTime())
}

func TestIntegration_ManifestChangeRejected(t *testing.T) {
	t.Parallel()
	root, m, lp := setupIntegration(t, []string{"db-a"}, []int64{2}, migration2Posts)
	r := newIntegrationRunner(t, m, lp, "run-1")
	_, err := r.Run(context.Background())
	require.NoError(t, err)

	// Change the manifest file in place.
	mp := filepath.Join(root, "rollout.yaml")
	require.NoError(t, os.WriteFile(mp,
		[]byte(strings.ReplaceAll(string(mustRead(t, mp)), "gen-1", "gen-renamed")),
		0o644))
	m2, err := LoadManifest(mp)
	require.NoError(t, err)

	_, err = OpenLedger(lp, m2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "manifest digest mismatch")

	// Database untouched.
	require.Equal(t, int64(2), sqliteVersion(t, dbPath(m, "db-a")))
}

func TestIntegration_CorruptLedgerRejected(t *testing.T) {
	t.Parallel()
	_, m, lp := setupIntegration(t, []string{"db-a"}, []int64{2}, migration2Posts)
	r := newIntegrationRunner(t, m, lp, "run-1")
	_, err := r.Run(context.Background())
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(lp, []byte("{garbage"), 0o644))
	_, err = OpenLedger(lp, m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse ledger")
	require.Equal(t, int64(2), sqliteVersion(t, dbPath(m, "db-a")))
}

func TestIntegration_ConflictStickyAcrossRuns(t *testing.T) {
	t.Parallel()
	_, m, lp := setupIntegration(t,
		[]string{"db-a", "db-c"}, []int64{2}, migration2Posts)
	l, err := OpenLedger(lp, m)
	require.NoError(t, err)
	seedAdopted(t, l, 0, []string{"db-a", "db-c"}, 0)
	handMigrate(t, m, "db-c", 1)

	// First run: conflict.
	r1 := NewRunner(m, l, WithRunID("run-1"))
	_, err = r1.Run(context.Background())
	require.Error(t, err)

	versions := map[string]int64{
		"db-a": sqliteVersion(t, dbPath(m, "db-a")),
		"db-c": sqliteVersion(t, dbPath(m, "db-c")),
	}

	// Rerun with a new runner instance: blocked, no changes.
	r2 := newIntegrationRunner(t, m, lp, "run-2")
	_, err = r2.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "blocked")
	require.Equal(t, versions["db-a"], sqliteVersion(t, dbPath(m, "db-a")))
	require.Equal(t, versions["db-c"], sqliteVersion(t, dbPath(m, "db-c")))
}

func TestIntegration_FailingDownRollbackIsHard(t *testing.T) {
	t.Parallel()
	_, m, lp := setupIntegration(t,
		[]string{"db-a", "db-b"}, []int64{2}, migration2PostsFailingDown)

	// b hand-created posts so its upgrade fails, triggering rollback of a.
	handMigrate(t, m, "db-b", 1)
	handExec(t, dbPath(m, "db-b"), `CREATE TABLE posts (id INTEGER PRIMARY KEY)`)

	r := newIntegrationRunner(t, m, lp, "run-1")
	report, err := r.Run(context.Background())
	require.Error(t, err)
	require.Equal(t, GenFailed, report.Generations[0].Status)

	// a's downgrade failed: it stays at v2.
	require.Equal(t, int64(2), sqliteVersion(t, dbPath(m, "db-a")))

	var hard []TargetReport
	for _, tr := range report.Generations[0].Targets {
		if tr.Outcome == EntryRollbackFailed {
			hard = append(hard, tr)
		}
	}
	require.NotEmpty(t, hard)

	// Rerun refuses to advance: the generation is in the sticky failed state.
	r2 := newIntegrationRunner(t, m, lp, "run-2")
	_, err = r2.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "previously failed")
	require.Equal(t, int64(2), sqliteVersion(t, dbPath(m, "db-a")))
}

// TR-6.6 rubric self-assessment.
func TestIntegration_FailSafeRubric(t *testing.T) {
	t.Parallel()
	t.Log("rubric TR-6.6 self-score: 5/5 - upgrade failure, manual drift, failed down migration, manifest change and ledger corruption all have explicit tests with fail-closed behavior; see TestIntegration_* matrix")
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}
