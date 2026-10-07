package reconcile

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/pressly/goose/v3/database"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

var fixedNow = time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "shared.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// writeApp creates a migration directory rooted with the given version->SQL up-statement pairs.
func writeApp(t *testing.T, root, name string, versions map[int64]string) {
	t.Helper()
	dir := filepath.Join(root, name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	for v, stmt := range versions {
		content := "-- +goose Up\n" + stmt + "\n-- +goose Down\nSELECT 1;\n"
		p := filepath.Join(dir, sqlFilename(v))
		require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
	}
}

func sqlFilename(v int64) string {
	switch {
	case v < 10:
		return "0000" + itoa(v) + "_m.sql"
	case v < 100:
		return "000" + itoa(v) + "_m.sql"
	default:
		return "00" + itoa(v) + "_m.sql"
	}
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}

// appSpec builds an AppSpec whose FS is rooted at root/<name>.
func appSpec(name, table string, deps []string, root string) AppSpec {
	return AppSpec{
		Name:      name,
		Table:     table,
		DependsOn: deps,
		FS:        os.DirFS(filepath.Join(root, name)),
	}
}

func newRunner(t *testing.T, db *sql.DB, cfg *Config, opts ...Option) *Runner {
	t.Helper()
	opts = append([]Option{WithLogger(discardLogger{}), withClock(func() time.Time { return fixedNow })}, opts...)
	r, err := New(db, cfg, opts...)
	require.NoError(t, err)
	return r
}

func sqliteConfig(t *testing.T, apps ...AppSpec) *Config {
	t.Helper()
	return &Config{
		Dialect:   string(database.DialectSQLite3),
		LedgerDir: filepath.Join(t.TempDir(), "ledgers"),
		Apps:      apps,
	}
}

func TestConvergeInDependencyOrder(t *testing.T) {
	db := testDB(t)
	root := t.TempDir()
	// users creates its table; orders inserts into it and can only succeed after users converged.
	writeApp(t, root, "users", map[int64]string{
		1: "CREATE TABLE users (id INTEGER PRIMARY KEY);",
	})
	writeApp(t, root, "orders", map[int64]string{
		1: "CREATE TABLE orders (id INTEGER PRIMARY KEY);\nINSERT INTO users (id) VALUES (1);",
	})
	cfg := sqliteConfig(t,
		appSpec("users", "goose_users_version", nil, root),
		appSpec("orders", "goose_orders_version", []string{"users"}, root),
	)

	report, err := newRunner(t, db, cfg).Run(context.Background())
	require.NoError(t, err)
	require.False(t, report.HasAnomalies())
	require.Len(t, report.Apps, 2)
	require.Equal(t, "users", report.Apps[0].Name)
	require.Equal(t, "orders", report.Apps[1].Name)
	for _, app := range report.Apps {
		require.Equal(t, StatusConverged, app.Status, app.Name)
		require.False(t, app.TableExisted, app.Name)
		require.Equal(t, int64(1), app.VersionAfter, app.Name)
	}

	// Ledgers exist for every app and record the convergence result.
	for _, name := range []string{"users", "orders"} {
		l, err := ReadLedger(cfg.LedgerDir, name)
		require.NoError(t, err)
		require.NotNil(t, l, name)
		require.Equal(t, StatusConverged, l.Status, name)
		require.Equal(t, fixedNow, l.ConvergedAt, name)
		require.Equal(t, []int64{1}, l.Applied, name)
		require.NotEmpty(t, l.RegistryDigest, name)
	}

	// Re-run is a no-op: tables exist, nothing is applied again.
	report2, err := newRunner(t, db, cfg).Run(context.Background())
	require.NoError(t, err)
	for _, app := range report2.Apps {
		require.Equal(t, StatusConverged, app.Status)
		require.True(t, app.TableExisted)
		require.Empty(t, app.Applied)
		require.Empty(t, app.PendingBefore)
	}
}

func TestDuplicateTableConflictNotTouched(t *testing.T) {
	db := testDB(t)
	root := t.TempDir()
	writeApp(t, root, "appA", map[int64]string{1: "CREATE TABLE a (id INTEGER);"})
	writeApp(t, root, "appB", map[int64]string{1: "CREATE TABLE b (id INTEGER);"})
	writeApp(t, root, "appC", map[int64]string{1: "CREATE TABLE c (id INTEGER);"})
	cfg := sqliteConfig(t,
		appSpec("appA", "goose_shared_version", nil, root),
		appSpec("appB", "goose_shared_version", nil, root),
		appSpec("appC", "goose_c_version", nil, root),
	)

	report, err := newRunner(t, db, cfg).Run(context.Background())
	require.Error(t, err)
	require.True(t, report.HasAnomalies())

	byName := map[string]AppResult{}
	for _, app := range report.Apps {
		byName[app.Name] = app
	}
	require.Equal(t, StatusConflict, byName["appA"].Status)
	require.Contains(t, byName["appA"].Reason, "appB")
	require.Equal(t, StatusConflict, byName["appB"].Status)
	require.Equal(t, StatusConverged, byName["appC"].Status)

	// The conflicting table was never created; appC's table was.
	exists := func(table string) bool {
		var n int
		err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&n)
		require.NoError(t, err)
		return n == 1
	}
	require.False(t, exists("goose_shared_version"))
	require.True(t, exists("goose_c_version"))

	// Conflict ledgers are still written so operators can see the blocked state.
	l, err := ReadLedger(cfg.LedgerDir, "appA")
	require.NoError(t, err)
	require.Equal(t, StatusConflict, l.Status)
}

func TestBlockedDependencyPropagates(t *testing.T) {
	db := testDB(t)
	root := t.TempDir()
	writeApp(t, root, "appA", map[int64]string{1: "SELECT 1;"})
	writeApp(t, root, "appB", map[int64]string{1: "SELECT 1;"})
	writeApp(t, root, "appC", map[int64]string{1: "CREATE TABLE c (id INTEGER);"})
	cfg := sqliteConfig(t,
		appSpec("appA", "goose_shared_version", nil, root),
		appSpec("appB", "goose_shared_version", []string{"appA"}, root),
		appSpec("appC", "goose_c_version", []string{"appB"}, root),
	)

	report, err := newRunner(t, db, cfg).Run(context.Background())
	require.Error(t, err)
	byName := map[string]AppResult{}
	for _, app := range report.Apps {
		byName[app.Name] = app
	}
	require.Equal(t, StatusConflict, byName["appA"].Status)
	require.Equal(t, StatusBlockedDependency, byName["appB"].Status)
	require.Equal(t, StatusBlockedDependency, byName["appC"].Status)
	require.Contains(t, byName["appC"].Reason, "appB")
}

func TestDriftBlocksConvergence(t *testing.T) {
	db := testDB(t)
	root := t.TempDir()
	writeApp(t, root, "app", map[int64]string{
		1: "CREATE TABLE owned (id INTEGER);",
		2: "CREATE TABLE owned2 (id INTEGER);",
	})
	// Pre-existing version table carrying an applied version this app's migration list does not
	// know about: evidence someone else owns (or previously wrote to) the table.
	_, err := db.Exec(`
CREATE TABLE goose_app_version (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	version_id INTEGER NOT NULL,
	is_applied INTEGER NOT NULL,
	tstamp TIMESTAMP DEFAULT (datetime('now'))
);
INSERT INTO goose_app_version(version_id, is_applied) VALUES (0,1),(99,1);`)
	require.NoError(t, err)

	cfg := sqliteConfig(t, appSpec("app", "goose_app_version", nil, root))
	report, err := newRunner(t, db, cfg).Run(context.Background())
	require.Error(t, err)
	res := report.Apps[0]
	require.Equal(t, StatusDriftBlocked, res.Status)
	require.Equal(t, []int64{99}, res.UnknownVersions)
	require.True(t, res.NewDrift)
	require.True(t, res.PendingContains(1, 2))

	// Nothing was applied and the pending migrations did not run.
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='owned'`).Scan(&n))
	require.Zero(t, n)

	// The drift ledger records the blocked state.
	l, err := ReadLedger(cfg.LedgerDir, "app")
	require.NoError(t, err)
	require.Equal(t, StatusDriftBlocked, l.Status)
	require.Equal(t, []int64{99}, l.UnknownVersions)

	// Re-running keeps the table untouched and no longer flags the same unknown version as new.
	report2, err := newRunner(t, db, cfg).Run(context.Background())
	require.Error(t, err)
	require.Equal(t, StatusDriftBlocked, report2.Apps[0].Status)
	require.False(t, report2.Apps[0].NewDrift)
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='owned'`).Scan(&n))
	require.Zero(t, n)
}

func TestResumeAfterInterruption(t *testing.T) {
	db := testDB(t)
	root := t.TempDir()
	writeApp(t, root, "app", map[int64]string{
		1: "CREATE TABLE t1 (id INTEGER);",
		2: "CREATE TABLE t2 (id INTEGER);",
		3: "CREATE TABLE t3 (id INTEGER);",
	})
	// Simulate a previous run that died after applying version 1.
	_, err := db.Exec(`
CREATE TABLE goose_app_version (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	version_id INTEGER NOT NULL,
	is_applied INTEGER NOT NULL,
	tstamp TIMESTAMP DEFAULT (datetime('now'))
);
INSERT INTO goose_app_version(version_id, is_applied) VALUES (0,1),(1,1);`)
	require.NoError(t, err)

	cfg := sqliteConfig(t, appSpec("app", "goose_app_version", nil, root))
	report, err := newRunner(t, db, cfg).Run(context.Background())
	require.NoError(t, err)
	res := report.Apps[0]
	require.Equal(t, StatusConverged, res.Status)
	require.Equal(t, []int64{2, 3}, res.Applied)
	require.Equal(t, int64(1), res.VersionBefore)
	require.Equal(t, int64(3), res.VersionAfter)

	// Re-run converges nothing new.
	report2, err := newRunner(t, db, cfg).Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, report2.Apps[0].Applied)
	require.Equal(t, int64(3), report2.Apps[0].VersionAfter)
}

func TestUnregisteredTablesReportedNotTouched(t *testing.T) {
	db := testDB(t)
	root := t.TempDir()
	writeApp(t, root, "app", map[int64]string{1: "CREATE TABLE owned (id INTEGER);"})
	// A "wild" goose-shaped table nobody registered.
	_, err := db.Exec(`
CREATE TABLE goose_wild_version (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	version_id INTEGER NOT NULL,
	is_applied INTEGER NOT NULL,
	tstamp TIMESTAMP DEFAULT (datetime('now'))
);
INSERT INTO goose_wild_version(version_id, is_applied) VALUES (0,1),(7,1);`)
	require.NoError(t, err)

	finder := fakeFinder{tables: []string{"goose_wild_version", "goose_app_version"}}
	cfg := sqliteConfig(t, appSpec("app", "goose_app_version", nil, root))
	report, err := newRunner(t, db, cfg, withFinder(finder)).Run(context.Background())
	require.Error(t, err)
	require.True(t, report.DiscoverySupported)
	require.Equal(t, []string{"goose_wild_version"}, report.UnregisteredTables)
	require.Equal(t, StatusConverged, report.Apps[0].Status)

	// The wild table content is untouched.
	var maxVersion int64
	require.NoError(t, db.QueryRow(`SELECT max(version_id) FROM goose_wild_version`).Scan(&maxVersion))
	require.Equal(t, int64(7), maxVersion)
}

func TestDryRunDoesNotMutate(t *testing.T) {
	db := testDB(t)
	root := t.TempDir()
	writeApp(t, root, "app", map[int64]string{1: "CREATE TABLE owned (id INTEGER);"})
	cfg := sqliteConfig(t, appSpec("app", "goose_app_version", nil, root))

	report, err := newRunner(t, db, cfg, WithDryRun(true)).Run(context.Background())
	require.NoError(t, err)
	res := report.Apps[0]
	require.True(t, report.DryRun)
	require.Equal(t, StatusConverged, res.Status)
	require.Equal(t, []int64{1}, res.PendingBefore)
	require.Empty(t, res.Applied)
	require.Empty(t, res.LedgerPath)

	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='goose_app_version'`).Scan(&n))
	require.Zero(t, n)
	entries, err := os.ReadDir(cfg.LedgerDir)
	if !os.IsNotExist(err) {
		require.NoError(t, err)
		require.Empty(t, entries)
	}
}

func TestTopoOrderAndValidation(t *testing.T) {
	t.Run("cycle", func(t *testing.T) {
		apps := []AppSpec{
			{Name: "a", Table: "ta", DependsOn: []string{"b"}, FS: os.DirFS(t.TempDir())},
			{Name: "b", Table: "tb", DependsOn: []string{"a"}, FS: os.DirFS(t.TempDir())},
		}
		_, err := topoOrder(apps)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cycle")
	})
	t.Run("missing dependency", func(t *testing.T) {
		cfg := &Config{Dialect: "sqlite3", Apps: []AppSpec{
			{Name: "a", Table: "ta", DependsOn: []string{"ghost"}, FS: os.DirFS(t.TempDir())},
		}}
		require.ErrorContains(t, cfg.Validate(), "unknown app")
	})
	t.Run("duplicate app name", func(t *testing.T) {
		cfg := &Config{Dialect: "sqlite3", Apps: []AppSpec{
			{Name: "a", Table: "ta", FS: os.DirFS(t.TempDir())},
			{Name: "a", Table: "tb", FS: os.DirFS(t.TempDir())},
		}}
		require.ErrorContains(t, cfg.Validate(), "duplicate app name")
	})
	t.Run("bad dialect", func(t *testing.T) {
		cfg := &Config{Dialect: "oracle", Apps: []AppSpec{
			{Name: "a", Table: "ta", FS: os.DirFS(t.TempDir())},
		}}
		require.ErrorContains(t, cfg.Validate(), "unknown dialect")
	})
	t.Run("invalid app name", func(t *testing.T) {
		cfg := &Config{Dialect: "sqlite3", Apps: []AppSpec{
			{Name: "../evil", Table: "ta", FS: os.DirFS(t.TempDir())},
		}}
		require.Error(t, cfg.Validate())
	})
	t.Run("empty dialect allowed at validation, rejected by New", func(t *testing.T) {
		cfg := &Config{Apps: []AppSpec{
			{Name: "a", Table: "ta", FS: os.DirFS(t.TempDir())},
		}}
		require.NoError(t, cfg.Validate())
		_, err := New(testDB(t), cfg)
		require.ErrorContains(t, err, "dialect must not be empty")
	})
}

func TestParseConfigFromYAML(t *testing.T) {
	root := t.TempDir()
	writeApp(t, root, "migrations/users", map[int64]string{1: "SELECT 1;"})
	writeApp(t, root, "migrations/orders", map[int64]string{1: "SELECT 1;"})
	registry := `
version: 1
dialect: tidb
apps:
  - name: users
    table: goose_users_version
    dir: migrations/users
  - name: orders
    table: goose_orders_version
    dir: migrations/orders
    depends_on: [users]
    allow_missing: true
`
	path := filepath.Join(root, "registry.yaml")
	require.NoError(t, os.WriteFile(path, []byte(registry), 0o644))

	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	require.Equal(t, "tidb", cfg.Dialect)
	require.Equal(t, filepath.Join(root, DefaultLedgerDir), cfg.LedgerDir)
	require.Len(t, cfg.Apps, 2)
	require.Equal(t, []string{"users"}, cfg.Apps[1].DependsOn)
	require.True(t, cfg.Apps[1].AllowMissing)
	require.NotNil(t, cfg.Apps[0].FS)
	require.NotEmpty(t, cfg.digest)
	order, err := topoOrder(cfg.Apps)
	require.NoError(t, err)
	require.Equal(t, []string{"users", "orders"}, order)

	// The resolved FS really points at the migration directory.
	data, err := fs.ReadFile(cfg.Apps[0].FS, sqlFilename(1))
	require.NoError(t, err)
	require.Contains(t, string(data), "+goose Up")
}

type fakeFinder struct{ tables []string }

func (f fakeFinder) findGooseTables(_ context.Context, _ *sql.DB) ([]string, error) {
	return f.tables, nil
}

// PendingContains is a test helper on AppResult.
func (r AppResult) PendingContains(versions ...int64) bool {
	for _, v := range versions {
		found := false
		for _, p := range r.PendingBefore {
			if p == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
