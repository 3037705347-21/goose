package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3/internal/testing/testdb"
	"github.com/pressly/goose/v3/reconcile"
	"github.com/stretchr/testify/require"
)

// TestMySQLReconcile runs the full reconciliation flow against MariaDB: a goose version table
// nobody registered is reported but never touched, while two confirmed applications converge in
// dependency order and a second run is a no-op.
func TestMySQLReconcile(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	db, cleanup, err := testdb.NewMariaDB()
	require.NoError(t, err)
	t.Cleanup(cleanup)

	// A "wild" goose-shaped version table present in the database but registered by no app.
	_, err = db.Exec(`
CREATE TABLE goose_wild_version (
	id bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
	version_id bigint NOT NULL,
	is_applied boolean NOT NULL,
	tstamp datetime NULL DEFAULT CURRENT_TIMESTAMP
)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO goose_wild_version (version_id, is_applied) VALUES (0, 1), (7, 1)`)
	require.NoError(t, err)

	root := t.TempDir()
	writeReconcileMigration(t, filepath.Join(root, "users"), "00001_users.sql",
		"CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR(20))",
		"DROP TABLE users",
	)
	writeReconcileMigration(t, filepath.Join(root, "orders"), "00001_orders.sql",
		"CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT);\nINSERT INTO users (id, name) VALUES (1, 'alice')",
		"DROP TABLE orders",
	)

	cfg := &reconcile.Config{
		Dialect:   "mysql",
		LedgerDir: filepath.Join(root, "ledgers"),
		Apps: []reconcile.AppSpec{
			{Name: "users", Table: "goose_users_version", FS: os.DirFS(filepath.Join(root, "users"))},
			{
				Name:      "orders",
				Table:     "goose_orders_version",
				DependsOn: []string{"users"},
				FS:        os.DirFS(filepath.Join(root, "orders")),
			},
		},
	}

	runner, err := reconcile.New(db, cfg)
	require.NoError(t, err)

	report, err := runner.Run(context.Background())
	require.Error(t, err, "unregistered table must surface as an anomaly")
	require.True(t, report.DiscoverySupported)
	require.Equal(t, []string{"goose_wild_version"}, report.UnregisteredTables)
	require.Len(t, report.Apps, 2)
	require.Equal(t, "users", report.Apps[0].Name)
	require.Equal(t, "orders", report.Apps[1].Name)
	for _, app := range report.Apps {
		require.Equal(t, reconcile.StatusConverged, app.Status, app.Name)
		require.False(t, app.TableExisted, app.Name)
		require.Equal(t, []int64{1}, app.Applied, app.Name)
		require.Equal(t, int64(1), app.VersionAfter, app.Name)
	}

	// The unregistered table was not touched.
	var wildMax int64
	require.NoError(t, db.QueryRow(`SELECT MAX(version_id) FROM goose_wild_version`).Scan(&wildMax))
	require.Equal(t, int64(7), wildMax)

	// Ledgers were written for both confirmed applications.
	for _, name := range []string{"users", "orders"} {
		ledger, err := reconcile.ReadLedger(cfg.LedgerDir, name)
		require.NoError(t, err)
		require.NotNil(t, ledger, name)
		require.Equal(t, reconcile.StatusConverged, ledger.Status, name)
	}

	// Re-running is a no-op for the confirmed apps; the wild table is still reported.
	report2, err := runner.Run(context.Background())
	require.Error(t, err)
	require.Equal(t, report.UnregisteredTables, report2.UnregisteredTables)
	for _, app := range report2.Apps {
		require.Equal(t, reconcile.StatusConverged, app.Status, app.Name)
		require.Empty(t, app.Applied, app.Name)
		require.True(t, app.TableExisted, app.Name)
	}
	require.NoError(t, db.QueryRow(`SELECT MAX(version_id) FROM goose_wild_version`).Scan(&wildMax))
	require.Equal(t, int64(7), wildMax)
}

func writeReconcileMigration(t *testing.T, dir, name, up, down string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	content := "-- +goose Up\n" + up + "\n-- +goose Down\n" + down + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644))
}
