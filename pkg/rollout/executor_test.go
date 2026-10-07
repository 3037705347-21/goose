package rollout

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestDriverMapping(t *testing.T) {
	t.Parallel()
	drivers := []string{
		"postgres", "pgx", "redshift", "mysql", "tidb", "starrocks",
		"sqlite3", "sqlite", "mssql", "azuresql", "sqlserver",
		"spanner", "clickhouse", "ydb", "turso", "vertica",
	}
	for _, d := range drivers {
		sd, ok := goose.SQLDriver(d)
		require.True(t, ok, "sql driver for %s", d)
		require.NotEmpty(t, sd)
		dl, ok := goose.DriverDialect(d)
		require.True(t, ok, "dialect for %s", d)
		require.NotEmpty(t, dl)
	}

	_, ok := goose.SQLDriver("nope")
	require.False(t, ok)
	_, ok = goose.DriverDialect("nope")
	require.False(t, ok)
}

func TestGooseExecutor_VersionRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeMigrationFiles(t, dir, 1, 2, 3)

	dbPath := filepath.Join(dir, "test.db")
	target := Target{Name: "db", Driver: "sqlite3", DSN: dbPath}
	factory := NewExecutorFactory(filepath.Join(dir, "migrations"), nil)

	exec, err := factory(context.Background(), target)
	require.NoError(t, err)
	defer exec.Close()

	ctx := context.Background()
	v, err := exec.CurrentVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), v)

	require.NoError(t, exec.UpgradeTo(ctx, 2))
	v, err = exec.CurrentVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), v)

	// DownTo(1) rolls back everything above 1, leaving the target at 1.
	require.NoError(t, exec.DowngradeTo(ctx, 1))
	v, err = exec.CurrentVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), v)
}

func TestGooseExecutor_UnknownDriver(t *testing.T) {
	t.Parallel()
	factory := NewExecutorFactory(t.TempDir(), nil)
	_, err := factory(context.Background(), Target{Name: "x", Driver: "bogus", DSN: "x"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported driver")
}

func TestGooseExecutor_Hooks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeMigrationFiles(t, dir, 1, 2)

	var calls []string
	abort := errors.New("before abort")
	hooks := &Hooks{
		Before: func(_ context.Context, target string, action Action, to int64) error {
			calls = append(calls, "before:"+string(action))
			if target == "abort" {
				return abort
			}
			return nil
		},
		After: func(_ context.Context, _ string, action Action, _ int64, _ error) error {
			calls = append(calls, "after:"+string(action))
			return nil
		},
	}
	factory := NewExecutorFactory(filepath.Join(dir, "migrations"), hooks)

	// Aborting before hook prevents the action entirely.
	execAbort, err := factory(context.Background(), Target{Name: "abort", Driver: "sqlite3", DSN: filepath.Join(dir, "a.db")})
	require.NoError(t, err)
	defer execAbort.Close()
	err = execAbort.UpgradeTo(context.Background(), 2)
	require.ErrorIs(t, err, abort)

	// Normal target: before and after both fire in order.
	execOK, err := factory(context.Background(), Target{Name: "ok", Driver: "sqlite3", DSN: filepath.Join(dir, "b.db")})
	require.NoError(t, err)
	defer execOK.Close()
	require.NoError(t, execOK.UpgradeTo(context.Background(), 2))

	require.Equal(t,
		[]string{"before:upgrade", "before:upgrade", "after:upgrade"},
		calls)
}

func TestGooseExecutor_HookReplacesActionError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeMigrationFiles(t, dir, 1, 2)

	replaced := errors.New("replaced")
	hooks := &Hooks{
		After: func(context.Context, string, Action, int64, error) error {
			return replaced
		},
	}
	factory := NewExecutorFactory(filepath.Join(dir, "migrations"), hooks)
	exec, err := factory(context.Background(), Target{Name: "x", Driver: "sqlite3", DSN: filepath.Join(dir, "a.db")})
	require.NoError(t, err)
	defer exec.Close()
	// Downgrade of a fresh DB is a no-op error path; regardless of action
	// outcome, the after hook replaces the result.
	err = exec.DowngradeTo(context.Background(), 5)
	require.ErrorIs(t, err, replaced)
}
