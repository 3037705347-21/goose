package rollout

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// validManifestYAML references two targets across two generations. Migration
// files 1..3 are created by writeMigrationFiles.
const validManifestYAML = `
version: 1
migrations_dir: migrations
targets:
  - name: db-a
    driver: sqlite3
    dsn: ./a.db
  - name: db-b
    driver: sqlite3
    dsn: ./b.db
generations:
  - name: gen-1
    target_version: 2
    targets: [db-a, db-b]
  - name: gen-2
    target_version: 3
    targets: [db-a, db-b]
`

func writeMigrationFiles(t *testing.T, dir string, versions ...int) {
	t.Helper()
	mdir := filepath.Join(dir, "migrations")
	require.NoError(t, os.MkdirAll(mdir, 0o755))
	for _, v := range versions {
		name := filepath.Join(mdir,
			"0000"+strconv.Itoa(v)+"_test.sql")
		require.NoError(t, os.WriteFile(name, []byte("-- +goose Up\n"), 0o644))
	}
}

func writeManifest(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, "rollout.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

func TestLoadManifest_Valid(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeMigrationFiles(t, dir, 1, 2, 3)
	path := writeManifest(t, dir, validManifestYAML)

	m, err := LoadManifest(path)
	require.NoError(t, err)
	require.Equal(t, 1, m.Version)
	require.Equal(t, filepath.Join(dir, "migrations"), m.MigrationsDir)
	require.Len(t, m.Targets, 2)
	require.Len(t, m.Generations, 2)
	require.Equal(t, int64(2), m.Generations[0].TargetVersion)
	require.Equal(t, int64(3), m.Generations[1].TargetVersion)

	ta, ok := m.Target("db-a")
	require.True(t, ok)
	require.Equal(t, "sqlite3", ta.Driver)
	_, ok = m.Target("missing")
	require.False(t, ok)
}

func TestLoadManifest_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(string) string
		wantErr string
	}{
		{
			name:    "unsupported version",
			modify:  func(s string) string { return strings.Replace(s, "version: 1", "version: 2", 1) },
			wantErr: "unsupported manifest version",
		},
		{
			name:    "empty migrations dir",
			modify:  func(s string) string { return strings.Replace(s, "migrations_dir: migrations", "migrations_dir: \"\"", 1) },
			wantErr: "migrations_dir must not be empty",
		},
		{
			name: "duplicate target name",
			modify: func(s string) string {
				return strings.Replace(s, "  - name: db-b", "  - name: db-a", 1)
			},
			wantErr: "duplicate target name",
		},
		{
			name: "unknown target reference",
			modify: func(s string) string {
				return strings.Replace(s, "targets: [db-a, db-b]", "targets: [db-a, db-x]", 1)
			},
			wantErr: "unknown target",
		},
		{
			name: "duplicate target within generation",
			modify: func(s string) string {
				return strings.Replace(s, "targets: [db-a, db-b]", "targets: [db-a, db-a]", 1)
			},
			wantErr: "listed more than once",
		},
		{
			name: "non-increasing generation version",
			modify: func(s string) string {
				return strings.Replace(s, "    target_version: 3", "    target_version: 2", 1)
			},
			wantErr: "must be greater than previous generation version",
		},
		{
			name:    "target version does not exist",
			modify:  func(s string) string { return strings.Replace(s, "target_version: 3", "target_version: 4", 1) },
			wantErr: "is not a migration",
		},
		{
			name: "empty generation targets",
			modify: func(s string) string {
				return strings.Replace(s, "    targets: [db-a, db-b]\n  - name: gen-2", "    targets: []\n  - name: gen-2", 1)
			},
			wantErr: "at least one target is required",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writeMigrationFiles(t, dir, 1, 2, 3)
			path := writeManifest(t, dir, tc.modify(validManifestYAML))

			_, err := LoadManifest(path)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestLoadManifest_MissingMigrations(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Create manifest but no migration files.
	path := writeManifest(t, dir, validManifestYAML)
	_, err := LoadManifest(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read migrations_dir")
}

func TestLoadManifest_MissingFile(t *testing.T) {
	t.Parallel()
	_, err := LoadManifest(filepath.Join(t.TempDir(), "nope.yaml"))
	require.Error(t, err)
}

func TestManifestDigest(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeMigrationFiles(t, dir, 1, 2, 3)

	p1 := writeManifest(t, dir, validManifestYAML)
	m1, err := LoadManifest(p1)
	require.NoError(t, err)

	p2 := writeManifest(t, dir, validManifestYAML)
	m2, err := LoadManifest(p2)
	require.NoError(t, err)
	require.Equal(t, m1.Digest(), m2.Digest(), "identical content must have identical digest")

	// Rewrite with one byte changed; digest must differ.
	require.NoError(t, os.WriteFile(p2, []byte(strings.Replace(validManifestYAML, "gen-1", "gen-9", 1)), 0o644))
	m3, err := LoadManifest(p2)
	require.NoError(t, err)
	require.NotEqual(t, m1.Digest(), m3.Digest())
}

func TestDefaultLedgerPath(t *testing.T) {
	t.Parallel()
	require.Equal(t,
		filepath.Join("dir", "rollout.ledger.json"),
		DefaultLedgerPath(filepath.Join("dir", "rollout.yaml")))
}
