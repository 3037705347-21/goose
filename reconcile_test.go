package goose_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

func mapMigrationFS(files map[string]string) fs.FS {
	m := fstest.MapFS{}
	for name, data := range files {
		m[name] = &fstest.MapFile{Data: []byte(data)}
	}
	return m
}

const (
	pkgV1 = `-- +goose Up
CREATE TABLE foo (id INTEGER PRIMARY KEY);
-- +goose Down
DROP TABLE foo;
`
	pkgV1Modified = `-- +goose Up
CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT);
-- +goose Down
DROP TABLE foo;
`
	localV2 = `-- +goose Up
CREATE TABLE bar (id INTEGER PRIMARY KEY);
-- +goose Down
DROP TABLE bar;
`
	pkgV2 = `-- +goose Up
CREATE TABLE package_bar (id INTEGER PRIMARY KEY);
-- +goose Down
DROP TABLE package_bar;
`
)

func findConflict(t *testing.T, err error, kind goose.ConflictKind, version int64) goose.Conflict {
	t.Helper()
	var reconcileErr *goose.ReconcileError
	require.ErrorAs(t, err, &reconcileErr)
	for _, c := range reconcileErr.Conflicts {
		if c.Kind == kind && c.Version == version {
			return c
		}
	}
	require.Failf(t, "conflict not found", "kind=%s version=%d in %v", kind, version, reconcileErr.Conflicts)
	return goose.Conflict{}
}

func TestReconcileHappyPath(t *testing.T) {
	packageFS := mapMigrationFS(map[string]string{
		"00001_init.sql": pkgV1,
	})
	localFS := mapMigrationFS(map[string]string{
		"00002_bar.sql": localV2,
	})
	r, err := goose.NewReconciler([]goose.MigrationSource{
		goose.NewMigrationSource("package", packageFS),
		goose.NewMigrationSource("local", localFS),
	})
	require.NoError(t, err)

	manifest, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, goose.ManifestVersion, manifest.ManifestVersion)
	require.Equal(t, []string{"local", "package"}, manifest.Sources)
	require.Len(t, manifest.Entries, 2)
	require.Equal(t, int64(1), manifest.Entries[0].Version)
	require.Equal(t, "package", manifest.Entries[0].Owner)
	require.Equal(t, int64(2), manifest.Entries[1].Version)
	require.Equal(t, "local", manifest.Entries[1].Owner)
	require.NotEmpty(t, manifest.Entries[0].Checksum)
	require.Equal(t, goose.TypeSQL, manifest.Entries[0].Type)

	// Export must be deterministic: same inputs produce identical bytes.
	var first, second bytes.Buffer
	require.NoError(t, manifest.Export(&first))
	manifest2, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	require.NoError(t, manifest2.Export(&second))
	require.Equal(t, first.String(), second.String())
}

func TestReconcileDuplicateVersions(t *testing.T) {
	// Identical contents: ambiguous ownership.
	t.Run("identical content", func(t *testing.T) {
		packageFS := mapMigrationFS(map[string]string{
			"00001_init.sql": pkgV1,
			"00002_bar.sql":  localV2,
		})
		localFS := mapMigrationFS(map[string]string{
			"00002_bar.sql": localV2,
		})
		r, err := goose.NewReconciler([]goose.MigrationSource{
			goose.NewMigrationSource("package", packageFS),
			goose.NewMigrationSource("local", localFS),
		})
		require.NoError(t, err)
		_, err = r.Reconcile(context.Background())
		require.ErrorIs(t, err, goose.ErrReconcileConflict)
		c := findConflict(t, err, goose.ConflictDuplicateVersion, 2)
		require.Equal(t, []string{"local", "package"}, c.Owners)
		require.Len(t, c.Paths, 2)
	})

	// Different contents: diverged, the dangerous case seen in production.
	t.Run("diverged content", func(t *testing.T) {
		packageFS := mapMigrationFS(map[string]string{
			"00002_bar.sql": pkgV2,
		})
		localFS := mapMigrationFS(map[string]string{
			"00002_bar.sql": localV2,
		})
		r, err := goose.NewReconciler([]goose.MigrationSource{
			goose.NewMigrationSource("package", packageFS),
			goose.NewMigrationSource("local", localFS),
		})
		require.NoError(t, err)
		_, err = r.Reconcile(context.Background())
		require.ErrorIs(t, err, goose.ErrReconcileConflict)
		findConflict(t, err, goose.ConflictContentDiverged, 2)
	})
}

func TestReconcileMissingSource(t *testing.T) {
	missing := os.DirFS(filepath.Join(t.TempDir(), "does-not-exist"))

	t.Run("required source missing", func(t *testing.T) {
		r, err := goose.NewReconciler([]goose.MigrationSource{
			goose.NewMigrationSource("package", mapMigrationFS(map[string]string{"00001_init.sql": pkgV1})),
			goose.NewMigrationSource("local", missing),
		})
		require.NoError(t, err)
		_, err = r.Reconcile(context.Background())
		require.ErrorIs(t, err, goose.ErrReconcileConflict)
		c := findConflict(t, err, goose.ConflictSourceMissing, 0)
		require.Equal(t, "local", c.Source)
	})

	t.Run("optional source missing is tolerated", func(t *testing.T) {
		r, err := goose.NewReconciler([]goose.MigrationSource{
			goose.NewMigrationSource("package", mapMigrationFS(map[string]string{"00001_init.sql": pkgV1})),
			{Name: "local", FS: missing, Required: false},
		})
		require.NoError(t, err)
		manifest, err := r.Reconcile(context.Background())
		require.NoError(t, err)
		require.Equal(t, []string{"package"}, manifest.Sources)
	})
}

func TestReconcileOfflineDrift(t *testing.T) {
	prior := &goose.Manifest{
		ManifestVersion: goose.ManifestVersion,
		Sources:         []string{"local", "package"},
		Entries: []goose.ManifestEntry{
			{Version: 1, Owner: "package", Type: goose.TypeSQL, Path: "00001_init.sql", Checksum: "deadbeef"},
			{Version: 2, Owner: "local", Type: goose.TypeSQL, Path: "00002_bar.sql", Checksum: "cafebabe"},
			{Version: 3, Owner: "local", Type: goose.TypeSQL, Path: "00003_baz.sql", Checksum: "old123"},
		},
	}

	// v1 content changes, v2 moves from local to package, v3 vanishes.
	r, err := goose.NewReconciler([]goose.MigrationSource{
		goose.NewMigrationSource("package", mapMigrationFS(map[string]string{
			"00001_init.sql": pkgV1Modified,
			"00002_bar.sql":  pkgV2,
		})),
		goose.NewMigrationSource("local", mapMigrationFS(nil)),
	}, goose.WithPriorManifest(prior))
	require.NoError(t, err)
	_, err = r.Reconcile(context.Background())
	require.ErrorIs(t, err, goose.ErrReconcileConflict)
	findConflict(t, err, goose.ConflictModified, 1)
	findConflict(t, err, goose.ConflictOwnershipChanged, 2)
	findConflict(t, err, goose.ConflictEntryVanished, 3)
}

func TestReconcileWithApplied(t *testing.T) {
	// v2 was local and is now provided by package; v3 was local and vanished.
	prior := &goose.Manifest{
		ManifestVersion: goose.ManifestVersion,
		Sources:         []string{"package"},
		Entries: []goose.ManifestEntry{
			{Version: 2, Owner: "local", Type: goose.TypeSQL, Path: "00002_bar.sql", Checksum: "whatever"},
			{Version: 3, Owner: "local", Type: goose.TypeSQL, Path: "00003_baz.sql", Checksum: "whatever"},
		},
	}
	packageFS := mapMigrationFS(map[string]string{
		"00001_init.sql": pkgV1,
		"00002_bar.sql":  pkgV2,
	})
	r, err := goose.NewReconciler([]goose.MigrationSource{
		goose.NewMigrationSource("package", packageFS),
	}, goose.WithPriorManifest(prior))
	require.NoError(t, err)

	t.Run("applied versions are protected", func(t *testing.T) {
		// v2 applied under local, v3 applied and now vanished.
		_, err := r.ReconcileWithApplied(context.Background(), []int64{2, 3})
		require.ErrorIs(t, err, goose.ErrReconcileConflict)
		findConflict(t, err, goose.ConflictOwnershipChanged, 2)
		findConflict(t, err, goose.ConflictEntryVanished, 3)
	})

	t.Run("pending versions may change owners and converge", func(t *testing.T) {
		manifest, err := r.ReconcileWithApplied(context.Background(), nil)
		require.NoError(t, err)
		require.Len(t, manifest.Entries, 2) // v3 dropped, v2 adopted by package, v1 new
		var v2 *goose.ManifestEntry
		for i := range manifest.Entries {
			if manifest.Entries[i].Version == 2 {
				v2 = &manifest.Entries[i]
			}
		}
		require.NotNil(t, v2)
		require.Equal(t, "package", v2.Owner)
	})

	t.Run("applied version with no source and no prior entry", func(t *testing.T) {
		empty, err := goose.NewReconciler([]goose.MigrationSource{
			goose.NewMigrationSource("package", mapMigrationFS(map[string]string{"00001_init.sql": pkgV1})),
		})
		require.NoError(t, err)
		_, err = empty.ReconcileWithApplied(context.Background(), []int64{9})
		require.ErrorIs(t, err, goose.ErrReconcileConflict)
		findConflict(t, err, goose.ConflictEntryVanished, 9)
	})
}

func TestManifestAtomicSaveConverges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	manifest := &goose.Manifest{
		Sources: []string{"local", "package"},
		Entries: []goose.ManifestEntry{
			{Version: 2, Owner: "local", Type: goose.TypeSQL, Path: "00002_bar.sql", Checksum: "bbb"},
			{Version: 1, Owner: "package", Type: goose.TypeSQL, Path: "00001_init.sql", Checksum: "aaa"},
		},
	}
	require.NoError(t, goose.SaveManifest(path, manifest))

	first, err := os.ReadFile(path)
	require.NoError(t, err)

	// Simulate a crash mid-write: a partial temporary file is left behind and the existing
	// manifest stays intact. Re-saving must converge to identical bytes and clean the temp file.
	require.NoError(t, os.WriteFile(path+".tmp", []byte("{partial garbage"), 0o644))
	require.NoError(t, goose.SaveManifest(path, manifest))
	require.NoError(t, goose.SaveManifest(path, manifest))

	second, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, first, second, "re-running the write must converge")
	require.True(t, bytes.HasSuffix(second, []byte("\n")))
	require.True(t, bytes.Contains(second, []byte(`"owner": "package"`)))
	require.True(t, bytes.Contains(second, []byte(`"manifest_version": 1`)))

	_, statErr := os.Stat(path + ".tmp")
	require.True(t, errors.Is(statErr, fs.ErrNotExist), "stale temp file must not remain")

	loaded, err := goose.LoadManifest(path)
	require.NoError(t, err)
	require.Equal(t, int64(1), loaded.Entries[0].Version, "entries must be sorted by version on load")

	missing, err := goose.LoadManifest(filepath.Join(t.TempDir(), "nope.json"))
	require.NoError(t, err)
	require.Nil(t, missing)
}

// *** End-to-end provider tests with sqlite and real deployment directories. ***

func writeMultiSourceDirs(t *testing.T) (root, pkgDir, localDir string) {
	t.Helper()
	root = t.TempDir()
	pkgDir = filepath.Join(root, "package")
	localDir = filepath.Join(root, "local")
	require.NoError(t, os.MkdirAll(pkgDir, 0o755))
	require.NoError(t, os.MkdirAll(localDir, 0o755))
	return root, pkgDir, localDir
}

func writeSQL(t *testing.T, dir, name, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644))
}

func multiSourceProvider(t *testing.T, db *sql.DB, root string) *goose.Provider {
	t.Helper()
	p, err := goose.NewProvider(
		goose.DialectSQLite3, db, nil,
		goose.WithMigrationSources(
			goose.NewMigrationSource("package", os.DirFS(filepath.Join(root, "package"))),
			goose.NewMigrationSource("local", os.DirFS(filepath.Join(root, "local"))),
		),
		goose.WithManifestPath(filepath.Join(root, "goose-manifest.json")),
	)
	require.NoError(t, err)
	return p
}

func TestProviderMultiSourceHappyPath(t *testing.T) {
	root, pkgDir, localDir := writeMultiSourceDirs(t)
	writeSQL(t, pkgDir, "00001_init.sql", pkgV1)
	writeSQL(t, localDir, "00002_bar.sql", localV2)

	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "app.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	p := multiSourceProvider(t, db, root)

	sources := p.ListSources()
	require.Len(t, sources, 2)
	require.Equal(t, "package", sources[0].Owner)
	require.Equal(t, "local", sources[1].Owner)

	results, err := p.Up(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, "package", results[0].Source.Owner)
	require.Equal(t, "local", results[1].Source.Owner)

	// The manifest was written and attributes every version to its owner.
	manifest, err := goose.LoadManifest(filepath.Join(root, "goose-manifest.json"))
	require.NoError(t, err)
	require.NotNil(t, manifest)
	require.Len(t, manifest.Entries, 2)
	require.Equal(t, "package", manifest.Entries[0].Owner)
	require.Equal(t, "local", manifest.Entries[1].Owner)
	require.Equal(t, manifest.Entries, p.ReconciledManifest().Entries)

	// SQL from both filesystems actually executed.
	version, err := p.GetDBVersion(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), version)
}

func TestProviderMultiSourceDuplicateRefusedAtConstruction(t *testing.T) {
	root, pkgDir, localDir := writeMultiSourceDirs(t)
	writeSQL(t, pkgDir, "00001_init.sql", pkgV1)
	writeSQL(t, pkgDir, "00002_bar.sql", pkgV2)
	writeSQL(t, localDir, "00002_bar.sql", localV2)

	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "app.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = goose.NewProvider(
		goose.DialectSQLite3, db, nil,
		goose.WithMigrationSources(
			goose.NewMigrationSource("package", os.DirFS(filepath.Join(root, "package"))),
			goose.NewMigrationSource("local", os.DirFS(filepath.Join(root, "local"))),
		),
	)
	require.ErrorIs(t, err, goose.ErrReconcileConflict)
	findConflict(t, err, goose.ConflictContentDiverged, 2)
}

func TestProviderMultiSourceDirectoryDeleted(t *testing.T) {
	root, pkgDir, localDir := writeMultiSourceDirs(t)
	writeSQL(t, pkgDir, "00001_init.sql", pkgV1)
	writeSQL(t, localDir, "00002_bar.sql", localV2)

	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "app.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	p := multiSourceProvider(t, db, root)
	_, err = p.Up(context.Background())
	require.NoError(t, err)
	require.NoError(t, p.Close())

	// The local deployment directory is deleted entirely.
	require.NoError(t, os.RemoveAll(localDir))

	_, err = goose.NewProvider(
		goose.DialectSQLite3, db, nil,
		goose.WithMigrationSources(
			goose.NewMigrationSource("package", os.DirFS(filepath.Join(root, "package"))),
			goose.NewMigrationSource("local", os.DirFS(filepath.Join(root, "local"))),
		),
		goose.WithManifestPath(filepath.Join(root, "goose-manifest.json")),
	)
	require.ErrorIs(t, err, goose.ErrReconcileConflict)
	findConflict(t, err, goose.ConflictSourceMissing, 0)
}

func TestProviderAppliedVersionOwnerTakeover(t *testing.T) {
	root, pkgDir, localDir := writeMultiSourceDirs(t)
	writeSQL(t, pkgDir, "00001_init.sql", pkgV1)
	writeSQL(t, localDir, "00002_bar.sql", localV2)

	dbPath := filepath.Join(t.TempDir(), "app.db")
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)

	p := multiSourceProvider(t, db, root)
	_, err = p.Up(context.Background())
	require.NoError(t, err)
	require.NoError(t, p.Close())

	// New deploy: local drops 00002 and the package ships a same-numbered, different migration.
	require.NoError(t, os.Remove(filepath.Join(localDir, "00002_bar.sql")))
	writeSQL(t, pkgDir, "00002_bar.sql", pkgV2)

	db2, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	// Construction itself is fine (single owner now), but any execution must be refused by the
	// database guard.
	p2 := multiSourceProvider(t, db2, root)
	_, err = p2.Up(context.Background())
	require.ErrorIs(t, err, goose.ErrReconcileConflict)
	findConflict(t, err, goose.ConflictOwnershipChanged, 2)

	// The guard refuses every operation, including read-only ones.
	_, err = p2.Status(context.Background())
	require.ErrorIs(t, err, goose.ErrReconcileConflict)

	// The applied version table must be untouched.
	var count int
	require.NoError(t, db2.QueryRow(`SELECT count(*) FROM goose_db_version WHERE version_id = 2`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestProviderAppliedVersionModified(t *testing.T) {
	root, pkgDir, _ := writeMultiSourceDirs(t)
	writeSQL(t, pkgDir, "00001_init.sql", pkgV1)

	dbPath := filepath.Join(t.TempDir(), "app.db")
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)

	p := multiSourceProvider(t, db, root)
	_, err = p.Up(context.Background())
	require.NoError(t, err)
	require.NoError(t, p.Close())

	// Same owner, but the already applied migration file was edited.
	writeSQL(t, pkgDir, "00001_init.sql", pkgV1Modified)

	db2, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	p2 := multiSourceProvider(t, db2, root)
	_, err = p2.Up(context.Background())
	require.ErrorIs(t, err, goose.ErrReconcileConflict)
	findConflict(t, err, goose.ConflictModified, 1)
}

func TestProviderAppliedVersionVanished(t *testing.T) {
	root, pkgDir, localDir := writeMultiSourceDirs(t)
	writeSQL(t, pkgDir, "00001_init.sql", pkgV1)
	writeSQL(t, localDir, "00002_bar.sql", localV2)

	dbPath := filepath.Join(t.TempDir(), "app.db")
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)

	p := multiSourceProvider(t, db, root)
	_, err = p.Up(context.Background())
	require.NoError(t, err)
	require.NoError(t, p.Close())

	// The directory survives but the applied migration file disappears and a newer one appears:
	// this is the "migrations keep running anyway" scenario.
	require.NoError(t, os.Remove(filepath.Join(localDir, "00002_bar.sql")))
	writeSQL(t, localDir, "00003_baz.sql", `-- +goose Up
CREATE TABLE baz (id INTEGER PRIMARY KEY);
`)

	db2, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	p2 := multiSourceProvider(t, db2, root)
	_, err = p2.Up(context.Background())
	require.ErrorIs(t, err, goose.ErrReconcileConflict)
	findConflict(t, err, goose.ConflictEntryVanished, 2)
}

func TestProviderPendingMigrationChangesConverge(t *testing.T) {
	root, pkgDir, _ := writeMultiSourceDirs(t)
	writeSQL(t, pkgDir, "00001_init.sql", pkgV1)
	writeSQL(t, pkgDir, "00002_bar.sql", pkgV2)

	dbPath := filepath.Join(t.TempDir(), "app.db")
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)

	p := multiSourceProvider(t, db, root)
	// A read-only operation triggers reconciliation and writes the manifest; nothing is applied.
	_, err = p.Status(context.Background())
	require.NoError(t, err)
	require.NoError(t, p.Close())

	// Pending migrations may still be edited; the next run adopts the new contents instead of
	// refusing.
	writeSQL(t, pkgDir, "00002_bar.sql", localV2)

	db2, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	p2 := multiSourceProvider(t, db2, root)
	_, err = p2.Up(context.Background())
	require.NoError(t, err)
	require.Equal(t, "package", p2.ReconciledManifest().Entries[1].Owner)
}

func TestProviderMultiSourceOptionValidation(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "app.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	t.Run("fsys must be nil", func(t *testing.T) {
		_, err := goose.NewProvider(
			goose.DialectSQLite3, db, mapMigrationFS(map[string]string{"00001_init.sql": pkgV1}),
			goose.WithMigrationSources(
				goose.NewMigrationSource("package", mapMigrationFS(map[string]string{"00001_init.sql": pkgV1})),
			),
		)
		require.ErrorContains(t, err, "fsys must be nil")
	})

	t.Run("manifest requires sources", func(t *testing.T) {
		_, err := goose.NewProvider(
			goose.DialectSQLite3, db, nil,
			goose.WithManifestPath(filepath.Join(t.TempDir(), "m.json")),
		)
		require.ErrorContains(t, err, "WithManifestPath requires WithMigrationSources")
	})

	t.Run("duplicate source names rejected", func(t *testing.T) {
		fsys := mapMigrationFS(map[string]string{"00001_init.sql": pkgV1})
		_, err := goose.NewProvider(
			goose.DialectSQLite3, db, nil,
			goose.WithMigrationSources(
				goose.NewMigrationSource("same", fsys),
				goose.NewMigrationSource("same", fsys),
			),
		)
		require.ErrorContains(t, err, "duplicate migration source name")
	})
}

// Guard against accidental drift of the exported format.
func TestManifestExportFormat(t *testing.T) {
	_, pkgDir, _ := writeMultiSourceDirs(t)
	writeSQL(t, pkgDir, "00001_init.sql", pkgV1)

	r, err := goose.NewReconciler([]goose.MigrationSource{
		goose.NewMigrationSource("package", os.DirFS(pkgDir)),
	})
	require.NoError(t, err)
	manifest, err := r.Reconcile(context.Background())
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, manifest.Export(&buf))
	require.Contains(t, buf.String(), fmt.Sprintf(`"owner": "package"`))
	require.Contains(t, buf.String(), `"sources": [
    "package"
  ]`)
}
