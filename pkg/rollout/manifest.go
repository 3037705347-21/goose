package rollout

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	gooselib "github.com/pressly/goose/v3"
	yaml "go.yaml.in/yaml/v3"
)

// supportedManifestVersion is the only manifest schema version understood by
// this package.
const supportedManifestVersion = 1

// Manifest is a generation rollout manifest. It declares the databases
// (targets) and the ordered generations that converge them to specific goose
// versions.
type Manifest struct {
	// Version is the manifest schema version.
	Version int `yaml:"version"`
	// MigrationsDir is the directory shared by all targets and containing the
	// migration files. Relative paths are resolved against the manifest file
	// directory when loaded with [LoadManifest].
	MigrationsDir string `yaml:"migrations_dir"`
	// Targets is the set of databases that may participate in the rollout.
	Targets []Target `yaml:"targets"`
	// Generations are processed strictly in order.
	Generations []Generation `yaml:"generations"`

	// raw is the unmodified file content used for the content digest. It is
	// populated by [LoadManifest].
	raw []byte
	// path is the manifest file path, populated by [LoadManifest].
	path string
	// lookup maps target name to target for O(1) reference resolution.
	lookup map[string]*Target
}

// Target describes one database in the rollout fleet.
type Target struct {
	// Name uniquely identifies the target within the manifest.
	Name string `yaml:"name"`
	// Driver is the goose driver name, e.g. "postgres", "mysql", "sqlite3".
	Driver string `yaml:"driver"`
	// DSN is the connection string passed to the driver.
	DSN string `yaml:"dsn"`
}

// Generation declares one rollout wave: the listed targets must converge to
// TargetVersion before the next generation may start.
type Generation struct {
	// Name uniquely identifies the generation within the manifest.
	Name string `yaml:"name"`
	// TargetVersion is the goose version every listed target must reach.
	TargetVersion int64 `yaml:"target_version"`
	// Targets lists target names participating in this generation.
	Targets []string `yaml:"targets"`
}

// LoadManifest reads, parses and validates the manifest at the given path.
//
// Relative migrations_dir values are resolved against the directory of the
// manifest file.
func LoadManifest(path string) (*Manifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest %q: %w", path, err)
	}
	m := &Manifest{raw: raw, path: path}
	if err := yaml.Unmarshal(raw, m); err != nil {
		return nil, fmt.Errorf("parse manifest %q: %w", path, err)
	}
	if m.MigrationsDir == "" {
		return nil, fmt.Errorf("validate manifest %q: migrations_dir must not be empty", path)
	}
	if !filepath.IsAbs(m.MigrationsDir) {
		m.MigrationsDir = filepath.Join(filepath.Dir(path), m.MigrationsDir)
	}
	if err := m.validate(); err != nil {
		return nil, fmt.Errorf("validate manifest %q: %w", path, err)
	}
	return m, nil
}

// Path returns the file path the manifest was loaded from.
func (m *Manifest) Path() string { return m.path }

// Target returns the target with the given name and reports whether it exists.
func (m *Manifest) Target(name string) (Target, bool) {
	if t, ok := m.lookup[name]; ok {
		return *t, true
	}
	return Target{}, false
}

// Digest returns a hex-encoded SHA-256 digest of the manifest's raw file
// content. The ledger binds to this digest to detect in-place manifest changes.
func (m *Manifest) Digest() string {
	sum := sha256.Sum256(m.raw)
	return hex.EncodeToString(sum[:])
}

// DefaultLedgerPath returns the default ledger path for a manifest file:
// "<manifest-dir>/<manifest-base-without-ext>.ledger.json".
func DefaultLedgerPath(manifestPath string) string {
	dir, file := filepath.Split(manifestPath)
	ext := filepath.Ext(file)
	base := file[:len(file)-len(ext)]
	return filepath.Join(dir, base+".ledger.json")
}

func (m *Manifest) validate() error {
	if m.Version != supportedManifestVersion {
		return fmt.Errorf("unsupported manifest version %d, expected %d", m.Version, supportedManifestVersion)
	}
	if m.MigrationsDir == "" {
		return errors.New("migrations_dir must not be empty")
	}
	if len(m.Targets) == 0 {
		return errors.New("at least one target is required")
	}
	if len(m.Generations) == 0 {
		return errors.New("at least one generation is required")
	}

	m.lookup = make(map[string]*Target, len(m.Targets))
	for i := range m.Targets {
		t := &m.Targets[i]
		if t.Name == "" {
			return fmt.Errorf("targets[%d]: name must not be empty", i)
		}
		if t.Driver == "" {
			return fmt.Errorf("target %q: driver must not be empty", t.Name)
		}
		if t.DSN == "" {
			return fmt.Errorf("target %q: dsn must not be empty", t.Name)
		}
		if _, exists := m.lookup[t.Name]; exists {
			return fmt.Errorf("duplicate target name %q", t.Name)
		}
		m.lookup[t.Name] = t
	}

	available, err := migrationVersions(m.MigrationsDir)
	if err != nil {
		return err
	}

	genNames := make(map[string]bool, len(m.Generations))
	// lastVersion tracks the most recent target version for every target, to
	// enforce strictly increasing versions across generations.
	lastVersion := make(map[string]int64, len(m.Targets))

	for gi := range m.Generations {
		g := &m.Generations[gi]
		if g.Name == "" {
			return fmt.Errorf("generations[%d]: name must not be empty", gi)
		}
		if genNames[g.Name] {
			return fmt.Errorf("duplicate generation name %q", g.Name)
		}
		genNames[g.Name] = true
		if g.TargetVersion < 1 {
			return fmt.Errorf("generation %q: target_version must be >= 1", g.Name)
		}
		if !available[g.TargetVersion] {
			return fmt.Errorf("generation %q: target_version %d is not a migration in %q",
				g.Name, g.TargetVersion, m.MigrationsDir)
		}
		if len(g.Targets) == 0 {
			return fmt.Errorf("generation %q: at least one target is required", g.Name)
		}
		seen := make(map[string]bool, len(g.Targets))
		for _, name := range g.Targets {
			if _, ok := m.lookup[name]; !ok {
				return fmt.Errorf("generation %q: unknown target %q", g.Name, name)
			}
			if seen[name] {
				return fmt.Errorf("generation %q: target %q listed more than once", g.Name, name)
			}
			seen[name] = true
			if prev, ok := lastVersion[name]; ok && g.TargetVersion <= prev {
				return fmt.Errorf(
					"generation %q: target_version %d for target %q must be greater than previous generation version %d",
					g.Name, g.TargetVersion, name, prev)
			}
			lastVersion[name] = g.TargetVersion
		}
	}
	return nil
}

// migrationVersions scans dir for migration versions using the same filename
// rules as goose itself. Non-migration files are ignored.
func migrationVersions(dir string) (map[int64]bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations_dir %q: %w", dir, err)
	}
	versions := make(map[int64]bool)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		v, err := gooselib.NumericComponent(entry.Name())
		if err != nil {
			continue
		}
		versions[v] = true
	}
	if len(versions) == 0 {
		return nil, fmt.Errorf("no migration files found in %q", dir)
	}
	return versions, nil
}
