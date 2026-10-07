// Package reconcile provides ownership reconciliation for multiple applications that share a
// single database.
//
// Each application owns its own goose version table. A registry file declares which application
// owns which version table, where its migration files live and which applications it depends on.
// The [Runner] reconciles the registry against the database:
//
//   - version tables registered to more than one application are reported as conflicts and never
//     touched;
//   - goose version tables present in the database but absent from the registry are reported as
//     unregistered and never touched;
//   - a registered table containing applied versions unknown to the owning application's
//     migration list is reported as drift and never touched;
//   - all other (confirmed) applications are converged with [github.com/pressly/goose/v3.Provider.Up]
//     in dependency order.
//
// A JSON ledger with the last convergence result is written for every application. Convergence is
// idempotent: interrupted or repeated runs re-discover pending migrations and converge again.
package reconcile

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"

	"github.com/pressly/goose/v3/database"
	yaml "go.yaml.in/yaml/v3"
)

// DefaultLedgerDir is the default directory used to store one JSON ledger per application.
const DefaultLedgerDir = ".goose-reconcile"

const supportedRegistryVersion = 1

// appNamePattern restricts application names so they can safely be used as ledger file names.
var appNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// Config is the registry of applications sharing a database.
type Config struct {
	// Dialect is the goose dialect shared by all applications, e.g. "mysql", "tidb".
	Dialect string `yaml:"dialect"`
	// LedgerDir is the directory JSON ledgers are written to. Defaults to [DefaultLedgerDir].
	LedgerDir string `yaml:"ledger_dir"`
	// Apps is the list of registered applications, in declaration order.
	Apps []AppSpec `yaml:"apps"`

	// digest is the sha256 of the canonical registry bytes, recorded in every ledger.
	digest string `yaml:"-"`
}

// AppSpec declares a single application and the goose version table it owns.
type AppSpec struct {
	// Name is the unique application identifier.
	Name string `yaml:"name"`
	// Table is the goose version table owned by this application.
	Table string `yaml:"table"`
	// Dir is the directory containing the application's migration files. When loading from a
	// registry file it is resolved relative to the registry file. It is ignored when FS is set.
	Dir string `yaml:"dir"`
	// DependsOn lists application names that must be converged before this one.
	DependsOn []string `yaml:"depends_on"`
	// AllowMissing allows out-of-order migrations for this application, equivalent to
	// [github.com/pressly/goose/v3.WithAllowOutofOrder].
	AllowMissing bool `yaml:"allow_missing"`

	// FS is the filesystem containing the migration files at its root. When nil, the runner uses
	// os.DirFS(Dir). Set this when building [Config] programmatically, for example with an
	// embedded filesystem.
	FS fs.FS `yaml:"-"`
}

type registryFile struct {
	Version   int       `yaml:"version"`
	Dialect   string    `yaml:"dialect"`
	LedgerDir string    `yaml:"ledger_dir"`
	Apps      []AppSpec `yaml:"apps"`
}

// LoadConfig loads and validates a YAML registry file.
//
// Application migration directories and a relative ledger directory are resolved relative to the
// directory containing the registry file.
func LoadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("reconcile: open registry %q: %w", path, err)
	}
	defer f.Close()
	baseDir := filepath.Dir(path)
	cfg, err := ParseConfig(f, baseDir)
	if err != nil {
		return nil, fmt.Errorf("reconcile: registry %q: %w", path, err)
	}
	return cfg, nil
}

// ParseConfig parses a YAML registry from r. baseDir is used to resolve relative migration
// directories and relative ledger directories; pass "." when building a config in memory.
func ParseConfig(r io.Reader, baseDir string) (*Config, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read registry: %w", err)
	}
	var file registryFile
	if err := yaml.Unmarshal(raw, &file); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	if file.Version != supportedRegistryVersion {
		return nil, fmt.Errorf("unsupported registry version: got %d, want %d", file.Version, supportedRegistryVersion)
	}
	if baseDir == "" {
		baseDir = "."
	}
	cfg := &Config{
		Dialect:   file.Dialect,
		LedgerDir: file.LedgerDir,
		Apps:      file.Apps,
		digest:    "sha256:" + sha256Hex(raw),
	}
	if cfg.LedgerDir == "" {
		cfg.LedgerDir = DefaultLedgerDir
	}
	if !filepath.IsAbs(cfg.LedgerDir) {
		cfg.LedgerDir = filepath.Join(baseDir, cfg.LedgerDir)
	}
	for i := range cfg.Apps {
		app := &cfg.Apps[i]
		if app.FS == nil {
			dir := app.Dir
			if !filepath.IsAbs(dir) {
				dir = filepath.Join(baseDir, dir)
			}
			app.FS = os.DirFS(dir)
		}
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate checks the registry for structural errors. An empty dialect is allowed here so a
// registry file can omit it and have the caller fill it in (as the CLI does from the driver);
// [New] requires a non-empty, known dialect. Tables claimed by more than one application are not
// validation errors: they are classified as conflicts by the [Runner].
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("reconcile: nil config")
	}
	if c.Dialect != "" && !isKnownDialect(database.Dialect(c.Dialect)) {
		return fmt.Errorf("reconcile: unknown dialect %q", c.Dialect)
	}
	if len(c.Apps) == 0 {
		return fmt.Errorf("reconcile: registry must declare at least one app")
	}
	names := make(map[string]bool, len(c.Apps))
	for i := range c.Apps {
		app := &c.Apps[i]
		if app.Name == "" {
			return fmt.Errorf("reconcile: app at index %d: name must not be empty", i)
		}
		if !appNamePattern.MatchString(app.Name) {
			return fmt.Errorf("reconcile: app %q: name may only contain letters, digits, dot, underscore or hyphen and must start alphanumerically", app.Name)
		}
		if names[app.Name] {
			return fmt.Errorf("reconcile: duplicate app name %q", app.Name)
		}
		names[app.Name] = true
		if app.Table == "" {
			return fmt.Errorf("reconcile: app %q: table must not be empty", app.Name)
		}
		if app.FS == nil && app.Dir == "" {
			return fmt.Errorf("reconcile: app %q: either dir or FS must be set", app.Name)
		}
	}
	for _, app := range c.Apps {
		seen := make(map[string]bool, len(app.DependsOn))
		for _, dep := range app.DependsOn {
			if dep == app.Name {
				return fmt.Errorf("reconcile: app %q must not depend on itself", app.Name)
			}
			if !names[dep] {
				return fmt.Errorf("reconcile: app %q depends on unknown app %q", app.Name, dep)
			}
			if seen[dep] {
				return fmt.Errorf("reconcile: app %q lists dependency %q more than once", app.Name, dep)
			}
			seen[dep] = true
		}
	}
	return nil
}

// digestOrDefault returns the registry file digest, or a deterministic digest of the config for
// registries built programmatically.
func (c *Config) digestOrDefault() string {
	if c.digest != "" {
		return c.digest
	}
	b, err := yaml.Marshal(c)
	if err != nil {
		return ""
	}
	return "sha256:" + sha256Hex(b)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// isKnownDialect reports whether d is one of goose's built-in dialects.
func isKnownDialect(d database.Dialect) bool {
	switch d {
	case database.DialectClickHouse,
		database.DialectAuroraDSQL,
		database.DialectMSSQL,
		database.DialectMySQL,
		database.DialectPostgres,
		database.DialectRedshift,
		database.DialectSQLite3,
		database.DialectSpanner,
		database.DialectStarrocks,
		database.DialectTiDB,
		database.DialectTurso,
		database.DialectVertica,
		database.DialectYdB:
		return true
	default:
		return false
	}
}

// topoOrder returns the application names in dependency order (dependencies first) using Kahn's
// algorithm. Applications independent of each other keep their declaration order. It returns an
// error if the dependency graph contains a cycle.
func topoOrder(apps []AppSpec) ([]string, error) {
	byName := make(map[string]*AppSpec, len(apps))
	indegree := make(map[string]int, len(apps))
	dependents := make(map[string][]string, len(apps))
	declared := make([]string, 0, len(apps))
	for i := range apps {
		app := &apps[i]
		byName[app.Name] = app
		declared = append(declared, app.Name)
		indegree[app.Name] = len(app.DependsOn)
		for _, dep := range app.DependsOn {
			dependents[dep] = append(dependents[dep], app.Name)
		}
	}
	queue := make([]string, 0, len(apps))
	for _, name := range declared { // deterministic seed order
		if indegree[name] == 0 {
			queue = append(queue, name)
		}
	}
	order := make([]string, 0, len(apps))
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		order = append(order, name)
		next := slices.Clone(dependents[name])
		slices.Sort(next)
		for _, dependent := range next {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}
	if len(order) != len(apps) {
		var remaining []string
		for name, n := range indegree {
			if n > 0 {
				remaining = append(remaining, name)
			}
		}
		slices.Sort(remaining)
		return nil, fmt.Errorf("reconcile: dependency cycle detected among apps: %s", joinNames(remaining))
	}
	return order, nil
}

func joinNames(names []string) string {
	out := ""
	for i, name := range names {
		if i > 0 {
			out += ", "
		}
		out += name
	}
	return out
}
