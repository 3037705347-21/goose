package reconcile

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
)

// Runner reconciles registered applications against a shared database. A Runner is safe to run
// repeatedly; convergence is idempotent.
type Runner struct {
	db   *sql.DB
	cfg  *Config
	opts options
}

// New creates a Runner for cfg against db.
func New(db *sql.DB, cfg *Config, opts ...Option) (*Runner, error) {
	if db == nil {
		return nil, fmt.Errorf("reconcile: db must not be nil")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Dialect == "" {
		return nil, fmt.Errorf("reconcile: dialect must not be empty")
	}
	r := &Runner{
		db:  db,
		cfg: cfg,
		opts: options{
			logger:    log.Default(),
			ledgerDir: cfg.LedgerDir,
			now:       time.Now,
		},
	}
	for _, opt := range opts {
		if err := opt(&r.opts); err != nil {
			return nil, err
		}
	}
	if r.opts.ledgerDir == "" {
		r.opts.ledgerDir = DefaultLedgerDir
	}
	return r, nil
}

// Run performs ownership reconciliation and converges every confirmed application in dependency
// order.
//
// Conflicts, unregistered tables, drift and application failures are returned both in the report
// and as an error: the report always carries the structured results, while the error is non-nil
// when [Report.HasAnomalies] is true. Hard failures (invalid registry, catalog access errors,
// dependency cycles) return a nil report.
func (r *Runner) Run(ctx context.Context) (*Report, error) {
	order, err := topoOrder(r.cfg.Apps)
	if err != nil {
		return nil, err
	}
	specs := make(map[string]*AppSpec, len(r.cfg.Apps))
	for i := range r.cfg.Apps {
		specs[r.cfg.Apps[i].Name] = &r.cfg.Apps[i]
	}
	results := make(map[string]*AppResult, len(r.cfg.Apps))
	for _, name := range order {
		spec := specs[name]
		results[name] = &AppResult{
			Name:      spec.Name,
			Table:     spec.Table,
			DependsOn: slices.Clone(spec.DependsOn),
		}
	}

	report := &Report{
		RanAt:              r.opts.now(),
		DryRun:             r.opts.dryRun,
		DiscoverySupported: false,
		Apps:               make([]AppResult, 0, len(order)),
	}

	// Tables registered by more than one application are conflicts: never build a provider and
	// never touch them.
	conflictCoOwners := make(map[string][]string)
	ownersByTable := make(map[string][]string)
	for _, app := range r.cfg.Apps {
		ownersByTable[app.Table] = append(ownersByTable[app.Table], app.Name)
	}
	for _, owners := range ownersByTable {
		if len(owners) < 2 {
			continue
		}
		sorted := slices.Clone(owners)
		slices.Sort(sorted)
		for _, owner := range sorted {
			others := slices.DeleteFunc(slices.Clone(sorted), func(n string) bool { return n == owner })
			conflictCoOwners[owner] = others
		}
	}

	// Discover goose version tables nobody registered. Only supported on dialects with a schema
	// catalog; other dialects skip discovery without failing.
	finder := r.opts.finder
	if finder == nil {
		finder = newTableFinder(database.Dialect(r.cfg.Dialect))
	}
	registeredTables := make(map[string]bool, len(r.cfg.Apps))
	for _, app := range r.cfg.Apps {
		registeredTables[app.Table] = true
	}
	if finder != nil {
		found, err := finder.findGooseTables(ctx, r.db)
		if err != nil {
			return nil, err
		}
		report.DiscoverySupported = true
		for _, table := range found {
			if !registeredTables[table] {
				report.UnregisteredTables = append(report.UnregisteredTables, table)
			}
		}
		slices.Sort(report.UnregisteredTables)
	}

	for _, name := range order {
		spec := specs[name]
		res := results[name]

		if blocker := blockedDependency(spec, results); blocker != "" {
			res.Status = StatusBlockedDependency
			res.Reason = fmt.Sprintf("dependency %q did not converge", blocker)
			r.record(spec, res)
			r.opts.logger.Printf("reconcile: app %q skipped: %s", name, res.Reason)
			continue
		}
		if coOwners, ok := conflictCoOwners[name]; ok {
			res.Status = StatusConflict
			res.Reason = fmt.Sprintf("version table %q is also registered to: %s", spec.Table, joinNames(coOwners))
			r.record(spec, res)
			r.opts.logger.Printf("reconcile: app %q conflict, not touched: %s", name, res.Reason)
			continue
		}

		r.converge(ctx, spec, res)
	}

	for _, name := range order {
		report.Apps = append(report.Apps, *results[name])
	}
	return report, report.Error()
}

// blockedDependency returns a direct dependency name whose result is anomalous. Because apps are
// processed in topological order, transitively blocked dependencies surface here as
// blocked_dependency on the direct dependency.
func blockedDependency(spec *AppSpec, results map[string]*AppResult) string {
	for _, dep := range spec.DependsOn {
		if r, ok := results[dep]; ok && r.Status != "" && r.Status.IsAnomaly() {
			return dep
		}
	}
	return ""
}

// converge builds the application's provider, verifies ownership against the recorded versions
// and runs Up when ownership is confirmed. It never returns an error: every outcome is recorded
// in res.
func (r *Runner) converge(ctx context.Context, spec *AppSpec, res *AppResult) {
	// Caller-supplied options first so the runner's table name and dialect settings always win.
	providerOpts := make([]goose.ProviderOption, 0, len(r.opts.providerOpts)+2)
	providerOpts = append(providerOpts, r.opts.providerOpts...)
	providerOpts = append(providerOpts, goose.WithTableName(spec.Table))
	if spec.AllowMissing {
		providerOpts = append(providerOpts, goose.WithAllowOutofOrder(true))
	}

	provider, err := goose.NewProvider(database.Dialect(r.cfg.Dialect), r.db, spec.FS, providerOpts...)
	if err != nil {
		r.fail(spec, res, fmt.Sprintf("create provider: %v", err))
		return
	}
	expected := sourceVersions(provider.ListSources())

	state, err := inspectTable(ctx, database.Dialect(r.cfg.Dialect), r.db, spec.Table)
	if err != nil {
		r.fail(spec, res, err.Error())
		return
	}
	res.TableExisted = state.exists
	res.ExpectedVersions = expected
	res.ObservedVersions = state.applied
	res.UnknownVersions = subtract(state.applied, expected)
	res.PendingBefore = subtract(expected, state.applied)
	res.VersionBefore = maxInt64(state.applied)
	res.TargetVersion = maxInt64(expected)

	if len(res.UnknownVersions) > 0 {
		res.Status = StatusDriftBlocked
		res.Reason = fmt.Sprintf("version table contains applied versions outside this app's migration list: %v", res.UnknownVersions)
		res.VersionAfter = res.VersionBefore
		prev, err := ReadLedger(r.opts.ledgerDir, spec.Name)
		if err != nil {
			r.opts.logger.Printf("reconcile: app %q: read previous ledger: %v", spec.Name, err)
		}
		if prev == nil {
			res.NewDrift = true
		} else {
			for _, v := range res.UnknownVersions {
				if !slices.Contains(prev.UnknownVersions, v) {
					res.NewDrift = true
					break
				}
			}
		}
		r.record(spec, res)
		r.opts.logger.Printf("reconcile: app %q drift blocked, not touched: %s", spec.Name, res.Reason)
		return
	}

	if r.opts.dryRun {
		res.Status = StatusConverged
		res.VersionAfter = res.VersionBefore
		r.opts.logger.Printf("reconcile: app %q confirmed (dry-run, %d pending)", spec.Name, len(res.PendingBefore))
		return
	}

	r.opts.logger.Printf("reconcile: app %q confirmed, converging %d pending migration(s)", spec.Name, len(res.PendingBefore))
	_, upErr := provider.Up(ctx)

	// Re-inspect regardless of success to capture applied versions, including partial progress
	// left behind by a failed run.
	after, inspectErr := inspectTable(ctx, database.Dialect(r.cfg.Dialect), r.db, spec.Table)
	if inspectErr != nil {
		r.opts.logger.Printf("reconcile: app %q: inspect after up: %v", spec.Name, inspectErr)
	}
	res.Applied = subtract(after.applied, state.applied)
	res.VersionAfter = maxInt64(after.applied)
	if upErr != nil {
		r.fail(spec, res, fmt.Sprintf("up: %v", upErr))
		return
	}
	res.Status = StatusConverged
	r.record(spec, res)
	r.opts.logger.Printf("reconcile: app %q converged to version %d", spec.Name, res.VersionAfter)
}

// fail marks the application failed and records its ledger.
func (r *Runner) fail(spec *AppSpec, res *AppResult, reason string) {
	res.Status = StatusFailed
	res.Reason = reason
	r.record(spec, res)
	r.opts.logger.Printf("reconcile: app %q failed: %s", spec.Name, reason)
}

// record writes the per-app ledger unless this is a dry run. Ledger write failures are logged but
// do not change the reconciliation result.
func (r *Runner) record(spec *AppSpec, res *AppResult) {
	if r.opts.dryRun {
		return
	}
	l := &Ledger{
		App:              res.Name,
		VersionTable:     res.Table,
		Status:           res.Status,
		Reason:           res.Reason,
		ConvergedAt:      r.opts.now(),
		RegistryDigest:   r.cfg.digestOrDefault(),
		TableExisted:     res.TableExisted,
		ExpectedVersions: res.ExpectedVersions,
		ObservedVersions: res.ObservedVersions,
		UnknownVersions:  res.UnknownVersions,
		PendingBefore:    res.PendingBefore,
		Applied:          res.Applied,
		VersionBefore:    res.VersionBefore,
		VersionAfter:     res.VersionAfter,
		TargetVersion:    res.TargetVersion,
		NewDrift:         res.NewDrift,
	}
	path, err := writeLedger(r.opts.ledgerDir, l)
	if err != nil {
		r.opts.logger.Printf("reconcile: app %q: write ledger: %v", spec.Name, err)
		return
	}
	res.LedgerPath = path
}

func sourceVersions(sources []*goose.Source) []int64 {
	out := make([]int64, 0, len(sources))
	for _, s := range sources {
		out = append(out, s.Version)
	}
	slices.Sort(out)
	return out
}

// subtract returns versions present in a but not in b. Both slices must be sorted.
func subtract(a, b []int64) []int64 {
	var out []int64
	for _, v := range a {
		if !slices.Contains(b, v) {
			out = append(out, v)
		}
	}
	return out
}

func maxInt64(vs []int64) int64 {
	var max int64
	for _, v := range vs {
		if v > max {
			max = v
		}
	}
	return max
}
