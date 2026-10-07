package ownership

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pressly/goose/v3/database"
)

// Report summarizes one rollback reconciliation.
type Report struct {
	// Version is the migration version that was reconciled.
	Version int64
	// Backend is the catalog backend name ("postgres", "mysql", "sqlite").
	Backend string
	// Dropped are objects that were deleted and detached from the ledger.
	Dropped []Key
	// Pruned are ledger entries removed because the objects were already absent, e.g. leftovers
	// of an interrupted previous run.
	Pruned []Key
	// Drifts are ledger objects that had to be kept. When non-empty, ReconcileDown returns a
	// [DriftError] and the migration version is left applied.
	Drifts []Drift
}

// DriftError is returned by [Guard.ReconcileDown] when one or more ledger objects cannot be
// deleted safely. The rollback performs no destructive action in that case and the migration
// version is left applied, so an operator can resolve the drift and re-run; the re-run converges.
type DriftError struct {
	Report *Report
}

func (e *DriftError) Error() string {
	if e == nil || e.Report == nil {
		return "ownership: rollback blocked by object drift"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "ownership: rollback of migration version %d blocked, %d object(s) left untouched:",
		e.Report.Version, len(e.Report.Drifts))
	for _, d := range e.Report.Drifts {
		fmt.Fprintf(&b, "\n  - %s (%s)", d.Entry.Key, d.Detail)
	}
	return b.String()
}

// Guard ties a catalog backend to its ledger and implements the UP/DOWN reconciliation protocol.
type Guard struct {
	backend *Backend
	once    sync.Once
	ready   bool
}

// Option configures a [Guard].
type Option func(*guardConfig)

type guardConfig struct {
	ledgerTable string
}

// WithLedgerTable overrides the ledger table name. It defaults to [DefaultLedgerTable].
func WithLedgerTable(name string) Option {
	return func(c *guardConfig) { c.ledgerTable = name }
}

// NewGuard creates a Guard for the given goose dialect name. versionTable is the goose version
// table; it and the ledger table are excluded from object snapshots.
func NewGuard(dialectName, versionTable string, opts ...Option) (*Guard, error) {
	cfg := &guardConfig{ledgerTable: DefaultLedgerTable}
	for _, o := range opts {
		o(cfg)
	}
	backend, err := NewBackend(dialectName, versionTable, cfg.ledgerTable)
	if err != nil {
		return nil, err
	}
	return &Guard{backend: backend}, nil
}

// BackendName returns the resolved backend name ("postgres", "mysql", "sqlite").
func (g *Guard) BackendName() string { return g.backend.Name() }

// LedgerTableName returns the name of the ledger table backing this guard.
func (g *Guard) LedgerTableName() string { return g.backend.Ledger().TableName() }

// Ensure creates the ledger table once per Guard instance. It is safe to call repeatedly.
func (g *Guard) Ensure(ctx context.Context, db database.DBTxConn) error {
	var err error
	g.once.Do(func() {
		err = g.backend.Ledger().EnsureTable(ctx, db)
		if err == nil {
			g.ready = true
		}
	})
	if err != nil {
		return err
	}
	if !g.ready {
		// A previous call failed; retry directly, because sync.Once will not run again.
		if err := g.backend.Ledger().EnsureTable(ctx, db); err != nil {
			return err
		}
		g.ready = true
	}
	return nil
}

// Snapshot returns the current catalog objects (tables, columns and non-primary indexes).
func (g *Guard) Snapshot(ctx context.Context, db database.DBTxConn) ([]Object, error) {
	objects, err := g.backend.Catalog().ListObjects(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("ownership: snapshot catalog: %w", err)
	}
	return objects, nil
}

// Claim records objects that appeared between before and the current catalog as belonging to
// version. Objects that pre-existed (including anything added outside goose) are never claimed,
// which is exactly what keeps externally added columns and indexes alive on rollback.
//
// Claim is idempotent: re-claiming an entry that already exists keeps the original fingerprint.
func (g *Guard) Claim(ctx context.Context, db database.DBTxConn, version int64, before []Object) error {
	if version < 1 {
		return fmt.Errorf("ownership: cannot claim objects for version %d", version)
	}
	after, err := g.Snapshot(ctx, db)
	if err != nil {
		return err
	}
	existed := make(map[Key]struct{}, len(before))
	for _, o := range before {
		existed[o.Key] = struct{}{}
	}
	var entries []Entry
	for _, o := range after {
		if _, ok := existed[o.Key]; ok {
			continue
		}
		entries = append(entries, Entry{Version: version, Object: o})
	}
	if err := g.backend.Ledger().Put(ctx, db, entries); err != nil {
		return fmt.Errorf("ownership: claim objects for version %d: %w", version, err)
	}
	return nil
}

// ReconcileDown performs rollback reconciliation for one migration version:
//
//   - objects that match the ledger are dropped and detached;
//   - objects already absent have their ledger entries pruned (interrupted runs converge);
//   - renamed or type-drifted objects, and tables that still contain external children, block the
//     rollback and are reported as [Drift].
//
// It executes no destructive statement when blocking drift exists.
func (g *Guard) ReconcileDown(ctx context.Context, db database.DBTxConn, version int64) (*Report, error) {
	if err := g.Ensure(ctx, db); err != nil {
		return nil, err
	}
	report := &Report{Version: version, Backend: g.backend.Name()}

	owned, err := g.backend.Ledger().ListByVersion(ctx, db, version)
	if err != nil {
		return nil, fmt.Errorf("ownership: load ledger for version %d: %w", version, err)
	}
	if len(owned) == 0 {
		// Nothing was claimed for this version. Nothing in the catalog can be safely attributed
		// to it, so no destructive statement is issued.
		return report, nil
	}
	current, err := g.Snapshot(ctx, db)
	if err != nil {
		return nil, err
	}
	p := plan(version, owned, current)

	// Convergence first: drop stale rows for objects that are already gone. This is safe even
	// when blocking drift is present for other entries.
	prunes := p.Prunes()
	pruneKeys := make([]Key, 0, len(prunes))
	for _, a := range prunes {
		pruneKeys = append(pruneKeys, a.Entry.Key)
	}
	if err := g.backend.Ledger().Delete(ctx, db, version, pruneKeys); err != nil {
		return nil, err
	}
	report.Pruned = append(report.Pruned, pruneKeys...)

	// Any blocking drift stops the rollback before a single DROP is issued.
	if blocking := p.Blocking(); len(blocking) > 0 {
		for _, a := range blocking {
			report.Drifts = append(report.Drifts, Drift{
				Entry:   a.Entry,
				Current: a.Current,
				Reason:  a.Reason,
				Detail:  a.Detail,
			})
		}
		return report, &DriftError{Report: report}
	}

	// Execute approved drops in dependency order. Each drop is immediately followed by its ledger
	// row removal; on transactional-DDL backends the caller wraps both in the migration
	// transaction, and everywhere else a crash between the two is healed by the prune step on the
	// next run.
	for _, a := range p.Drops() {
		stmt, err := g.backend.Catalog().DropStatement(a.Current)
		if err != nil {
			return report, fmt.Errorf("ownership: render drop for %s: %w", a.Entry.Key, err)
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return report, fmt.Errorf("ownership: drop %s: %w", a.Entry.Key, err)
		}
		if err := g.backend.Ledger().Delete(ctx, db, version, []Key{a.Entry.Key}); err != nil {
			return report, err
		}
		report.Dropped = append(report.Dropped, a.Entry.Key)
	}
	return report, nil
}
