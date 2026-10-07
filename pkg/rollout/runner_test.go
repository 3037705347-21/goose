package rollout

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeExec is an in-memory Executor used to drive the runner state machine.
type fakeExec struct {
	mu       sync.Mutex
	version  int64
	failTo   map[int64]bool // fail upgrading/downgrading when the operand matches
	upgCalls int
	downCalls int
	downs    []int64
}

func (f *fakeExec) CurrentVersion(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.version, nil
}

func (f *fakeExec) UpgradeTo(_ context.Context, v int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upgCalls++
	if f.failTo[v] {
		return errors.New("boom")
	}
	if v > f.version {
		f.version = v
	}
	return nil
}

func (f *fakeExec) DowngradeTo(_ context.Context, v int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.downCalls++
	f.downs = append(f.downs, v)
	if f.failTo[v] {
		return errors.New("down boom")
	}
	f.version = v
	return nil
}

func (f *fakeExec) Close() error { return nil }

// fakeFleet is a factory plus the set of executors.
type fakeFleet struct {
	targets map[string]*fakeExec
	created []string
}

func newFakeFleet(targets ...string) *fakeFleet {
	f := &fakeFleet{targets: map[string]*fakeExec{}}
	for _, t := range targets {
		f.targets[t] = &fakeExec{failTo: map[int64]bool{}}
	}
	return f
}

func (f *fakeFleet) factory() ExecutorFactory {
	return func(_ context.Context, t Target) (Executor, error) {
		e, ok := f.targets[t.Name]
		if !ok {
			return nil, errors.New("unexpected target " + t.Name)
		}
		f.created = append(f.created, t.Name)
		return e, nil
	}
}

func newRunnerWithFleet(t *testing.T, m *Manifest, fleet *fakeFleet, runID string) (*Runner, *Ledger) {
	t.Helper()
	lp := filepath.Join(t.TempDir(), "ledger.json")
	l, err := OpenLedger(lp, m)
	require.NoError(t, err)
	r := NewRunner(m, l, WithExecutorFactory(fleet.factory()), WithRunID(runID))
	return r, l
}

func TestRun_AllGenerationsSucceedInOrder(t *testing.T) {
	t.Parallel()
	_, m := newTestManifest(t)
	fleet := newFakeFleet("db-a", "db-b")
	r, l := newRunnerWithFleet(t, m, fleet, "run-1")

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Generations, 2)
	require.Equal(t, GenSucceeded, report.Generations[0].Status)
	require.Equal(t, GenSucceeded, report.Generations[1].Status)
	require.True(t, report.Generations[0].FinishedAt.Before(report.Generations[1].StartedAt) ||
		report.Generations[0].FinishedAt.Equal(report.Generations[1].StartedAt))

	for name, e := range fleet.targets {
		require.Equal(t, int64(3), e.version, name)
	}

	g0, err := l.Generation(0)
	require.NoError(t, err)
	require.Equal(t, GenSucceeded, g0.Status)
	g1, err := l.Generation(1)
	require.NoError(t, err)
	require.Equal(t, GenSucceeded, g1.Status)

	// Rerun: converged, no new upgrade calls, still success.
	upgBefore := make(map[string]int)
	for name, e := range fleet.targets {
		upgBefore[name] = e.upgCalls
	}
	r2 := NewRunner(m, l, WithExecutorFactory(fleet.factory()), WithRunID("run-2"))
	report2, err := r2.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report2.Generations, 2)
	for name, e := range fleet.targets {
		require.Equal(t, upgBefore[name], e.upgCalls, "rerun must not invoke upgrades")
	}
}

func TestRun_AdoptAndSkip(t *testing.T) {
	t.Parallel()
	_, m := newTestManifest(t)
	fleet := newFakeFleet("db-a", "db-b")
	// First-ever observation is already at the gen-1 target: adopt baseline 2
	// then skip gen-1; gen-2 upgrades to 3.
	fleet.targets["db-a"].version = 2
	fleet.targets["db-b"].version = 2

	r, l := newRunnerWithFleet(t, m, fleet, "run-1")
	report, err := r.Run(context.Background())
	require.NoError(t, err)

	require.Equal(t, GenSucceeded, report.Generations[0].Status)
	dbA := report.Generations[0].Targets[0]
	require.Equal(t, EntrySkipped, dbA.Outcome)
	require.Equal(t, int64(2), dbA.Baseline)

	require.Equal(t, int64(3), fleet.targets["db-a"].version)

	// adopted entries must exist in the ledger.
	var adopted int
	for _, e := range l.Entries() {
		if e.Kind == EntryAdopted {
			adopted++
		}
	}
	require.Equal(t, 2, adopted)
}

func TestRun_ConflictTriggersRollback(t *testing.T) {
	t.Parallel()
	// Three targets, one generation to v2. Ledger has adopted all three at
	// baseline 0. a and b upgrade fine; c has been hand-modified to v1 and
	// drifts from its recorded 0.
	dir := t.TempDir()
	writeMigrationFiles(t, dir, 1, 2, 3)
	content := `
version: 1
migrations_dir: migrations
targets:
  - {name: db-a, driver: sqlite3, dsn: a}
  - {name: db-b, driver: sqlite3, dsn: b}
  - {name: db-c, driver: sqlite3, dsn: c}
generations:
  - {name: gen-1, target_version: 2, targets: [db-a, db-b, db-c]}
`
	mp := writeManifest(t, dir, content)
	m, err := LoadManifest(mp)
	require.NoError(t, err)

	fleet := newFakeFleet("db-a", "db-b", "db-c")
	// c was hand-changed to 1 outside this flow.
	fleet.targets["db-c"].version = 1

	l, err := OpenLedger(filepath.Join(t.TempDir(), "ledger.json"), m)
	require.NoError(t, err)
	for _, name := range []string{"db-a", "db-b", "db-c"} {
		require.NoError(t, l.AppendEntry(Entry{
			RunID: "seed", GenIndex: 0, Target: name,
			Kind: EntryAdopted, Baseline: 0, Observed: 0,
		}))
	}

	r := NewRunner(m, l, WithExecutorFactory(fleet.factory()), WithRunID("run-1"))
	report, err := r.Run(context.Background())
	require.Error(t, err)
	require.Equal(t, GenRolledBack, report.Generations[0].Status)

	// a and b upgraded then rolled back to baseline 0.
	require.Equal(t, int64(0), fleet.targets["db-a"].version)
	require.Equal(t, int64(0), fleet.targets["db-b"].version)
	// c was never touched by the orchestrator and keeps its drifted version.
	require.Equal(t, int64(1), fleet.targets["db-c"].version)

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
	require.NotEmpty(t, conflict.Detail)
}

func TestRun_FailureRollsBackWholeGeneration(t *testing.T) {
	t.Parallel()
	// Three targets; third fails upgrading to 2. Manifest needs 3 targets.
	dir := t.TempDir()
	writeMigrationFiles(t, dir, 1, 2, 3)
	content := `
version: 1
migrations_dir: migrations
targets:
  - {name: db-a, driver: sqlite3, dsn: a}
  - {name: db-b, driver: sqlite3, dsn: b}
  - {name: db-c, driver: sqlite3, dsn: c}
generations:
  - {name: gen-1, target_version: 2, targets: [db-a, db-b, db-c]}
  - {name: gen-2, target_version: 3, targets: [db-a, db-b, db-c]}
`
	mp := writeManifest(t, dir, content)
	m, err := LoadManifest(mp)
	require.NoError(t, err)

	fleet := newFakeFleet("db-a", "db-b", "db-c")
	fleet.targets["db-c"].failTo[2] = true
	r, l := newRunnerWithFleet(t, m, fleet, "run-1")

	report, err := r.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "gen-1")

	// All three databases back at baseline 0.
	for name, e := range fleet.targets {
		v, _ := e.CurrentVersion(context.Background())
		require.Equal(t, int64(0), v, name)
	}
	require.Equal(t, GenRolledBack, report.Generations[0].Status)
	g1, err := l.Generation(1)
	require.NoError(t, err)
	require.Equal(t, GenPending, g1.Status, "later generation must not start")

	// a and b rolled back exactly to baseline once each.
	require.Equal(t, []int64{0}, fleet.targets["db-a"].downs)
	require.Equal(t, []int64{0}, fleet.targets["db-b"].downs)
}

func TestRun_OpenUpgradingIntentConvergence(t *testing.T) {
	tests := []struct {
		name       string
		observed   int64
		wantUpgrade bool // whether the runner re-runs the upgrade
		wantErr    bool
	}{
		{"completed before crash", 2, false, false},
		{"nothing applied", 0, true, false},
		{"drifted to other version", 1, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir, m := newTestManifest(t)
			one := singleGenManifest(t, m, 2)
			fleet := newFakeFleet("db-a", "db-b")
			fleet.targets["db-a"].version = tc.observed
			fleet.targets["db-b"].version = tc.observed

			l, err := OpenLedger(filepath.Join(t.TempDir(), "ledger.json"), one)
			require.NoError(t, err)
			// Seed open upgrading intents for both targets from a crashed run.
			for _, name := range []string{"db-a", "db-b"} {
				require.NoError(t, l.AppendEntry(Entry{
					RunID: "crashed", GenIndex: 0, Target: name,
					Kind: IntentUpgrading, Baseline: 0, From: 0, To: 2,
				}))
			}
			require.NoError(t, l.StartGeneration(0))
			r := NewRunner(one, l, WithExecutorFactory(fleet.factory()), WithRunID("resume"))
			report, err := r.Run(context.Background())
			if tc.wantErr {
				require.Error(t, err)
				require.Equal(t, GenRolledBack, report.Generations[0].Status)
			} else {
				require.NoError(t, err)
				require.Equal(t, GenSucceeded, report.Generations[0].Status)
				require.Equal(t, int64(2), fleet.targets["db-a"].version)
			}
			require.Equal(t, tc.wantUpgrade, fleet.targets["db-a"].upgCalls == 1)
			_ = dir
		})
	}
}

func TestRun_OpenRollingBackIntentConvergence(t *testing.T) {
	tests := []struct {
		name     string
		observed int64
		wantDown bool
		hard     bool
	}{
		{"rollback completed before crash", 0, false, false},
		{"still upgraded, redo downgrade", 2, true, false},
		{"drifted mid-rollback", 1, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir, m := newTestManifest(t)
			one := singleGenManifest(t, m, 2)
			fleet := newFakeFleet("db-a")
			fleet.targets["db-a"].version = tc.observed

			l, err := OpenLedger(filepath.Join(t.TempDir(), "ledger.json"), one)
			require.NoError(t, err)
			// Seed an upgraded entry plus an open rolling_back intent.
			require.NoError(t, l.AppendEntry(Entry{
				RunID: "crashed", GenIndex: 0, Target: "db-a",
				Kind: EntryUpgraded, Baseline: 0, From: 0, To: 2,
			}))
			require.NoError(t, l.AppendEntry(Entry{
				RunID: "crashed", GenIndex: 0, Target: "db-a",
				Kind: IntentRollingBack, Baseline: 0, From: 2, To: 0,
			}))
			require.NoError(t, l.StartGeneration(0))
			r := NewRunner(one, l, WithExecutorFactory(fleet.factory()), WithRunID("resume"))
			report, err := r.Run(context.Background())
			if tc.hard {
				require.Error(t, err)
				require.Equal(t, GenFailed, report.Generations[0].Status)
			} else {
				require.Error(t, err, "generation still aborts, but rollback is clean")
				require.Equal(t, GenRolledBack, report.Generations[0].Status)
				require.Equal(t, int64(0), fleet.targets["db-a"].version)
			}
			require.Equal(t, tc.wantDown, fleet.targets["db-a"].downCalls == 1)
			_ = dir
		})
	}
}

func TestRun_StickyEntriesBlockWithoutTouchingDBs(t *testing.T) {
	tests := []struct {
		name   string
		kind   EntryKind
	}{
		{"conflict sticky", EntryConflict},
		{"rollback failed sticky", EntryRollbackFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir, m := newTestManifest(t)
			one := singleGenManifest(t, m, 2)
			fleet := newFakeFleet("db-a", "db-b")

			l, err := OpenLedger(filepath.Join(t.TempDir(), "ledger.json"), one)
			require.NoError(t, err)
			require.NoError(t, l.AppendEntry(Entry{
				RunID: "prev", GenIndex: 0, Target: "db-a",
				Kind: tc.kind, Baseline: 0, Detail: "unresolved",
			}))
			r := NewRunner(one, l, WithExecutorFactory(fleet.factory()), WithRunID("retry"))
			_, err = r.Run(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), "blocked")
			require.Empty(t, fleet.created, "no executor may be created while blocked")
			_ = dir
		})
	}
}

func TestRun_RollbackFailureIsHard(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeMigrationFiles(t, dir, 1, 2)
	content := `
version: 1
migrations_dir: migrations
targets:
  - {name: db-a, driver: sqlite3, dsn: a}
  - {name: db-b, driver: sqlite3, dsn: b}
generations:
  - {name: gen-1, target_version: 2, targets: [db-a, db-b]}
`
	mp := writeManifest(t, dir, content)
	m, err := LoadManifest(mp)
	require.NoError(t, err)

	fleet := newFakeFleet("db-a", "db-b")
	// db-a upgrades fine but its downgrade fails; db-b fails upgrade,
	// triggering the rollback.
	fleet.targets["db-a"].failTo[0] = true
	fleet.targets["db-b"].failTo[2] = true

	r, _ := newRunnerWithFleet(t, m, fleet, "run-1")
	report, err := r.Run(context.Background())
	require.Error(t, err)
	require.Equal(t, GenFailed, report.Generations[0].Status)

	var hard []TargetReport
	for _, tr := range report.Generations[0].Targets {
		if tr.Outcome == EntryRollbackFailed {
			hard = append(hard, tr)
		}
	}
	require.NotEmpty(t, hard)
}

func TestRun_OnlyManifestTargetsAreContacted(t *testing.T) {
	t.Parallel()
	_, m := newTestManifest(t)
	fleet := newFakeFleet("db-a", "db-b", "db-outsider")
	r, _ := newRunnerWithFleet(t, m, fleet, "run-1")
	_, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"db-a", "db-b"}, fleet.created)
}

// singleGenManifest returns a validated one-generation manifest with the same
// targets as m.
func singleGenManifest(t *testing.T, m *Manifest, target int64) *Manifest {
	t.Helper()
	dir := t.TempDir()
	writeMigrationFiles(t, dir, 1, 2, 3)
	var b strings.Builder
	b.WriteString("version: 1\nmigrations_dir: migrations\ntargets:\n")
	for _, tg := range m.Targets {
		b.WriteString("  - {name: " + tg.Name + ", driver: sqlite3, dsn: x}\n")
	}
	b.WriteString("generations:\n")
	b.WriteString("  - {name: only, target_version: " + strconv.FormatInt(target, 10) + ", targets: [")
	for i, tg := range m.Targets {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(tg.Name)
	}
	b.WriteString("]}\n")
	mp := writeManifest(t, dir, b.String())
	mm, err := LoadManifest(mp)
	require.NoError(t, err)
	return mm
}
