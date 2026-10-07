package rollout

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestManifest(t *testing.T) (string, *Manifest) {
	t.Helper()
	dir := t.TempDir()
	writeMigrationFiles(t, dir, 1, 2, 3)
	mp := writeManifest(t, dir, validManifestYAML)
	m, err := LoadManifest(mp)
	require.NoError(t, err)
	return dir, m
}

func TestLedger_CreateAppendReopen(t *testing.T) {
	t.Parallel()
	dir, m := newTestManifest(t)
	lp := filepath.Join(dir, "rollout.ledger.json")

	l, err := OpenLedger(lp, m)
	require.NoError(t, err)
	g0, err := l.Generation(0)
	require.NoError(t, err)
	require.Equal(t, GenPending, g0.Status)

	require.NoError(t, l.StartGeneration(0))
	require.NoError(t, l.AppendEntry(Entry{
		RunID:    "run-1",
		GenIndex: 0,
		GenName:  "gen-1",
		Target:   "db-a",
		Kind:     EntryAdopted,
		Baseline: 1,
		Observed: 1,
	}))
	require.NoError(t, l.AppendEntry(Entry{
		RunID:    "run-1",
		GenIndex: 0,
		GenName:  "gen-1",
		Target:   "db-a",
		Kind:     EntryUpgraded,
		Baseline: 1,
		From:     1,
		To:       2,
		Detail:   "",
	}))
	require.NoError(t, l.FinishGeneration(0, GenSucceeded, ""))

	// Reopen from disk.
	l2, err := OpenLedger(lp, m)
	require.NoError(t, err)
	g0, err = l2.Generation(0)
	require.NoError(t, err)
	require.Equal(t, GenSucceeded, g0.Status)
	require.NotNil(t, g0.StartedAt)
	require.NotNil(t, g0.FinishedAt)
	require.Len(t, l2.Entries(), 2)

	last, ok := l2.LastEntry("db-a")
	require.True(t, ok)
	require.Equal(t, EntryUpgraded, last.Kind)
	require.Equal(t, int64(2), RecordedVersion(last))
	require.False(t, last.At.IsZero())

	ge := l2.GenEntries(0)
	require.Len(t, ge, 2)
}

func TestLedger_ManifestDigestMismatch(t *testing.T) {
	t.Parallel()
	dir, m := newTestManifest(t)
	lp := filepath.Join(dir, "rollout.ledger.json")
	_, err := OpenLedger(lp, m)
	require.NoError(t, err)

	// Build a second, valid manifest with different content.
	mp2 := writeManifest(t, dir, validManifestYAML+"\n")
	// Trailing newline changes raw bytes but yaml is still valid.
	m2, err := LoadManifest(mp2)
	require.NoError(t, err)
	_, err = OpenLedger(lp, m2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "manifest digest mismatch")
}

func TestLedger_CorruptJSON(t *testing.T) {
	t.Parallel()
	dir, m := newTestManifest(t)
	lp := filepath.Join(dir, "rollout.ledger.json")
	_, err := OpenLedger(lp, m)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(lp, []byte("{not json"), 0o644))
	_, err = OpenLedger(lp, m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse ledger")
}

func TestLedger_SaveInterruptedBeforeRename(t *testing.T) {
	t.Parallel()
	dir, m := newTestManifest(t)
	lp := filepath.Join(dir, "rollout.ledger.json")
	l, err := OpenLedger(lp, m)
	require.NoError(t, err)

	injected := errors.New("simulated crash")
	l.failSave = func(stage string) error {
		if stage == "temp" {
			return injected
		}
		return nil
	}
	// This append writes the temp file but never renames.
	err = l.AppendEntry(Entry{Kind: EntryConflict, Target: "db-a"})
	require.ErrorIs(t, err, injected)

	// The on-disk ledger must still be the last complete version and parse.
	l2, err := OpenLedger(lp, m)
	require.NoError(t, err)
	require.Empty(t, l2.Entries(), "interrupted append must not be visible")

	// Leftover temp file must not block subsequent saves.
	l2.failSave = nil
	require.NoError(t, l2.AppendEntry(Entry{
		Kind:   EntryAdopted,
		Target: "db-a",
	}))
	l3, err := OpenLedger(lp, m)
	require.NoError(t, err)
	require.Len(t, l3.Entries(), 1)
}

func TestLedger_ExclusiveLock(t *testing.T) {
	t.Parallel()
	dir, m := newTestManifest(t)
	lp := filepath.Join(dir, "rollout.ledger.json")
	l1, err := OpenLedger(lp, m)
	require.NoError(t, err)
	l2, err := OpenLedger(lp, m)
	require.NoError(t, err)

	require.NoError(t, l1.Lock())
	err = l2.Lock()
	require.ErrorIs(t, err, ErrLedgerLocked)

	require.NoError(t, l1.Unlock())
	require.NoError(t, l2.Lock())
	require.NoError(t, l2.Unlock())
}

func TestLedger_OpenIntent(t *testing.T) {
	t.Parallel()
	dir, m := newTestManifest(t)
	lp := filepath.Join(dir, "rollout.ledger.json")
	l, err := OpenLedger(lp, m)
	require.NoError(t, err)

	_, ok := l.OpenIntent("db-a")
	require.False(t, ok)

	require.NoError(t, l.AppendEntry(Entry{
		Kind:   IntentUpgrading,
		Target: "db-a",
		From:   1,
		To:     2,
	}))
	intent, ok := l.OpenIntent("db-a")
	require.True(t, ok)
	require.Equal(t, IntentUpgrading, intent.Kind)
	require.True(t, intent.Kind.IsIntent())
	require.Equal(t, int64(1), RecordedVersion(intent))

	require.NoError(t, l.AppendEntry(Entry{
		Kind:   EntryUpgraded,
		Target: "db-a",
		From:   1,
		To:     2,
	}))
	_, ok = l.OpenIntent("db-a")
	require.False(t, ok, "closed intent must not be reported open")
}
