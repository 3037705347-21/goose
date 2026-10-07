package rollout

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

const ledgerSchemaVersion = 1

// ErrLedgerLocked is returned when another runner already holds the ledger
// lock.
var ErrLedgerLocked = errors.New("ledger is locked by another runner")

// GenStatus is the status of a generation in the ledger.
type GenStatus string

const (
	// GenPending: generation has not started.
	GenPending GenStatus = "pending"
	// GenInProgress: generation is currently being processed.
	GenInProgress GenStatus = "in_progress"
	// GenSucceeded: every target in the generation reached the target version.
	GenSucceeded GenStatus = "succeeded"
	// GenRolledBack: the generation failed and every upgraded target was
	// returned to its baseline.
	GenRolledBack GenStatus = "rolled_back"
	// GenFailed: the generation failed in a way that did not result in a clean
	// rollback (e.g. rollback itself failed).
	GenFailed GenStatus = "failed"
)

// EntryKind classifies a ledger entry.
type EntryKind string

const (
	EntryAdopted        EntryKind = "adopted"
	EntryUpgraded       EntryKind = "upgraded"
	EntrySkipped        EntryKind = "skipped"
	EntryFailed         EntryKind = "failed"
	EntryRolledBack     EntryKind = "rolled_back"
	EntryRollbackFailed EntryKind = "rollback_failed"
	EntryConflict       EntryKind = "conflict"

	// Intent kinds: an intent is persisted before the database is changed.
	IntentUpgrading   EntryKind = "upgrading"
	IntentRollingBack EntryKind = "rolling_back"
)

// IsIntent reports whether the entry kind is an unclosed intent.
func (k EntryKind) IsIntent() bool {
	return k == IntentUpgrading || k == IntentRollingBack
}

// Entry is one persisted fact about a target within a generation.
type Entry struct {
	// RunID identifies the run that produced the entry.
	RunID string `json:"run_id"`
	// GenIndex is the 0-based index of the generation in the manifest.
	GenIndex int `json:"gen_index"`
	// GenName is the generation name.
	GenName string `json:"gen_name"`
	// Target is the target name.
	Target string `json:"target"`
	// Kind is the entry kind.
	Kind EntryKind `json:"kind"`
	// Baseline is the target's baseline version for the generation.
	Baseline int64 `json:"baseline_version,omitempty"`
	// From is the version the target had before the action.
	From int64 `json:"from_version,omitempty"`
	// To is the version the action moves the target to.
	To int64 `json:"to_version,omitempty"`
	// Observed is the version observed in the database.
	Observed int64 `json:"observed_version,omitempty"`
	// Expected is the version the ledger last recorded.
	Expected int64 `json:"expected_version,omitempty"`
	// Detail carries error text or other explanatory information.
	Detail string `json:"detail,omitempty"`
	// At is when the entry was persisted.
	At time.Time `json:"at"`
}

// GenerationState is the persisted status of one generation.
type GenerationState struct {
	Index         int        `json:"index"`
	Name          string    `json:"name"`
	TargetVersion int64     `json:"target_version"`
	Status        GenStatus `json:"status"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	Detail        string    `json:"detail,omitempty"`
}

type ledgerFile struct {
	SchemaVersion  int               `json:"schema_version"`
	ManifestDigest string            `json:"manifest_digest"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	Generations     []GenerationState `json:"generations"`
	Entries         []Entry           `json:"entries"`
}

// Ledger is the durable, append-oriented rollout ledger.
type Ledger struct {
	path string
	data *ledgerFile

	// lockFile is a dedicated, never-renamed file used for advisory locking.
	lockFile *os.File

	// failSave, when non-nil, is invoked during a save with the stage name
	// ("temp" after the temp file is written, "rename" after rename). It is a
	// test hook used to simulate crashes.
	failSave func(stage string) error
}

// OpenLedger opens the ledger at path, creating it from the manifest if it does
// not exist.
//
// The manifest digest is bound on creation and verified on every subsequent
// open. This function does not acquire the exclusive run lock; callers running
// mutations must additionally invoke [Ledger.Lock].
func OpenLedger(path string, m *Manifest) (*Ledger, error) {
	if m == nil {
		return nil, errors.New("manifest must not be nil")
	}
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		lf := new(ledgerFile)
		if err := json.Unmarshal(raw, lf); err != nil {
			return nil, fmt.Errorf("parse ledger %q: %w", path, err)
		}
		if lf.SchemaVersion != ledgerSchemaVersion {
			return nil, fmt.Errorf("unsupported ledger schema version %d, expected %d",
				lf.SchemaVersion, ledgerSchemaVersion)
		}
		if lf.ManifestDigest != m.Digest() {
			return nil, fmt.Errorf(
				"manifest digest mismatch: ledger %q is bound to %s, manifest %q has %s; refusing to run",
				path, lf.ManifestDigest, m.Path(), m.Digest())
		}
		if len(lf.Generations) != len(m.Generations) {
			return nil, fmt.Errorf("ledger generation count %d does not match manifest %d",
				len(lf.Generations), len(m.Generations))
		}
		return &Ledger{path: path, data: lf}, nil
	case errors.Is(err, fs.ErrNotExist):
		now := time.Now()
		lf := &ledgerFile{
			SchemaVersion:  ledgerSchemaVersion,
			ManifestDigest: m.Digest(),
			CreatedAt:      now,
			UpdatedAt:      now,
			Generations:    make([]GenerationState, 0, len(m.Generations)),
			Entries:        []Entry{},
		}
		for i := range m.Generations {
			g := &m.Generations[i]
			lf.Generations = append(lf.Generations, GenerationState{
				Index:         i,
				Name:          g.Name,
				TargetVersion: g.TargetVersion,
				Status:        GenPending,
			})
		}
		l := &Ledger{path: path, data: lf}
		if err := l.save(); err != nil {
			return nil, fmt.Errorf("create ledger %q: %w", path, err)
		}
		return l, nil
	default:
		return nil, fmt.Errorf("read ledger %q: %w", path, err)
	}
}

// Path returns the ledger file path.
func (l *Ledger) Path() string { return l.path }

// Lock acquires an exclusive, non-blocking advisory lock for this ledger. It
// returns [ErrLedgerLocked] if another runner holds it. Pair every successful
// Lock with [Ledger.Unlock].
func (l *Ledger) Lock() error {
	if l.lockFile != nil {
		return errors.New("ledger already locked")
	}
	lockPath := l.path + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open lock file %q: %w", lockPath, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return ErrLedgerLocked
		}
		return fmt.Errorf("acquire ledger lock: %w", err)
	}
	l.lockFile = f
	return nil
}

// Unlock releases the advisory lock acquired with [Ledger.Lock].
func (l *Ledger) Unlock() error {
	if l.lockFile == nil {
		return nil
	}
	err := unix.Flock(int(l.lockFile.Fd()), unix.LOCK_UN)
	err = errors.Join(err, l.lockFile.Close())
	l.lockFile = nil
	return err
}

// Generation returns the state of the generation at idx.
func (l *Ledger) Generation(idx int) (GenerationState, error) {
	if idx < 0 || idx >= len(l.data.Generations) {
		return GenerationState{}, fmt.Errorf("generation index %d out of range", idx)
	}
	return l.data.Generations[idx], nil
}

// Entries returns a copy of all ledger entries in persistence order.
func (l *Ledger) Entries() []Entry {
	out := make([]Entry, len(l.data.Entries))
	copy(out, l.data.Entries)
	return out
}

// GenEntries returns all entries for the generation at idx.
func (l *Ledger) GenEntries(idx int) []Entry {
	var out []Entry
	for _, e := range l.data.Entries {
		if e.GenIndex == idx {
			out = append(out, e)
		}
	}
	return out
}

// LastEntry returns the most recent entry for target and whether one exists.
func (l *Ledger) LastEntry(target string) (Entry, bool) {
	for i := len(l.data.Entries) - 1; i >= 0; i-- {
		if l.data.Entries[i].Target == target {
			return l.data.Entries[i], true
		}
	}
	return Entry{}, false
}

// OpenIntent returns the most recent unclosed intent for target, if any.
func (l *Ledger) OpenIntent(target string) (Entry, bool) {
	last, ok := l.LastEntry(target)
	if !ok || !last.Kind.IsIntent() {
		return Entry{}, false
	}
	return last, true
}

// RecordedVersion returns the version the ledger last recorded for the given
// entry, i.e. what the database is believed to be at.
func RecordedVersion(e Entry) int64 {
	switch e.Kind {
	case EntryAdopted, EntrySkipped:
		return e.Observed
	case EntryUpgraded, EntryFailed, EntryRolledBack, EntryRollbackFailed:
		return e.To
	case EntryConflict:
		return e.Expected
	case IntentUpgrading, IntentRollingBack:
		return e.From
	default:
		return 0
	}
}

// StartGeneration marks generation idx as in progress.
func (l *Ledger) StartGeneration(idx int) error {
	g, err := l.gen(idx)
	if err != nil {
		return err
	}
	if g.Status == GenPending {
		now := time.Now()
		g.StartedAt = &now
	}
	g.Status = GenInProgress
	return l.save()
}

// FinishGeneration marks generation idx with the given terminal status.
func (l *Ledger) FinishGeneration(idx int, status GenStatus, detail string) error {
	g, err := l.gen(idx)
	if err != nil {
		return err
	}
	now := time.Now()
	if g.StartedAt == nil {
		g.StartedAt = &now
	}
	g.FinishedAt = &now
	g.Status = status
	g.Detail = detail
	return l.save()
}

// AppendEntry persists a new entry, stamping At if zero.
func (l *Ledger) AppendEntry(e Entry) error {
	if e.At.IsZero() {
		e.At = time.Now()
	}
	l.data.Entries = append(l.data.Entries, e)
	return l.save()
}

func (l *Ledger) gen(idx int) (*GenerationState, error) {
	if idx < 0 || idx >= len(l.data.Generations) {
		return nil, fmt.Errorf("generation index %d out of range", idx)
	}
	return &l.data.Generations[idx], nil
}

// save atomically persists the ledger: temp file in the same directory, fsync,
// rename, fsync directory.
func (l *Ledger) save() error {
	l.data.UpdatedAt = time.Now()
	payload, err := json.MarshalIndent(l.data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal ledger: %w", err)
	}
	dir := filepath.Dir(l.path)
	tmpPath := l.path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create temp ledger %q: %w", tmpPath, err)
	}
	if _, err := f.Write(payload); err != nil {
		_ = f.Close()
		return fmt.Errorf("write temp ledger %q: %w", tmpPath, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync temp ledger %q: %w", tmpPath, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp ledger %q: %w", tmpPath, err)
	}
	if l.failSave != nil {
		if err := l.failSave("temp"); err != nil {
			return err
		}
	}
	if err := os.Rename(tmpPath, l.path); err != nil {
		return fmt.Errorf("replace ledger %q: %w", l.path, err)
	}
	if l.failSave != nil {
		if err := l.failSave("rename"); err != nil {
			return err
		}
	}
	return syncDir(dir)
}

// syncDir fsyncs a directory so a rename is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open directory %q: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync directory %q: %w", dir, err)
	}
	return nil
}
