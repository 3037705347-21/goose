package goose

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"sort"
	"strings"
)

// This file implements reconciliation between multiple migration sources (for example migrations
// shipped inside the application binary and migrations living in a local deployment directory).
//
// The reconciler produces a single authoritative [Manifest]. Every manifest entry is attributed to
// exactly one owner (the source name). Reconciliation classifies conflicts and refuses to produce a
// clean manifest when:
//
//   - the same version is provided by more than one source (ConflictDuplicateVersion, or
//     ConflictContentDiverged when the contents differ),
//   - a required source has disappeared (ConflictSourceMissing),
//   - a version previously recorded in the manifest has vanished from every source
//     (ConflictEntryVanished), changed owners (ConflictOwnershipChanged) or changed contents
//     (ConflictModified),
//   - an already applied version is no longer provided by any source.
//
// The manifest is serialized deterministically and persisted atomically, so interrupted writes
// converge on the next run.

// ManifestVersion is the schema version of the serialized [Manifest].
const ManifestVersion = 1

// MigrationSource is a named source of migration files. The name acts as the stable owner
// identifier recorded in the [Manifest] for every version, for example "package" for migrations
// embedded in the binary or "local" for migrations in a deployment directory.
type MigrationSource struct {
	// Name is the stable, unique owner identifier. It must not be empty or change between deploys.
	Name string
	// FS is the filesystem root containing the migration files.
	FS fs.FS
	// Required reports whether a missing or unreadable source root must be reported as a conflict.
	// Use [NewMigrationSource] to construct a required source.
	Required bool
}

// NewMigrationSource returns a required [MigrationSource] with the given owner name and
// filesystem.
func NewMigrationSource(name string, fsys fs.FS) MigrationSource {
	return MigrationSource{Name: name, FS: fsys, Required: true}
}

// ConflictKind classifies a reconciliation conflict.
type ConflictKind string

const (
	// ConflictDuplicateVersion means the same version is currently provided by more than one
	// source and all copies have identical contents. Execution must be refused because ownership
	// of the version is ambiguous.
	ConflictDuplicateVersion ConflictKind = "duplicate_version"
	// ConflictContentDiverged means the same version is currently provided by more than one
	// source and the copies have different contents.
	ConflictContentDiverged ConflictKind = "content_diverged"
	// ConflictOwnershipChanged means a version recorded under one owner is now provided by a
	// different owner. When database state is known this is only raised for already applied
	// versions; in offline (release verification) mode it is always raised.
	ConflictOwnershipChanged ConflictKind = "ownership_changed"
	// ConflictSourceMissing means a required source root is absent or unreadable, for example the
	// local deployment directory was deleted.
	ConflictSourceMissing ConflictKind = "source_missing"
	// ConflictEntryVanished means a previously recorded version, or an already applied version, is
	// no longer provided by any source.
	ConflictEntryVanished ConflictKind = "entry_vanished"
	// ConflictModified means the contents of a recorded version changed. When database state is
	// known this is only raised for already applied versions; in offline (release verification)
	// mode it is always raised.
	ConflictModified ConflictKind = "modified"
)

// Conflict describes a single reconciliation problem.
type Conflict struct {
	// Kind is the conflict classification.
	Kind ConflictKind `json:"kind"`
	// Version is the migration version involved. It is zero for source-level conflicts.
	Version int64 `json:"version,omitempty"`
	// Source is the owner name involved in source-level conflicts.
	Source string `json:"source,omitempty"`
	// Owners lists the owner names involved, sorted alphabetically.
	Owners []string `json:"owners,omitempty"`
	// Paths lists the "owner:path" entries involved, sorted alphabetically.
	Paths []string `json:"paths,omitempty"`
	// Detail is a human-readable description.
	Detail string `json:"detail,omitempty"`
}

func (c Conflict) description() string {
	var b strings.Builder
	fmt.Fprintf(&b, "[%s]", c.Kind)
	if c.Version > 0 {
		fmt.Fprintf(&b, " version %d", c.Version)
	}
	if c.Source != "" {
		fmt.Fprintf(&b, " source %q", c.Source)
	}
	if len(c.Owners) > 0 {
		fmt.Fprintf(&b, " owners %s", strings.Join(c.Owners, ","))
	}
	if c.Detail != "" {
		fmt.Fprintf(&b, ": %s", c.Detail)
	}
	for _, p := range c.Paths {
		fmt.Fprintf(&b, "\n\t      %s", p)
	}
	return b.String()
}

// ErrReconcileConflict is the sentinel error wrapped by [ReconcileError].
var ErrReconcileConflict = errors.New("migration reconciliation failed")

// ReconcileError is returned when one or more conflicts are detected. Callers can use
// errors.Is(err, ErrReconcileConflict) and inspect the typed [Conflict] list.
type ReconcileError struct {
	Conflicts []Conflict
}

func (e *ReconcileError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s with %d conflict(s):", ErrReconcileConflict, len(e.Conflicts))
	for _, c := range e.Conflicts {
		b.WriteString("\n\t- " + c.description())
	}
	return b.String()
}

func (e *ReconcileError) Unwrap() error { return ErrReconcileConflict }

// ManifestEntry attributes a single migration version to one owner.
type ManifestEntry struct {
	Version  int64         `json:"version"`
	Owner    string        `json:"owner"`
	Type     MigrationType `json:"type"`
	Path     string        `json:"path"`
	Checksum string        `json:"checksum"`
}

// Manifest is the authoritative, ownership-attributed list of migrations.
type Manifest struct {
	// ManifestVersion is the schema version of the serialized manifest.
	ManifestVersion int `json:"manifest_version"`
	// Sources are the owner names present during reconciliation, sorted alphabetically.
	Sources []string `json:"sources"`
	// Entries are the migration entries, sorted by version.
	Entries []ManifestEntry `json:"entries"`
}

// marshal returns the canonical, deterministic JSON encoding of the manifest. Entries and sources
// are sorted and no environment-specific data (timestamps, host names) is included, so identical
// inputs always produce identical bytes.
func (m *Manifest) marshal() ([]byte, error) {
	normalized := &Manifest{
		ManifestVersion: ManifestVersion,
		Sources:         append([]string(nil), m.Sources...),
		Entries:         make([]ManifestEntry, len(m.Entries)),
	}
	sort.Strings(normalized.Sources)
	copy(normalized.Entries, m.Entries)
	sort.Slice(normalized.Entries, func(i, j int) bool {
		return normalized.Entries[i].Version < normalized.Entries[j].Version
	})
	data, err := json.MarshalIndent(normalized, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest: %w", err)
	}
	return append(data, '\n'), nil
}

// Export writes the canonical JSON encoding of the manifest to w for release verification.
func (m *Manifest) Export(w io.Writer) error {
	data, err := m.marshal()
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("failed to export manifest: %w", err)
	}
	return nil
}

// SaveManifest atomically persists the manifest to path. The manifest is first written to a
// temporary file in the same directory and then renamed into place, so path always contains a
// complete manifest. Re-running with the same manifest converges to identical bytes, even if a
// previous write was interrupted.
func SaveManifest(path string, manifest *Manifest) error {
	if path == "" {
		return errors.New("manifest path must not be empty")
	}
	if manifest == nil {
		return errors.New("manifest must not be nil")
	}
	data, err := manifest.marshal()
	if err != nil {
		return err
	}
	temp := path + ".tmp"
	f, err := os.OpenFile(temp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("failed to create temporary manifest %q: %w", temp, err)
	}
	writeErr := func() error {
		if _, err := f.Write(data); err != nil {
			return fmt.Errorf("failed to write temporary manifest %q: %w", temp, err)
		}
		// Syncing is best-effort: some filesystems do not support fsync. Rename is what makes the
		// swap atomic.
		_ = f.Sync()
		if err := f.Close(); err != nil {
			return fmt.Errorf("failed to close temporary manifest %q: %w", temp, err)
		}
		return nil
	}()
	if writeErr != nil {
		_ = f.Close()
		_ = os.Remove(temp)
		return writeErr
	}
	if err := os.Rename(temp, path); err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("failed to replace manifest %q: %w", path, err)
	}
	return nil
}

// LoadManifest loads a manifest previously written with [SaveManifest]. If no manifest exists at
// path, it returns (nil, nil).
func LoadManifest(path string) (*Manifest, error) {
	if path == "" {
		return nil, errors.New("manifest path must not be empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read manifest %q: %w", path, err)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest %q: %w", path, err)
	}
	if manifest.ManifestVersion != ManifestVersion {
		return nil, fmt.Errorf("unsupported manifest version %d in %q, expected %d",
			manifest.ManifestVersion, path, ManifestVersion)
	}
	return &manifest, nil
}

// ownedFile is a migration file attributed to an owner.
type ownedFile struct {
	owner    string
	source   Source
	checksum string
}

// sourceScan is the result of scanning all configured sources once.
type sourceScan struct {
	// files are all discovered migration files, sorted by owner name and version.
	files []ownedFile
	// conflicts contains phase-1 conflicts that can be detected without database state: missing
	// required sources and duplicate versions (within or across sources).
	conflicts []Conflict
	// present reports which owner roots were readable.
	present map[string]bool
	// sourceFS maps owner name to its filesystem.
	sourceFS map[string]fs.FS
}

// validateMigrationSources validates the configured sources and returns them in declared order.
func validateMigrationSources(sources []MigrationSource) error {
	if len(sources) == 0 {
		return errors.New("at least one migration source is required")
	}
	seen := make(map[string]bool, len(sources))
	for _, s := range sources {
		if s.Name == "" {
			return errors.New("migration source name must not be empty")
		}
		if seen[s.Name] {
			return fmt.Errorf("duplicate migration source name: %q", s.Name)
		}
		seen[s.Name] = true
		if s.FS == nil {
			return fmt.Errorf("migration source %q: fs must not be nil", s.Name)
		}
	}
	return nil
}

// checksumFile reads the migration file and returns its sha256 checksum.
func checksumFile(fsys fs.FS, path string) (string, error) {
	f, err := fsys.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// scanMigrationSources probes every source root and collects its migration files. A missing root
// is detected with fs.Stat because fs.Glob silently swallows I/O errors.
func scanMigrationSources(sources []MigrationSource) (*sourceScan, error) {
	if err := validateMigrationSources(sources); err != nil {
		return nil, err
	}
	scan := &sourceScan{
		present:  make(map[string]bool, len(sources)),
		sourceFS: make(map[string]fs.FS, len(sources)),
	}
	// versionOwners maps a version to every file providing it, across all sources.
	versionOwners := make(map[int64][]ownedFile)
	for _, src := range sources {
		if _, err := fs.Stat(src.FS, "."); err != nil {
			if src.Required {
				scan.conflicts = append(scan.conflicts, Conflict{
					Kind:   ConflictSourceMissing,
					Source: src.Name,
					Detail: fmt.Sprintf("source root is absent or unreadable: %v", err),
				})
			}
			// Optional, absent sources are skipped.
			continue
		}
		scan.present[src.Name] = true
		scan.sourceFS[src.Name] = src.FS

		collected, err := collectFilesystemSources(src.FS, false, nil, nil)
		if err != nil {
			// A duplicate version inside a single filesystem is the same class of problem as a
			// cross-source collision.
			scan.conflicts = append(scan.conflicts, Conflict{
				Kind:   ConflictDuplicateVersion,
				Source: src.Name,
				Detail: err.Error(),
			})
			continue
		}
		var readErr error
		add := func(s Source) {
			if readErr != nil {
				return
			}
			checksum, cerr := checksumFile(src.FS, s.Path)
			if cerr != nil {
				readErr = fmt.Errorf("source %q: failed to read migration %q: %w", src.Name, s.Path, cerr)
				return
			}
			of := ownedFile{owner: src.Name, source: s, checksum: checksum}
			scan.files = append(scan.files, of)
			versionOwners[s.Version] = append(versionOwners[s.Version], of)
		}
		for _, s := range collected.sqlSources {
			add(s)
		}
		for _, s := range collected.goSources {
			add(s)
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	// Phase-1 cross-source collision detection.
	versions := make([]int64, 0, len(versionOwners))
	for v := range versionOwners {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	for _, v := range versions {
		group := versionOwners[v]
		if len(group) < 2 {
			continue
		}
		owners := make([]string, 0, len(group))
		paths := make([]string, 0, len(group))
		checksums := make(map[string]bool)
		for _, f := range group {
			owners = append(owners, f.owner)
			paths = append(paths, f.owner+":"+f.source.Path)
			checksums[f.checksum] = true
		}
		sort.Strings(owners)
		sort.Strings(paths)
		kind := ConflictDuplicateVersion
		detail := "version is provided by more than one source; ownership is ambiguous"
		if len(checksums) > 1 {
			kind = ConflictContentDiverged
			detail = "version is provided by more than one source with different contents"
		}
		scan.conflicts = append(scan.conflicts, Conflict{
			Kind:    kind,
			Version: v,
			Owners:  owners,
			Paths:   paths,
			Detail:  detail,
		})
	}
	sort.Slice(scan.files, func(i, j int) bool {
		if scan.files[i].owner != scan.files[j].owner {
			return scan.files[i].owner < scan.files[j].owner
		}
		return scan.files[i].source.Version < scan.files[j].source.Version
	})
	// Stable conflict ordering: by kind-independent (version, then source) for predictable output.
	sort.SliceStable(scan.conflicts, func(i, j int) bool {
		a, b := scan.conflicts[i], scan.conflicts[j]
		if a.Version != b.Version {
			return a.Version < b.Version
		}
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		return a.Kind < b.Kind
	})
	return scan, nil
}

// buildManifest reconciles the scanned sources against the prior manifest and the optional set of
// versions applied in the database, producing the authoritative manifest and the phase-2 conflicts.
//
// If applied is nil reconciliation runs in offline (strict) mode: any owner, content or existence
// change of a recorded entry is a conflict, because it cannot be proven the version is unapplied.
//
// If applied is non-nil (online mode) ownership and content changes are only conflicts for already
// applied versions; changes to pending versions are adopted so normal development converges.
func buildManifest(scan *sourceScan, prior *Manifest, applied map[int64]bool) (*Manifest, []Conflict) {
	current := make(map[int64]ownedFile, len(scan.files))
	// Versions with cross-source collisions are excluded from the manifest; phase 1 already
	// reported them.
	colliding := make(map[int64]bool)
	for _, c := range scan.conflicts {
		if c.Version > 0 && (c.Kind == ConflictDuplicateVersion || c.Kind == ConflictContentDiverged) {
			colliding[c.Version] = true
		}
	}
	for _, f := range scan.files {
		if !colliding[f.source.Version] {
			current[f.source.Version] = f
		}
	}
	priorEntries := make(map[int64]ManifestEntry)
	if prior != nil {
		for _, e := range prior.Entries {
			priorEntries[e.Version] = e
		}
	}
	offline := applied == nil

	versions := make(map[int64]bool, len(current)+len(priorEntries))
	for v := range current {
		versions[v] = true
	}
	for v := range priorEntries {
		versions[v] = true
	}
	if applied != nil {
		for v := range applied {
			if v > 0 {
				versions[v] = true
			}
		}
	}
	ordered := make([]int64, 0, len(versions))
	for v := range versions {
		ordered = append(ordered, v)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })

	manifest := &Manifest{ManifestVersion: ManifestVersion}
	for name := range scan.present {
		manifest.Sources = append(manifest.Sources, name)
	}
	sort.Strings(manifest.Sources)

	var conflicts []Conflict
	for _, v := range ordered {
		// Cross-source collisions were already reported in phase 1; do not double-report them as
		// vanished in phase 2.
		if colliding[v] {
			continue
		}
		cur, hasCurrent := current[v]
		prev, hasPrior := priorEntries[v]
		isApplied := applied != nil && applied[v]

		switch {
		case !hasCurrent:
			// The version existed in the prior manifest or is applied, but no source provides it
			// now.
			switch {
			case offline && hasPrior:
				conflicts = append(conflicts, Conflict{
					Kind:    ConflictEntryVanished,
					Version: v,
					Source:  prev.Owner,
					Paths:   []string{prev.Owner + ":" + prev.Path},
					Detail:  "recorded version is no longer provided by any source",
				})
			case isApplied:
				owner := ""
				path := ""
				if hasPrior {
					owner, path = prev.Owner, prev.Path
				}
				conflicts = append(conflicts, Conflict{
					Kind:    ConflictEntryVanished,
					Version: v,
					Source:  owner,
					Paths:   ownerPath(owner, path),
					Detail:  "already applied version is no longer provided by any source",
				})
			}
			// Pending, unrecorded-in-db versions that vanished are simply dropped on rerun.
			continue
		case hasPrior && prev.Owner != cur.owner:
			// Owner changes take precedence over checksum changes: a different owner means the
			// migration's provenance can no longer be trusted.
			if offline || isApplied {
				conflicts = append(conflicts, Conflict{
					Kind:    ConflictOwnershipChanged,
					Version: v,
					Owners:  sortedPair(prev.Owner, cur.owner),
					Paths: []string{
						prev.Owner + ":" + prev.Path,
						cur.owner + ":" + cur.source.Path,
					},
					Detail: knownVersionAdjective(offline) +
						" version is now provided by a different owner",
				})
				continue
			}
			// Pending versions may move owners; adopt the new owner below.
		case hasPrior && prev.Checksum != cur.checksum:
			if offline || isApplied {
				conflicts = append(conflicts, Conflict{
					Kind:    ConflictModified,
					Version: v,
					Source:  cur.owner,
					Paths:   []string{cur.owner + ":" + cur.source.Path},
					Detail: knownVersionAdjective(offline) +
						" version changed contents; refusing to replace a known migration",
				})
				continue
			}
			// Pending migrations may be edited; adopt the new checksum below.
		}

		manifest.Entries = append(manifest.Entries, ManifestEntry{
			Version:  v,
			Owner:    cur.owner,
			Type:     cur.source.Type,
			Path:     cur.source.Path,
			Checksum: cur.checksum,
		})
	}
	conflicts = append(conflicts, scan.conflicts...)
	sort.SliceStable(conflicts, func(i, j int) bool {
		a, b := conflicts[i], conflicts[j]
		if a.Version != b.Version {
			return a.Version < b.Version
		}
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		return a.Kind < b.Kind
	})
	return manifest, conflicts
}

func ownerPath(owner, path string) []string {
	if owner == "" && path == "" {
		return nil
	}
	return []string{owner + ":" + path}
}

func sortedPair(a, b string) []string {
	out := []string{a, b}
	sort.Strings(out)
	return slices.Compact(out)
}

// knownVersionAdjective describes a recorded version depending on the reconciliation mode.
func knownVersionAdjective(offline bool) string {
	if offline {
		return "recorded"
	}
	return "already applied"
}

// Reconciler reconciles multiple [MigrationSource]s into an authoritative [Manifest].
//
// By default reconciliation runs in offline (strict) mode, suitable for release verification: it
// does not need a database and any owner, content or existence change of a previously recorded
// version is a conflict. Use [Reconciler.ReconcileWithApplied] when the set of applied versions is
// known, in which case pending versions may change and only already applied versions are protected
// from owner/content changes.
type Reconciler struct {
	sources []MigrationSource
	prior   *Manifest
}

// ReconcilerOption configures a [Reconciler].
type ReconcilerOption func(*Reconciler)

// WithPriorManifest seeds reconciliation with a previously generated manifest, enabling drift
// detection (owner changes, vanished entries, modified contents) between runs.
func WithPriorManifest(manifest *Manifest) ReconcilerOption {
	return func(r *Reconciler) {
		r.prior = manifest
	}
}

// NewReconciler returns a [Reconciler] for the given sources.
func NewReconciler(sources []MigrationSource, opts ...ReconcilerOption) (*Reconciler, error) {
	if err := validateMigrationSources(sources); err != nil {
		return nil, err
	}
	r := &Reconciler{sources: sources}
	for _, opt := range opts {
		opt(r)
	}
	return r, nil
}

// Reconcile runs offline (strict) reconciliation and returns the authoritative manifest. If
// conflicts are detected, a [ReconcileError] is returned and the returned manifest is nil.
func (r *Reconciler) Reconcile(ctx context.Context) (*Manifest, error) {
	return r.reconcile(ctx, nil)
}

// ReconcileWithApplied runs online reconciliation using the set of versions applied in the
// database. Pending (not applied) versions may move owners or change contents; already applied
// versions are protected.
func (r *Reconciler) ReconcileWithApplied(ctx context.Context, applied []int64) (*Manifest, error) {
	appliedSet := make(map[int64]bool, len(applied))
	for _, v := range applied {
		appliedSet[v] = true
	}
	return r.reconcile(ctx, appliedSet)
}

func (r *Reconciler) reconcile(_ context.Context, applied map[int64]bool) (*Manifest, error) {
	scan, err := scanMigrationSources(r.sources)
	if err != nil {
		return nil, err
	}
	manifest, conflicts := buildManifest(scan, r.prior, applied)
	if len(conflicts) > 0 {
		return nil, &ReconcileError{Conflicts: conflicts}
	}
	return manifest, nil
}
