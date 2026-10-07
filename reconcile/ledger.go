package reconcile

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// Ledger is the per-app record of the last reconciliation result. One Ledger is stored as a
// single JSON file in the configured ledger directory.
type Ledger struct {
	App          string `json:"app"`
	VersionTable string `json:"version_table"`
	Status       Status `json:"status"`
	Reason       string `json:"reason,omitempty"`

	ConvergedAt    time.Time `json:"converged_at"`
	RegistryDigest string    `json:"registry_digest"`

	TableExisted bool `json:"table_existed"`

	ExpectedVersions []int64 `json:"expected_versions"`
	ObservedVersions []int64 `json:"observed_versions"`
	UnknownVersions  []int64 `json:"unknown_versions,omitempty"`
	PendingBefore    []int64 `json:"pending_before,omitempty"`
	Applied          []int64 `json:"applied,omitempty"`

	VersionBefore int64 `json:"version_before"`
	VersionAfter  int64 `json:"version_after"`
	TargetVersion int64 `json:"target_version"`

	NewDrift bool `json:"new_drift,omitempty"`
}

// ledgerPath returns the ledger file path for an application. App names are restricted to
// filesystem-safe characters during config validation.
func ledgerPath(dir, appName string) string {
	return filepath.Join(dir, appName+".json")
}

// ReadLedger reads the last ledger for appName. It returns (nil, nil) when no ledger exists yet.
func ReadLedger(dir, appName string) (*Ledger, error) {
	b, err := os.ReadFile(ledgerPath(dir, appName))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read ledger for %q: %w", appName, err)
	}
	var l Ledger
	if err := json.Unmarshal(b, &l); err != nil {
		return nil, fmt.Errorf("parse ledger for %q: %w", appName, err)
	}
	return &l, nil
}

// writeLedger atomically writes the ledger to <dir>/<app>.json via a temp file and rename.
func writeLedger(dir string, l *Ledger) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create ledger directory %q: %w", dir, err)
	}
	b, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal ledger for %q: %w", l.App, err)
	}
	b = append(b, '\n')

	path := ledgerPath(dir, l.App)
	tmp, err := os.CreateTemp(dir, "."+l.App+".*.json.tmp")
	if err != nil {
		return "", fmt.Errorf("create temp ledger for %q: %w", l.App, err)
	}
	tmpName := tmp.Name()
	defer func() {
		if tmp != nil {
			_ = tmp.Close()
		}
		_ = os.Remove(tmpName)
	}()
	if _, err := tmp.Write(b); err != nil {
		return "", fmt.Errorf("write temp ledger for %q: %w", l.App, err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close temp ledger for %q: %w", l.App, err)
	}
	tmp = nil
	if err := os.Rename(tmpName, path); err != nil {
		return "", fmt.Errorf("replace ledger for %q: %w", l.App, err)
	}
	return path, nil
}
