package reconcile

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// Status is the reconciliation outcome for a single application.
type Status string

const (
	// StatusConverged means the application's ownership was confirmed and its migration list is
	// fully applied (the version table may have been created during the run).
	StatusConverged Status = "converged"
	// StatusDriftBlocked means the version table contains applied versions not present in the
	// application's migration list. The table was not touched.
	StatusDriftBlocked Status = "drift_blocked"
	// StatusConflict means the same version table is registered to more than one application.
	// The table was not touched.
	StatusConflict Status = "conflict"
	// StatusBlockedDependency means a dependency (transitively) did not converge, so this
	// application was skipped.
	StatusBlockedDependency Status = "blocked_dependency"
	// StatusFailed means convergence failed, for example because migration files were missing or
	// Provider.Up returned an error.
	StatusFailed Status = "failed"
)

// AppResult is the reconciliation result for one application.
type AppResult struct {
	// Name is the application name.
	Name string `json:"name"`
	// Table is the goose version table claimed by the application.
	Table string `json:"table"`
	// DependsOn is the declared dependency list.
	DependsOn []string `json:"depends_on"`
	// Status is the final status of the application.
	Status Status `json:"status"`
	// Reason gives human-readable detail for non-converged statuses.
	Reason string `json:"reason,omitempty"`
	// TableExisted reports whether the version table already existed before the run.
	TableExisted bool `json:"table_existed"`

	// ExpectedVersions are all versions declared by the application's migration list.
	ExpectedVersions []int64 `json:"expected_versions"`
	// ObservedVersions are applied versions found in the version table before convergence.
	ObservedVersions []int64 `json:"observed_versions"`
	// UnknownVersions are applied versions that are not part of the migration list.
	UnknownVersions []int64 `json:"unknown_versions,omitempty"`
	// PendingBefore are versions that were pending before convergence.
	PendingBefore []int64 `json:"pending_before,omitempty"`
	// Applied are the versions applied during this run.
	Applied []int64 `json:"applied,omitempty"`

	// VersionBefore is the highest applied version before the run, 0 for a missing table.
	VersionBefore int64 `json:"version_before"`
	// VersionAfter is the highest applied version after convergence.
	VersionAfter int64 `json:"version_after"`
	// TargetVersion is the highest version in the migration list.
	TargetVersion int64 `json:"target_version"`

	// NewDrift reports unknown versions that were not recorded as unknown in the previous ledger.
	NewDrift bool `json:"new_drift,omitempty"`
	// LedgerPath is the path of the ledger written for this application, if any.
	LedgerPath string `json:"ledger_path,omitempty"`
}

// Report is the result of one reconciliation run, in application dependency order.
type Report struct {
	// RanAt is when the run completed.
	RanAt time.Time `json:"ran_at"`
	// DryRun reports whether mutations were disabled.
	DryRun bool `json:"dry_run"`
	// DiscoverySupported reports whether the dialect can scan for unregistered goose tables.
	DiscoverySupported bool `json:"discovery_supported"`
	// UnregisteredTables are goose version tables present in the database but declared by no
	// application. They are never touched.
	UnregisteredTables []string `json:"unregistered_tables,omitempty"`
	// Apps are per-application results, in dependency order.
	Apps []AppResult `json:"apps"`
}

// app returns the result for the named application, or nil.
func (r *Report) app(name string) *AppResult {
	for i := range r.Apps {
		if r.Apps[i].Name == name {
			return &r.Apps[i]
		}
	}
	return nil
}

// IsAnomaly reports whether the status blocks convergence and therefore must not be touched.
func (s Status) IsAnomaly() bool {
	switch s {
	case StatusConverged:
		return false
	default:
		return true
	}
}

// HasAnomalies reports whether the run found conflicts, unregistered tables, drift, blocked
// dependencies or application failures.
func (r *Report) HasAnomalies() bool {
	if len(r.UnregisteredTables) > 0 {
		return true
	}
	for _, app := range r.Apps {
		if app.Status.IsAnomaly() {
			return true
		}
	}
	return false
}

// Error aggregates all anomalies into a single error. It returns nil when [Report.HasAnomalies]
// is false.
func (r *Report) Error() error {
	if !r.HasAnomalies() {
		return nil
	}
	var b strings.Builder
	b.WriteString("reconcile: anomalies found")
	if len(r.UnregisteredTables) > 0 {
		b.WriteString(fmt.Sprintf("\n\tunregistered goose version tables (not touched): %s", joinNames(r.UnregisteredTables)))
	}
	for _, app := range r.Apps {
		if !app.Status.IsAnomaly() {
			continue
		}
		line := fmt.Sprintf("\n\t%s: %s", app.Name, app.Status)
		if app.Reason != "" {
			line += ": " + app.Reason
		}
		b.WriteString(line)
	}
	return reportError(b.String())
}

// reportError is a distinct error type so callers can detect reconciliation anomalies.
type reportError string

func (e reportError) Error() string { return string(e) }

// WriteText renders a human-readable reconciliation report to w.
func (r *Report) WriteText(w io.Writer) {
	mode := "CONVERGE"
	if r.DryRun {
		mode = "DRY-RUN"
	}
	fmt.Fprintf(w, "goose reconcile (%s) %s\n", mode, r.RanAt.Format(time.RFC3339))
	if len(r.UnregisteredTables) > 0 {
		fmt.Fprintf(w, "\nUnregistered goose version tables (reported, not touched):\n")
		for _, name := range r.UnregisteredTables {
			fmt.Fprintf(w, "  - %s\n", name)
		}
	}
	if !r.DiscoverySupported {

		fmt.Fprintf(w, "\nNote: unregistered-table discovery is not supported for this dialect; only registered tables were reconciled.\n")
	}
	fmt.Fprintf(w, "\n%-20s %-28s %-18s %s\n", "APP", "VERSION TABLE", "STATUS", "DETAIL")
	fmt.Fprintf(w, "%-20s %-28s %-18s %s\n",
		strings.Repeat("─", 20), strings.Repeat("─", 28), strings.Repeat("─", 18), strings.Repeat("─", 24))
	for _, app := range r.Apps {
		detail := app.Reason
		if detail == "" {
			switch {
			case len(app.Applied) > 0:
				detail = fmt.Sprintf("applied %v (%d -> %d)", app.Applied, app.VersionBefore, app.VersionAfter)
			case app.TableExisted:
				detail = fmt.Sprintf("up to date at version %d", app.VersionAfter)
			default:
				detail = fmt.Sprintf("table created, converged to %d", app.VersionAfter)
			}
			if r.DryRun && len(app.PendingBefore) > 0 {
				detail = fmt.Sprintf("would apply %v (%d -> %d)", app.PendingBefore, app.VersionBefore, app.TargetVersion)
			}
		}
		fmt.Fprintf(w, "%-20s %-28s %-18s %s\n", app.Name, app.Table, app.Status, detail)
	}
}
