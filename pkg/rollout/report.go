package rollout

import "time"

// Report is the outcome of a run or a read-only reconciliation.
type Report struct {
	// RunID identifies the run that produced the report.
	RunID string `json:"run_id"`
	// Generations holds one report per processed generation, in order.
	Generations []GenReport `json:"generations"`
}

// GenReport describes what happened to one generation.
type GenReport struct {
	Index         int       `json:"index"`
	Name          string    `json:"name"`
	TargetVersion int64     `json:"target_version"`
	Status        GenStatus `json:"status"`
	Targets       []TargetReport `json:"targets,omitempty"`
	StartedAt     time.Time `json:"started_at,omitempty"`
	FinishedAt    time.Time `json:"finished_at,omitempty"`
	Detail        string    `json:"detail,omitempty"`
}

// TargetReport describes the reconciliation of one target within a generation.
type TargetReport struct {
	Name string `json:"name"`
	// Desired is the generation's target version.
	Desired int64 `json:"desired"`
	// Recorded is the version last recorded in the ledger.
	Recorded int64 `json:"recorded"`
	// Observed is the version reported by the database.
	Observed int64 `json:"observed"`
	// Outcome is the resulting entry kind.
	Outcome EntryKind `json:"outcome"`
	// Baseline is the target's baseline for the generation.
	Baseline int64 `json:"baseline"`
	Detail   string `json:"detail,omitempty"`
}

// Entry factories.

func upgradedEntry(runID string, gi int, genName string, base Entry, observed int64) Entry {
	return Entry{
		RunID: runID, GenIndex: gi, GenName: genName, Target: base.Target,
		Kind: EntryUpgraded, Baseline: base.Baseline,
		From: base.From, To: base.To, Observed: observed,
	}
}

func rolledBackEntry(runID string, gi int, genName string, base Entry, observed int64) Entry {
	return Entry{
		RunID: runID, GenIndex: gi, GenName: genName, Target: base.Target,
		Kind: EntryRolledBack, Baseline: base.Baseline,
		From: base.From, To: base.To, Observed: observed,
	}
}

func failedEntry(runID string, gi int, genName string, base Entry, detail string) Entry {
	return Entry{
		RunID: runID, GenIndex: gi, GenName: genName, Target: base.Target,
		Kind: EntryFailed, Baseline: base.Baseline,
		From: base.From, To: base.To, Detail: detail,
	}
}

func conflictEntry(runID string, gi int, genName string, base Entry, expected, observed int64, detail string) Entry {
	return Entry{
		RunID: runID, GenIndex: gi, GenName: genName, Target: base.Target,
		Kind: EntryConflict, Baseline: base.Baseline,
		Expected: expected, Observed: observed, Detail: detail,
	}
}

// Report factories.

func upgradedReport(name string, desired int64, base Entry, observed int64) TargetReport {
	return TargetReport{
		Name: name, Desired: desired,
		Recorded: base.From, Observed: observed,
		Outcome: EntryUpgraded, Baseline: base.Baseline,
	}
}

func failedReport(name string, desired int64, detail string) TargetReport {
	return TargetReport{Name: name, Desired: desired, Outcome: EntryFailed, Detail: detail}
}

func failedReportWithBaseline(name string, desired, baseline int64, detail string) TargetReport {
	return TargetReport{
		Name: name, Desired: desired, Outcome: EntryFailed,
		Baseline: baseline, Detail: detail,
	}
}

func conflictReport(name string, desired, baseline, expected, observed int64, detail string) TargetReport {
	return TargetReport{
		Name: name, Desired: desired, Recorded: expected, Observed: observed,
		Outcome: EntryConflict, Baseline: baseline, Detail: detail,
	}
}

func newReport(runID string) *Report {
	return &Report{RunID: runID, Generations: []GenReport{}}
}

func genReportFromState(s GenerationState) GenReport {
	gr := GenReport{
		Index:         s.Index,
		Name:          s.Name,
		TargetVersion: s.TargetVersion,
		Status:        s.Status,
		Detail:        s.Detail,
	}
	if s.StartedAt != nil {
		gr.StartedAt = *s.StartedAt
	}
	if s.FinishedAt != nil {
		gr.FinishedAt = *s.FinishedAt
	}
	return gr
}
