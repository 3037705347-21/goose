package rollout

import "context"

// Status builds a read-only reconciliation report for the entire manifest.
//
// For every target in every generation it presents the desired version
// (manifest), the recorded version and outcome (ledger) and the observed
// version (database). Status does not acquire the run lock, write the ledger or
// modify any database. Targets that cannot be reached are reported with the
// error in Detail; this does not abort the report.
func (r *Runner) Status(ctx context.Context) (*Report, error) {
	report := newReport(r.runID)
	defer r.closeExecutors()

	for gi := range r.m.Generations {
		g := &r.m.Generations[gi]
		state, err := r.ledger.Generation(gi)
		gr := GenReport{
			Index: gi, Name: g.Name, TargetVersion: g.TargetVersion,
			Status:  GenPending,
			Targets: []TargetReport{},
		}
		if err == nil {
			gr.Status = state.Status
			gr.Detail = state.Detail
			if state.StartedAt != nil {
				gr.StartedAt = *state.StartedAt
			}
			if state.FinishedAt != nil {
				gr.FinishedAt = *state.FinishedAt
			}
		}

		for _, name := range g.Targets {
			tr := TargetReport{Name: name, Desired: g.TargetVersion}

			if last, ok := r.ledger.LastEntry(name); ok {
				tr.Recorded = RecordedVersion(last)
				tr.Baseline = last.Baseline
			}
			if latest := latestGenEntry(r.ledger, gi, name); latest.Kind != "" {
				tr.Outcome = latest.Kind
			}

			exec, execErr := r.executor(ctx, name)
			if execErr != nil {
				tr.Detail = execErr.Error()
				gr.Targets = append(gr.Targets, tr)
				continue
			}
			observed, obsErr := exec.CurrentVersion(ctx)
			if obsErr != nil {
				tr.Detail = obsErr.Error()
			} else {
				tr.Observed = observed
			}
			gr.Targets = append(gr.Targets, tr)
		}
		report.Generations = append(report.Generations, gr)
	}
	return report, nil
}
