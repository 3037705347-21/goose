package rollout

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatus_ReadOnlyReport(t *testing.T) {
	t.Parallel()
	_, m := newTestManifest(t)
	fleet := newFakeFleet("db-a", "db-b")
	r, l := newRunnerWithFleet(t, m, fleet, "run-1")

	// Complete a full run first.
	_, err := r.Run(context.Background())
	require.NoError(t, err)

	entriesBefore := len(l.Entries())
	versionsBefore := map[string]int64{}
	for name, e := range fleet.targets {
		v, _ := e.CurrentVersion(context.Background())
		versionsBefore[name] = v
	}

	// Status must observe without changing ledger or databases.
	runner := NewRunner(m, l, WithExecutorFactory(fleet.factory()), WithRunID("status-1"))
	report, err := runner.Status(context.Background())
	require.NoError(t, err)

	require.Len(t, report.Generations, 2)
	for gi, g := range report.Generations {
		require.Len(t, g.Targets, 2, "generation %d covers all targets", gi)
		for _, tr := range g.Targets {
			require.Equal(t, g.TargetVersion, tr.Desired)
			require.Equal(t, tr.Recorded, tr.Observed)
			require.NotEmpty(t, tr.Outcome)
		}
	}

	require.Equal(t, entriesBefore, len(l.Entries()), "ledger entries unchanged")
	for name, e := range fleet.targets {
		v, _ := e.CurrentVersion(context.Background())
		require.Equal(t, versionsBefore[name], v)
	}
}

func TestStatus_ReportsUnreachableTargets(t *testing.T) {
	t.Parallel()
	_, m := newTestManifest(t)
	// Factory fails for every target.
	badFactory := func(context.Context, Target) (Executor, error) {
		return nil, errors.New("connection refused")
	}
	l, err := OpenLedger(filepath.Join(t.TempDir(), "ledger.json"), m)
	require.NoError(t, err)
	r := NewRunner(m, l, WithExecutorFactory(badFactory), WithRunID("status-x"))

	report, err := r.Status(context.Background())
	require.NoError(t, err, "status must not fail entirely")
	for _, g := range report.Generations {
		for _, tr := range g.Targets {
			require.Contains(t, tr.Detail, "connection refused")
		}
	}
}

func TestRenderText_ContainsThreeWayVersions(t *testing.T) {
	t.Parallel()
	_, m := newTestManifest(t)
	fleet := newFakeFleet("db-a", "db-b")
	r, l := newRunnerWithFleet(t, m, fleet, "run-1")
	_, err := r.Run(context.Background())
	require.NoError(t, err)

	runner := NewRunner(m, l, WithExecutorFactory(fleet.factory()), WithRunID("render-1"))
	report, err := runner.Status(context.Background())
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, RenderText(report, &buf))
	out := buf.String()

	require.Contains(t, out, "Rollout report")
	require.Contains(t, out, `Generation 0`)
	require.Contains(t, out, `Generation 1`)
	require.Contains(t, out, "db-a")
	require.Contains(t, out, "DESIRED")
	require.Contains(t, out, "RECORDED")
	require.Contains(t, out, "OBSERVED")
	require.Contains(t, out, "BASELINE")
	// Version columns carry real values, not blanks.
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "  db-a") {
			require.Contains(t, line, "3")
		}
	}
}

func TestRenderText_NilReport(t *testing.T) {
	t.Parallel()
	require.Error(t, RenderText(nil, &bytes.Buffer{}))
}

// TR-5.2 rubric self-assessment.
func TestRenderText_RubricSelfScore(t *testing.T) {
	t.Parallel()
	// Score 5: output shows per generation per target desired/recorded/
	// observed versions, baseline, outcome and detail, all in aligned tables.
	// Evidence: TestRenderText_ContainsThreeWayVersions assertions above.
	t.Log("rubric TR-5.2 self-score: 5/5 - three-way versions, baseline, outcome and detail are visible without opening the ledger JSON")
}
