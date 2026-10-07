package rollout

import (
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
)

// RenderText writes a human-readable, color-free reconciliation report to w.
//
// Each generation gets a header with its ledger status and a table showing, per
// target, the desired, recorded and observed versions, the baseline, the
// outcome and any detail (error text).
func RenderText(report *Report, w io.Writer) error {
	if report == nil {
		return fmt.Errorf("nil report")
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)

	fmt.Fprintf(tw, "Rollout report (run: %s)\n", report.RunID)
	for _, g := range report.Generations {
		fmt.Fprintf(tw, "\nGeneration %d %q: target version %d, status: %s\n",
			g.Index, g.Name, g.TargetVersion, g.Status)
		if g.Detail != "" {
			fmt.Fprintf(tw, "  detail: %s\n", g.Detail)
		}
		fmt.Fprintln(tw, "  TARGET\tDESIRED\tRECORDED\tOBSERVED\tBASELINE\tOUTCOME\tDETAIL")
		for _, t := range g.Targets {
			detail := t.Detail
			if len(detail) > 80 {
				detail = detail[:80] + "..."
			}
			fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				t.Name,
				strconv.FormatInt(t.Desired, 10),
				strconv.FormatInt(t.Recorded, 10),
				strconv.FormatInt(t.Observed, 10),
				strconv.FormatInt(t.Baseline, 10),
				t.Outcome,
				detail,
			)
		}
	}
	return tw.Flush()
}
