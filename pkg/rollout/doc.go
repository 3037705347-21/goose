// Package rollout provides generation-based, reconcilable canary rollouts for a
// fleet of goose-managed databases.
//
// A manifest declares an ordered list of generations. Each generation names a
// target goose version and the databases that must converge to it. The rollout
// Runner advances generations strictly in order and records every observation,
// intent and outcome in a local JSON ledger. If any database in a generation
// fails, every database already upgraded in that generation is rolled back to
// its per-database baseline.
//
// Reconciliation is three-way:
//
//   - desired:   the manifest;
//   - recorded:  the ledger;
//   - observed:  the version reported by each database (goose_db_version).
//
// When observed and recorded disagree the database is reported as a conflict
// and left untouched. Databases not listed in the manifest are never
// contacted. An interrupted run is safely resumable: the next run reconciles
// open intents against the observed state and converges.
package rollout
