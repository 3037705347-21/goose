package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/pressly/goose/v3/reconcile"
)

// Reconcile-specific flags are registered on the package-level flag set because xflag's
// ParseToEnd parses flags interspersed with positional arguments all the way to the end.
var (
	reconcileRegistry = flags.String(
		"registry",
		os.Getenv("GOOSE_RECONCILE_FILE"),
		"reconcile: path to the YAML app registry file (GOOSE_RECONCILE_FILE supported)",
	)
	reconcileRegistryShort = flags.String(
		"f",
		os.Getenv("GOOSE_RECONCILE_FILE"),
		"reconcile: alias for -registry",
	)
	reconcileLedgerDir = flags.String(
		"ledger-dir",
		"",
		"reconcile: directory to store one JSON drift ledger per app (overrides registry)",
	)
	reconcileDryRun = flags.Bool(
		"dry-run",
		false,
		"reconcile: report ownership and drift without migrating or writing ledgers",
	)
)

// gooseReconcile implements the "reconcile" command:
//
//	goose DRIVER DBSTRING reconcile [-f|--registry FILE] [--ledger-dir DIR] [--dry-run]
//
// It reconciles per-application goose version tables in a shared database against a YAML
// registry, reports conflicts, drift and unregistered tables without touching them, and
// converges confirmed applications in dependency order.
func gooseReconcile(ctx context.Context, driver string, db *sql.DB, positional []string) error {
	registryPath := *reconcileRegistry
	if registryPath == "" {
		// Fall back to the alias, then to a positional path: goose DRIVER DBSTRING reconcile FILE
		registryPath = *reconcileRegistryShort
	}
	if registryPath == "" && len(positional) > 0 {
		registryPath = positional[0]
		positional = positional[1:]
	}
	if registryPath == "" {
		return fmt.Errorf("reconcile requires a registry file: goose DRIVER DBSTRING reconcile -f registry.yaml")
	}
	if len(positional) > 0 {
		return fmt.Errorf("reconcile: unexpected arguments: %v", positional)
	}

	cfg, err := reconcile.LoadConfig(registryPath)
	if err != nil {
		return err
	}
	d, err := driverToDialect(driver)
	if err != nil {
		return err
	}
	if cfg.Dialect == "" {
		cfg.Dialect = d
	} else if cfg.Dialect != d {
		return fmt.Errorf("reconcile: registry dialect %q does not match driver %q (dialect %q)", cfg.Dialect, driver, d)
	}

	opts := []reconcile.Option{}
	if *reconcileLedgerDir != "" {
		opts = append(opts, reconcile.WithLedgerDir(*reconcileLedgerDir))
	}
	if *reconcileDryRun {
		opts = append(opts, reconcile.WithDryRun(true))
	}

	runner, err := reconcile.New(db, cfg, opts...)
	if err != nil {
		return err
	}
	report, err := runner.Run(ctx)
	if report != nil {
		report.WriteText(os.Stdout)
	}
	return err
}

// driverToDialect maps a goose CLI driver name to a dialect accepted by the reconcile package.
func driverToDialect(driver string) (string, error) {
	switch driver {
	case "postgres", "pgx":
		return "postgres", nil
	case "mysql":
		return "mysql", nil
	case "tidb", "starrocks":
		return driver, nil
	case "sqlite3", "sqlite":
		return "sqlite3", nil
	case "spanner":
		return "spanner", nil
	case "mssql", "azuresql", "sqlserver":
		return "mssql", nil
	case "redshift":
		return "redshift", nil
	case "clickhouse":
		return "clickhouse", nil
	case "ydb":
		return "ydb", nil
	case "turso":
		return "turso", nil
	default:
		return "", fmt.Errorf("reconcile: cannot infer dialect from driver %q; set dialect in the registry", driver)
	}
}
