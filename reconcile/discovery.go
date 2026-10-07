package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/internal/controller"
)

// tableFinder lists goose version tables present in the database. Discovery is best-effort and
// only supported on dialects with a queryable schema catalog.
type tableFinder interface {
	findGooseTables(ctx context.Context, db *sql.DB) ([]string, error)
}

// newTableFinder returns the dialect-specific table finder, or nil when unregistered tables
// cannot be discovered for this dialect.
func newTableFinder(d database.Dialect) tableFinder {
	switch d {
	case database.DialectMySQL, database.DialectTiDB:
		return mysqlTableFinder{}
	default:
		return nil
	}
}

// mysqlTableFinder discovers tables that look like goose version tables in the current database.
// A goose version table is defined as a table having both version_id and is_applied columns,
// which matches the DDL emitted by every goose dialect.
type mysqlTableFinder struct{}

var _ tableFinder = mysqlTableFinder{}

func (mysqlTableFinder) findGooseTables(ctx context.Context, db *sql.DB) ([]string, error) {
	const q = `
SELECT table_name
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND column_name IN ('version_id', 'is_applied')
GROUP BY table_name
HAVING COUNT(DISTINCT column_name) = 2`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("discover goose version tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan discovered table: %w", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("discover goose version tables: %w", err)
	}
	return tables, nil
}

// tableState describes the versions recorded in a registered application's version table.
type tableState struct {
	exists  bool
	applied []int64 // distinct versions whose latest record is applied, ascending
}

// inspectTable reports whether the version table exists and which versions are recorded as
// applied. It never creates or mutates the table.
func inspectTable(ctx context.Context, d database.Dialect, db *sql.DB, tableName string) (tableState, error) {
	store, err := database.NewStore(d, tableName)
	if err != nil {
		return tableState{}, err
	}
	sc := controller.NewStoreController(store)

	var rows []*database.ListMigrationsResult
	switch ok, err := sc.TableExists(ctx, db); {
	case err == nil && !ok:
		return tableState{exists: false}, nil
	case err == nil:
		rows, err = sc.ListMigrations(ctx, db)
		if err != nil {
			return tableState{}, fmt.Errorf("list migrations from %q: %w", tableName, err)
		}
	case errors.Is(err, errors.ErrUnsupported):
		// The dialect has no catalog probe. Fall back to querying the table directly, mirroring
		// goose's own version-table bootstrap: any error means the table is treated as absent.
		rows, err = sc.ListMigrations(ctx, db)
		if err != nil {
			return tableState{exists: false}, nil
		}
	default:
		return tableState{}, fmt.Errorf("check version table %q: %w", tableName, err)
	}

	// ListMigrations returns rows ordered by id DESC, so the first row seen for a version is its
	// latest record and determines whether the version is currently applied.
	latest := make(map[int64]bool)
	for _, row := range rows {
		if _, seen := latest[row.Version]; seen {
			continue
		}
		latest[row.Version] = row.IsApplied
	}
	state := tableState{exists: true}
	for version, applied := range latest {
		if version == 0 || !applied {
			continue
		}
		state.applied = append(state.applied, version)
	}
	slices.Sort(state.applied)
	return state, nil
}
