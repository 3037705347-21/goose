package ownership

import (
	"context"
	"fmt"

	"github.com/pressly/goose/v3/database"
)

// Catalog reads the live object catalog of one database backend and renders the DDL used to drop
// objects. Every supported backend implements this against its own system views; the reconciler
// only ever sees canonical [Object] values.
type Catalog interface {
	// ListObjects returns the current objects of the requested kinds. Objects belonging to
	// goose-internal tables (version table, ledger table) are excluded.
	ListObjects(ctx context.Context, db database.DBTxConn, kinds ...Kind) ([]Object, error)
	// DropStatement renders backend-specific DDL that drops obj. The reconciler only calls this
	// for objects it has approved for deletion, so implementations do not need to re-check
	// ownership.
	DropStatement(obj Object) (string, error)
}

// Backend bundles the catalog reader and ledger storage for one database backend.
type Backend struct {
	name    string
	catalog Catalog
	ledger  LedgerStore
}

// Name returns the canonical backend name ("postgres", "mysql", "sqlite").
func (b *Backend) Name() string { return b.name }

// Catalog returns the backend's catalog reader.
func (b *Backend) Catalog() Catalog { return b.catalog }

// Ledger returns the backend's ledger storage.
func (b *Backend) Ledger() LedgerStore { return b.ledger }

// NewBackend resolves a goose dialect name to a supported backend.
//
// versionTable and ledgerTable are excluded from catalog snapshots so goose never claims its own
// bookkeeping objects.
//
// The two production backends are PostgreSQL and MySQL; SQLite is additionally supported so the
// full reconciliation flow can be exercised without an external database.
func NewBackend(dialectName, versionTable, ledgerTable string) (*Backend, error) {
	if versionTable == "" {
		return nil, fmt.Errorf("ownership: version table name must not be empty")
	}
	if ledgerTable == "" {
		ledgerTable = DefaultLedgerTable
	}
	excludes := map[string]struct{}{
		canonicalize(versionTable): {},
		canonicalize(ledgerTable):  {},
	}
	switch dialectName {
	case "postgres", "pgx", "dsql":
		return &Backend{
			name:    "postgres",
			catalog: &postgresCatalog{exclude: excludes},
			ledger:  newPostgresLedger(ledgerTable),
		}, nil
	case "mysql", "tidb":
		return &Backend{
			name:    "mysql",
			catalog: &mysqlCatalog{exclude: excludes},
			ledger:  newMysqlLedger(ledgerTable),
		}, nil
	case "sqlite3", "sqlite", "turso":
		return &Backend{
			name:    "sqlite",
			catalog: &sqliteCatalog{exclude: excludes},
			ledger:  newSQLiteLedger(ledgerTable),
		}, nil
	default:
		return nil, fmt.Errorf("ownership: dialect %q is not supported by object ownership reconciliation", dialectName)
	}
}

// isExcluded reports whether a table name belongs to goose bookkeeping and must never be claimed.
func isExcluded(exclude map[string]struct{}, table string) bool {
	_, ok := exclude[canonicalize(table)]
	return ok
}
