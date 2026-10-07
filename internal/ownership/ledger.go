package ownership

import (
	"context"
	"fmt"
	"strconv"

	"github.com/pressly/goose/v3/database"
)

// DefaultLedgerTable is the default name of the object-ownership ledger table.
const DefaultLedgerTable = "goose_object_ownership"

// LedgerStore persists object ownership entries. Implementations are expected to work with both
// *sql.DB and *sql.Tx through [database.DBTxConn], so ledger writes can participate in the same
// transaction as the migration DDL on backends with transactional DDL.
type LedgerStore interface {
	// TableName returns the ledger table name.
	TableName() string
	// EnsureTable creates the ledger table if it does not already exist.
	EnsureTable(ctx context.Context, db database.DBTxConn) error
	// ListByVersion returns all objects owned by the given migration version.
	ListByVersion(ctx context.Context, db database.DBTxConn, version int64) ([]Entry, error)
	// Put claims objects for a version. It is idempotent: entries that already exist are kept,
	// which makes interrupted UP runs convergent.
	Put(ctx context.Context, db database.DBTxConn, entries []Entry) error
	// Delete removes the identified ledger entries belonging to version. Deleting rows that do not
	// exist is not an error.
	Delete(ctx context.Context, db database.DBTxConn, version int64, keys []Key) error
}

// sqlDialect captures the small set of SQL differences the ledger storage cares about.
type sqlDialect struct {
	createTable      string
	insertIgnore     string // expects 6 args: version, kind, schema, parent, name, fingerprint
	deleteOne        string
	listByVersion    string
	placeholderStyle placeholderStyle
}

type placeholderStyle int

const (
	placeholdersNumbered placeholderStyle = iota // $1, $2, ...
	placeholdersQuestion                         // ?, ?, ...
)

func (s placeholderStyle) args(n int) string {
	if s == placeholdersQuestion {
		return qMarks(n)
	}
	var b []byte
	for i := 1; i <= n; i++ {
		if i > 1 {
			b = append(b, ',')
		}
		b = append(b, '$')
		b = append(b, byte('0'+i))
	}
	return string(b)
}

func qMarks(n int) string {
	if n == 0 {
		return ""
	}
	b := make([]byte, 0, n*2-1)
	for i := 0; i < n; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '?')
	}
	return string(b)
}

// placeholder returns the single placeholder for the 1-based position pos.
func (s placeholderStyle) placeholder(pos int) string {
	if s == placeholdersQuestion {
		return "?"
	}
	return "$" + strconv.Itoa(pos)
}

// sqLLedger is a portable LedgerStore implementation used by postgres, mysql and sqlite.
type sqlLedger struct {
	table   string
	dialect sqlDialect
}

var _ LedgerStore = (*sqlLedger)(nil)

func (l *sqlLedger) TableName() string { return l.table }

func (l *sqlLedger) EnsureTable(ctx context.Context, db database.DBTxConn) error {
	if _, err := db.ExecContext(ctx, l.dialect.createTable); err != nil {
		return fmt.Errorf("ownership: create ledger table %q: %w", l.table, err)
	}
	return nil
}

func (l *sqlLedger) ListByVersion(ctx context.Context, db database.DBTxConn, version int64) ([]Entry, error) {
	rows, err := db.QueryContext(ctx, l.dialect.listByVersion, version)
	if err != nil {
		return nil, fmt.Errorf("ownership: list ledger entries for version %d: %w", version, err)
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var (
			e                              Entry
			kind, schema, parent, name, fp string
		)
		if err := rows.Scan(&e.Version, &kind, &schema, &parent, &name, &fp); err != nil {
			return nil, fmt.Errorf("ownership: scan ledger entry: %w", err)
		}
		key, err := keyFromFields(Kind(kind), schema, parent, name)
		if err != nil {
			return nil, err
		}
		e.Key = key
		e.Fingerprint = fp
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ownership: iterate ledger entries: %w", err)
	}
	return out, nil
}

func (l *sqlLedger) Put(ctx context.Context, db database.DBTxConn, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	for _, e := range entries {
		if _, err := db.ExecContext(
			ctx,
			l.dialect.insertIgnore,
			e.Version, string(e.Key.Kind), e.Key.Schema, e.Key.Parent, e.Key.Name, e.Fingerprint,
		); err != nil {
			return fmt.Errorf("ownership: claim %s for version %d: %w", e.Key, e.Version, err)
		}
	}
	return nil
}

func (l *sqlLedger) Delete(ctx context.Context, db database.DBTxConn, version int64, keys []Key) error {
	for _, k := range keys {
		if _, err := db.ExecContext(
			ctx,
			l.dialect.deleteOne,
			version, string(k.Kind), k.Schema, k.Parent, k.Name,
		); err != nil {
			return fmt.Errorf("ownership: delete ledger entry %s (version %d): %w", k, version, err)
		}
	}
	return nil
}

// newSQLLedger builds the portable ledger implementation for one of the supported backend names.
func newSQLLedger(table string, style placeholderStyle, ddl string) *sqlLedger {
	l := &sqlLedger{table: table}
	args := style.args
	l.dialect = sqlDialect{
		placeholderStyle: style,
		createTable:      fmt.Sprintf(ddl, table),
		insertIgnore: fmt.Sprintf(
			"INSERT %s INTO %s (version_id, object_kind, schema_name, parent_name, object_name, fingerprint) VALUES (%s)",
			ignoreKeyword(style), table, args(6),
		),
		deleteOne: fmt.Sprintf(
			"DELETE FROM %s WHERE version_id=%s AND object_kind=%s AND schema_name=%s AND parent_name=%s AND object_name=%s",
			table, style.placeholder(1), style.placeholder(2), style.placeholder(3),
			style.placeholder(4), style.placeholder(5),
		),
		listByVersion: fmt.Sprintf(
			"SELECT version_id, object_kind, schema_name, parent_name, object_name, fingerprint FROM %s WHERE version_id=%s",
			table, args(1),
		),
	}
	return l
}

func ignoreKeyword(style placeholderStyle) string {
	if style == placeholdersQuestion {
		// MySQL and SQLite both accept INSERT OR IGNORE? MySQL uses INSERT IGNORE. Caller picks the
		// DDL/keyword per backend, so this helper is only used for sqlite question-style.
		return "OR IGNORE"
	}
	return ""
}

// postgresDDL / mysqlDDL / sqliteDDL define identical logical schemas; only column types and the
// upsert syntax differ.
const postgresDDL = `CREATE TABLE IF NOT EXISTS %s (
	id BIGSERIAL PRIMARY KEY,
	version_id BIGINT NOT NULL,
	object_kind TEXT NOT NULL,
	schema_name TEXT NOT NULL DEFAULT '',
	parent_name TEXT NOT NULL DEFAULT '',
	object_name TEXT NOT NULL,
	fingerprint TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	UNIQUE (version_id, object_kind, schema_name, parent_name, object_name)
)`

const mysqlDDL = `CREATE TABLE IF NOT EXISTS %s (
	id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
	version_id BIGINT NOT NULL,
	object_kind VARCHAR(32) NOT NULL,
	schema_name VARCHAR(191) NOT NULL DEFAULT '',
	parent_name VARCHAR(191) NOT NULL DEFAULT '',
	object_name VARCHAR(191) NOT NULL,
	fingerprint TEXT NOT NULL,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

	UNIQUE KEY uq_ownership_version_object (version_id, object_kind, schema_name, parent_name, object_name)
)`

const sqliteDDL = `CREATE TABLE IF NOT EXISTS %s (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	version_id INTEGER NOT NULL,
	object_kind TEXT NOT NULL,
	schema_name TEXT NOT NULL DEFAULT '',
	parent_name TEXT NOT NULL DEFAULT '',
	object_name TEXT NOT NULL,
	fingerprint TEXT NOT NULL,
	created_at TIMESTAMP NOT NULL DEFAULT (datetime('now')),
	UNIQUE (version_id, object_kind, schema_name, parent_name, object_name)
)`

func newPostgresLedger(table string) *sqlLedger {
	l := newSQLLedger(table, placeholdersNumbered, postgresDDL)
	// PostgreSQL has no INSERT IGNORE; ON CONFLICT DO NOTHING keeps the first claim.
	l.dialect.insertIgnore = fmt.Sprintf(
		"INSERT INTO %s (version_id, object_kind, schema_name, parent_name, object_name, fingerprint) VALUES (%s) ON CONFLICT DO NOTHING",
		table, l.dialect.placeholderStyle.args(6),
	)
	return l
}

func newMysqlLedger(table string) *sqlLedger {
	l := newSQLLedger(table, placeholdersQuestion, mysqlDDL)
	l.dialect.insertIgnore = fmt.Sprintf(
		"INSERT IGNORE INTO %s (version_id, object_kind, schema_name, parent_name, object_name, fingerprint) VALUES (%s)",
		table, qMarks(6),
	)
	return l
}

func newSQLiteLedger(table string) *sqlLedger {
	return newSQLLedger(table, placeholdersQuestion, sqliteDDL)
}
