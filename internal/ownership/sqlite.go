package ownership

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pressly/goose/v3/database"
)

type sqliteCatalog struct {
	exclude map[string]struct{}
}

var _ Catalog = (*sqliteCatalog)(nil)

// SQLite has no schema concept for attached databases in scope; the empty schema is canonical and
// matches unqualified migration DDL.
const sqliteSchema = ""

func (c *sqliteCatalog) ListObjects(ctx context.Context, db database.DBTxConn, kinds ...Kind) ([]Object, error) {
	want := wantedKinds(kinds)
	var out []Object

	if want[KindTable] {
		rows, err := db.QueryContext(ctx,
			`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
		if err != nil {
			return nil, fmt.Errorf("ownership/sqlite: list tables: %w", err)
		}
		var tables []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return nil, fmt.Errorf("ownership/sqlite: scan table: %w", err)
			}
			tables = append(tables, name)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("ownership/sqlite: iterate tables: %w", err)
		}
		rows.Close()
		for _, t := range tables {
			if isExcluded(c.exclude, t) {
				continue
			}
			o, err := NewObject(KindTable, sqliteSchema, "", t, FingerprintTable)
			if err != nil {
				return nil, err
			}
			out = append(out, o)
		}
	}

	// Columns and indexes are discovered per table through PRAGMAs. Only iterate user tables,
	// re-querying sqlite_master keeps the implementation independent of prior query sections.
	tableRows, err := db.QueryContext(ctx,
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return nil, fmt.Errorf("ownership/sqlite: list tables for children: %w", err)
	}
	var tables []string
	for tableRows.Next() {
		var name string
		if err := tableRows.Scan(&name); err != nil {
			tableRows.Close()
			return nil, err
		}
		tables = append(tables, name)
	}
	if err := tableRows.Err(); err != nil {
		tableRows.Close()
		return nil, err
	}
	tableRows.Close()

	for _, table := range tables {
		if isExcluded(c.exclude, table) {
			continue
		}
		if want[KindColumn] {
			cols, err := c.listColumns(ctx, db, table)
			if err != nil {
				return nil, err
			}
			out = append(out, cols...)
		}
		if want[KindIndex] {
			idxs, err := c.listIndexes(ctx, db, table)
			if err != nil {
				return nil, err
			}
			out = append(out, idxs...)
		}
	}
	return out, nil
}

func (c *sqliteCatalog) listColumns(ctx context.Context, db database.DBTxConn, table string) ([]Object, error) {
	// PRAGMA table_info does not accept bound parameters; the identifier is quoted and escaped.
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", quotePGIdent(table)))
	if err != nil {
		return nil, fmt.Errorf("ownership/sqlite: table_info %s: %w", table, err)
	}
	defer rows.Close()
	var out []Object
	for rows.Next() {
		var (
			cid     int
			name    string
			typ     string
			notNull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return nil, fmt.Errorf("ownership/sqlite: scan column of %s: %w", table, err)
		}
		o, err := NewObject(KindColumn, sqliteSchema, table, name,
			FingerprintColumn(typ, notNull == 0))
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (c *sqliteCatalog) listIndexes(ctx context.Context, db database.DBTxConn, table string) ([]Object, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_list(%s)", quotePGIdent(table)))
	if err != nil {
		return nil, fmt.Errorf("ownership/sqlite: index_list %s: %w", table, err)
	}
	type idxMeta struct {
		name   string
		unique bool
		origin string
	}
	var metas []idxMeta
	for rows.Next() {
		var (
			seq             int
			name            string
			unique          int
			origin, partial sql.NullString
		)
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			rows.Close()
			return nil, fmt.Errorf("ownership/sqlite: scan index of %s: %w", table, err)
		}
		metas = append(metas, idxMeta{name: name, unique: unique == 1, origin: origin.String})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	var out []Object
	for _, m := range metas {
		// Skip primary-key backing indexes and sqlite-managed auto indexes that cannot be dropped
		// by name.
		if m.origin == "pk" || strings.HasPrefix(m.name, "sqlite_autoindex") {
			continue
		}
		infoRows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_info(%s)", quotePGIdent(m.name)))
		if err != nil {
			return nil, fmt.Errorf("ownership/sqlite: index_info %s: %w", m.name, err)
		}
		var cols []string
		for infoRows.Next() {
			var (
				seqno, cid int
				name       sql.NullString
			)
			if err := infoRows.Scan(&seqno, &cid, &name); err != nil {
				infoRows.Close()
				return nil, fmt.Errorf("ownership/sqlite: scan index_info %s: %w", m.name, err)
			}
			if name.Valid && name.String != "" {
				cols = append(cols, name.String)
			}
		}
		if err := infoRows.Err(); err != nil {
			infoRows.Close()
			return nil, err
		}
		infoRows.Close()
		o, err := NewObject(KindIndex, sqliteSchema, table, m.name, FingerprintIndex(m.unique, cols...))
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, nil
}

func (c *sqliteCatalog) DropStatement(o Object) (string, error) {
	switch o.Kind {
	case KindTable:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s", quotePGIdent(o.Name)), nil
	case KindIndex:
		return fmt.Sprintf("DROP INDEX IF EXISTS %s", quotePGIdent(o.Name)), nil
	case KindColumn:
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", quotePGIdent(o.Parent), quotePGIdent(o.Name)), nil
	default:
		return "", fmt.Errorf("ownership/sqlite: unsupported object kind %q", o.Kind)
	}
}
