package ownership

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pressly/goose/v3/database"
)

type postgresCatalog struct {
	exclude map[string]struct{}
}

var _ Catalog = (*postgresCatalog)(nil)

func (c *postgresCatalog) ListObjects(ctx context.Context, db database.DBTxConn, kinds ...Kind) ([]Object, error) {
	want := wantedKinds(kinds)
	var out []Object

	if want[KindTable] {
		rows, err := db.QueryContext(ctx, `
			SELECT current_schema(), tablename
			FROM pg_tables
			WHERE schemaname = current_schema()`)
		if err != nil {
			return nil, fmt.Errorf("ownership/postgres: list tables: %w", err)
		}
		tables, err := scanPairs(rows)
		rows.Close()
		if err != nil {
			return nil, err
		}
		for _, t := range tables {
			if isExcluded(c.exclude, t.second) {
				continue
			}
			o, err := NewObject(KindTable, t.first, "", t.second, FingerprintTable)
			if err != nil {
				return nil, err
			}
			out = append(out, o)
		}
	}

	if want[KindColumn] {
		rows, err := db.QueryContext(ctx, `
			SELECT current_schema(), c.table_name, c.column_name, c.udt_name, c.is_nullable,
			       c.character_maximum_length, c.numeric_precision, c.numeric_scale
			FROM information_schema.columns c
			WHERE c.table_schema = current_schema()
			ORDER BY c.table_name, c.ordinal_position`)
		if err != nil {
			return nil, fmt.Errorf("ownership/postgres: list columns: %w", err)
		}
		var (
			schema, table, name, udt, nullable string
			maxLength, precision, scale        sql.NullInt64
		)
		for rows.Next() {
			if err := rows.Scan(&schema, &table, &name, &udt, &nullable, &maxLength, &precision, &scale); err != nil {
				rows.Close()
				return nil, fmt.Errorf("ownership/postgres: scan column: %w", err)
			}
			if isExcluded(c.exclude, table) {
				continue
			}
			o, err := NewObject(KindColumn, schema, table, name,
				FingerprintColumn(pgColumnType(udt, maxLength, precision, scale), nullable == "YES"))
			if err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, o)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("ownership/postgres: iterate columns: %w", err)
		}
		rows.Close()
	}

	if want[KindIndex] {
		rows, err := db.QueryContext(ctx, `
			SELECT current_schema(), t.relname, i.relname, ix.indisunique, ix.indisprimary,
			       pg_get_indexdef(ix.indexrelid)
			FROM pg_index ix
			JOIN pg_class t ON t.oid = ix.indrelid
			JOIN pg_class i ON i.oid = ix.indexrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			WHERE n.nspname = current_schema()
			  AND t.relkind IN ('r', 'p')`)
		if err != nil {
			return nil, fmt.Errorf("ownership/postgres: list indexes: %w", err)
		}
		var (
			schema, table, index, def string
			unique, primary           bool
		)
		for rows.Next() {
			if err := rows.Scan(&schema, &table, &index, &unique, &primary, &def); err != nil {
				return nil, fmt.Errorf("ownership/postgres: scan index: %w", err)
			}
			// Primary-key backing indexes are part of the table lifecycle.
			if primary || isExcluded(c.exclude, table) {
				continue
			}
			cols := parseIndexColumns(def)
			o, err := NewObject(KindIndex, schema, table, index, FingerprintIndex(unique, cols...))
			if err != nil {
				return nil, err
			}
			out = append(out, o)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("ownership/postgres: iterate indexes: %w", err)
		}
	}

	return out, nil
}

func (c *postgresCatalog) DropStatement(o Object) (string, error) {
	schema := quotePGIdent(o.Schema)
	switch o.Kind {
	case KindTable:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", schema, quotePGIdent(o.Name)), nil
	case KindIndex:
		// Indexes live in the schema directly, not under the table.
		return fmt.Sprintf("DROP INDEX IF EXISTS %s.%s", schema, quotePGIdent(o.Name)), nil
	case KindColumn:
		return fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN IF EXISTS %s",
			schema, quotePGIdent(o.Parent), quotePGIdent(o.Name)), nil
	default:
		return "", fmt.Errorf("ownership/postgres: unsupported object kind %q", o.Kind)
	}
}

// pgColumnType reconstructs a normalized-able raw type token from information_schema columns.
func pgColumnType(udt string, maxLength sql.NullInt64, precision, scale sql.NullInt64) string {
	switch udt {
	case "varchar", "bpchar":
		if maxLength.Valid && maxLength.Int64 > 0 {
			return fmt.Sprintf("%s(%d)", udt, maxLength.Int64)
		}
		return udt
	case "numeric":
		if precision.Valid && precision.Int64 > 0 {
			if scale.Valid && scale.Int64 > 0 {
				return fmt.Sprintf("numeric(%d,%d)", precision.Int64, scale.Int64)
			}
			return fmt.Sprintf("numeric(%d)", precision.Int64)
		}
		return "numeric"
	default:
		return udt
	}
}

// quotePGIdent quotes a PostgreSQL identifier, escaping embedded quotes.
func quotePGIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// parseIndexColumns extracts the ordered column list from a pg_get_indexdef definition, e.g.
// "CREATE UNIQUE INDEX idx ON public.t USING btree (a, b)".
func parseIndexColumns(def string) []string {
	open := strings.LastIndexByte(def, '(')
	closeIdx := strings.LastIndexByte(def, ')')
	if open < 0 || closeIdx < 0 || closeIdx < open {
		return nil
	}
	raw := strings.Split(def[open+1:closeIdx], ",")
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		col := strings.TrimSpace(r)
		col = strings.Trim(col, `"`)
		// Expression indexes (e.g. lower(col)) cannot be matched by column name; keep the whole
		// stripped expression, which still yields a stable fingerprint.
		if col != "" {
			out = append(out, col)
		}
	}
	return out
}
