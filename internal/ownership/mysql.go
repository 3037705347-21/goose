package ownership

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pressly/goose/v3/database"
)

type mysqlCatalog struct {
	exclude map[string]struct{}
}

var _ Catalog = (*mysqlCatalog)(nil)

func (c *mysqlCatalog) ListObjects(ctx context.Context, db database.DBTxConn, kinds ...Kind) ([]Object, error) {
	want := wantedKinds(kinds)
	var out []Object

	if want[KindTable] {
		rows, err := db.QueryContext(ctx, `
			SELECT DATABASE(), table_name
			FROM information_schema.tables
			WHERE table_schema = DATABASE()
			  AND table_type = 'BASE TABLE'`)
		if err != nil {
			return nil, fmt.Errorf("ownership/mysql: list tables: %w", err)
		}
		tables, err := scanPairs(rows)
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
			SELECT DATABASE(), table_name, column_name, data_type, is_nullable,
			       character_maximum_length, numeric_precision, numeric_scale
			FROM information_schema.columns
			WHERE table_schema = DATABASE()
			ORDER BY table_name, ordinal_position`)
		if err != nil {
			return nil, fmt.Errorf("ownership/mysql: list columns: %w", err)
		}
		var (
			schema, table, name, dataType, nullable string
			maxLength, precision, scale             sql.NullInt64
		)
		for rows.Next() {
			if err := rows.Scan(&schema, &table, &name, &dataType, &nullable, &maxLength, &precision, &scale); err != nil {
				rows.Close()
				return nil, fmt.Errorf("ownership/mysql: scan column: %w", err)
			}
			if isExcluded(c.exclude, table) {
				continue
			}
			o, err := NewObject(KindColumn, schema, table, name,
				FingerprintColumn(mysqlColumnType(dataType, maxLength, precision, scale), nullable == "YES"))
			if err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, o)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("ownership/mysql: iterate columns: %w", err)
		}
		rows.Close()
	}

	if want[KindIndex] {
		// Index metadata is one row per indexed column, so it is grouped into index objects.
		rows, err := db.QueryContext(ctx, `
			SELECT DATABASE(), table_name, index_name, non_unique, seq_in_index, column_name
			FROM information_schema.statistics
			WHERE table_schema = DATABASE()
			ORDER BY table_name, index_name, seq_in_index`)
		if err != nil {
			return nil, fmt.Errorf("ownership/mysql: list indexes: %w", err)
		}
		type idxKey struct {
			schema, table, name string
		}
		type idxAcc struct {
			unique bool
			cols   []string
		}
		ordered := make([]idxKey, 0)
		groups := make(map[idxKey]*idxAcc)
		for rows.Next() {
			var (
				schema, table, index, column string
				nonUnique, seq               int
			)
			if err := rows.Scan(&schema, &table, &index, &nonUnique, &seq, &column); err != nil {
				rows.Close()
				return nil, fmt.Errorf("ownership/mysql: scan index: %w", err)
			}
			// The PRIMARY index follows the table lifecycle and is never dropped individually.
			if index == "PRIMARY" || isExcluded(c.exclude, table) {
				continue
			}
			k := idxKey{schema, table, index}
			acc, ok := groups[k]
			if !ok {
				acc = &idxAcc{unique: nonUnique == 0}
				groups[k] = acc
				ordered = append(ordered, k)
			}
			acc.cols = append(acc.cols, column)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("ownership/mysql: iterate indexes: %w", err)
		}
		rows.Close()
		for _, k := range ordered {
			acc := groups[k]
			o, err := NewObject(KindIndex, k.schema, k.table, k.name, FingerprintIndex(acc.unique, acc.cols...))
			if err != nil {
				return nil, err
			}
			out = append(out, o)
		}
	}

	return out, nil
}

func (c *mysqlCatalog) DropStatement(o Object) (string, error) {
	switch o.Kind {
	case KindTable:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteMySQLIdent(o.Name)), nil
	case KindIndex:
		// MySQL requires DROP INDEX ... ON table; schema is implicit in the connection database.
		return fmt.Sprintf("DROP INDEX %s ON %s", quoteMySQLIdent(o.Name), quoteMySQLIdent(o.Parent)), nil
	case KindColumn:
		// MySQL has no portable IF EXISTS for DROP COLUMN; the reconciler only emits this after
		// confirming the column exists, and re-runs converge because the missing column is then
		// pruned.
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", quoteMySQLIdent(o.Parent), quoteMySQLIdent(o.Name)), nil
	default:
		return "", fmt.Errorf("ownership/mysql: unsupported object kind %q", o.Kind)
	}
}

// mysqlColumnType mirrors [pgColumnType]: it reconstructs a raw type token that normalizeType
// understands from information_schema metadata. Display widths on integers are intentionally
// ignored because they carry no type information.
func mysqlColumnType(dataType string, maxLength, precision, scale sql.NullInt64) string {
	switch dataType {
	case "varchar", "char":
		if maxLength.Valid && maxLength.Int64 > 0 {
			return fmt.Sprintf("%s(%d)", dataType, maxLength.Int64)
		}
		return dataType
	case "decimal", "numeric":
		if precision.Valid && precision.Int64 > 0 {
			if scale.Valid && scale.Int64 > 0 {
				return fmt.Sprintf("numeric(%d,%d)", precision.Int64, scale.Int64)
			}
			return fmt.Sprintf("numeric(%d)", precision.Int64)
		}
		return "numeric"
	default:
		return dataType
	}
}

// quoteMySQLIdent quotes a MySQL identifier with backticks.
func quoteMySQLIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
