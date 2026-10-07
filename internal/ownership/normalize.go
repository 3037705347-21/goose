package ownership

import (
	"regexp"
	"strconv"
	"strings"
)

// FingerprintTable is the fingerprint of a table object. Tables are identified by kind alone: a
// name match on KindTable means the same named table exists, and child objects are reconciled
// separately.
const FingerprintTable = "table"

var (
	// collapseSpace turns runs of whitespace into a single space and trims.
	collapseSpace = regexp.MustCompile(`\s+`)
	// typeWithArgs parses a normalized type name with an optional "(arg, ...)" suffix, e.g.
	// "varchar(255)" or "numeric(10, 2)".
	typeWithArgs = regexp.MustCompile(`^\s*([a-z0-9 ]+?)(?:\s*\(\s*([0-9,\s]+)\))?\s*$`)
)

// normalizeType folds backend-specific type names and decorations onto one canonical type token.
//
// Examples of folding:
//
//   - PostgreSQL "integer", "int4" and MySQL "int", "integer", "int(11)" all become "integer".
//   - "character varying(255)" (pg) and "varchar(255)" (mysql) both become "varchar(255)".
//   - "double precision" (pg) and "double" (mysql) both become "float8".
//
// Only well-known equivalent spellings are folded. Types outside the alias table keep their
// normalized spelling, which means genuinely different backends types (e.g. json vs jsonb) still
// mismatch on purpose.
func normalizeType(raw string) string {
	t := collapseSpace.ReplaceAllString(strings.ToLower(strings.TrimSpace(raw)), " ")
	if t == "" {
		return ""
	}
	m := typeWithArgs.FindStringSubmatch(t)
	if m == nil {
		return t
	}
	base := strings.TrimSpace(m[1])
	args := strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(m[2]), " ", ""), ",", ",")

	switch base {
	// Integer family.
	case "int", "integer", "int4", "pg_catalog.int4":
		base = "integer"
	case "bigint", "int8", "pg_catalog.int8":
		base = "bigint"
	case "smallint", "int2", "pg_catalog.int2", "tinyint":
		base = "smallint"
	// Strings.
	case "varchar", "character varying", "pg_catalog.varchar":
		base = "varchar"
	case "char", "character", "bpchar", "pg_catalog.bpchar":
		base = "char"
	// Floating point.
	case "real", "float4", "pg_catalog.float4":
		base = "float4"
	case "double precision", "double", "float8", "pg_catalog.float8":
		base = "float8"
	// Exact decimal.
	case "numeric", "decimal":
		base = "numeric"
	// Boolean.
	case "bool", "boolean":
		base = "boolean"
	// Time-ish.
	case "timestamp without time zone", "timestamp":
		base = "timestamp"
	case "timestamp with time zone", "timestamptz":
		base = "timestamptz"
	// Blob/text.
	case "bytea", "blob", "varbinary", "binary varying":
		base = "blob"
	}
	// Integer display widths ("int(11)") carry no type information and are dropped. Length-bearing
	// strings and numeric precision/scale are retained.
	switch base {
	case "integer", "bigint", "smallint":
		args = ""
	}
	if args != "" {
		return base + "(" + args + ")"
	}
	return base
}

// FingerprintColumn builds a column fingerprint from its normalized type and nullability. Columns
// that differ only in defaults or column order are intentionally considered equal: the dangerous
// drift operators care about is type change, not cosmetic differences.
func FingerprintColumn(rawType string, nullable bool) string {
	return "column:" + normalizeType(rawType) + ":" + strconv.FormatBool(nullable)
}

// FingerprintIndex builds an index fingerprint from its uniqueness flag and the ordered list of
// indexed columns. Column names are canonicalized so that backend-specific quoting differences
// do not produce false mismatches, but ordering is preserved because index order can be
// semantically significant.
func FingerprintIndex(unique bool, columns ...string) string {
	parts := make([]string, 0, len(columns))
	for _, c := range columns {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		parts = append(parts, canonicalize(c))
	}
	return "index:unique=" + strconv.FormatBool(unique) + ":cols=" + strings.Join(parts, ",")
}
