package ownership

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestNormalizeType(t *testing.T) {
	cases := map[string]string{
		// Cross-backend aliases.
		"int4":             "integer",
		"integer":          "integer",
		"INT(11)":          "integer",
		"int(11)":          "integer",
		"bigint":           "bigint",
		"int8":             "bigint",
		"bool":             "boolean",
		"boolean":          "boolean",
		"double precision": "float8",
		"double":           "float8",
		"real":             "float4",
		// Length-bearing types keep their arguments.
		"varchar(255)":           "varchar(255)",
		"character varying(255)": "varchar(255)",
		"numeric(10,2)":          "numeric(10,2)",
		"decimal(10, 2)":         "numeric(10,2)",
		// Types outside the alias table survive untouched.
		"jsonb": "jsonb",
		"uuid":  "uuid",
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			assert.Equal(t, want, normalizeType(in))
		})
	}
}

func TestDropStatements(t *testing.T) {
	table, err := NewObject(KindTable, "public", "", "users", FingerprintTable)
	require.NoError(t, err)
	column, err := NewObject(KindColumn, "public", "users", "email", "x")
	require.NoError(t, err)
	index, err := NewObject(KindIndex, "public", "users", "idx_email", "x")
	require.NoError(t, err)

	pg := &postgresCatalog{}
	stmt, err := pg.DropStatement(table)
	require.NoError(t, err)
	assert.Equal(t, `DROP TABLE IF EXISTS "public"."users"`, stmt)
	stmt, _ = pg.DropStatement(column)
	assert.Equal(t, `ALTER TABLE "public"."users" DROP COLUMN IF EXISTS "email"`, stmt)
	stmt, _ = pg.DropStatement(index)
	assert.Equal(t, `DROP INDEX IF EXISTS "public"."idx_email"`, stmt)

	my := &mysqlCatalog{}
	myTable := Object{Key: Key{Kind: KindTable, Schema: "db", Name: "users"}, Fingerprint: FingerprintTable}
	myColumn := Object{Key: Key{Kind: KindColumn, Schema: "db", Parent: "users", Name: "email"}, Fingerprint: "x"}
	myIndex := Object{Key: Key{Kind: KindIndex, Schema: "db", Parent: "users", Name: "idx_email"}, Fingerprint: "x"}
	stmt, _ = my.DropStatement(myTable)
	assert.Equal(t, "DROP TABLE IF EXISTS `users`", stmt)
	stmt, _ = my.DropStatement(myColumn)
	assert.Equal(t, "ALTER TABLE `users` DROP COLUMN `email`", stmt)
	stmt, _ = my.DropStatement(myIndex)
	assert.Equal(t, "DROP INDEX `idx_email` ON `users`", stmt)

	li := &sqliteCatalog{}
	stmt, _ = li.DropStatement(Object{Key: Key{Kind: KindTable, Name: "users"}, Fingerprint: FingerprintTable})
	assert.Equal(t, `DROP TABLE IF EXISTS "users"`, stmt)
}

func TestNewBackend_UnsupportedDialect(t *testing.T) {
	_, err := NewGuard("clickhouse", "goose_db_version")
	require.Error(t, err)
}

// The ledger store must be portable across backends and idempotent on re-claim. SQLite stands in
// for the question-mark placeholder family; SQL generation for postgres/mysql is covered above
// and via build.
func TestSQLLedger_PutListDelete(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	ctx := context.Background()

	ledger := newSQLiteLedger("go_ownership")
	require.NoError(t, ledger.EnsureTable(ctx, db))
	// Idempotent table creation.
	require.NoError(t, ledger.EnsureTable(ctx, db))

	col, err := NewObject(KindColumn, "", "users", "email", FingerprintColumn("varchar(255)", true))
	require.NoError(t, err)
	idx, err := NewObject(KindIndex, "", "users", "idx_email", FingerprintIndex(false, "email"))
	require.NoError(t, err)
	entries := []Entry{{Version: 2, Object: col}, {Version: 2, Object: idx}}

	require.NoError(t, ledger.Put(ctx, db, entries))
	// Re-claim with a changed fingerprint keeps the first claim (first writer wins).
	drifted := col
	drifted.Fingerprint = FingerprintColumn("text", true)
	require.NoError(t, ledger.Put(ctx, db, []Entry{{Version: 2, Object: drifted}}))

	got, err := ledger.ListByVersion(ctx, db, 2)
	require.NoError(t, err)
	require.Len(t, got, 2)
	byName := map[string]Entry{}
	for _, e := range got {
		byName[e.Key.Name] = e
	}
	assert.Equal(t, FingerprintColumn("varchar(255)", true), byName["email"].Fingerprint)

	require.NoError(t, ledger.Delete(ctx, db, 2, []Key{idx.Key}))
	got, err = ledger.ListByVersion(ctx, db, 2)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "email", got[0].Key.Name)

	// Deleting a missing row is not an error.
	require.NoError(t, ledger.Delete(ctx, db, 2, []Key{idx.Key}))
}
