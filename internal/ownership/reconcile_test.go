package ownership

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustObj(t *testing.T, kind Kind, schema, parent, name, fingerprint string) Object {
	t.Helper()
	o, err := NewObject(kind, schema, parent, name, fingerprint)
	require.NoError(t, err)
	return o
}

func entry(version int64, o Object) Entry { return Entry{Version: version, Object: o} }

func dropKeys(actions []Action) []string {
	out := make([]string, 0, len(actions))
	for _, a := range actions {
		out = append(out, a.Entry.Key.String())
	}
	return out
}

func pruneKeys(actions []Action) []string {
	out := make([]string, 0, len(actions))
	for _, a := range actions {
		out = append(out, a.Entry.Key.String())
	}
	return out
}

func skipReasons(actions []Action) map[string]SkipReason {
	out := make(map[string]SkipReason, len(actions))
	for _, a := range actions {
		out[a.Entry.Key.String()] = a.Reason
	}
	return out
}

// v1 creates table users(id) plus a secondary index; all fingerprints still match. The table
// drop absorbs the child objects.
func TestPlan_DropOwnedTableAbsorbsChildren(t *testing.T) {
	colID := FingerprintColumn("integer", false)
	idx := FingerprintIndex(true, "id")

	owned := []Entry{
		entry(1, mustObj(t, KindTable, "public", "", "users", FingerprintTable)),
		entry(1, mustObj(t, KindColumn, "public", "users", "id", colID)),
		entry(1, mustObj(t, KindIndex, "public", "users", "users_id_idx", idx)),
	}
	current := []Object{
		mustObj(t, KindTable, "public", "", "users", FingerprintTable),
		mustObj(t, KindColumn, "public", "users", "id", colID),
		mustObj(t, KindIndex, "public", "users", "users_id_idx", idx),
	}

	p := plan(1, owned, current)
	assert.Empty(t, p.Blocking())
	assert.Empty(t, p.Prunes())
	assert.Equal(t, []string{"table:public.users"}, dropKeys(p.Drops()),
		"the table drop must absorb its owned columns and indexes")
}

// Columns and indexes added to an older table by the migration being rolled back are dropped
// individually, and objects absent from the ledger survive.
func TestPlan_ExternalObjectsSurviveColumnRollback(t *testing.T) {
	fpID := FingerprintColumn("integer", false)
	fpEmail := FingerprintColumn("varchar(255)", true)
	fpManual := FingerprintColumn("text", true)
	idxEmail := FingerprintIndex(false, "email")
	idxManual := FingerprintIndex(false, "legacy_code")

	owned := []Entry{
		entry(2, mustObj(t, KindColumn, "public", "users", "email", fpEmail)),
		entry(2, mustObj(t, KindIndex, "public", "users", "idx_users_email", idxEmail)),
	}
	current := []Object{
		// Pre-existing table and primary column (owned by an older applied migration).
		mustObj(t, KindTable, "public", "", "users", FingerprintTable),
		mustObj(t, KindColumn, "public", "users", "id", fpID),
		mustObj(t, KindColumn, "public", "users", "email", fpEmail),
		// Someone added this column by hand; it is not in the ledger.
		mustObj(t, KindColumn, "public", "users", "manual_note", fpManual),
		mustObj(t, KindIndex, "public", "users", "idx_users_email", idxEmail),
		// ... and this hand-made index too.
		mustObj(t, KindIndex, "public", "users", "idx_users_manual", idxManual),
	}

	p := plan(2, owned, current)
	assert.Empty(t, p.Blocking())
	assert.Equal(t,
		[]string{"index:public.users.idx_users_email", "column:public.users.email"},
		dropKeys(p.Drops()),
		"only ledger objects are dropped, index before its column")

	// Sanity: manual objects are simply not represented in any action.
	for _, a := range p.Drops() {
		assert.NotContains(t, a.Entry.Key.Name, "manual")
	}
}

// A renamed column: creation name gone, same fingerprint at a new name -> block everything.
func TestPlan_RenamedColumnBlocks(t *testing.T) {
	fp := FingerprintColumn("varchar(255)", true)
	owned := []Entry{
		entry(2, mustObj(t, KindColumn, "public", "users", "email", fp)),
	}
	current := []Object{
		mustObj(t, KindColumn, "public", "users", "id", FingerprintColumn("integer", false)),
		mustObj(t, KindColumn, "public", "users", "email_address", fp),
	}

	p := plan(2, owned, current)
	assert.Empty(t, p.Drops())
	blocking := p.Blocking()
	require.Len(t, blocking, 1)
	assert.Equal(t, SkipRenamed, blocking[0].Reason)
	assert.Equal(t, "email_address", blocking[0].Current.Key.Name)
}

// A renamed index is detected through its column-list fingerprint.
func TestPlan_RenamedIndexBlocks(t *testing.T) {
	fp := FingerprintIndex(false, "email")
	owned := []Entry{
		entry(2, mustObj(t, KindIndex, "public", "users", "idx_email", fp)),
	}
	current := []Object{
		mustObj(t, KindIndex, "public", "users", "idx_email_new", fp),
	}

	p := plan(2, owned, current)
	require.Len(t, p.Blocking(), 1)
	assert.Equal(t, SkipRenamed, p.Blocking()[0].Reason)
	assert.Empty(t, p.Drops())
}

// Same name, changed type -> type mismatch, leave untouched.
func TestPlan_TypeMismatchBlocks(t *testing.T) {
	owned := []Entry{
		entry(2, mustObj(t, KindColumn, "public", "users", "email",
			FingerprintColumn("varchar(255)", true))),
	}
	current := []Object{
		mustObj(t, KindColumn, "public", "users", "email",
			FingerprintColumn("text", true)),
	}

	p := plan(2, owned, current)
	require.Len(t, p.Blocking(), 1)
	assert.Equal(t, SkipTypeMismatch, p.Blocking()[0].Reason)
	assert.Empty(t, p.Drops())
}

// Objects that are simply gone (e.g. a previous interrupted run already dropped them) prune
// their ledger entries and never block.
func TestPlan_MissingObjectsPrune(t *testing.T) {
	owned := []Entry{
		entry(2, mustObj(t, KindColumn, "public", "users", "email",
			FingerprintColumn("varchar(255)", true))),
		entry(2, mustObj(t, KindIndex, "public", "users", "idx_email",
			FingerprintIndex(false, "email"))),
	}
	p := plan(2, owned, nil)

	assert.Empty(t, p.Blocking())
	assert.Empty(t, p.Drops())
	assert.ElementsMatch(t,
		[]string{"column:public.users.email", "index:public.users.idx_email"},
		pruneKeys(p.Prunes()))
}

// A table that still contains non-owned children cannot be dropped; the whole owned subgraph is
// retained.
func TestPlan_TableWithExternalChildrenBlocksSubgraph(t *testing.T) {
	fpID := FingerprintColumn("integer", false)
	fpName := FingerprintColumn("text", true)
	fpManual := FingerprintColumn("varchar(64)", true)

	owned := []Entry{
		entry(1, mustObj(t, KindTable, "public", "", "users", FingerprintTable)),
		entry(1, mustObj(t, KindColumn, "public", "users", "id", fpID)),
		entry(1, mustObj(t, KindColumn, "public", "users", "name", fpName)),
	}
	current := []Object{
		mustObj(t, KindTable, "public", "", "users", FingerprintTable),
		mustObj(t, KindColumn, "public", "users", "id", fpID),
		mustObj(t, KindColumn, "public", "users", "name", fpName),
		// Hand-added after the migration ran.
		mustObj(t, KindColumn, "public", "users", "legacy_ref", fpManual),
	}

	p := plan(1, owned, current)
	assert.Empty(t, p.Drops())
	reasons := skipReasons(p.Blocking())
	assert.Len(t, reasons, 3)
	assert.Equal(t, SkipHasDependents, reasons["table:public.users"])
	assert.Equal(t, SkipHasDependents, reasons["column:public.users.id"])
	assert.Equal(t, SkipHasDependents, reasons["column:public.users.name"])
}

// Full interrupted-run convergence story: run 1 drops the index and crashes; run 2 must prune
// it, drop what remains, and never error on the stale row.
func TestPlan_InterruptedRerunConverges(t *testing.T) {
	fpEmail := FingerprintColumn("varchar(255)", true)
	fpIdx := FingerprintIndex(false, "email")

	owned := []Entry{
		entry(2, mustObj(t, KindColumn, "public", "users", "email", fpEmail)),
		entry(2, mustObj(t, KindIndex, "public", "users", "idx_email", fpIdx)),
	}
	// Snapshot after the crash: the index is gone (already dropped, ledger row orphaned), the
	// column is still there.
	current := []Object{
		mustObj(t, KindColumn, "public", "users", "email", fpEmail),
	}

	p := plan(2, owned, current)
	assert.Empty(t, p.Blocking())
	assert.Equal(t, []string{"column:public.users.email"}, dropKeys(p.Drops()))
	assert.Equal(t, []string{"index:public.users.idx_email"}, pruneKeys(p.Prunes()))

	// Second re-run: everything gone -> all prune, still no blocking, no drops.
	p2 := plan(2, owned, nil)
	assert.Empty(t, p2.Blocking())
	assert.Empty(t, p2.Drops())
	assert.Len(t, p2.Prunes(), 2)
}

// The same canonical ownership semantics drive two different backends even though each reports
// types differently. PostgreSQL says "int4", MySQL says "int"/"int(11)"; both fold onto the same
// fingerprint, so a ledger claimed from one backend's catalog representation reconciles against
// the other's. (Each backend keeps its own ledger rows keyed by its local schema; what is shared
// is the canonical object model and ownership rules.)
func TestPlan_SharedOwnershipAcrossBackends(t *testing.T) {
	key := func() Key {
		k, err := keyFromFields(KindColumn, "public", "users", "id")
		require.NoError(t, err)
		return k
	}()

	// Entry fingerprint as captured from a PostgreSQL catalog, current object as reported by a
	// MySQL catalog: equivalent types, same identity -> convergent drop on both.
	pgEntry := Entry{Version: 1, Object: Object{
		Key:         key,
		Fingerprint: FingerprintColumn("int4", false),
	}}
	mysqlObject := Object{Key: key, Fingerprint: FingerprintColumn("int(11)", false)}
	pOnMySQL := plan(1, []Entry{pgEntry}, []Object{mysqlObject})
	assert.Empty(t, pOnMySQL.Blocking())
	require.Len(t, pOnMySQL.Drops(), 1)

	// And vice versa: varchar spellings fold together.
	mysqlEntry := Entry{Version: 1, Object: Object{
		Key:         key,
		Fingerprint: FingerprintColumn("varchar(255)", true),
	}}
	pgObject := Object{Key: key, Fingerprint: FingerprintColumn("character varying(255)", true)}
	pOnPostgres := plan(1, []Entry{mysqlEntry}, []Object{pgObject})
	assert.Empty(t, pOnPostgres.Blocking())
	require.Len(t, pOnPostgres.Drops(), 1)

	// Genuinely different types must still mismatch across backends.
	drift := plan(1,
		[]Entry{{Version: 1, Object: Object{
			Key:         key,
			Fingerprint: FingerprintColumn("bigint", false),
		}}},
		[]Object{{Key: key, Fingerprint: FingerprintColumn("int", false)}},
	)
	require.Len(t, drift.Blocking(), 1)
	assert.Equal(t, SkipTypeMismatch, drift.Blocking()[0].Reason)
}
