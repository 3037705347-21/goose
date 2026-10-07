// Package ownership implements rollback-time object ownership reconciliation for goose.
//
// The problem it solves: a migration's DOWN script drops database objects by name, which assumes
// every object currently present was created by migrations. Operators sometimes add columns or
// indexes by hand; blindly running DOWN deletes those external objects together with the ones the
// migration actually created.
//
// The reconciliation model:
//
//   - During UP, objects introduced by the migration are claimed into an ownership ledger. The
//     ledger records, per migration version, the canonical identity of each object plus a
//     fingerprint captured at creation time.
//   - During DOWN, the current database catalog is compared against the ledger. Only objects the
//     ledger attributes to the migration being rolled back, and that still match the recorded
//     fingerprint, are dropped. Objects absent from the ledger (added externally) are never
//     touched.
//   - If an object was renamed (its fingerprint is found under a different name) or its type no
//     longer matches the fingerprint, the object is left untouched and the rollback is blocked so
//     an operator can investigate.
//   - The whole procedure is convergent: running it again after an interruption produces the same
//     outcome. Objects already missing have their ledger entries pruned and cause no error.
//
// The canonical object representation is shared by every supported backend. Each backend supplies
// its own catalog reader and drop-DDL builder, but the ownership semantics are identical, so two
// different databases running the same migration set converge independently against the same
// notion of ownership.
package ownership

import (
	"fmt"
	"strings"
)

// Kind is the canonical kind of a database object.
type Kind string

const (
	// KindTable is a base table. Parent is always empty for tables.
	KindTable Kind = "table"
	// KindColumn is a column of the table identified by Parent.
	KindColumn Kind = "column"
	// KindIndex is a secondary index on the table identified by Parent. Primary-key indexes are
	// considered part of the table and are never claimed or dropped individually.
	KindIndex Kind = "index"
)

// valid reports whether k is a supported object kind.
func (k Kind) valid() bool {
	switch k {
	case KindTable, KindColumn, KindIndex:
		return true
	}
	return false
}

// Key is the backend-neutral identity of an object. Two objects with the same key represent the
// same logical object regardless of which database backend reported them.
type Key struct {
	Kind   Kind
	Schema string
	// Parent is the owning table for columns and indexes, empty for tables.
	Parent string
	Name   string
}

// String renders a stable, human-readable identifier used in logs and error messages.
func (k Key) String() string {
	base := k.Name
	if k.Parent != "" {
		base = k.Parent + "." + k.Name
	}
	if k.Schema != "" {
		base = k.Schema + "." + base
	}
	return fmt.Sprintf("%s:%s", k.Kind, base)
}

// Object is a catalog object in canonical form.
type Object struct {
	Key
	// Fingerprint is a normalized description of the object's type/definition. It is captured
	// when the object is claimed on UP and compared on DOWN to detect type drift.
	Fingerprint string
}

// Entry is one ownership ledger record: version owns the object identified by Key.
type Entry struct {
	Version int64
	Object
}

// canonicalize folds backend-specific quirks (identifier case, whitespace) into one comparable
// form. Identifiers are compared case-insensitively because PostgreSQL folds unquoted identifiers
// to lower case while MySQL is case-sensitive only on some platforms; using the lower-cased form
// matches how operators actually write migration SQL.
func canonicalize(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// keyFromFields builds a Key with every identifier canonicalized.
func keyFromFields(kind Kind, schema, parent, name string) (Key, error) {
	k := Kind(canonicalize(string(kind)))
	if !k.valid() {
		return Key{}, fmt.Errorf("ownership: unknown object kind %q", kind)
	}
	name = canonicalize(name)
	if name == "" {
		return Key{}, fmt.Errorf("ownership: object name must not be empty for kind %s", k)
	}
	return Key{
		Kind:   k,
		Schema: canonicalize(schema),
		Parent: canonicalize(parent),
		Name:   name,
	}, nil
}

// NewObject builds a canonical Object. The fingerprint must already be normalized; use the
// Fingerprint* helpers or a backend adapter to construct it.
func NewObject(kind Kind, schema, parent, name, fingerprint string) (Object, error) {
	key, err := keyFromFields(kind, schema, parent, name)
	if err != nil {
		return Object{}, err
	}
	fingerprint = strings.TrimSpace(fingerprint)
	if fingerprint == "" {
		return Object{}, fmt.Errorf("ownership: fingerprint must not be empty for %s", key)
	}
	return Object{Key: key, Fingerprint: fingerprint}, nil
}
