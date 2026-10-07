package ownership

import (
	"fmt"
	"sort"
)

// SkipReason explains why a ledger object was kept instead of dropped.
type SkipReason string

const (
	// SkipRenamed means the object name is gone but an object with the creation fingerprint
	// exists elsewhere in the same scope, i.e. it was (most likely) renamed.
	SkipRenamed SkipReason = "renamed"
	// SkipTypeMismatch means an object with the same identity exists but its type/definition no
	// longer matches the fingerprint captured at creation.
	SkipTypeMismatch SkipReason = "type_mismatch"
	// SkipHasDependents means a table cannot be dropped because it currently contains columns or
	// indexes that this migration does not own (added by an older-but-still-applied migration or
	// by hand). The whole owned subgraph of that table is left untouched.
	SkipHasDependents SkipReason = "has_external_children"
)

// Drift describes one kept ledger object and why reconciliation would not remove it.
type Drift struct {
	// Entry is the ledger record that was kept.
	Entry Entry
	// Current is the live object involved, when one exists (type mismatch, rename target, or the
	// blocked table itself).
	Current Object
	// Reason classifies the drift.
	Reason SkipReason
	// Detail is a human-readable explanation.
	Detail string
}

// ActionKind enumerates what the reconciler decided for one ledger entry.
type ActionKind int

const (
	// actionDrop approves the object for deletion and ledger-row removal.
	actionDrop ActionKind = iota
	// actionPrune removes the ledger row for an object that is already gone, so an interrupted run
	// converges instead of failing.
	actionPrune
	// actionSkip keeps both object and ledger row; the rollback must not proceed.
	actionSkip
)

// Action is the planner's decision for one [Entry].
type Action struct {
	Entry   Entry
	Kind    ActionKind
	Current Object
	Reason  SkipReason
	Detail  string
}

// dropOrder defines the order in which kinds are removed. Indexes and columns must go before the
// tables that contain them.
var dropOrder = []Kind{KindIndex, KindColumn, KindTable}

// Plan is the outcome of comparing a version's ledger entries against the live catalog.
type Plan struct {
	Version int64
	actions []Action
}

// Drops returns approved drops in dependency order (indexes, then columns, then tables).
func (p *Plan) Drops() []Action {
	byKind := make(map[Kind][]Action)
	for _, a := range p.actions {
		if a.Kind != actionDrop {
			continue
		}
		byKind[a.Entry.Kind] = append(byKind[a.Entry.Kind], a)
	}
	var out []Action
	for _, knd := range dropOrder {
		group := byKind[knd]
		sort.SliceStable(group, func(i, j int) bool {
			return group[i].Entry.Key.String() < group[j].Entry.Key.String()
		})
		// Child objects leave together with a table that is itself being dropped, so emitting
		// individual DROP INDEX / DROP COLUMN statements for them would be redundant (and fail on
		// backends without IF EXISTS).
		if knd == KindColumn || knd == KindIndex {
			for _, a := range group {
				if p.dropsWithTable(a.Entry.Key) {
					continue
				}
				out = append(out, a)
			}
			continue
		}
		out = append(out, group...)
	}
	return out
}

// Prunes returns ledger rows to remove because the underlying objects are already absent.
func (p *Plan) Prunes() []Action {
	return p.filter(actionPrune)
}

// Blocking returns drift actions that prevent the rollback from completing.
func (p *Plan) Blocking() []Action {
	return p.filter(actionSkip)
}

func (p *Plan) filter(k ActionKind) []Action {
	var out []Action
	for _, a := range p.actions {
		if a.Kind == k {
			out = append(out, a)
		}
	}
	return out
}

// dropsWithTable reports whether key is a child object whose table is also approved for drop in
// this plan; in that case dropping the child individually is redundant.
func (p *Plan) dropsWithTable(key Key) bool {
	if key.Parent == "" {
		return false
	}
	for _, a := range p.actions {
		if a.Kind == actionDrop && a.Entry.Kind == KindTable &&
			a.Entry.Schema == key.Schema && a.Entry.Name == key.Parent {
			return true
		}
	}
	return false
}

// scopeKey identifies the rename-search scope of an object: tables are scoped to the schema,
// columns and indexes to their parent table.
type scopeKey struct {
	kind   Kind
	schema string
	parent string
}

// plan is the pure reconciliation decision function. It takes the ledger entries of exactly the
// version being rolled back and a snapshot of the current catalog, and returns a deterministic
// plan. It performs no I/O so the whole decision surface is unit-testable.
func plan(version int64, owned []Entry, current []Object) *Plan {
	byKey := make(map[Key]Object, len(current))
	fingerprints := make(map[scopeKey]map[string][]Object)
	children := make(map[Key][]Object) // table identity -> live child objects

	for _, o := range current {
		byKey[o.Key] = o
		scope := scopeKey{kind: o.Kind, schema: o.Schema, parent: o.Parent}
		if fingerprints[scope] == nil {
			fingerprints[scope] = make(map[string][]Object)
		}
		fingerprints[scope][o.Fingerprint] = append(fingerprints[scope][o.Fingerprint], o)
		if o.Kind == KindColumn || o.Kind == KindIndex {
			tk := Key{Kind: KindTable, Schema: o.Schema, Name: o.Parent}
			children[tk] = append(children[tk], o)
		}
	}

	out := &Plan{Version: version}
	ownedByKey := make(map[Key]struct{}, len(owned))
	for _, e := range owned {
		ownedByKey[e.Key] = struct{}{}
	}

	for _, e := range owned {
		cur, found := byKey[e.Key]
		switch {
		case !found:
			// The object is no longer at its creation name.
			if renamed, ok := findRenamed(e, fingerprints); ok {
				out.actions = append(out.actions, Action{
					Entry:   e,
					Kind:    actionSkip,
					Current: renamed,
					Reason:  SkipRenamed,
					Detail: fmt.Sprintf(
						"%s is gone but an object with its creation fingerprint now exists as %s; leaving everything untouched",
						e.Key, renamed.Key),
				})
			} else {
				// Already deleted (possibly by a previous interrupted run): converge.
				out.actions = append(out.actions, Action{
					Entry:  e,
					Kind:   actionPrune,
					Detail: fmt.Sprintf("%s no longer exists; pruning stale ledger entry", e.Key),
				})
			}
		case cur.Fingerprint != e.Fingerprint:
			out.actions = append(out.actions, Action{
				Entry:   e,
				Kind:    actionSkip,
				Current: cur,
				Reason:  SkipTypeMismatch,
				Detail: fmt.Sprintf(
					"%s exists but its type drifted from %q to %q; leaving it untouched",
					e.Key, e.Fingerprint, cur.Fingerprint),
			})
		default:
			out.actions = append(out.actions, Action{
				Entry:   e,
				Kind:    actionDrop,
				Current: cur,
			})
		}
	}

	// Table safety: dropping a table destroys every column/index it currently contains. If any live
	// child is not claimed by this version, the table stays — and so does every object this
	// version owns underneath it ("leave the whole thing untouched").
	for i := range out.actions {
		a := &out.actions[i]
		if a.Kind != actionDrop || a.Entry.Kind != KindTable {
			continue
		}
		var protected []Object
		for _, child := range children[a.Entry.Key] {
			if _, ownedHere := ownedByKey[child.Key]; !ownedHere {
				protected = append(protected, child)
			}
		}
		if len(protected) == 0 {
			continue
		}
		sort.Slice(protected, func(x, y int) bool {
			return protected[x].Key.String() < protected[y].Key.String()
		})
		names := make([]string, 0, len(protected))
		for _, ch := range protected {
			names = append(names, ch.Key.String())
		}
		a.Kind = actionSkip
		a.Reason = SkipHasDependents
		a.Detail = fmt.Sprintf(
			"table %s still contains objects not owned by this migration (%s); leaving the table and its owned objects untouched",
			a.Entry.Key, joinNames(names))
		// Downgrade the table's same-version children from drop to skip so the subgraph is kept
		// whole.
		for j := range out.actions {
			ca := &out.actions[j]
			if ca.Kind != actionDrop {
				continue
			}
			if ca.Entry.Parent == a.Entry.Name && ca.Entry.Schema == a.Entry.Schema &&
				(ca.Entry.Kind == KindColumn || ca.Entry.Kind == KindIndex) {
				ca.Kind = actionSkip
				ca.Reason = SkipHasDependents
				ca.Detail = fmt.Sprintf("parent table %s is retained because it has external children", a.Entry.Key)
			}
		}
	}
	return out
}

// findRenamed looks for a live object with the entry's creation fingerprint under a different
// name in the same scope. Tables always share the same fingerprint, so rename detection is only
// meaningful for columns and indexes. Any match (even ambiguous) blocks deletion: being
// conservative here is the entire point of the guard.
func findRenamed(e Entry, byScope map[scopeKey]map[string][]Object) (Object, bool) {
	if e.Kind == KindTable {
		return Object{}, false
	}
	scope := scopeKey{kind: e.Kind, schema: e.Schema, parent: e.Parent}
	group, ok := byScope[scope]
	if !ok {
		return Object{}, false
	}
	matches := group[e.Fingerprint]
	for _, m := range matches {
		if m.Key == e.Key {
			continue // same name is handled by the caller
		}
		return m, true
	}
	return Object{}, false
}

func joinNames(names []string) string {
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ", "
		}
		out += n
	}
	return out
}
