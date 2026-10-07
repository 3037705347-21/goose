package ownership

import (
	"database/sql"
)

// wantedKinds converts a variadic kind list into a lookup set. An empty list means every kind,
// matching the principle of least surprise for callers that snapshot the whole catalog.
func wantedKinds(kinds []Kind) map[Kind]bool {
	out := make(map[Kind]bool, len(kinds))
	if len(kinds) == 0 {
		out[KindTable] = true
		out[KindColumn] = true
		out[KindIndex] = true
		return out
	}
	for _, k := range kinds {
		out[k] = true
	}
	return out
}

type namePair struct {
	first, second string
}

// scanPairs scans (schema, object_name) rows and closes the result set on return.
func scanPairs(rows *sql.Rows) ([]namePair, error) {
	defer rows.Close()
	var out []namePair
	for rows.Next() {
		var p namePair
		if err := rows.Scan(&p.first, &p.second); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
