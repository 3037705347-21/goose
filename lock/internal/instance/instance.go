// Package instance provides identifiers for lock owners.
package instance

import (
	"cmp"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

// NewID generates a unique instance identifier of the form "hostname-pid-random".
//
// The same identifier can be pinned across components (for example a reconciler and a locker
// taking over locks on its behalf) via configuration.
func NewID() string {
	hostname, _ := os.Hostname()
	hostname = cmp.Or(hostname, "unknown-hostname")
	return fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), RandomHex(4))
}

// RandomHex returns n random bytes encoded as a lowercase hexadecimal string. It falls back to a
// time-based suffix if the system entropy source is unavailable.
func RandomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%0*x", n*2, time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
