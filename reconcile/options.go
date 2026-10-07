package reconcile

import (
	"fmt"
	"time"

	"github.com/pressly/goose/v3"
)

// Logger is the minimal logging interface used during reconciliation. The standard library
// *log.Logger satisfies it.
type Logger interface {
	Printf(format string, args ...any)
}

type options struct {
	logger       Logger
	ledgerDir    string
	dryRun       bool
	now          func() time.Time
	providerOpts []goose.ProviderOption
	finder       tableFinder
}

// Option configures a [Runner].
type Option func(*options) error

// WithLogger sets the logger used while reconciling. Defaults to the standard library default
// logger.
func WithLogger(l Logger) Option {
	return func(o *options) error {
		if l == nil {
			return fmt.Errorf("reconcile: logger must not be nil")
		}
		o.logger = l
		return nil
	}
}

// WithLedgerDir overrides the ledger directory from the registry.
func WithLedgerDir(dir string) Option {
	return func(o *options) error {
		if dir == "" {
			return fmt.Errorf("reconcile: ledger dir must not be empty")
		}
		o.ledgerDir = dir
		return nil
	}
}

// WithDryRun classifies ownership and drift without migrating or writing ledgers. Confirmed
// applications report what would be applied.
func WithDryRun(b bool) Option {
	return func(o *options) error {
		o.dryRun = b
		return nil
	}
}

// WithProviderOptions passes goose provider options to every per-application provider, such as
// [github.com/pressly/goose/v3.WithLocker] or [github.com/pressly/goose/v3.WithSlog].
//
// WithTableName and WithStore must not be supplied: the runner sets each provider's version table
// from the registry.
func WithProviderOptions(opts ...goose.ProviderOption) Option {
	return func(o *options) error {
		o.providerOpts = append(o.providerOpts, opts...)
		return nil
	}
}

// withClock overrides time tracking, intended for tests.
func withClock(now func() time.Time) Option {
	return func(o *options) error {
		o.now = now
		return nil
	}
}

// withFinder overrides table discovery, intended for tests.
func withFinder(f tableFinder) Option {
	return func(o *options) error {
		o.finder = f
		return nil
	}
}
