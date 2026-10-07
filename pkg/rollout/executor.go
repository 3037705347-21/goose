package rollout

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"

	"github.com/pressly/goose/v3"
)

// Executor performs versioned migration operations against a single target
// database.
type Executor interface {
	// CurrentVersion returns the version currently recorded by the target.
	CurrentVersion(ctx context.Context) (int64, error)
	// UpgradeTo migrates the target up to, and including, version.
	UpgradeTo(ctx context.Context, version int64) error
	// DowngradeTo rolls the target down to, but not including, version. The
	// target is left at version.
	DowngradeTo(ctx context.Context, version int64) error
	// Close releases resources held by the executor.
	Close() error
}

// ExecutorFactory creates an Executor for a target.
type ExecutorFactory func(ctx context.Context, t Target) (Executor, error)

// Action identifies a migration action for hook purposes.
type Action string

const (
	// ActionUpgrade is an UpTo operation.
	ActionUpgrade Action = "upgrade"
	// ActionDowngrade is a DownTo operation.
	ActionDowngrade Action = "downgrade"
)

// Hooks are interceptors invoked around every migration action performed by an
// executor. They may be used for instrumentation or deterministic fault
// injection.
//
// Before is invoked before the action; returning an error aborts the action.
// After is invoked after the action completes (including on failure);
// returning an error replaces the action's result with that error.
type Hooks struct {
	Before func(ctx context.Context, target string, action Action, toVersion int64) error
	After  func(ctx context.Context, target string, action Action, toVersion int64, actionErr error) error
}

// NewExecutorFactory returns an [ExecutorFactory] backed by goose providers.
//
// dir is the migration directory (typically [Manifest.MigrationsDir]).
// providerOpts are applied to every created provider, allowing callers to
// configure the version table, lockers, Go migrations, etc. hooks, when
// non-nil, are applied to every created executor.
func NewExecutorFactory(
	dir string,
	hooks *Hooks,
	providerOpts ...goose.ProviderOption,
) ExecutorFactory {
	return func(_ context.Context, t Target) (Executor, error) {
		dialect, ok := goose.DriverDialect(t.Driver)
		if !ok {
			return nil, fmt.Errorf("target %q: unsupported driver %q", t.Name, t.Driver)
		}
		sqlDriver, ok := goose.SQLDriver(t.Driver)
		if !ok {
			return nil, fmt.Errorf("target %q: unsupported driver %q", t.Name, t.Driver)
		}
		db, err := sql.Open(sqlDriver, t.DSN)
		if err != nil {
			return nil, fmt.Errorf("target %q: open %s: %w", t.Name, t.Driver, err)
		}
		provider, err := goose.NewProvider(dialect, db, fsForDir(dir), providerOpts...)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("target %q: create provider: %w", t.Name, err)
		}
		return &gooseExecutor{
			target:  t.Name,
			provider: provider,
			hooks:   hooks,
		}, nil
	}
}

// fsForDir returns a filesystem rooted at dir.
func fsForDir(dir string) fs.FS { return os.DirFS(dir) }

type gooseExecutor struct {
	target   string
	provider *goose.Provider
	hooks    *Hooks
}

func (e *gooseExecutor) CurrentVersion(ctx context.Context) (int64, error) {
	v, err := e.provider.GetDBVersion(ctx)
	if err != nil {
		return 0, fmt.Errorf("target %q: get version: %w", e.target, err)
	}
	return v, nil
}

func (e *gooseExecutor) UpgradeTo(ctx context.Context, version int64) error {
	return e.runAction(ctx, ActionUpgrade, version, func() error {
		_, err := e.provider.UpTo(ctx, version)
		return err
	})
}

func (e *gooseExecutor) DowngradeTo(ctx context.Context, version int64) error {
	return e.runAction(ctx, ActionDowngrade, version, func() error {
		_, err := e.provider.DownTo(ctx, version)
		return err
	})
}

func (e *gooseExecutor) Close() error { return e.provider.Close() }

func (e *gooseExecutor) runAction(
	ctx context.Context,
	action Action,
	toVersion int64,
	fn func() error,
) error {
	if e.hooks != nil && e.hooks.Before != nil {
		if err := e.hooks.Before(ctx, e.target, action, toVersion); err != nil {
			return fmt.Errorf("target %q: %s aborted before execution: %w", e.target, action, err)
		}
	}
	actionErr := fn()
	if actionErr != nil {
		actionErr = fmt.Errorf("target %q: %s to %d: %w", e.target, action, toVersion, actionErr)
	}
	if e.hooks != nil && e.hooks.After != nil {
		if err := e.hooks.After(ctx, e.target, action, toVersion, actionErr); err != nil {
			return fmt.Errorf("target %q: %s post-hook: %w", e.target, action, err)
		}
	}
	return actionErr
}
