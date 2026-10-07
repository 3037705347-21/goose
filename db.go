package goose

import (
	"database/sql"
	"fmt"
)

// driverInfo pairs the database/sql driver name with the goose dialect for a
// goose driver name.
type driverInfo struct {
	sqlDriver string
	dialect   Dialect
}

// driverMapping is the single source of truth mapping goose driver names to
// the underlying database/sql driver and goose dialect.
var driverMapping = map[string]driverInfo{
	"postgres":  {"pgx", DialectPostgres},
	"pgx":       {"pgx", DialectPostgres},
	"redshift":  {"pgx", DialectRedshift},
	"mysql":     {"mysql", DialectMySQL},
	"tidb":      {"mysql", DialectTiDB},
	"starrocks": {"mysql", DialectStarrocks},
	"sqlite3":   {"sqlite", DialectSQLite3},
	"sqlite":    {"sqlite", DialectSQLite3},
	"mssql":     {"sqlserver", DialectMSSQL},
	"azuresql":  {"sqlserver", DialectMSSQL},
	"sqlserver": {"sqlserver", DialectMSSQL},
	"spanner":   {"spanner", DialectSpanner},
	"clickhouse": {"clickhouse", DialectClickHouse},
	"ydb":       {"ydb", DialectYdB},
	"turso":     {"libsql", DialectTurso},
	"vertica":   {"vertica", DialectVertica},
}

// SQLDriver returns the database/sql driver name registered for a goose driver
// name. It reports false for an unknown driver.
func SQLDriver(driver string) (string, bool) {
	info, ok := driverMapping[driver]
	if !ok {
		return "", false
	}
	return info.sqlDriver, true
}

// DriverDialect returns the dialect associated with a goose driver name. It
// reports false for an unknown driver.
func DriverDialect(driver string) (Dialect, bool) {
	info, ok := driverMapping[driver]
	if !ok {
		return "", false
	}
	return info.dialect, true
}

// OpenDBWithDriver creates a connection to a database, and modifies goose internals to be
// compatible with the supplied driver by calling SetDialect.
func OpenDBWithDriver(driver string, dbstring string) (*sql.DB, error) {
	if err := SetDialect(driver); err != nil {
		return nil, err
	}
	sqlDriver, ok := SQLDriver(driver)
	if !ok {
		return nil, fmt.Errorf("unsupported driver %s", driver)
	}
	return sql.Open(sqlDriver, dbstring)
}
