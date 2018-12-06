package athenasql

import (
	"context"
	"database/sql/driver"
	"fmt"
)

// Conn is single driver connection
type Conn struct {
	Config Config
}

// Prepare returns a prepared statement, bound to this connection.
func (conn *Conn) Prepare(query string) (driver.Stmt, error) {
	return &Stmt{
		query:  query,
		config: conn.Config,
	}, nil
}

// PrepareContext returns a prepared statement, bound to this connection.
// context is for the preparation of the statement,
// it must not store the context within the statement itself.
func (conn *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return conn.Prepare(query)
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (conn *Conn) Close() error {
	return nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (conn *Conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("n/a")
}

// BeginTx starts and returns a new transaction.
// If the context is canceled by the user the sql package will
// call Tx.Rollback before discarding and closing the connection.
//
// This must check opts.Isolation to determine if there is a set
// isolation level. If the driver does not support a non-default
// level and one is set or if there is a non-default isolation level
// that is not supported, an error must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only
// value is true to either set the read-only transaction property if supported
// or return an error if it is not supported.
func (conn *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return conn.Begin()
}
