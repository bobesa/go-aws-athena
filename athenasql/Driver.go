package athenasql

import (
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("athena", &Driver{})
}

// Driver is athena driver
type Driver struct{}

// Open returns a new connection to the database.
func (d Driver) Open(dsn string) (driver.Conn, error) {
	config, err := ConfigFromDSN(dsn)
	if err != nil {
		return nil, err
	}

	return &Conn{
		Config: config,
	}, nil
}
