package athenasql

import (
	"database/sql/driver"
	"io"

	"github.com/aws/aws-sdk-go/service/athena"
)

// Rows is an iterator over an executed query's results.
type Rows struct {
	resultSet *athena.ResultSet
	stats     *athena.QueryExecutionStatistics
	current   int
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (rows *Rows) Columns() []string {
	colInfo := rows.resultSet.ResultSetMetadata.ColumnInfo
	columns := make([]string, len(colInfo))
	for i, col := range colInfo {
		columns[i] = *col.Name
	}
	return columns
}

// Close closes the rows iterator.
func (rows *Rows) Close() error {
	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (rows *Rows) Next(dest []driver.Value) error {
	if rows.current >= len(rows.resultSet.Rows) {
		return io.EOF
	}

	row := rows.resultSet.Rows[rows.current]
	for i, d := range row.Data {
		dest[i] = *d.VarCharValue
	}

	rows.current++
	return nil
}
