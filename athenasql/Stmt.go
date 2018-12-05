package athenasql

import (
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
)

// Statuses
const (
	ExecutionStatusRunning   = "RUNNING"
	ExecutionStatusSucceeded = "SUCCEEDED"
)

// Stmt is a prepared statement. It is bound to a Conn and not used by multiple
// goroutines concurrently.
type Stmt struct {
	query  string
	config Config
}

// Close closes the statement.
func (stmt *Stmt) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
func (stmt *Stmt) NumInput() int {
	// TODO: Implement it
	return -1
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
func (stmt *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("n/a")
}

// Query executes a query that may return rows, such as a SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	sessionGenerator, found := sessionGenerators[stmt.config.SessionGenerator]
	if !found {
		return nil, fmt.Errorf(`unable to create session: session generator %q not found`, stmt.config.SessionGenerator)
	}

	sess, err := sessionGenerator(stmt.config)
	if err != nil {
		return nil, fmt.Errorf(`unable to create session: %s`, err.Error())
	}

	svc := athena.New(sess, aws.NewConfig().WithRegion(stmt.config.Region))
	var s athena.StartQueryExecutionInput
	s.SetQueryString(stmt.query)

	var q athena.QueryExecutionContext
	q.SetDatabase(stmt.config.Database)
	s.SetQueryExecutionContext(&q)

	var r athena.ResultConfiguration
	r.SetOutputLocation("s3://" + stmt.config.S3Bucket)
	s.SetResultConfiguration(&r)

	result, err := svc.StartQueryExecution(&s)
	if err != nil {
		return nil, fmt.Errorf(`unable to start query: %s`, err.Error())
	}

	var qri athena.GetQueryExecutionInput
	qri.SetQueryExecutionId(*result.QueryExecutionId)

	var qrop *athena.GetQueryExecutionOutput
	duration := time.Duration(2) * time.Second // Pause for 2 seconds

	for {
		qrop, err = svc.GetQueryExecution(&qri)
		if err != nil {
			return nil, fmt.Errorf(`unable to check status: %s`, err.Error())
		}
		if *qrop.QueryExecution.Status.State != ExecutionStatusRunning {
			break
		}
		time.Sleep(duration)

	}

	if *qrop.QueryExecution.Status.State != ExecutionStatusSucceeded {
		return nil, fmt.Errorf(`execution failed: %s`, *qrop.QueryExecution.Status.State)
	}

	var ip athena.GetQueryResultsInput
	ip.SetQueryExecutionId(*result.QueryExecutionId)

	op, err := svc.GetQueryResults(&ip)
	if err != nil {
		return nil, fmt.Errorf(`unable to receive results: %s`, err.Error())
	}

	var ep athena.GetQueryExecutionInput
	ep.SetQueryExecutionId(*result.QueryExecutionId)
	ex, err := svc.GetQueryExecution(&ep)
	if err != nil {
		return nil, fmt.Errorf(`unable to receive stats: %s`, err.Error())
	}

	return &Rows{
		resultSet: op.ResultSet,
		stats:     ex.QueryExecution.Statistics,
		current:   1, // skip headers
	}, nil
}
