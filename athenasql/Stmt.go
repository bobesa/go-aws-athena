package athenasql

import (
	"context"
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
	return stmt.ExecContext(context.Background(), valuesToNamedValues(args))
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (stmt *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return nil, fmt.Errorf("n/a")
}

// Query executes a query that may return rows, such as a SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.QueryContext(context.Background(), valuesToNamedValues(args))
}

// QueryContext executes a query that may return rows, such as a SELECT.
//
// QueryContext must honor the context timeout and return when it is canceled.
func (stmt *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	query, err := queryWithArgs(stmt.query, args)
	if err != nil {
		return nil, fmt.Errorf(`unable to provide arguments for query: %s`, err.Error())
	}

	sessionGenerator, found := sessionGenerators[stmt.config.SessionGenerator]
	if !found {
		return nil, fmt.Errorf(`unable to create session: session generator %q not found`, stmt.config.SessionGenerator)
	}

	awsSession, err := sessionGenerator(stmt.config)
	if err != nil {
		return nil, fmt.Errorf(`unable to create session: %s`, err.Error())
	}

	athenaService := athena.New(awsSession, aws.NewConfig().WithRegion(stmt.config.Region))
	var queryStartInput athena.StartQueryExecutionInput
	queryStartInput.SetQueryString(query)

	var queryContext athena.QueryExecutionContext
	queryContext.SetDatabase(stmt.config.Database)
	queryStartInput.SetQueryExecutionContext(&queryContext)

	var resultConfig athena.ResultConfiguration
	resultConfig.SetOutputLocation("s3://" + stmt.config.S3Bucket)
	queryStartInput.SetResultConfiguration(&resultConfig)

	result, err := athenaService.StartQueryExecution(&queryStartInput)
	if err != nil {
		return nil, fmt.Errorf(`unable to start query: %s`, err.Error())
	}

	var queryInput athena.GetQueryExecutionInput
	queryInput.SetQueryExecutionId(*result.QueryExecutionId)

	var queryOutput *athena.GetQueryExecutionOutput
	duration := time.Duration(2) * time.Second // Pause for 2 seconds

	for {
		queryOutput, err = athenaService.GetQueryExecution(&queryInput)
		if err != nil {
			return nil, fmt.Errorf(`unable to check status: %s`, err.Error())
		}
		if *queryOutput.QueryExecution.Status.State != ExecutionStatusRunning {
			break
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf(`query was cancelled`)
		default:
			time.Sleep(duration)
		}
	}

	if *queryOutput.QueryExecution.Status.State != ExecutionStatusSucceeded {
		return nil, fmt.Errorf(`execution failed: %s`, *queryOutput.QueryExecution.Status.State)
	}

	var queryResultsInput athena.GetQueryResultsInput
	queryResultsInput.SetQueryExecutionId(*result.QueryExecutionId)

	queryResultsOutput, err := athenaService.GetQueryResults(&queryResultsInput)
	if err != nil {
		return nil, fmt.Errorf(`unable to receive results: %s`, err.Error())
	}

	var queryExecInput athena.GetQueryExecutionInput
	queryExecInput.SetQueryExecutionId(*result.QueryExecutionId)
	queryExecutionInfo, err := athenaService.GetQueryExecution(&queryExecInput)
	if err != nil {
		return nil, fmt.Errorf(`unable to receive stats: %s`, err.Error())
	}

	return &Rows{
		resultSet: queryResultsOutput.ResultSet,
		stats:     queryExecutionInfo.QueryExecution.Statistics,
		current:   1, // skip headers
	}, nil
}
