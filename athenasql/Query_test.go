package athenasql

import (
	"database/sql/driver"
	"testing"
)

func makeArgs(args ...interface{}) []driver.NamedValue {
	vals := make([]driver.Value, len(args))
	for i, arg := range args {
		vals[i] = arg
	}
	return valuesToNamedValues(vals)
}

func TestQueryWithArgs(t *testing.T) {
	for _, testCase := range []struct {
		Query, Result string
		Arguments     []driver.NamedValue
		Errors        bool
	}{
		{Query: `a=? AND b=$1`, Errors: true}, // indexed & anonymous
		{Query: `a=?`, Errors: true},
		{Query: `a=$a`, Errors: true},
		{Query: `a=$1`, Errors: true},
		{Query: `a=$2`, Errors: true},
		{Query: `a=b`, Result: `a=b`}, // nothing to provide
		{Query: `a=$2`, Arguments: makeArgs(1), Errors: true},
		{Query: `a=$444`, Arguments: makeArgs(1, 2, 3), Errors: true},
		{Query: `a=?`, Arguments: makeArgs(1), Result: `a=1`},
		{Query: `a=?,b=?`, Arguments: makeArgs(1, 2), Result: `a=1,b=2`},
		{Query: `a=$1`, Arguments: makeArgs(1), Result: `a=1`},
		{Query: `a=$1,b=$2`, Arguments: makeArgs(1, 2), Result: `a=1,b=2`},
		{Query: `a=$2,b=$1`, Arguments: makeArgs(1, 2), Result: `a=2,b=1`},
	} {
		res, err := queryWithArgs(testCase.Query, testCase.Arguments)
		if testCase.Errors && err == nil {
			t.Errorf(`Expected error for %q, but got none instead`, testCase.Query)
		} else if !testCase.Errors && err != nil {
			t.Errorf(`Expected no error for %q, but got %q instead`, testCase.Query, err.Error())
		} else if !testCase.Errors && res != testCase.Result {
			t.Errorf(`Expected %q, but got %q instead`, testCase.Result, res)
		}
	}
}
