package athenasql

import (
	"testing"
)

func TestDriverOpen(t *testing.T) {
	_, err := Driver{}.Open("foo bar")
	if err == nil {
		t.Errorf(`Expected error, but got none instead`)
	}

	_, err = Driver{}.Open("s3_bucket=xxx")
	if err != nil {
		t.Errorf(`Expected no error, but got %q instead`, err.Error())
	}
}
