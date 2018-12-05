package athenasql

import "testing"

func TestConfigFromDSN(t *testing.T) {
	for _, testCase := range []struct {
		DSN    string
		Config Config
		Errors bool
	}{
		// Empty SDN == Empty config
		{
			Config: Config{},
		},

		// Read Region
		{
			DSN:    "region=aaa",
			Config: Config{Region: "aaa"},
		},

		// Read S3 Bucket
		{
			DSN:    "s3_bucket=aaa",
			Config: Config{S3Bucket: "aaa"},
		},

		// Read Session Generator
		{
			DSN:    "session_generator=aaa",
			Config: Config{SessionGenerator: "aaa"},
		},

		// Read Database
		{
			DSN:    "db=aaa",
			Config: Config{Database: "aaa"},
		},

		// Read All of above
		{
			DSN:    "session_generator=sg region=rrr s3_bucket=s3b db=dbdb",
			Config: Config{SessionGenerator: "sg", Region: "rrr", S3Bucket: "s3b", Database: "dbdb"},
		},
	} {
		cfg, err := ConfigFromDSN(testCase.DSN)
		if err == nil && testCase.Errors {
			t.Errorf(`Expected %q to error, but did not`, testCase.DSN)
		} else if err != nil && !testCase.Errors {
			t.Errorf(`Expected not errors for %q, but got %q instead`, testCase.DSN, err.Error())
		} else if cfg != testCase.Config {
			t.Errorf(`Expected %v for %q, but got %v instead`, testCase.Config, testCase.DSN, cfg)
		}
	}
}
