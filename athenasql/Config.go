package athenasql

import (
	"fmt"
	"strings"
)

// Config contains all configuration from passed dsn
type Config struct {
	Region           string
	Database         string
	SessionGenerator string
	S3Bucket         string
}

// ConfigFromDSN returns new config based on given dsn
func ConfigFromDSN(dsn string) (Config, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return Config{}, nil
	}

	// TODO: Make this somewhat better, this is just temporary
	cfg := Config{}
	for _, part := range strings.Split(dsn, " ") {
		pair := strings.SplitN(part, "=", 2)
		if len(pair) != 2 {
			return Config{}, fmt.Errorf(`%q is not a key-value pair (foo=bar)`, part)
		}

		switch strings.ToLower(pair[0]) {
		case "session_generator":
			cfg.SessionGenerator = pair[1]
		case "s3_bucket":
			cfg.S3Bucket = pair[1]
		case "region":
			cfg.Region = pair[1]
		case "db", "database":
			cfg.Database = pair[1]
		default:
			return Config{}, fmt.Errorf(`%q is not a valid parameter`, pair[0])
		}
	}
	return cfg, nil
}
