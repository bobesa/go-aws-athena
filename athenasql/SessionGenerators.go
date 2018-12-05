package athenasql

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

var sessionGenerators = map[string]CustomSessionGenerator{
	"": basicSessionGenerator,
}

func basicSessionGenerator(cfg Config) (*session.Session, error) {
	return session.NewSession(&aws.Config{
		Region: aws.String(cfg.Region),
	})
}

// RegisterCustomSessionGenerator registers session generator
func RegisterCustomSessionGenerator(identifier string, generator CustomSessionGenerator) error {
	if _, found := sessionGenerators[identifier]; found {
		return fmt.Errorf(`custom session generator with identifier %q already exists`, identifier)
	}
	sessionGenerators[identifier] = generator
	return nil
}

// CustomSessionGenerator is custom session generator function for AWS
type CustomSessionGenerator func(Config) (*session.Session, error)
