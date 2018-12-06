# Go AWS Athena driver for database/sql

Go database/sql driver for AWS Athena wraps Athena's AWS SDK to provide go sql
driver as you would expect. This allows you to query AWS Athena in same way as
any other SQL database.

This driver also allows you to use **custom session generators** to use bastion
accounts or other ways of creating aws session.

## Install

```bash
go get github.com/bobesa/go-aws-athena
```

## Usage

Register the driver trough the go imports, this will allow you to use `athena` driver

```go
import _ "github.com/bobesa/go-aws-athena/athenasql"

db, err := sql.Open("athena", "foo=bar")
```

## DSN Attributes

Driver support multiple attributes to be passed

### Database

Database name to operate on. Required for execution

```go
db, err := sql.Open("athena", "db=sampledb")
```

### Region

AWS Region

```go
db, err := sql.Open("athena", "region=eu-west-1")
```

### S3 Results Bucket

S3 bucket for results. Supply without `s3://`

```go
db, err := sql.Open("athena", "s3_bucket=aws-athena-query-results-xxxxxxxx-eu-west-1")
```

### Session Generator

Sessiong Generator to create AWS session. This allows you to use custom session generators

```go
db, err := sql.Open("athena", "session_generator=mysessiongenerator")
```

## Custom Session Generators

To create custom session generator just use `athenasql.RegisterCustomSessionGenerator` to provide it.
Function signature is `func(cfg athenasql.Config) (*session.Session, error)`.
Config (`athenasql.Config`) will provide you with Region, S3 Bucket and other stuff.

```go
import "github.com/bobesa/go-aws-athena/athenasql"

athenasql.RegisterCustomSessionGenerator("mysessiongenerator", func(cfg athenasql.Config) (*session.Session, error) {
	return session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
})
```

Afterwards you can use it when opening sql db connection

```go
db, err := sql.Open("athena", "session_generator=mysessiongenerator")
```

## Notes

- Driver is not written for Exec, only Query
- Original inspirational athena querying gist code: [Kartiksura's GitHub Gist](https://gist.github.com/kartiksura/93160be1078648a14ec0ddc125c35546)