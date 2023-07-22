package secrets

import "os"

var (
	AwsAccessKey    = os.Getenv("AWS_ACCESS_KEY_ID")
	AwsSecretKey    = os.Getenv("AWS_SECRET_ACCESS_KEY")
	AwsBucket       = "my_bucket"
	AwsBucketPrefix = "path/to/my/files"
	MaxConcurrency  = 24
)
