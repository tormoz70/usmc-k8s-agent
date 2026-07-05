package s3

import (
	"context"
	"fmt"
	"io"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// UploadInput describes a single object upload using request-scoped credentials.
type UploadInput struct {
	Endpoint       string
	Region         string
	ForcePathStyle bool
	Bucket         string
	Key            string
	AccessKeyID    string
	SecretAccessKey string
	Body           io.Reader
	ContentLength  int64
	ContentType    string
}

// Client uploads objects to S3-compatible storage.
type Client struct {
	defaultEndpoint       string
	defaultRegion         string
	defaultForcePathStyle bool
}

func NewClient(endpoint, region string, forcePathStyle bool) *Client {
	return &Client{
		defaultEndpoint:       endpoint,
		defaultRegion:         region,
		defaultForcePathStyle: forcePathStyle,
	}
}

// Upload puts an object using credentials from the request payload.
func (c *Client) Upload(ctx context.Context, in UploadInput) error {
	endpoint := in.Endpoint
	if endpoint == "" {
		endpoint = c.defaultEndpoint
	}
	region := in.Region
	if region == "" {
		region = c.defaultRegion
	}
	forcePathStyle := in.ForcePathStyle || c.defaultForcePathStyle

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			in.AccessKeyID, in.SecretAccessKey, "",
		)),
	)
	if err != nil {
		return fmt.Errorf("aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = awsString(endpoint)
		}
		o.UsePathStyle = forcePathStyle
	})

	putIn := &s3.PutObjectInput{
		Bucket:      awsString(in.Bucket),
		Key:         awsString(in.Key),
		Body:        in.Body,
		ContentType: awsString(in.ContentType),
	}
	if in.ContentLength > 0 {
		putIn.ContentLength = &in.ContentLength
	}

	_, err = client.PutObject(ctx, putIn)
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func awsString(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}
