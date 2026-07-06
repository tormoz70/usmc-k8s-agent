package main

import (
	"context"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3ObjectInfo struct {
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	Exists      bool   `json:"exists"`
	Size        int64  `json:"size,omitempty"`
	ETag        string `json:"etag,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	LastModified string `json:"last_modified,omitempty"`
	ConsoleURL  string `json:"console_url,omitempty"`
}

func (s *server) headObject(ctx context.Context, bucket, key string) (*s3ObjectInfo, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(s.cfg.S3Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			s.cfg.S3AccessKey, s.cfg.S3SecretKey, "",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if s.cfg.S3Endpoint != "" {
			ep := s.cfg.S3Endpoint
			o.BaseEndpoint = &ep
		}
		o.UsePathStyle = s.cfg.S3PathStyle
	})

	out, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("head object: %w", err)
	}

	info := &s3ObjectInfo{
		Bucket: bucket,
		Key:    key,
		Exists: true,
	}
	if out.ContentLength != nil {
		info.Size = *out.ContentLength
	}
	if out.ETag != nil {
		info.ETag = *out.ETag
	}
	if out.ContentType != nil {
		info.ContentType = *out.ContentType
	}
	if out.LastModified != nil {
		info.LastModified = out.LastModified.UTC().Format("2006-01-02T15:04:05Z")
	}
	return info, nil
}
