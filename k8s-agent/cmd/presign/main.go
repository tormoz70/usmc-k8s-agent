package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	endpoint := flag.String("endpoint", envOr("MINIO_ENDPOINT", "minio.minio.svc.cluster.local:9000"), "MinIO S3 API endpoint (host:port)")
	accessKey := flag.String("access-key", envOr("MINIO_ACCESS_KEY", "minioadmin"), "MinIO access key")
	secretKey := flag.String("secret-key", envOr("MINIO_SECRET_KEY", "minioadmin"), "MinIO secret key")
	bucket := flag.String("bucket", "exports", "Bucket name")
	object := flag.String("object", "", "Object key (required)")
	secure := flag.Bool("secure", false, "Use HTTPS")
	expiry := flag.Duration("expiry", time.Hour, "Presigned URL lifetime")
	flag.Parse()

	if *object == "" {
		log.Fatal("-object is required")
	}

	client, err := minio.New(*endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(*accessKey, *secretKey, ""),
		Secure: *secure,
	})
	if err != nil {
		log.Fatalf("minio client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	url, err := client.PresignedPutObject(ctx, *bucket, *object, *expiry)
	if err != nil {
		log.Fatalf("presign PUT: %v", err)
	}

	fmt.Println(url.String())
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
