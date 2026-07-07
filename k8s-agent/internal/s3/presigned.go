package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Uploader struct {
	client *http.Client
}

func NewUploader() *Uploader {
	return &Uploader{
		client: &http.Client{Timeout: 30 * time.Minute},
	}
}

func (u *Uploader) Upload(ctx context.Context, presignedURL string, reader io.Reader, contentType string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, presignedURL, reader)
	if err != nil {
		return 0, fmt.Errorf("create PUT request: %w", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := u.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("PUT presigned URL: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == 403 {
			return 0, fmt.Errorf("PRESIGNED_URL_EXPIRED: status %d: %s", resp.StatusCode, string(body))
		}
		return 0, fmt.Errorf("upload failed status %d: %s", resp.StatusCode, string(body))
	}

	if resp.ContentLength > 0 {
		return resp.ContentLength, nil
	}
	return 0, nil
}

func HostFromPresignedURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse presigned URL: %w", err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("presigned URL has empty host")
	}
	return u.Host, nil
}

func UploadWithRetry(ctx context.Context, uploader *Uploader, presignedURL, contentType string, reader io.Reader, maxRetries int, baseDelay time.Duration) (int64, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(baseDelay * time.Duration(1<<attempt)):
			}
		}
		n, err := uploader.Upload(ctx, presignedURL, reader, contentType)
		if err == nil {
			return n, nil
		}
		lastErr = err
	}
	return 0, lastErr
}
