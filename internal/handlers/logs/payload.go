package logs

import (
	"encoding/json"
	"fmt"
	"time"
)

// CollectPayload is the logs.collect command body payload.
type CollectPayload struct {
	Namespace       string   `json:"namespace"`
	LabelSelector   string   `json:"label_selector"`
	Pods            []string `json:"pods"`
	Containers      any      `json:"containers"` // "all" or []string
	IncludeCurrent  bool     `json:"include_current"`
	IncludePrevious bool     `json:"include_previous"`
	SinceTime       *time.Time `json:"since_time"`
	TailLines       *int64   `json:"tail_lines"`
	LimitBytes      *int64   `json:"limit_bytes"`
	S3              S3Payload `json:"s3"`
}

type S3Payload struct {
	Bucket          string `json:"bucket"`
	Key             string `json:"key"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

func ParseCollectPayload(raw json.RawMessage) (*CollectPayload, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("payload is required")
	}
	var p CollectPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	if p.Namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}
	if p.S3.Bucket == "" || p.S3.Key == "" {
		return nil, fmt.Errorf("s3.bucket and s3.key are required")
	}
	if p.S3.AccessKeyID == "" || p.S3.SecretAccessKey == "" {
		return nil, fmt.Errorf("s3 credentials are required")
	}
	if !p.IncludeCurrent && !p.IncludePrevious {
		p.IncludeCurrent = true
	}
	return &p, nil
}

func (p *CollectPayload) ContainerNames(podContainers []string) []string {
	switch v := p.Containers.(type) {
	case string:
		if v == "" || v == "all" {
			return podContainers
		}
		return []string{v}
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		if len(out) == 0 {
			return podContainers
		}
		return out
	case []string:
		if len(v) == 0 {
			return podContainers
		}
		return v
	default:
		return podContainers
	}
}
