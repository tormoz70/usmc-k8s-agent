package command

import (
	"encoding/json"
	"fmt"
	"time"
)

const SchemaVersionV1 = "v1"

// Command is the Kafka request body envelope.
type Command struct {
	SchemaVersion  string          `json:"schema_version"`
	CommandID      string          `json:"command_id"`
	Type           string          `json:"type"`
	Issuer         string          `json:"issuer"`
	IdempotencyKey string          `json:"idempotency_key"`
	Timeout        string          `json:"timeout"`
	IssuedAt       time.Time       `json:"issued_at"`
	HTTP           *HTTPRequest    `json:"http,omitempty"`
	Payload        json.RawMessage `json:"payload,omitempty"`
}

// HTTPRequest is embedded for type=k8s.api commands.
type HTTPRequest struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Query   string            `json:"query,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}

// RequestMeta is carried outside the JSON body (Kafka headers + offsets).
type RequestMeta struct {
	CorrelationID string
	ReplyTopic    string
	Partition     int
	Offset        int64
}

// Validate checks required envelope fields.
func (c *Command) Validate() error {
	if c == nil {
		return fmt.Errorf("command is nil")
	}
	if c.SchemaVersion != SchemaVersionV1 {
		return fmt.Errorf("unsupported schema_version %q", c.SchemaVersion)
	}
	if c.CommandID == "" {
		return fmt.Errorf("command_id is required")
	}
	if c.Type == "" {
		return fmt.Errorf("type is required")
	}
	if c.IdempotencyKey == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	if c.IssuedAt.IsZero() {
		return fmt.Errorf("issued_at is required")
	}
	return nil
}

// TimeoutDuration parses the timeout field; zero means default is applied by caller.
func (c *Command) TimeoutDuration(defaultTimeout time.Duration) time.Duration {
	if c.Timeout == "" {
		return defaultTimeout
	}
	d, err := time.ParseDuration(c.Timeout)
	if err != nil || d <= 0 {
		return defaultTimeout
	}
	return d
}
