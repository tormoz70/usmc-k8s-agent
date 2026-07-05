package result

import (
	"encoding/json"
	"time"
)

const SchemaVersionV1 = "v1"

const (
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusRejected  = "rejected"
	StatusExecuting = "executing"
)

// PartialError describes a non-fatal failure during logs.collect fan-out.
type PartialError struct {
	Pod       string `json:"pod,omitempty"`
	Container string `json:"container,omitempty"`
	Reason    string `json:"reason"`
	Message   string `json:"message,omitempty"`
}

// LogsCollectFields are added to Response for type=logs.collect.
type LogsCollectFields struct {
	S3Bucket      string         `json:"s3_bucket,omitempty"`
	S3Key         string         `json:"s3_key,omitempty"`
	ByteSize      int64          `json:"byte_size,omitempty"`
	FileCount     int            `json:"file_count,omitempty"`
	PartialErrors []PartialError `json:"partial_errors,omitempty"`
	Truncated     bool           `json:"truncated,omitempty"`
}

// Response is published to reply_topic from Kafka request headers.
type Response struct {
	SchemaVersion   string          `json:"schema_version"`
	CommandID       string          `json:"command_id"`
	CorrelationID   string          `json:"correlation_id"`
	Status          string          `json:"status"`
	Reason          string          `json:"reason,omitempty"`
	HTTPStatus      int             `json:"http_status,omitempty"`
	HTTPBody        json.RawMessage `json:"http_body,omitempty"`
	ResourceRef     *ResourceRef    `json:"resource_ref,omitempty"`
	ResourceVersion string          `json:"resource_version,omitempty"`
	StartedAt       time.Time       `json:"started_at"`
	FinishedAt      time.Time       `json:"finished_at"`
	Error           *ErrorDetail    `json:"error"`

	LogsCollectFields

	SubscriptionID  string `json:"subscription_id,omitempty"`
	OutputTopic     string `json:"output_topic,omitempty"`
	IntervalSeconds int    `json:"interval_seconds,omitempty"`
	KeysWritten     int    `json:"keys_written,omitempty"`
	KeysDeleted     int    `json:"keys_deleted,omitempty"`
	KeysCleared     int    `json:"keys_cleared,omitempty"`
}

type ResourceRef struct {
	Group     string `json:"group"`
	Version   string `json:"version"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func Rejected(commandID, correlationID, reason, code, message string, started, finished time.Time) *Response {
	return &Response{
		SchemaVersion: SchemaVersionV1,
		CommandID:     commandID,
		CorrelationID: correlationID,
		Status:        StatusRejected,
		Reason:        reason,
		StartedAt:     started,
		FinishedAt:    finished,
		Error:         &ErrorDetail{Code: code, Message: message},
	}
}

func Failed(commandID, correlationID, reason, code, message string, started, finished time.Time) *Response {
	return &Response{
		SchemaVersion: SchemaVersionV1,
		CommandID:     commandID,
		CorrelationID: correlationID,
		Status:        StatusFailed,
		Reason:        reason,
		StartedAt:     started,
		FinishedAt:    finished,
		Error:         &ErrorDetail{Code: code, Message: message},
	}
}

func Completed(commandID, correlationID string, started, finished time.Time) *Response {
	return &Response{
		SchemaVersion: SchemaVersionV1,
		CommandID:     commandID,
		CorrelationID: correlationID,
		Status:        StatusCompleted,
		StartedAt:     started,
		FinishedAt:    finished,
		Error:         nil,
	}
}
