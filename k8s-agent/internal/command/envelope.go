package command

import (
	"encoding/json"
	"time"
)

type GVK struct {
	Group   string `json:"group"`
	Version string `json:"version"`
	Kind    string `json:"kind"`
}

type Target struct {
	Group     string `json:"group"`
	Version   string `json:"version"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name,omitempty"`
}

type Command struct {
	CommandID      string          `json:"command_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Type           string          `json:"type"`
	IssuedBy       string          `json:"issued_by"`
	TS             time.Time       `json:"ts"`
	DryRun         bool            `json:"dry_run"`
	Target         Target          `json:"target"`
	Payload        json.RawMessage `json:"payload"`
}

type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Result struct {
	CommandID      string          `json:"command_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Status         string          `json:"status"`
	Phase          string          `json:"phase,omitempty"`
	Progress       int             `json:"progress,omitempty"`
	StartedAt      time.Time       `json:"started_at"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	Details        json.RawMessage `json:"details,omitempty"`
	Error          *ErrorDetail    `json:"error"`
}

type ClusterEvent struct {
	SubscriptionID string          `json:"subscription_id"`
	EventType      string          `json:"event_type"`
	Resource       Target          `json:"resource"`
	ObservedAt     time.Time       `json:"observed_at"`
	Details        json.RawMessage `json:"details,omitempty"`
}

const (
	StatusReceived  = "received"
	StatusValidated = "validated"
	StatusExecuting = "executing"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusRejected  = "rejected"
)

const (
	TypeResourceList         = "resource.list"
	TypeFileFetch            = "file.fetch"
	TypeWatchSubscribe       = "watch.subscribe"
	TypeWatchUnsubscribe     = "watch.unsubscribe"
	TypeLogsStreamSubscribe  = "logs.stream.subscribe"
	TypeLogsStreamUnsubscribe = "logs.stream.unsubscribe"
)

const (
	EventAdd      = "ADD"
	EventUpdate   = "UPDATE"
	EventDelete   = "DELETE"
	EventLogLine  = "LOG_LINE"
)
