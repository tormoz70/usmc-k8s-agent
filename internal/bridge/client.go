package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

// ExecuteRequest carries a command and Kafka metadata to agent-service.
type ExecuteRequest struct {
	Command *command.Command    `json:"command"`
	Meta    command.RequestMeta `json:"meta"`
}

// RemoteExecutor forwards commands to agent-service over HTTP.
type RemoteExecutor struct {
	baseURL       string
	internalToken string
	client        *http.Client
}

func NewRemoteExecutor(baseURL, internalToken string) *RemoteExecutor {
	return &RemoteExecutor{
		baseURL:       baseURL,
		internalToken: internalToken,
		client: &http.Client{
			Timeout: 10 * time.Minute,
		},
	}
}

func (r *RemoteExecutor) Handle(ctx context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error) {
	body, err := json.Marshal(ExecuteRequest{Command: cmd, Meta: meta})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.baseURL+"/internal/v1/commands", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if r.internalToken != "" {
		req.Header.Set("Authorization", "Bearer "+r.internalToken)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("agent-service request: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// Async commands (e.g. logs.collect) acknowledge with 202 and nil body payload.
	if resp.StatusCode == http.StatusAccepted {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agent-service returned %d: %s", resp.StatusCode, string(data))
	}

	var out result.Response
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode agent-service response: %w", err)
	}
	return &out, nil
}
