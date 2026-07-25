package main

import (
	_ "embed"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"gopkg.in/yaml.v3"
)

//go:embed targets.yaml
var embeddedTargetsYAML []byte

// agentTarget describes a Core→agent routing endpoint (v1 or v2).
type agentTarget struct {
	ID              string `json:"id" yaml:"id"`
	Label           string `json:"label" yaml:"label"`
	ButtonLabel     string `json:"button_label,omitempty" yaml:"button_label"`
	Hint            string `json:"hint,omitempty" yaml:"hint"`
	Implementation  string `json:"implementation" yaml:"implementation"`
	ClusterID       string `json:"cluster_id" yaml:"cluster_id"`
	RequestTopic    string `json:"request_topic" yaml:"request_topic"`
	ReplyTopic      string `json:"reply_topic" yaml:"reply_topic"`
	AgentNamespace  string `json:"agent_namespace" yaml:"agent_namespace"`
	AgentHTTPURL    string `json:"agent_http_url" yaml:"agent_http_url"`
	EventsTopic     string `json:"events_topic" yaml:"events_topic"`
	LogsStreamTopic string `json:"logs_stream_topic" yaml:"logs_stream_topic"`
	HealthTopic     string `json:"health_topic" yaml:"health_topic"`
	LifecycleTopic  string `json:"lifecycle_topic" yaml:"lifecycle_topic"`
}

type targetsFile struct {
	Targets []agentTarget `yaml:"targets"`
}

type targetRegistry struct {
	mu       sync.RWMutex
	all      []agentTarget
	selected string
}

func defaultTargets(fallback agentTarget) []agentTarget {
	var f targetsFile
	if err := yaml.Unmarshal(embeddedTargetsYAML, &f); err == nil && len(f.Targets) > 0 {
		out := make([]agentTarget, len(f.Targets))
		copy(out, f.Targets)
		for i := range out {
			if fallback.AgentHTTPURL != "" {
				out[i].AgentHTTPURL = fallback.AgentHTTPURL
			}
			if out[i].ReplyTopic == "" && fallback.ReplyTopic != "" {
				out[i].ReplyTopic = fallback.ReplyTopic
			}
		}
		return out
	}
	return []agentTarget{fallback}
}

func loadTargets(path string, fallback agentTarget) (*targetRegistry, error) {
	reg := &targetRegistry{}
	builtins := defaultTargets(fallback)
	reg.all = builtins
	reg.selected = builtins[0].ID

	if path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			var f targetsFile
			if err := yaml.Unmarshal(data, &f); err != nil {
				return nil, fmt.Errorf("parse targets: %w", err)
			}
			if len(f.Targets) > 0 {
				reg.all = f.Targets
				reg.selected = f.Targets[0].ID
				slog.Info("loaded agent targets from file", "path", path, "count", len(reg.all))
			}
		} else if !os.IsNotExist(err) {
			return nil, err
		} else {
			slog.Info("targets file not found; using embedded Agent 1/2 defaults", "path", path, "count", len(reg.all))
		}
	} else {
		slog.Info("using embedded Agent 1/2 targets", "count", len(reg.all))
	}

	if id := os.Getenv("MOCK_CORE_UI_TARGET"); id != "" {
		for _, t := range reg.all {
			if t.ID == id {
				reg.selected = id
				break
			}
		}
	}
	return reg, nil
}

func (r *targetRegistry) List() []agentTarget {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]agentTarget, len(r.all))
	copy(out, r.all)
	return out
}

func (r *targetRegistry) SelectedID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.selected
}

func (r *targetRegistry) Current() (agentTarget, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, t := range r.all {
		if t.ID == r.selected {
			return t, nil
		}
	}
	if len(r.all) > 0 {
		return r.all[0], nil
	}
	return agentTarget{}, fmt.Errorf("no targets configured")
}

func (r *targetRegistry) Select(id string) (agentTarget, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range r.all {
		if t.ID == id {
			r.selected = id
			return t, nil
		}
	}
	return agentTarget{}, fmt.Errorf("unknown target %q", id)
}

func (t agentTarget) MonitorTopics() []string {
	seen := map[string]struct{}{}
	var out []string
	add := func(s string) {
		if s == "" {
			return
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	add(t.RequestTopic)
	add(t.ReplyTopic)
	add(t.EventsTopic)
	add(t.LogsStreamTopic)
	add(t.HealthTopic)
	add(t.LifecycleTopic)
	return out
}

func (s *server) currentRequestTopic() string {
	if t, err := s.targets.Current(); err == nil && t.RequestTopic != "" {
		return t.RequestTopic
	}
	return s.cfg.RequestTopic
}

func (s *server) currentReplyTopic() string {
	if t, err := s.targets.Current(); err == nil && t.ReplyTopic != "" {
		return t.ReplyTopic
	}
	return s.cfg.ReplyTopic
}
