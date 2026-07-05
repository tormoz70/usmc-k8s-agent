package cachehandler

import (
	"encoding/json"
	"fmt"
)

type PutEntry struct {
	Key        string `json:"key"`
	Value      string `json:"value"`
	TTLSeconds int    `json:"ttl_seconds"`
}

type PutPayload struct {
	Entries []PutEntry `json:"entries"`
}

type DeletePayload struct {
	Keys []string `json:"keys"`
}

func ParsePutPayload(raw json.RawMessage) (*PutPayload, error) {
	var p PutPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	if len(p.Entries) == 0 {
		return nil, fmt.Errorf("entries must not be empty")
	}
	for i, e := range p.Entries {
		if e.Key == "" {
			return nil, fmt.Errorf("entries[%d].key is required", i)
		}
	}
	return &p, nil
}

type ClearPayload struct {
	Prefix string `json:"prefix"`
}

func ParseClearPayload(raw json.RawMessage) (*ClearPayload, error) {
	var p ClearPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	return &p, nil
}

func ParseDeletePayload(raw json.RawMessage) (*DeletePayload, error) {
	var p DeletePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	if len(p.Keys) == 0 {
		return nil, fmt.Errorf("keys must not be empty")
	}
	return &p, nil
}
