package observability

import "sync/atomic"

// RuntimeState tracks agent readiness for HTTP probes and metrics.
type RuntimeState struct {
	kafkaConnected atomic.Bool
	isLeader       atomic.Bool
	apiserverOK    atomic.Bool
}

func NewRuntimeState() *RuntimeState {
	return &RuntimeState{}
}

func (s *RuntimeState) SetKafkaConnected(v bool) {
	s.kafkaConnected.Store(v)
}

func (s *RuntimeState) SetLeader(v bool) {
	s.isLeader.Store(v)
}

func (s *RuntimeState) SetAPIServerOK(v bool) {
	s.apiserverOK.Store(v)
}

func (s *RuntimeState) KafkaConnected() bool {
	return s.kafkaConnected.Load()
}

func (s *RuntimeState) IsLeader() bool {
	return s.isLeader.Load()
}

func (s *RuntimeState) APIServerOK() bool {
	return s.apiserverOK.Load()
}

func (s *RuntimeState) Ready() bool {
	if !s.kafkaConnected.Load() {
		return false
	}
	if !s.apiserverOK.Load() {
		return false
	}
	return true
}
