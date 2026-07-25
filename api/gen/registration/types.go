package registrationpb

import "encoding/json"

// AgentRegistrationRequest stub (replace with generated protobuf when Java protos arrive).
type AgentRegistrationRequest struct {
	ClusterID           string   `json:"cluster_id"`
	AgentInstanceID     string   `json:"agent_instance_id"`
	ClusterName         string   `json:"cluster_name"`
	Modules             []string `json:"modules"`
	AgentImplementation string   `json:"agent_implementation,omitempty"`
	LogsBackend         string   `json:"logs_backend,omitempty"`
}

func (m *AgentRegistrationRequest) Marshal() ([]byte, error) { return json.Marshal(m) }
func (m *AgentRegistrationRequest) Unmarshal(b []byte) error  { return json.Unmarshal(b, m) }

type AgentRegistrationResponse struct {
	Accepted bool   `json:"accepted"`
	Message  string `json:"message"`
	Reason   string `json:"reason,omitempty"`
}

func (m *AgentRegistrationResponse) Marshal() ([]byte, error) { return json.Marshal(m) }
func (m *AgentRegistrationResponse) Unmarshal(b []byte) error  { return json.Unmarshal(b, m) }

const MessageTypeRequest = "AgentRegistrationRequest"
const MessageTypeResponse = "AgentRegistrationResponse"
const RequestTypeRegister = "REGISTER"

// ReasonAgentAlreadyRegistered is returned by Core when another agent already owns cluster_id.
const ReasonAgentAlreadyRegistered = "AgentAlreadyRegistered"
