package registrationpb

import "encoding/json"

// AgentRegistrationRequest stub (replace with generated protobuf when Java protos arrive).
type AgentRegistrationRequest struct {
	ClusterID        string   `json:"cluster_id"`
	AgentInstanceID  string   `json:"agent_instance_id"`
	ClusterName      string   `json:"cluster_name"`
	Modules          []string `json:"modules"`
}

func (m *AgentRegistrationRequest) Marshal() ([]byte, error) { return json.Marshal(m) }
func (m *AgentRegistrationRequest) Unmarshal(b []byte) error  { return json.Unmarshal(b, m) }

type AgentRegistrationResponse struct {
	Accepted bool   `json:"accepted"`
	Message  string `json:"message"`
}

func (m *AgentRegistrationResponse) Marshal() ([]byte, error) { return json.Marshal(m) }
func (m *AgentRegistrationResponse) Unmarshal(b []byte) error  { return json.Unmarshal(b, m) }

const MessageTypeRequest = "AgentRegistrationRequest"
const MessageTypeResponse = "AgentRegistrationResponse"
const RequestTypeRegister = "REGISTER"
