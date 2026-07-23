package ottlogstruepb

import "encoding/json"

type OttLogTrueConfigRequest struct {
	ClusterID string `json:"cluster_id"`
}

func (m *OttLogTrueConfigRequest) Marshal() ([]byte, error) { return json.Marshal(m) }
func (m *OttLogTrueConfigRequest) Unmarshal(b []byte) error  { return json.Unmarshal(b, m) }

type OttLogTrueConfigResponse struct {
	NamespacesOfCluster            []string `json:"namespaces_of_cluster"`
	ScanPeriod                     int64    `json:"scan_period"`
	SidecarMask                    string   `json:"sidecar_mask"`
	ReplyTopicEvents               string   `json:"reply_topic_events"`
	HealthcheckPeriod              int64    `json:"healthcheck_period"`
	ScanPeriodFromStartPodSeconds  int64    `json:"scan_period_from_start_pod_seconds"`
	ResyncMs                       int64    `json:"resync_ms"`
	CheckLoggerNameRegex           string   `json:"check_logger_name_regex"`
}

func (m *OttLogTrueConfigResponse) Marshal() ([]byte, error) { return json.Marshal(m) }
func (m *OttLogTrueConfigResponse) Unmarshal(b []byte) error  { return json.Unmarshal(b, m) }

type OttLogTrueSidecarEvent struct {
	RequestResult       string `json:"request_result"`
	RequestPath         string `json:"request_path"`
	RequestMethod       string `json:"request_method"`
	ModuleIDProvider    string `json:"module_id_provider"`
	ModuleIDConsumer    string `json:"module_id_consumer"`
	Host                string `json:"host"`
	HealthcheckRecord   bool   `json:"healthcheck_record"`
	Received            int64  `json:"received"`
	DeploymentUnitKind  string `json:"deployment_unit_kind"`
	DeploymentUnitName  string `json:"deployment_unit_name"`
	Namespace           string `json:"namespace"`
	PodName             string `json:"pod_name"`
	SidecarName         string `json:"sidecar_name"`
}

type OttLogTrueSidecarBucket struct {
	Events      []OttLogTrueSidecarEvent `json:"events"`
	Namespace   string                   `json:"namespace"`
	PodName     string                   `json:"pod_name"`
	SidecarName string                   `json:"sidecar_name"`
}

func (m *OttLogTrueSidecarBucket) Marshal() ([]byte, error) { return json.Marshal(m) }
func (m *OttLogTrueSidecarBucket) Unmarshal(b []byte) error  { return json.Unmarshal(b, m) }

const (
	MessageTypeConfigRequest  = "OttLogTrueConfigRequest"
	MessageTypeConfigResponse = "OttLogTrueConfigResponse"
	MessageTypeSidecarBucket  = "OttLogTrueSidecarBucket"
	AddresseeOttConsumer      = "uamc-ottlogstrue-consumer"
)
