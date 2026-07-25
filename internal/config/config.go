package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultHTTPPort              = 8080
	DefaultInternalHTTPPort      = 8081
	DefaultLogsNodeHTTPPort      = 8083
	DefaultRequestTopic          = "k8s.commands.request"
	DefaultConsumerGroup         = "k8s-agent"
	DefaultLogsCollectMaxJobs    = 20
	DefaultLogsCollectMaxBytes   = 524288000 // 500 MiB
	DefaultSyncWorkerConcurrency = 1
	DefaultPodLogsRoot           = "/var/log/pods"

	ComponentAll          = "all"
	ComponentIngress      = "ingress"
	ComponentEgress       = "egress"
	ComponentAgentService = "agent-service"
	ComponentLogsNode     = "logs-node"

	ImplementationV1 = "v1"
	ImplementationV2 = "v2"

	LogsBackendAPI      = "api"
	LogsBackendNodeLocal = "nodelocal"

	KafkaModeJSON     = "json"
	KafkaModeProtobuf = "protobuf"
	KafkaModeDual     = "dual"
)

// Config holds agent runtime configuration loaded from environment variables.
type Config struct {
	ClusterID string

	Kafka  KafkaConfig
	S3     S3Config
	Agent  AgentConfig
	HTTP   HTTPConfig
	Policy PolicyConfig
	Logs   LogsConfig
}

// LogsConfig controls how pod logs are collected (API vs node-local DaemonSet).
type LogsConfig struct {
	Backend           string // api | nodelocal
	PodLogsRoot       string
	NodeName          string
	NodeHTTPPort      int
	NodeAgentSelector string
}

type KafkaConfig struct {
	Brokers                 []string
	RequestTopic            string
	ConsumerGroup           string
	EventsTopic             string
	LogsStreamTopic         string
	HealthTopic             string
	LifecycleTopic          string
	CommitOnReceive         bool
	SecurityMode            string
	TLSRequired             bool
	ClientPrincipal         string
	TLS                     TLSConfig
	Mode                    string // json | protobuf | dual
	InRequestTopicTemplate  string
	OutResponseTopicTemplate string
	OutRequestTopic         string
	EventsWatcherTopic      string
	MetricsWatcherTopic     string
	ShadowConsumerGroup     string
	ResponseTimeoutMs       int
	HeartbeatTimeoutMs      int
}

type TLSConfig struct {
	Enabled  bool
	CAFile   string
	CertFile string
	KeyFile  string
}

type S3Config struct {
	Endpoint       string
	Region         string
	ForcePathStyle bool
}

type AgentConfig struct {
	Component               string
	Implementation          string // v1 | v2
	AgentServiceURL         string
	InternalHTTPPort        int
	LeaderLeaseName         string
	LeaderElection          bool
	LeaderOnlyCommands      bool
	SyncWorkerConcurrency   int
	LogsCollectMaxJobs      int
	LogsCollectMaxBytes     int64
	LogStreamMaxPerPod      int
	LogStreamBacklogMax     int
	HealthMaxPodsPerMessage int
	K8sAPIQPS               float32
	K8sAPIBurst             int
	InstanceID              string
	RegistrationEnabled     bool
	RegistrationDelayMs     int
}

type HTTPConfig struct {
	Port                int
	BearerToken         string
	InternalBearerToken string
}

type PolicyConfig struct {
	NamespacesFile string
	PolicyFile     string
	FeaturesFile   string
}

// Load reads configuration from environment with sensible defaults.
func Load() (*Config, error) {
	brokers := splitCSV(env("KAFKA_BROKERS", "localhost:9092"))
	component := env("AGENT_COMPONENT", ComponentAll)
	if len(brokers) == 0 && component != ComponentLogsNode {
		return nil, fmt.Errorf("KAFKA_BROKERS must not be empty")
	}

	httpPort, err := strconv.Atoi(env("HTTP_PORT", strconv.Itoa(DefaultHTTPPort)))
	if err != nil {
		return nil, fmt.Errorf("HTTP_PORT: %w", err)
	}

	internalHTTPPort, err := internalHTTPPortFromEnv()
	if err != nil {
		return nil, err
	}

	syncConcurrency, err := strconv.Atoi(env("SYNC_WORKER_CONCURRENCY", strconv.Itoa(DefaultSyncWorkerConcurrency)))
	if err != nil {
		return nil, fmt.Errorf("SYNC_WORKER_CONCURRENCY: %w", err)
	}

	logsMaxJobs, err := strconv.Atoi(env("LOGS_COLLECT_MAX_JOBS", strconv.Itoa(DefaultLogsCollectMaxJobs)))
	if err != nil {
		return nil, fmt.Errorf("LOGS_COLLECT_MAX_JOBS: %w", err)
	}

	logsMaxBytes, err := strconv.ParseInt(env("LOGS_COLLECT_MAX_BYTES", strconv.FormatInt(DefaultLogsCollectMaxBytes, 10)), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("LOGS_COLLECT_MAX_BYTES: %w", err)
	}

	instanceID := env("AGENT_INSTANCE_ID", "")
	if instanceID == "" {
		if host, _ := os.Hostname(); host != "" {
			instanceID = host
		} else {
			instanceID = "unknown"
		}
	}

	cfg := &Config{
		ClusterID: env("CLUSTER_ID", "local"),
		Kafka: KafkaConfig{
			Brokers:         brokers,
			RequestTopic:    env("KAFKA_REQUEST_TOPIC", DefaultRequestTopic),
			ConsumerGroup:   env("KAFKA_CONSUMER_GROUP", DefaultConsumerGroup),
			EventsTopic:     env("KAFKA_EVENTS_TOPIC", "cluster.events"),
			LogsStreamTopic: env("KAFKA_LOGS_STREAM_TOPIC", "logs.stream"),
			HealthTopic:     env("KAFKA_HEALTH_TOPIC", "cluster.health"),
			LifecycleTopic:  env("KAFKA_LIFECYCLE_TOPIC", "agent.lifecycle"),
			CommitOnReceive: envBool("KAFKA_COMMIT_ON_RECEIVE", false),
			SecurityMode:    strings.ToLower(env("KAFKA_SECURITY_MODE", "")),
			TLSRequired:     envBool("KAFKA_TLS_REQUIRED", false),
			ClientPrincipal: env("KAFKA_TLS_CLIENT_PRINCIPAL", ""),
			TLS: TLSConfig{
				Enabled:  envBool("KAFKA_TLS_ENABLED", false),
				CAFile:   env("KAFKA_TLS_CA_FILE", ""),
				CertFile: env("KAFKA_TLS_CERT_FILE", ""),
				KeyFile:  env("KAFKA_TLS_KEY_FILE", ""),
			},
			Mode:                     strings.ToLower(env("KAFKA_MODE", KafkaModeJSON)),
			InRequestTopicTemplate:   env("KAFKA_IN_REQUEST_TOPIC_TEMPLATE", "uamc-core.ssl.request.{cluster-id}-{uamc-agent}"),
			OutResponseTopicTemplate: env("KAFKA_OUT_RESPONSE_TOPIC_TEMPLATE", "uamc-agent.ssl.response.{cluster-id}-{uamc-agent}"),
			OutRequestTopic:          env("KAFKA_OUT_REQUEST_TOPIC", "uamc-agent.ssl.request"),
			EventsWatcherTopic:       env("KAFKA_EVENTS_WATCHER_TOPIC", "uamc-events-watcher.ssl.request"),
			MetricsWatcherTopic:      env("KAFKA_METRICS_WATCHER_TOPIC", "uamc-metrics-watcher.ssl.request"),
			ShadowConsumerGroup:      env("KAFKA_SHADOW_CONSUMER_GROUP", ""),
			ResponseTimeoutMs:        intFromEnv("KAFKA_RESPONSE_TIMEOUT_MS", 10000),
			HeartbeatTimeoutMs:       intFromEnv("KAFKA_HEARTBEAT_TIMEOUT_MS", 2000),
		},
		S3: S3Config{
			Endpoint:       env("S3_ENDPOINT", ""),
			Region:         env("S3_REGION", "us-east-1"),
			ForcePathStyle: envBool("S3_FORCE_PATH_STYLE", false),
		},
		Agent: AgentConfig{
			Component:               env("AGENT_COMPONENT", ComponentAll),
			Implementation:          strings.ToLower(env("AGENT_IMPLEMENTATION", ImplementationV1)),
			AgentServiceURL:         env("AGENT_SERVICE_URL", ""),
			InternalHTTPPort:        internalHTTPPort,
			LeaderLeaseName:         env("LEADER_LEASE_NAME", ""),
			LeaderElection:          envBool("LEADER_ELECTION_ENABLED", true),
			LeaderOnlyCommands:      envBool("LEADER_ONLY_COMMANDS", true),
			SyncWorkerConcurrency:   syncConcurrency,
			LogsCollectMaxJobs:      logsMaxJobs,
			LogsCollectMaxBytes:     logsMaxBytes,
			LogStreamMaxPerPod:      intFromEnv("LOG_STREAM_MAX_PER_POD", 1),
			LogStreamBacklogMax:     intFromEnv("LOG_STREAM_BACKLOG_MAX", 1000),
			HealthMaxPodsPerMessage: intFromEnv("HEALTH_MAX_PODS_PER_MESSAGE", 500),
			K8sAPIQPS:               float32FromEnv("K8S_API_QPS", 50),
			K8sAPIBurst:             intFromEnv("K8S_API_BURST", 100),
			InstanceID:              instanceID,
			RegistrationEnabled:     envBool("AGENT_REGISTRATION_ENABLED", false),
			RegistrationDelayMs:     intFromEnv("AGENT_REGISTRATION_DELAY_MS", 1000),
		},
		HTTP: HTTPConfig{
			Port:                httpPort,
			BearerToken:         env("HTTP_BEARER_TOKEN", ""),
			InternalBearerToken: env("HTTP_INTERNAL_BEARER_TOKEN", ""),
		},
		Policy: PolicyConfig{
			NamespacesFile: env("POLICY_NAMESPACES_FILE", "/etc/k8s-agent/policy/namespaces.yaml"),
			PolicyFile:     env("POLICY_FILE", "/etc/k8s-agent/policy/policy.yaml"),
			FeaturesFile:   env("FEATURES_FILE", "/etc/k8s-agent/policy/features.yaml"),
		},
		Logs: LogsConfig{
			Backend:           strings.ToLower(env("LOGS_BACKEND", LogsBackendAPI)),
			PodLogsRoot:       env("POD_LOGS_ROOT", DefaultPodLogsRoot),
			NodeName:          env("NODE_NAME", ""),
			NodeHTTPPort:      intFromEnv("LOGS_NODE_HTTP_PORT", DefaultLogsNodeHTTPPort),
			NodeAgentSelector: env("LOGS_NODE_AGENT_SELECTOR", "app.kubernetes.io/component=logs-node-agent"),
		},
	}

	if cfg.Agent.SyncWorkerConcurrency < 1 {
		return nil, fmt.Errorf("SYNC_WORKER_CONCURRENCY must be >= 1")
	}

	switch cfg.Kafka.Mode {
	case KafkaModeJSON, KafkaModeProtobuf, KafkaModeDual, "":
		if cfg.Kafka.Mode == "" {
			cfg.Kafka.Mode = KafkaModeJSON
		}
	default:
		return nil, fmt.Errorf("KAFKA_MODE must be one of: json, protobuf, dual")
	}

	switch cfg.Agent.Implementation {
	case ImplementationV1, ImplementationV2, "":
		if cfg.Agent.Implementation == "" {
			cfg.Agent.Implementation = ImplementationV1
		}
	default:
		return nil, fmt.Errorf("AGENT_IMPLEMENTATION must be one of: v1, v2")
	}

	switch cfg.Logs.Backend {
	case LogsBackendAPI, LogsBackendNodeLocal, "":
		if cfg.Logs.Backend == "" {
			cfg.Logs.Backend = LogsBackendAPI
		}
	default:
		return nil, fmt.Errorf("LOGS_BACKEND must be one of: api, nodelocal")
	}

	// v2 implies nodelocal unless explicitly overridden; v1 defaults to api.
	if cfg.Agent.Implementation == ImplementationV2 && env("LOGS_BACKEND", "") == "" {
		cfg.Logs.Backend = LogsBackendNodeLocal
	}

	if err := cfg.validateComponent(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// ResolveTopic replaces {cluster-id} and {uamc-agent} placeholders.
func ResolveTopic(template, clusterID string) string {
	r := strings.NewReplacer(
		"{cluster-id}", clusterID,
		"{uamc-agent}", "uamc-agent",
	)
	return r.Replace(template)
}

func (c *Config) validateComponent() error {
	switch c.Agent.Component {
	case ComponentAll, ComponentIngress, ComponentEgress, ComponentAgentService, ComponentLogsNode:
	default:
		return fmt.Errorf("AGENT_COMPONENT must be one of: all, ingress, egress, agent-service, logs-node")
	}
	if c.Agent.Component == ComponentIngress || c.Agent.Component == ComponentEgress {
		if c.Agent.AgentServiceURL == "" {
			return fmt.Errorf("AGENT_SERVICE_URL is required for component %q", c.Agent.Component)
		}
	}
	if c.Agent.Component == ComponentAgentService || c.Agent.Component == ComponentEgress || c.Agent.Component == ComponentLogsNode {
		if c.HTTP.InternalBearerToken == "" {
			return fmt.Errorf("HTTP_INTERNAL_BEARER_TOKEN is required for component %q", c.Agent.Component)
		}
	}
	if c.Agent.Component == ComponentLogsNode {
		if c.Logs.PodLogsRoot == "" {
			return fmt.Errorf("POD_LOGS_ROOT is required for component logs-node")
		}
	}
	if c.Logs.Backend == LogsBackendNodeLocal && (c.Agent.Component == ComponentAgentService || c.Agent.Component == ComponentAll) {
		if c.Logs.NodeHTTPPort < 1 {
			return fmt.Errorf("LOGS_NODE_HTTP_PORT must be >= 1 for nodelocal backend")
		}
	}
	if c.Agent.Component == ComponentEgress && c.Kafka.TLSRequired {
		if kafkaSecurityMode(c.Kafka) != "mtls" {
			return fmt.Errorf("egress with KAFKA_TLS_REQUIRED must use KAFKA_SECURITY_MODE=mtls (Kafka ACL client identity)")
		}
	}
	return nil
}

func kafkaSecurityMode(k KafkaConfig) string {
	if k.SecurityMode != "" {
		return k.SecurityMode
	}
	if !k.TLS.Enabled {
		return "plaintext"
	}
	if k.TLS.CertFile != "" && k.TLS.KeyFile != "" {
		return "mtls"
	}
	return "tls"
}

func internalHTTPPortFromEnv() (int, error) {
	httpPort, err := strconv.Atoi(env("INTERNAL_HTTP_PORT", strconv.Itoa(DefaultInternalHTTPPort)))
	if err != nil {
		return 0, fmt.Errorf("INTERNAL_HTTP_PORT: %w", err)
	}
	return httpPort, nil
}

// ShutdownGracePeriod returns time to wait for in-flight work on shutdown.
func (c *Config) ShutdownGracePeriod() time.Duration {
	return 30 * time.Second
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func intFromEnv(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func float32FromEnv(key string, fallback float32) float32 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(v, 32)
	if err != nil {
		return fallback
	}
	return float32(n)
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
