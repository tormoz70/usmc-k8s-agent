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
	DefaultRequestTopic          = "k8s.commands.request"
	DefaultConsumerGroup         = "k8s-agent"
	DefaultLogsCollectMaxJobs    = 20
	DefaultLogsCollectMaxBytes   = 524288000 // 500 MiB
	DefaultSyncWorkerConcurrency = 1
)

// Config holds agent runtime configuration loaded from environment variables.
type Config struct {
	ClusterID string

	Kafka KafkaConfig
	S3    S3Config
	Agent AgentConfig
	HTTP  HTTPConfig
	Policy PolicyConfig
}

type KafkaConfig struct {
	Brokers           []string
	RequestTopic      string
	ConsumerGroup     string
	EventsTopic       string
	LogsStreamTopic   string
	HealthTopic       string
	LifecycleTopic    string
	CommitOnReceive   bool
	TLS               TLSConfig
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
}

type HTTPConfig struct {
	Port        int
	BearerToken string
}

type PolicyConfig struct {
	NamespacesFile string
	PolicyFile     string
}

// Load reads configuration from environment with sensible defaults.
func Load() (*Config, error) {
	brokers := splitCSV(env("KAFKA_BROKERS", "localhost:9092"))
	if len(brokers) == 0 {
		return nil, fmt.Errorf("KAFKA_BROKERS must not be empty")
	}

	httpPort, err := strconv.Atoi(env("HTTP_PORT", strconv.Itoa(DefaultHTTPPort)))
	if err != nil {
		return nil, fmt.Errorf("HTTP_PORT: %w", err)
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
			CommitOnReceive: envBool("KAFKA_COMMIT_ON_RECEIVE", true),
			TLS: TLSConfig{
				Enabled:  envBool("KAFKA_TLS_ENABLED", false),
				CAFile:   env("KAFKA_TLS_CA_FILE", ""),
				CertFile: env("KAFKA_TLS_CERT_FILE", ""),
				KeyFile:  env("KAFKA_TLS_KEY_FILE", ""),
			},
		},
		S3: S3Config{
			Endpoint:       env("S3_ENDPOINT", ""),
			Region:         env("S3_REGION", "us-east-1"),
			ForcePathStyle: envBool("S3_FORCE_PATH_STYLE", false),
		},
		Agent: AgentConfig{
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
		},
		HTTP: HTTPConfig{
			Port:        httpPort,
			BearerToken: env("HTTP_BEARER_TOKEN", ""),
		},
		Policy: PolicyConfig{
			NamespacesFile: env("POLICY_NAMESPACES_FILE", "/etc/k8s-agent/policy/namespaces.yaml"),
			PolicyFile:     env("POLICY_FILE", "/etc/k8s-agent/policy/policy.yaml"),
		},
	}

	if cfg.Agent.SyncWorkerConcurrency < 1 {
		return nil, fmt.Errorf("SYNC_WORKER_CONCURRENCY must be >= 1")
	}

	return cfg, nil
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
