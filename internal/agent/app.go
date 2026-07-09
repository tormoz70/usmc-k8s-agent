package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/usmc/usmc-k8s-agent/internal/cache"
	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/features"
	apihandler "github.com/usmc/usmc-k8s-agent/internal/handlers/api"
	cachehandler "github.com/usmc/usmc-k8s-agent/internal/handlers/cache"
	healthhandler "github.com/usmc/usmc-k8s-agent/internal/handlers/health"
	loghandler "github.com/usmc/usmc-k8s-agent/internal/handlers/logs"
	logstreamhandler "github.com/usmc/usmc-k8s-agent/internal/handlers/logstream"
	watchhandler "github.com/usmc/usmc-k8s-agent/internal/handlers/watch"
	"github.com/usmc/usmc-k8s-agent/internal/healthreport"
	"github.com/usmc/usmc-k8s-agent/internal/httpapi"
	"github.com/usmc/usmc-k8s-agent/internal/k8s"
	"github.com/usmc/usmc-k8s-agent/internal/kafka"
	"github.com/usmc/usmc-k8s-agent/internal/leaderelection"
	"github.com/usmc/usmc-k8s-agent/internal/logstream"
	"github.com/usmc/usmc-k8s-agent/internal/observability"
	"github.com/usmc/usmc-k8s-agent/internal/policy"
	s3client "github.com/usmc/usmc-k8s-agent/internal/s3"
	"github.com/usmc/usmc-k8s-agent/internal/watch"
)

// App wires all agent modules.
type App struct {
	cfg          *config.Config
	log          *slog.Logger
	state        *observability.RuntimeState
	metrics      *observability.Metrics
	k8sClient    *k8s.Client
	policy       *policy.Engine
	cacheStore   *cache.Store
	watchMgr     *watch.Manager
	streamMgr    *logstream.Manager
	healthMgr    *healthreport.Manager
	router       *command.Router
	consumer     *kafka.Consumer
	publisher    *kafka.Publisher
	processor    *kafka.Processor
	http         *httpapi.Server
	podNamespace string
}

// Options customizes application wiring.
type Options struct {
	Config      *config.Config
	Logger      *slog.Logger
	K8sClient   *k8s.Client
	Policy      *policy.Engine
	Metrics     *observability.Metrics
	DevNoLeader bool
}

// New builds the application graph.
func New(opts Options) (*App, error) {
	if opts.Config == nil {
		c, err := config.Load()
		if err != nil {
			return nil, err
		}
		opts.Config = c
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics = observability.DefaultMetrics()
	}

	var k8sClient *k8s.Client
	var err error
	if opts.K8sClient != nil {
		k8sClient = opts.K8sClient
	} else {
		k8sClient, err = k8s.NewInClusterWithRateLimit(opts.Config.Agent.K8sAPIQPS, opts.Config.Agent.K8sAPIBurst)
		if err != nil {
			return nil, fmt.Errorf("k8s client: %w", err)
		}
	}

	engine := opts.Policy
	var featReg *features.Registry
	if engine == nil {
		var err error
		engine, featReg, err = policy.LoadFromFilesWithFeatures(
			opts.Config.Policy.PolicyFile,
			opts.Config.Policy.NamespacesFile,
			opts.Config.Policy.FeaturesFile,
		)
		if err != nil {
			return nil, fmt.Errorf("policy: %w", err)
		}
	} else {
		featReg = nil // tests: all handlers enabled when policy injected externally
	}

	kube, err := k8sClient.Kubernetes()
	if err != nil {
		return nil, fmt.Errorf("kubernetes clientset: %w", err)
	}

	dynamicBundle, err := k8s.NewDynamicBundle(k8sClient.RestConfig())
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}

	s3 := s3client.NewClient(opts.Config.S3.Endpoint, opts.Config.S3.Region, opts.Config.S3.ForcePathStyle)
	tempRoot := envOrDefault("LOGS_TEMP_DIR", os.TempDir())
	collector := loghandler.NewCollector(
		kube,
		s3,
		opts.Config.S3,
		opts.Config.Agent.LogsCollectMaxBytes,
		tempRoot,
	)

	consumer, err := kafka.NewConsumer(opts.Config.Kafka, log)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	publisher, err := kafka.NewPublisher(opts.Config.Kafka, log)
	if err != nil {
		return nil, fmt.Errorf("kafka publisher: %w", err)
	}
	watchPublisher := watch.NewKafkaPublisher(publisher)
	watchMgr := watch.NewManager(
		opts.Config.ClusterID,
		opts.Config.Kafka.EventsTopic,
		dynamicBundle,
		engine,
		watchPublisher,
		log,
	)

	cacheStore := cache.NewStore()
	streamMgr := logstream.NewManager(
		opts.Config.ClusterID,
		opts.Config.Kafka.LogsStreamTopic,
		kube,
		engine,
		publisher,
		opts.Config.Agent.LogStreamMaxPerPod,
		opts.Config.Agent.LogStreamBacklogMax,
		metrics,
		log,
	)
	healthMgr := healthreport.NewManager(
		opts.Config.ClusterID,
		opts.Config.Kafka.HealthTopic,
		kube,
		engine,
		publisher,
		opts.Config.Agent.HealthMaxPodsPerMessage,
		log,
	)

	logsHandler := loghandler.NewHandler(collector, engine, publisher, opts.Config.Agent.LogsCollectMaxJobs, log)
	logsHandler.SetMetrics(metrics)

	var handlers []command.Handler
	if featReg.CommandEnabled(command.TypeK8sAPI) {
		handlers = append(handlers, apihandler.NewHandler(k8sClient, engine, k8s.DefaultTrimOptions()))
	}
	if featReg.CommandEnabled(command.TypeLogsCollect) {
		handlers = append(handlers, logsHandler)
	}
	if featReg.CommandEnabled(command.TypeWatchSubscribe) {
		handlers = append(handlers, watchhandler.NewSubscribeHandler(watchMgr, opts.Config.Kafka.EventsTopic))
	}
	if featReg.CommandEnabled(command.TypeWatchUnsubscribe) {
		handlers = append(handlers, watchhandler.NewUnsubscribeHandler(watchMgr))
	}
	if featReg.CommandEnabled(command.TypeCachePut) {
		handlers = append(handlers, cachehandler.NewPutHandler(cacheStore, engine, metrics))
	}
	if featReg.CommandEnabled(command.TypeCacheDelete) {
		handlers = append(handlers, cachehandler.NewDeleteHandler(cacheStore, engine, metrics))
	}
	if featReg.CommandEnabled(command.TypeCacheClear) {
		handlers = append(handlers, cachehandler.NewClearHandler(cacheStore, engine, metrics))
	}
	if featReg.CommandEnabled(command.TypeLogsStreamStart) {
		handlers = append(handlers, logstreamhandler.NewStartHandler(streamMgr, opts.Config.Kafka.LogsStreamTopic))
	}
	if featReg.CommandEnabled(command.TypeLogsStreamStop) {
		handlers = append(handlers, logstreamhandler.NewStopHandler(streamMgr))
	}
	if featReg.CommandEnabled(command.TypeHealthReportStart) {
		handlers = append(handlers, healthhandler.NewStartHandler(healthMgr, opts.Config.Kafka.HealthTopic))
	}
	if featReg.CommandEnabled(command.TypeHealthReportStop) {
		handlers = append(handlers, healthhandler.NewStopHandler(healthMgr))
	}
	router := command.NewRouter(handlers...)

	commandGuard := kafka.NewCommandGuard(engine)
	processor := kafka.NewProcessor(consumer, publisher, router, commandGuard, opts.Config.Kafka.CommitOnReceive, metrics, log)
	state := observability.NewRuntimeState()

	var httpRouter *command.Router
	if opts.Config.Agent.Component == config.ComponentAgentService {
		httpRouter = router
	}
	httpServer := httpapi.NewServer(opts.Config.HTTP, state, cacheStore, log, httpRouter)

	return &App{
		cfg:          opts.Config,
		log:          log,
		state:        state,
		metrics:      metrics,
		k8sClient:    k8sClient,
		policy:       engine,
		cacheStore:   cacheStore,
		watchMgr:     watchMgr,
		streamMgr:    streamMgr,
		healthMgr:    healthMgr,
		router:       router,
		consumer:     consumer,
		publisher:    publisher,
		processor:    processor,
		http:         httpServer,
		podNamespace: envOrDefault("POD_NAMESPACE", "uamc-agent"),
	}, nil
}

// Run starts the configured agent component.
func (a *App) Run(ctx context.Context, devNoLeader bool) error {
	switch a.cfg.Agent.Component {
	case config.ComponentIngress:
		return a.runIngress(ctx)
	case config.ComponentEgress:
		return a.runEgress(ctx, devNoLeader)
	case config.ComponentAgentService:
		return a.runAgentService(ctx, devNoLeader)
	default:
		return a.runAll(ctx, devNoLeader)
	}
}

func (a *App) stopSubscriptions() {
	a.watchMgr.StopAll()
	a.streamMgr.StopAll()
	a.healthMgr.StopAll()
	a.syncSubscriptionMetrics()
}

func (a *App) syncSubscriptionMetrics() {
	if a.metrics == nil {
		return
	}
	a.metrics.WatchSubscriptions.Set(float64(a.watchMgr.ActiveCount()))
	a.metrics.LogStreamsActive.Set(float64(a.streamMgr.ActiveCount()))
	a.metrics.HealthReportsActive.Set(float64(a.healthMgr.ActiveCount()))
	a.metrics.CacheEntries.Set(float64(a.cacheStore.Len()))
}

func (a *App) patchLeaderLabel(ctx context.Context, leader bool) {
	kube, err := a.k8sClient.Kubernetes()
	if err != nil {
		a.log.Warn("leader label patch skipped", "error", err)
		return
	}
	if err := leaderelection.PatchLeaderLabel(ctx, kube, a.podNamespace, a.cfg.Agent.InstanceID, leader); err != nil {
		a.log.Warn("leader label patch failed", "error", err, "leader", leader)
	}
}

func (a *App) shutdown(ctx context.Context) error {
	a.state.SetKafkaConnected(false)
	a.metrics.SyncFromState(a.state)
	a.stopSubscriptions()
	_ = a.consumer.Close()
	_ = a.publisher.Close()
	if a.http != nil {
		return a.http.Shutdown(ctx)
	}
	return nil
}

// Router exposes the command router for tests.
func (a *App) Router() *command.Router {
	return a.router
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
