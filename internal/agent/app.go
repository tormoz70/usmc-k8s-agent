package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/cache"
	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/config"
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
	"github.com/usmc/usmc-k8s-agent/internal/lifecycle"
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
	if engine == nil {
		engine, err = policy.LoadFromFiles(opts.Config.Policy.PolicyFile, opts.Config.Policy.NamespacesFile)
		if err != nil {
			return nil, fmt.Errorf("policy: %w", err)
		}
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

	consumer := kafka.NewConsumer(opts.Config.Kafka, log)
	publisher := kafka.NewPublisher(opts.Config.Kafka, log)
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

	router := command.NewRouter(
		apihandler.NewHandler(k8sClient, engine, k8s.DefaultTrimOptions()),
		logsHandler,
		watchhandler.NewSubscribeHandler(watchMgr),
		watchhandler.NewUnsubscribeHandler(watchMgr),
		cachehandler.NewPutHandler(cacheStore, engine, metrics),
		cachehandler.NewDeleteHandler(cacheStore, engine, metrics),
		cachehandler.NewClearHandler(cacheStore, engine, metrics),
		logstreamhandler.NewStartHandler(streamMgr, opts.Config.Kafka.LogsStreamTopic),
		logstreamhandler.NewStopHandler(streamMgr),
		healthhandler.NewStartHandler(healthMgr, opts.Config.Kafka.HealthTopic),
		healthhandler.NewStopHandler(healthMgr),
	)

	processor := kafka.NewProcessor(consumer, publisher, router, opts.Config.Kafka.CommitOnReceive, metrics, log)
	state := observability.NewRuntimeState()
	httpServer := httpapi.NewServer(opts.Config.HTTP, state, cacheStore, log)

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
		podNamespace: envOrDefault("POD_NAMESPACE", "k8s-agent"),
	}, nil
}

// Run starts HTTP server and command processing loop.
func (a *App) Run(ctx context.Context, devNoLeader bool) error {
	if err := a.http.Start(); err != nil {
		return err
	}
	a.state.SetKafkaConnected(true)
	a.metrics.SyncFromState(a.state)

	if err := a.k8sClient.Ping(ctx); err != nil {
		a.log.Warn("apiserver ping failed", "error", err)
	} else {
		a.state.SetAPIServerOK(true)
		a.metrics.SyncFromState(a.state)
	}

	runCommands := func(ctx context.Context) {
		a.state.SetLeader(true)
		a.metrics.SyncFromState(a.state)
		defer func() {
			a.state.SetLeader(false)
			a.metrics.SyncFromState(a.state)
		}()
		defer a.stopSubscriptions()

		a.patchLeaderLabel(ctx, true)
		defer a.patchLeaderLabel(context.Background(), false)

		lc := lifecycle.NewPublisher(a.cfg.Kafka.LifecycleTopic, a.publisher.PublishRaw)
		if err := lc.PublishStarted(ctx, a.cfg.ClusterID, a.cfg.Agent.InstanceID, true); err != nil {
			a.log.Warn("publish agent.lifecycle failed", "error", err)
		}
		defer func() {
			pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := lc.PublishLeaderLost(pubCtx, a.cfg.ClusterID, a.cfg.Agent.InstanceID); err != nil {
				a.log.Warn("publish agent.lifecycle leader lost failed", "error", err)
			}
		}()

		a.log.Info("command processor started", "topic", a.cfg.Kafka.RequestTopic)
		if err := a.processor.Run(ctx); err != nil && ctx.Err() == nil {
			a.log.Error("command processor stopped", "error", err)
		}
	}

	if devNoLeader || !a.cfg.Agent.LeaderElection {
		go runCommands(ctx)
		<-ctx.Done()
		return a.shutdown(context.Background())
	}

	kube, err := a.k8sClient.Kubernetes()
	if err != nil {
		return err
	}

	err = leaderelection.Run(ctx, kube, leaderelection.Config{
		Identity: a.cfg.Agent.InstanceID,
	}, func(ctx context.Context) {
		a.state.SetLeader(true)
		a.metrics.SyncFromState(a.state)
	}, func(context.Context) {
		a.state.SetLeader(false)
		a.metrics.SyncFromState(a.state)
	}, runCommands, a.log)

	shutdownErr := a.shutdown(context.Background())
	if err != nil {
		return err
	}
	return shutdownErr
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
	return a.http.Shutdown(ctx)
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
