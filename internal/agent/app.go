package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/usmc/usmc-k8s-agent/internal/cache"
	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/coreclient"
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
	"github.com/usmc/usmc-k8s-agent/internal/modules"
	"github.com/usmc/usmc-k8s-agent/internal/modules/ailogs"
	"github.com/usmc/usmc-k8s-agent/internal/modules/cmdmods"
	"github.com/usmc/usmc-k8s-agent/internal/modules/eventswatcher"
	"github.com/usmc/usmc-k8s-agent/internal/modules/kubeapi"
	"github.com/usmc/usmc-k8s-agent/internal/modules/loglevel"
	"github.com/usmc/usmc-k8s-agent/internal/modules/metricswatcher"
	"github.com/usmc/usmc-k8s-agent/internal/modules/ottlogstrue"
	"github.com/usmc/usmc-k8s-agent/internal/nodeagent"
	"github.com/usmc/usmc-k8s-agent/internal/nodelocal"
	"github.com/usmc/usmc-k8s-agent/internal/observability"
	"github.com/usmc/usmc-k8s-agent/internal/policy"
	"github.com/usmc/usmc-k8s-agent/internal/protodispatch"
	"github.com/usmc/usmc-k8s-agent/internal/registration"
	s3client "github.com/usmc/usmc-k8s-agent/internal/s3"
	"github.com/usmc/usmc-k8s-agent/internal/watch"
	"time"
)

// App wires all agent modules.
type App struct {
	cfg          *config.Config
	log          *slog.Logger
	state        *observability.RuntimeState
	metrics      *observability.Metrics
	k8sClient    *k8s.Client
	policy       *policy.Engine
	featReg      *features.Registry
	modReg       *modules.Registry
	cacheStore   *cache.Store
	watchMgr     *watch.Manager
	streamMgr    *logstream.Manager
	healthMgr    *healthreport.Manager
	router       *command.Router
	consumer     *kafka.Consumer
	publisher    *kafka.Publisher
	processor    *kafka.Processor
	commandGuard *kafka.CommandGuard
	http         *httpapi.Server
	podNamespace string
	coreClient   *coreclient.Client
	protoDispatch *protodispatch.Dispatcher
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
	if opts.Config.Agent.Component == config.ComponentLogsNode {
		return newLogsNodeApp(opts, log)
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

	podNamespace := envOrDefault("POD_NAMESPACE", "uamc-agent")
	if opts.Config.Logs.Backend == config.LogsBackendNodeLocal {
		nodeClient := &nodelocal.NodeClient{
			Kube:      kube,
			Namespace: podNamespace,
			Selector:  opts.Config.Logs.NodeAgentSelector,
			Port:      opts.Config.Logs.NodeHTTPPort,
			Token:     opts.Config.HTTP.InternalBearerToken,
		}
		collector.WithNodeLocal(nodeClient)
		streamMgr.WithNodeLocal(nodeClient)
		log.Info("logs backend nodelocal enabled",
			"implementation", opts.Config.Agent.Implementation,
			"selector", opts.Config.Logs.NodeAgentSelector,
			"port", opts.Config.Logs.NodeHTTPPort,
		)
	}
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

	apiMod := cmdmods.NewHandlerModule("api",
		[]string{"cluster_inventory", "workload_manage", "istio_manage", "rbac_inspect"},
		[]string{command.TypeK8sAPI},
		apihandler.NewHandler(k8sClient, engine, k8s.DefaultTrimOptions()),
	)
	logsMod := cmdmods.NewHandlerModule("logs",
		[]string{"logs_collect"},
		[]string{command.TypeLogsCollect},
		logsHandler,
	)
	watchMod := cmdmods.NewHandlerModule("watch",
		[]string{"watch_events"},
		[]string{command.TypeWatchSubscribe, command.TypeWatchUnsubscribe},
		watchhandler.NewSubscribeHandler(watchMgr, opts.Config.Kafka.EventsTopic),
		watchhandler.NewUnsubscribeHandler(watchMgr),
	)
	cacheMod := cmdmods.NewHandlerModule("cache",
		[]string{"cache"},
		[]string{command.TypeCachePut, command.TypeCacheDelete, command.TypeCacheClear},
		cachehandler.NewPutHandler(cacheStore, engine, metrics),
		cachehandler.NewDeleteHandler(cacheStore, engine, metrics),
		cachehandler.NewClearHandler(cacheStore, engine, metrics),
	)
	logstreamMod := cmdmods.NewHandlerModule("logstream",
		[]string{"logs_stream"},
		[]string{command.TypeLogsStreamStart, command.TypeLogsStreamStop},
		logstreamhandler.NewStartHandler(streamMgr, opts.Config.Kafka.LogsStreamTopic),
		logstreamhandler.NewStopHandler(streamMgr),
	)
	healthMod := cmdmods.NewHandlerModule("health",
		[]string{"health_report"},
		[]string{command.TypeHealthReportStart, command.TypeHealthReportStop},
		healthhandler.NewStartHandler(healthMgr, opts.Config.Kafka.HealthTopic),
		healthhandler.NewStopHandler(healthMgr),
	)

	modReg := modules.NewRegistry(log, apiMod, logsMod, watchMod, cacheMod, logstreamMod, healthMod)

	coreClient := coreclient.New(
		publisher,
		opts.Config.ClusterID,
		opts.Config.Kafka.OutRequestTopic,
		time.Duration(opts.Config.Kafka.ResponseTimeoutMs)*time.Millisecond,
		log,
	)
	protoDisp := protodispatch.New(log)
	regMod := registration.New(opts.Config, coreClient, modReg.Names(), log)
	modReg.Register(regMod)
	modReg.Register(eventswatcher.NewBridge(opts.Config, coreClient, log))
	modReg.Register(metricswatcher.New(opts.Config, coreClient, kube, log))
	modReg.Register(loglevel.New(opts.Config, protoDisp, log))
	modReg.Register(kubeapi.New(opts.Config, k8sClient.RestConfig(), envOrDefault("KUBEAPI_LISTEN_ADDR", ":8082"), log))
	modReg.Register(ailogs.New(logsHandler, log))
	modReg.Register(ottlogstrue.New(opts.Config, coreClient, log))

	handlers := cmdmods.FilterHandlers(featReg, modReg.Handlers(opts.Config, featReg))
	router := command.NewRouter(handlers...)

	commandGuard := kafka.NewCommandGuard(engine)
	state := observability.NewRuntimeState()

	var httpRouter *command.Router
	if opts.Config.Agent.Component == config.ComponentAgentService {
		httpRouter = router
	}
	httpServer := httpapi.NewServer(opts.Config.HTTP, state, cacheStore, log, httpRouter)

	return &App{
		cfg:           opts.Config,
		log:           log,
		state:         state,
		metrics:       metrics,
		k8sClient:     k8sClient,
		policy:        engine,
		featReg:       featReg,
		modReg:        modReg,
		cacheStore:    cacheStore,
		watchMgr:      watchMgr,
		streamMgr:     streamMgr,
		healthMgr:     healthMgr,
		router:        router,
		publisher:     publisher,
		commandGuard:  commandGuard,
		http:          httpServer,
		podNamespace:  podNamespace,
		coreClient:    coreClient,
		protoDispatch: protoDisp,
	}, nil
}

// Run starts the configured agent component.
func (a *App) Run(ctx context.Context, devNoLeader bool) error {
	if a.modReg != nil {
		if err := a.modReg.Start(ctx, a.cfg, a.featReg); err != nil {
			return err
		}
		defer func() { _ = a.modReg.Stop(context.Background()) }()
	}
	switch a.cfg.Agent.Component {
	case config.ComponentIngress:
		return a.runIngress(ctx)
	case config.ComponentEgress:
		return a.runEgress(ctx, devNoLeader)
	case config.ComponentAgentService:
		return a.runAgentService(ctx, devNoLeader)
	case config.ComponentLogsNode:
		return a.runLogsNode(ctx)
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
	if a.consumer != nil {
		_ = a.consumer.Close()
		a.consumer = nil
	}
	if a.publisher != nil {
		_ = a.publisher.Close()
	}
	if a.http != nil {
		return a.http.Shutdown(ctx)
	}
	return nil
}

// Router exposes the command router for tests.
func (a *App) Router() *command.Router {
	return a.router
}

// Modules exposes the module registry for tests.
func (a *App) Modules() *modules.Registry {
	return a.modReg
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func newLogsNodeApp(opts Options, log *slog.Logger) (*App, error) {
	engine := opts.Policy
	if engine == nil {
		var err error
		engine, _, err = policy.LoadFromFilesWithFeatures(
			opts.Config.Policy.PolicyFile,
			opts.Config.Policy.NamespacesFile,
			opts.Config.Policy.FeaturesFile,
		)
		if err != nil {
			// Policy is best-effort on node agent; allow all namespaces if files missing in unit tests.
			log.Warn("logs-node policy load failed; namespace checks disabled", "error", err)
			engine = nil
		}
	}
	state := observability.NewRuntimeState()
	state.SetLeader(true)
	state.SetKafkaConnected(true)
	state.SetAPIServerOK(true)
	return &App{
		cfg:   opts.Config,
		log:   log,
		state: state,
		policy: engine,
		podNamespace: envOrDefault("POD_NAMESPACE", "uamc-agent"),
	}, nil
}

func (a *App) runLogsNode(ctx context.Context) error {
	port := a.cfg.Logs.NodeHTTPPort
	if port < 1 {
		port = config.DefaultLogsNodeHTTPPort
	}
	reader := nodelocal.NewReader(a.cfg.Logs.PodLogsRoot)
	srv := nodeagent.NewServer(
		fmt.Sprintf(":%d", port),
		reader,
		a.policy,
		a.cfg.HTTP.InternalBearerToken,
		a.cfg.Logs.NodeName,
		a.log,
	)
	if err := srv.Start(); err != nil {
		return err
	}
	a.log.Info("logs-node agent ready",
		"implementation", a.cfg.Agent.Implementation,
		"node", a.cfg.Logs.NodeName,
		"root", a.cfg.Logs.PodLogsRoot,
		"port", port,
	)
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownGracePeriod())
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
