package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/usmc/k8s-agent/internal/command"
	"github.com/usmc/k8s-agent/internal/config"
	"github.com/usmc/k8s-agent/internal/handlers/file"
	loghandler "github.com/usmc/k8s-agent/internal/handlers/logs"
	"github.com/usmc/k8s-agent/internal/handlers/resource"
	watchhandler "github.com/usmc/k8s-agent/internal/handlers/watch"
	"github.com/usmc/k8s-agent/internal/health"
	"github.com/usmc/k8s-agent/internal/k8s"
	"github.com/usmc/k8s-agent/internal/kafka"
	"github.com/usmc/k8s-agent/internal/leader"
	"github.com/usmc/k8s-agent/internal/logstream"
	"github.com/usmc/k8s-agent/internal/metrics"
	"github.com/usmc/k8s-agent/internal/policy"
	"github.com/usmc/k8s-agent/internal/ratelimit"
	"github.com/usmc/k8s-agent/internal/result"
	"github.com/usmc/k8s-agent/internal/s3"
	"github.com/usmc/k8s-agent/internal/watchmanager"
)

type Service struct {
	cfg            *config.Config
	logger         *slog.Logger
	clients        *k8s.Clients
	policy         *policy.Engine
	router         *command.Router
	resultPub      *result.Publisher
	dlqProducer    *kafka.Producer
	consumer       *kafka.Consumer
	health         *health.Server
	watchManager   *watchmanager.Manager
	logStreamMgr   *logstream.Manager
	rateLimiter    *ratelimit.Limiter
	inFlight       sync.WaitGroup
	shutdownOnce   sync.Once
	isLeader       bool
}

type Options struct {
	Config  *config.Config
	Logger  *slog.Logger
	Clients *k8s.Clients
	Policy  *policy.Engine
}

func New(opts Options) (*Service, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Config == nil {
		return nil, fmt.Errorf("config is required")
	}
	if opts.Clients == nil {
		return nil, fmt.Errorf("k8s clients are required")
	}
	if opts.Policy == nil {
		p, err := policy.Load(opts.Config.PolicyFile)
		if err != nil {
			return nil, err
		}
		opts.Policy = policy.NewEngine(p)
	}

	resultProducer := kafka.NewProducer(opts.Config.KafkaBrokers, opts.Config.TopicCommandsOut)
	eventProducer := kafka.NewProducer(opts.Config.KafkaBrokers, opts.Config.TopicClusterEvents)
	dlqProducer := kafka.NewProducer(opts.Config.KafkaBrokers, opts.Config.TopicDLQ)

	resultPub := result.NewPublisher(resultProducer)
	eventPub := result.NewEventPublisher(eventProducer)

	lister := k8s.NewLister(opts.Clients)
	logReader := k8s.NewLogReader(opts.Clients)
	uploader := s3.NewUploader()

	watchMgr := watchmanager.NewManager(opts.Clients, eventPub, opts.Logger)
	logStreamMgr := logstream.NewManager(logReader, eventPub, opts.Logger)

	progressFn := func(res *command.Result) error {
		return resultPub.Publish(context.Background(), res)
	}

	router := command.NewRouter(
		resource.NewListHandler(lister, opts.Policy, opts.Config.InlineListMaxBytes),
		file.NewFetchHandler(lister, logReader, opts.Policy, uploader, opts.Config, progressFn),
		watchhandler.NewSubscribeHandler(watchMgr, opts.Policy),
		watchhandler.NewUnsubscribeHandler(watchMgr),
		loghandler.NewSubscribeHandler(logStreamMgr, opts.Policy),
		loghandler.NewUnsubscribeHandler(logStreamMgr),
	)

	s := &Service{
		cfg:          opts.Config,
		logger:       opts.Logger,
		clients:      opts.Clients,
		policy:       opts.Policy,
		router:       router,
		resultPub:    resultPub,
		dlqProducer:  dlqProducer,
		health:       health.New(),
		watchManager: watchMgr,
		logStreamMgr: logStreamMgr,
		rateLimiter:  ratelimit.New(50, 100),
	}

	s.consumer = kafka.NewConsumer(opts.Config.KafkaBrokers, opts.Config.TopicCommandsIn, opts.Config.ConsumerGroup, s.handleMessage, opts.Logger)
	return s, nil
}

func (s *Service) Health() *health.Server {
	return s.health
}

func (s *Service) Run(ctx context.Context) error {
	errCh := make(chan error, 2)

	if s.cfg.LeaderElectionEnabled {
		go func() {
			errCh <- leader.Run(ctx, s.clients.Kube, s.cfg.LeaderLeaseNamespace, s.cfg.LeaderLeaseName, leader.Callbacks{
				OnStartedLeading: func(lctx context.Context) {
					s.setLeader(true)
				},
				OnStoppedLeading: func() {
					s.setLeader(false)
				},
			}, s.logger)
		}()
	} else {
		s.setLeader(true)
	}

	go func() {
		errCh <- s.consumer.Run(ctx)
	}()

	select {
	case <-ctx.Done():
		s.gracefulShutdown()
		return nil
	case err := <-errCh:
		return err
	}
}

func (s *Service) setLeader(v bool) {
	s.isLeader = v
	s.health.SetLeader(v)
	s.watchManager.SetLeader(v)
	s.logStreamMgr.SetLeader(v)
	metrics.WatchSubscriptions.Set(float64(s.watchManager.ActiveCount()))
	metrics.LogStreamSubscriptions.Set(float64(s.logStreamMgr.ActiveCount()))
}

func (s *Service) handleMessage(ctx context.Context, msg kafkago.Message) error {
	var cmd command.Command
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		_ = kafka.PublishDLQ(ctx, s.dlqProducer, msg.Value, err.Error())
		metrics.ErrorsTotal.WithLabelValues("unmarshal").Inc()
		return nil
	}

	start := time.Now()
	if err := command.Validate(&cmd); err != nil {
		res := result.Rejected(cmd, "VALIDATION_FAILED", err.Error())
		_ = s.resultPub.Publish(ctx, res)
		metrics.ObserveCommand(cmd.Type, command.StatusRejected, start)
		return nil
	}

	if err := s.policy.CheckCommand(cmd); err != nil {
		res := result.Rejected(cmd, "POLICY_VIOLATION", err.Error())
		_ = s.resultPub.Publish(ctx, res)
		metrics.ObserveCommand(cmd.Type, command.StatusRejected, start)
		return nil
	}

	if err := s.rateLimiter.Wait(ctx); err != nil {
		return err
	}

	s.inFlight.Add(1)
	defer s.inFlight.Done()

	res, err := s.router.Route(ctx, cmd)
	if err != nil {
		res = result.Failed(cmd, "HANDLER_ERROR", err.Error())
	}

	if res != nil {
		if pubErr := s.resultPub.Publish(ctx, res); pubErr != nil {
			metrics.ErrorsTotal.WithLabelValues("result_publish").Inc()
			return pubErr
		}
		metrics.ObserveCommand(cmd.Type, res.Status, start)
	}

	metrics.WatchSubscriptions.Set(float64(s.watchManager.ActiveCount()))
	metrics.LogStreamSubscriptions.Set(float64(s.logStreamMgr.ActiveCount()))
	return nil
}

func (s *Service) gracefulShutdown() {
	s.shutdownOnce.Do(func() {
		s.logger.Info("graceful shutdown: waiting for in-flight commands")
		done := make(chan struct{})
		go func() {
			s.inFlight.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			s.logger.Warn("shutdown timeout waiting for in-flight commands")
		}
	})
}
