package eventswatcher

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/coreclient"
	"github.com/usmc/usmc-k8s-agent/internal/features"
	"github.com/usmc/usmc-k8s-agent/internal/modules"
	"github.com/usmc/usmc-k8s-agent/internal/protoheaders"
	"github.com/usmc/usmc-k8s-agent/internal/watch"
)

// Bridge republishes JSON cluster.events onto the uamc-events-watcher protobuf topic.
type Bridge struct {
	cfg    *config.Config
	client *coreclient.Client
	log    *slog.Logger
}

var _ modules.Module = (*Bridge)(nil)

func NewBridge(cfg *config.Config, client *coreclient.Client, log *slog.Logger) *Bridge {
	if log == nil {
		log = slog.Default()
	}
	return &Bridge{cfg: cfg, client: client, log: log}
}

func (b *Bridge) Name() string { return "eventswatcher" }

func (b *Bridge) Enabled(cfg *config.Config, feat *features.Registry) bool {
	if cfg == nil {
		return false
	}
	if cfg.Kafka.Mode != config.KafkaModeProtobuf && cfg.Kafka.Mode != config.KafkaModeDual {
		return false
	}
	if feat != nil && !feat.Enabled("watch_events") && !feat.CommandEnabled("watch.subscribe") {
		return false
	}
	return true
}

func (b *Bridge) Start(context.Context) error {
	b.log.Info("eventswatcher protobuf bridge enabled", "topic", b.cfg.Kafka.EventsWatcherTopic)
	return nil
}

func (b *Bridge) Stop(context.Context) error { return nil }

// PublishEvent sends a watch event to the legacy events-watcher topic.
func (b *Bridge) PublishEvent(ctx context.Context, ev *watch.ClusterEvent) error {
	if b == nil || b.client == nil || ev == nil {
		return nil
	}
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	h := protoheaders.NewRequest(
		b.cfg.ClusterID,
		"ClusterEvent",
		"CLUSTER_EVENT",
		b.cfg.Kafka.EventsWatcherTopic,
		"",
	)
	h.Addressee = "uamc-events-watcher"
	return b.client.SendRequestVoid(ctx, h.ToMap(), body)
}
