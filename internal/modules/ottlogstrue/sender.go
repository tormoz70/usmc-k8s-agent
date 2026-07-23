package ottlogstrue

import (
	"context"
	"time"

	ottpb "github.com/usmc/usmc-k8s-agent/api/gen/ottlogstrue"
	"github.com/usmc/usmc-k8s-agent/internal/batcher"
	"github.com/usmc/usmc-k8s-agent/internal/coreclient"
	"github.com/usmc/usmc-k8s-agent/internal/protoheaders"
)

// Sender flushes sidecar event batches to Kafka (zipped optional).
type Sender struct {
	client    *coreclient.Client
	topic     string
	senderID  string
	batcher   *batcher.Batcher[ottpb.OttLogTrueSidecarEvent]
	namespace string
	pod       string
	sidecar   string
}

func NewSender(client *coreclient.Client, topic, senderID, ns, pod, sidecar string) *Sender {
	s := &Sender{
		client:    client,
		topic:     topic,
		senderID:  senderID,
		namespace: ns,
		pod:       pod,
		sidecar:   sidecar,
	}
	s.batcher = batcher.New(30, 30*time.Second, s.flush)
	return s
}

func (s *Sender) Add(ctx context.Context, ev ottpb.OttLogTrueSidecarEvent) error {
	return s.batcher.Add(ctx, ev)
}

func (s *Sender) Close(ctx context.Context) error {
	return s.batcher.Close(ctx)
}

func (s *Sender) flush(ctx context.Context, items []ottpb.OttLogTrueSidecarEvent) error {
	if s.client == nil || len(items) == 0 {
		return nil
	}
	bucket := &ottpb.OttLogTrueSidecarBucket{
		Events:      items,
		Namespace:   s.namespace,
		PodName:     s.pod,
		SidecarName: s.sidecar,
	}
	body, err := bucket.Marshal()
	if err != nil {
		return err
	}
	h := protoheaders.NewRequest(
		s.senderID,
		ottpb.MessageTypeSidecarBucket,
		"OTT_LOG_TRUE_BUCKET",
		s.topic,
		"",
	)
	h.Addressee = ottpb.AddresseeOttConsumer
	h.Zipped = true
	return s.client.SendRequestVoid(ctx, h.ToMap(), body)
}
