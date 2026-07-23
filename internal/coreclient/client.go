package coreclient

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/protoheaders"
)

// Publisher is the subset of Kafka publishing used by CoreClient.
type Publisher interface {
	PublishRaw(ctx context.Context, topic, key string, headers map[string]string, body []byte) error
}

// Client implements request/response over Kafka with correlationId (Java CoreClient analogue).
type Client struct {
	pub             Publisher
	log             *slog.Logger
	responseTimeout time.Duration
	mu              sync.Mutex
	pending         map[string]chan pendingResult
	outRequestTopic string
	sender          string
}

type pendingResult struct {
	headers map[string]string
	body    []byte
	err     error
}

func New(pub Publisher, sender, outRequestTopic string, responseTimeout time.Duration, log *slog.Logger) *Client {
	if responseTimeout <= 0 {
		responseTimeout = 10 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	return &Client{
		pub:             pub,
		log:             log,
		responseTimeout: responseTimeout,
		pending:         make(map[string]chan pendingResult),
		outRequestTopic: outRequestTopic,
		sender:          sender,
	}
}

// HandleInboundResponse resolves a pending request (call from protobuf consumer).
func (c *Client) HandleInboundResponse(headers map[string]string, body []byte) bool {
	h := protoheaders.FromMap(headers)
	if h.Direction != protoheaders.DirectionResponse || h.CorrelationID == "" {
		return false
	}
	c.mu.Lock()
	ch, ok := c.pending[h.CorrelationID]
	c.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- pendingResult{headers: headers, body: body}:
	default:
	}
	return true
}

// SendRequestVoid publishes without waiting for a response.
func (c *Client) SendRequestVoid(ctx context.Context, headers map[string]string, body []byte) error {
	h := protoheaders.FromMap(headers)
	topic := h.Topic
	if topic == "" {
		topic = c.outRequestTopic
	}
	key := h.CorrelationID
	if key == "" {
		key = h.MessageID
	}
	if h.Zipped {
		var err error
		body, err = gzipBytes(body)
		if err != nil {
			return err
		}
		headers = protoheaders.FromMap(headers).ToMap()
		headers[protoheaders.KeyZipped] = "true"
	}
	return c.pub.PublishRaw(ctx, topic, key, headers, body)
}

// SendRequest publishes and waits for a correlated RESPONSE.
func (c *Client) SendRequest(ctx context.Context, headers map[string]string, body []byte) ([]byte, map[string]string, error) {
	h := protoheaders.FromMap(headers)
	if h.CorrelationID == "" {
		return nil, nil, fmt.Errorf("correlationId required")
	}
	ch := make(chan pendingResult, 1)
	c.mu.Lock()
	c.pending[h.CorrelationID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, h.CorrelationID)
		c.mu.Unlock()
	}()

	if err := c.SendRequestVoid(ctx, headers, body); err != nil {
		return nil, nil, err
	}

	timeout := c.responseTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-timer.C:
		return nil, nil, fmt.Errorf("coreclient: response timeout after %s correlationId=%s", timeout, h.CorrelationID)
	case res := <-ch:
		if res.err != nil {
			return nil, nil, res.err
		}
		outBody := res.body
		rh := protoheaders.FromMap(res.headers)
		if rh.Zipped {
			decoded, err := gunzipBytes(outBody)
			if err != nil {
				return nil, nil, err
			}
			outBody = decoded
		}
		return outBody, res.headers, nil
	}
}

func gzipBytes(in []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(in); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gunzipBytes(in []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
