package watchmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/usmc/k8s-agent/internal/command"
	"github.com/usmc/k8s-agent/internal/k8s"
	"github.com/usmc/k8s-agent/internal/result"
	"github.com/wI2L/jsondiff"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

type SubscribePayload struct {
	SubscriptionID string            `json:"subscription_id"`
	GVK            command.GVK       `json:"gvk"`
	Namespace      string            `json:"namespace"`
	Selector       selector          `json:"selector"`
	EventFilter    []string          `json:"event_filter"`
	TTLSeconds     int64             `json:"ttl_seconds"`
}

type selector struct {
	LabelSelector string `json:"labelSelector"`
	FieldSelector string `json:"fieldSelector"`
}

type subscription struct {
	payload SubscribePayload
	cancel  context.CancelFunc
}

type Manager struct {
	clients  *k8s.Clients
	events   *result.EventPublisher
	logger   *slog.Logger
	mu       sync.RWMutex
	subs     map[string]*subscription
	isLeader bool
}

func NewManager(clients *k8s.Clients, events *result.EventPublisher, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		clients: clients,
		events:  events,
		logger:  logger,
		subs:    make(map[string]*subscription),
	}
}

func (m *Manager) SetLeader(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isLeader = v
	if !v {
		for id, sub := range m.subs {
			sub.cancel()
			delete(m.subs, id)
		}
	}
}

func (m *Manager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.subs)
}

func (m *Manager) Subscribe(ctx context.Context, payload SubscribePayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isLeader {
		return fmt.Errorf("watch subscriptions require leader")
	}
	if _, exists := m.subs[payload.SubscriptionID]; exists {
		return fmt.Errorf("subscription %s already exists", payload.SubscriptionID)
	}

	gvr, err := m.gvrFor(payload.GVK)
	if err != nil {
		return err
	}

	subCtx, cancel := context.WithCancel(ctx)
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(m.clients.Dynamic, time.Minute*10, payload.Namespace, nil)
	informer := factory.ForResource(gvr).Informer()

	filter := map[string]struct{}{}
	for _, e := range payload.EventFilter {
		filter[e] = struct{}{}
	}
	if len(filter) == 0 {
		filter[command.EventAdd] = struct{}{}
		filter[command.EventUpdate] = struct{}{}
		filter[command.EventDelete] = struct{}{}
	}

	_, err = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if _, ok := filter[command.EventAdd]; !ok {
				return
			}
			u := obj.(*unstructured.Unstructured)
			m.publishEvent(subCtx, payload, command.EventAdd, u, nil)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if _, ok := filter[command.EventUpdate]; !ok {
				return
			}
			oldU := oldObj.(*unstructured.Unstructured)
			newU := newObj.(*unstructured.Unstructured)
			m.publishEvent(subCtx, payload, command.EventUpdate, newU, oldU)
		},
		DeleteFunc: func(obj interface{}) {
			if _, ok := filter[command.EventDelete]; !ok {
				return
			}
			u := obj.(*unstructured.Unstructured)
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				u, _ = tombstone.Obj.(*unstructured.Unstructured)
			}
			m.publishEvent(subCtx, payload, command.EventDelete, u, nil)
		},
	})
	if err != nil {
		cancel()
		return err
	}

	factory.Start(subCtx.Done())
	if !cache.WaitForCacheSync(subCtx.Done(), informer.HasSynced) {
		cancel()
		return fmt.Errorf("informer cache sync failed")
	}

	m.subs[payload.SubscriptionID] = &subscription{payload: payload, cancel: cancel}
	return nil
}

func (m *Manager) Unsubscribe(subscriptionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sub, ok := m.subs[subscriptionID]
	if !ok {
		return fmt.Errorf("subscription %s not found", subscriptionID)
	}
	sub.cancel()
	delete(m.subs, subscriptionID)
	return nil
}

func (m *Manager) publishEvent(ctx context.Context, payload SubscribePayload, eventType string, obj, old *unstructured.Unstructured) {
	sanitized := k8s.SanitizeObject(obj, k8s.SanitizeOptions{StripStatus: false})
	details := map[string]any{
		"resource_version": sanitized.GetResourceVersion(),
	}
	if eventType == command.EventUpdate && old != nil {
		diff, err := computeDiff(old, sanitized)
		if err == nil {
			details["diff"] = diff
		}
	}

	detailsBytes, _ := json.Marshal(details)
	event := &command.ClusterEvent{
		SubscriptionID: payload.SubscriptionID,
		EventType:      eventType,
		Resource: command.Target{
			Group:     payload.GVK.Group,
			Version:   payload.GVK.Version,
			Kind:      payload.GVK.Kind,
			Namespace: sanitized.GetNamespace(),
			Name:      sanitized.GetName(),
		},
		ObservedAt: time.Now().UTC(),
		Details:    detailsBytes,
	}
	key := fmt.Sprintf("%s/%s", sanitized.GetNamespace(), sanitized.GetName())
	if err := m.events.Publish(ctx, key, event); err != nil {
		m.logger.Error("publish watch event", "error", err)
	}
}

func computeDiff(oldObj, newObj *unstructured.Unstructured) (map[string]any, error) {
	oldSan := k8s.SanitizeObject(oldObj, k8s.SanitizeOptions{})
	newSan := k8s.SanitizeObject(newObj, k8s.SanitizeOptions{})
	oldJSON, err := oldSan.MarshalJSON()
	if err != nil {
		return nil, err
	}
	newJSON, err := newSan.MarshalJSON()
	if err != nil {
		return nil, err
	}
	patch, err := jsondiff.CompareJSON(oldJSON, newJSON)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"operations": patch,
	}, nil
}

func (m *Manager) gvrFor(gvk command.GVK) (schema.GroupVersionResource, error) {
	mapper := m.clients.GetRESTMapper()
	gk := schema.GroupKind{Group: gvk.Group, Kind: gvk.Kind}
	mapping, err := mapper.RESTMapping(gk, gvk.Version)
	if err != nil {
		m.clients.ResetMapper()
		mapper = m.clients.GetRESTMapper()
		mapping, err = mapper.RESTMapping(gk, gvk.Version)
		if err != nil {
			return schema.GroupVersionResource{}, err
		}
	}
	return mapping.Resource, nil
}
