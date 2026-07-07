package watch

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/k8s"
	"github.com/usmc/usmc-k8s-agent/internal/policy"
)

// EventPublisher publishes cluster watch events to Kafka.
type EventPublisher interface {
	PublishEvent(ctx context.Context, topic, key string, event *ClusterEvent) error
}

type subscription struct {
	payload *SubscribePayload
	cancel  context.CancelFunc
}

type informerKey struct {
	group, version, kind, namespace, labelSelector, fieldSelector string
}

// Manager owns in-memory watch subscriptions and shared informers.
type Manager struct {
	clusterID   string
	defaultTopic string
	bundle      *k8s.DynamicBundle
	policy      *policy.Engine
	publisher   EventPublisher
	log         *slog.Logger

	mu          sync.Mutex
	subs        map[string]*subscription
	informers   map[informerKey]cache.SharedIndexInformer
	factories   map[informerKey]dynamicinformer.DynamicSharedInformerFactory
}

func NewManager(clusterID, defaultTopic string, bundle *k8s.DynamicBundle, engine *policy.Engine, publisher EventPublisher, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{
		clusterID:    clusterID,
		defaultTopic: defaultTopic,
		bundle:       bundle,
		policy:       engine,
		publisher:    publisher,
		log:          log,
		subs:         make(map[string]*subscription),
		informers:    make(map[informerKey]cache.SharedIndexInformer),
		factories:    make(map[informerKey]dynamicinformer.DynamicSharedInformerFactory),
	}
}

// Subscribe registers a watch subscription (leader-only).
func (m *Manager) Subscribe(ctx context.Context, payload *SubscribePayload) error {
	if err := m.policy.AllowCommandType(command.TypeWatchSubscribe); err != nil {
		return err
	}
	if err := m.policy.AllowNamespace(payload.Namespace); err != nil {
		return err
	}
	if err := m.policy.AllowGVK(payload.GVK); err != nil {
		return err
	}

	gvk := k8s.GVKFromParts(payload.GVK.Group, payload.GVK.Version, payload.GVK.Kind)
	gvr, err := m.bundle.GVRForGVK(gvk)
	if err != nil {
		return fmt.Errorf("resolve gvr: %w", err)
	}

	key := informerKey{
		group: gvk.Group, version: gvk.Version, kind: gvk.Kind,
		namespace: payload.Namespace, labelSelector: payload.LabelSelector, fieldSelector: payload.FieldSelector,
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.subs[payload.SubscriptionID]; exists {
		return fmt.Errorf("subscription %q already exists", payload.SubscriptionID)
	}

	subCtx, cancel := context.WithCancel(ctx)
	if payload.TTLSeconds > 0 {
		subCtx, cancel = context.WithTimeout(subCtx, time.Duration(payload.TTLSeconds)*time.Second)
	}

	inf, err := m.getOrCreateInformer(subCtx, key, gvr, payload)
	if err != nil {
		cancel()
		return err
	}

	if !inf.HasSynced() {
		syncCtx, syncCancel := context.WithTimeout(subCtx, 30*time.Second)
		if !cache.WaitForCacheSync(syncCtx.Done(), inf.HasSynced) {
			syncCancel()
			cancel()
			return fmt.Errorf("informer sync timeout for %s", gvk.Kind)
		}
		syncCancel()
	}

	handler := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			m.handleObject(subCtx, payload, gvk, EventAdded, nil, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			m.handleObject(subCtx, payload, gvk, EventModified, oldObj, newObj)
			if gvk.Kind == "Pod" {
				m.handlePodRestart(subCtx, payload, gvk, oldObj, newObj)
			}
		},
		DeleteFunc: func(obj interface{}) {
			m.handleObject(subCtx, payload, gvk, EventDeleted, nil, obj)
		},
	}
	inf.AddEventHandler(handler)

	m.subs[payload.SubscriptionID] = &subscription{payload: payload, cancel: cancel}
	m.log.Info("watch subscribed", "subscription_id", payload.SubscriptionID, "kind", gvk.Kind, "namespace", payload.Namespace)
	return nil
}

// Unsubscribe removes a subscription and stops its informer handler via cancel.
func (m *Manager) Unsubscribe(subscriptionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sub, ok := m.subs[subscriptionID]
	if !ok {
		return fmt.Errorf("subscription %q not found", subscriptionID)
	}
	sub.cancel()
	delete(m.subs, subscriptionID)
	m.log.Info("watch unsubscribed", "subscription_id", subscriptionID)
	return nil
}

// StopAll cancels every active subscription (leader shutdown).
func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, sub := range m.subs {
		sub.cancel()
		delete(m.subs, id)
	}
}

// ActiveCount returns the number of active watch subscriptions.
func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.subs)
}

func (m *Manager) getOrCreateInformer(ctx context.Context, key informerKey, gvr schema.GroupVersionResource, payload *SubscribePayload) (cache.SharedIndexInformer, error) {
	if inf, ok := m.informers[key]; ok {
		return inf, nil
	}

	tweak := func(opts *metav1.ListOptions) {
		if payload.LabelSelector != "" {
			opts.LabelSelector = payload.LabelSelector
		}
		if payload.FieldSelector != "" {
			opts.FieldSelector = payload.FieldSelector
		}
	}

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		m.bundle.Dynamic, 10*time.Minute, payload.Namespace, tweak,
	)
	inf := factory.ForResource(gvr).Informer()
	m.informers[key] = inf
	m.factories[key] = factory

	go factory.Start(ctx.Done())
	return inf, nil
}

func (m *Manager) handleObject(ctx context.Context, payload *SubscribePayload, gvk schema.GroupVersionKind, eventType string, oldObj, newObj interface{}) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	effectiveType := eventType
	if gvk.Kind == "Event" && payload.Allows(EventK8sEvent) {
		effectiveType = EventK8sEvent
	} else if !payload.Allows(eventType) {
		return
	}
	obj := newObj
	if eventType == EventDeleted {
		obj = newObj
	}
	u, ok := toUnstructured(obj)
	if !ok {
		return
	}

	details := map[string]any{}
	if eventType == EventModified && oldObj != nil {
		if oldU, ok := toUnstructured(oldObj); ok {
			details["diff"] = map[string]any{
				"old": compactObject(oldU),
				"new": compactObject(u),
			}
		}
	}
	if effectiveType == EventK8sEvent {
		details["message"] = nestedString(u.Object, "message")
		details["reason"] = nestedString(u.Object, "reason")
	}

	ev := &ClusterEvent{
		SchemaVersion:  "v1",
		SubscriptionID: payload.SubscriptionID,
		EventType:      effectiveType,
		Resource: ResourceRef{
			Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind,
			Namespace: u.GetNamespace(), Name: u.GetName(),
		},
		ObservedAt: time.Now().UTC(),
		Details:    details,
	}
	m.publish(ctx, payload, ev)
}

func (m *Manager) handlePodRestart(ctx context.Context, payload *SubscribePayload, gvk schema.GroupVersionKind, oldObj, newObj interface{}) {
	if !payload.Allows(EventRestart) {
		return
	}
	oldPod, ok1 := oldObj.(*unstructured.Unstructured)
	newPod, ok2 := newObj.(*unstructured.Unstructured)
	if !ok1 || !ok2 {
		oldU, okA := toUnstructured(oldObj)
		newU, okB := toUnstructured(newObj)
		if !okA || !okB {
			return
		}
		oldPod, newPod = oldU, newU
	}

	restarts := detectRestarts(oldPod, newPod)
	for container, count := range restarts {
		ev := &ClusterEvent{
			SchemaVersion:  "v1",
			SubscriptionID: payload.SubscriptionID,
			EventType:      EventRestart,
			Resource: ResourceRef{
				Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind,
				Namespace: newPod.GetNamespace(), Name: newPod.GetName(),
			},
			ObservedAt: time.Now().UTC(),
			Details: map[string]any{
				"container":     container,
				"restart_count": count,
			},
		}
		m.publish(ctx, payload, ev)
	}
}

func detectRestarts(oldPod, newPod *unstructured.Unstructured) map[string]int32 {
	out := map[string]int32{}
	oldStatuses, _, _ := unstructured.NestedSlice(oldPod.Object, "status", "containerStatuses")
	newStatuses, _, _ := unstructured.NestedSlice(newPod.Object, "status", "containerStatuses")
	oldMap := containerRestartMap(oldStatuses)
	newMap := containerRestartMap(newStatuses)
	for name, newCount := range newMap {
		oldCount := oldMap[name]
		if newCount > oldCount {
			out[name] = newCount
		}
	}
	return out
}

func containerRestartMap(statuses []interface{}) map[string]int32 {
	out := map[string]int32{}
	for _, item := range statuses {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		if name == "" {
			continue
		}
		if rc, ok := m["restartCount"].(int64); ok {
			out[name] = int32(rc)
		}
	}
	return out
}

func (m *Manager) publish(ctx context.Context, payload *SubscribePayload, ev *ClusterEvent) {
	topic := payload.OutputTopicOr(m.defaultTopic)
	key := k8s.EventResourceKey(m.clusterID, ev.Resource.Namespace, ev.Resource.Group, ev.Resource.Version, ev.Resource.Kind, ev.Resource.Name)
	if err := m.publisher.PublishEvent(ctx, topic, key, ev); err != nil {
		m.log.Warn("publish cluster event failed", "error", err, "subscription_id", payload.SubscriptionID)
	}
}

func toUnstructured(obj interface{}) (*unstructured.Unstructured, bool) {
	if obj == nil {
		return nil, false
	}
	switch t := obj.(type) {
	case *unstructured.Unstructured:
		return t, true
	case cache.DeletedFinalStateUnknown:
		return toUnstructured(t.Obj)
	default:
		return nil, false
	}
}

func compactObject(u *unstructured.Unstructured) map[string]any {
	out := map[string]any{
		"metadata": map[string]any{
			"name":            u.GetName(),
			"namespace":       u.GetNamespace(),
			"resourceVersion": u.GetResourceVersion(),
			"generation":      u.GetGeneration(),
		},
	}
	if spec, ok, _ := unstructured.NestedMap(u.Object, "spec"); ok {
		out["spec"] = spec
	}
	if status, ok, _ := unstructured.NestedMap(u.Object, "status"); ok {
		out["status"] = status
	}
	return out
}

func nestedString(obj map[string]interface{}, key string) string {
	if v, ok := obj[key].(string); ok {
		return v
	}
	return ""
}

// Ensure watch.Event types are referenced for future use.
var _ = watch.Added

// Ensure corev1 for pod types in tests
var _ corev1.Pod
