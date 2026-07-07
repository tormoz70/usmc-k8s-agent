package k8s

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

// DynamicBundle holds dynamic client and REST mapper for informers.
type DynamicBundle struct {
	Dynamic dynamic.Interface
	Mapper  meta.RESTMapper
}

// NewDynamicBundle creates a dynamic client and cached REST mapper.
func NewDynamicBundle(cfg *rest.Config) (*DynamicBundle, error) {
	if cfg == nil {
		return nil, fmt.Errorf("rest config is nil")
	}
	dc, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, err
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))
	return &DynamicBundle{Dynamic: dc, Mapper: mapper}, nil
}

// GVRForGVK resolves GroupVersionResource for a GVK.
func (b *DynamicBundle) GVRForGVK(gvk schema.GroupVersionKind) (schema.GroupVersionResource, error) {
	if b == nil || b.Mapper == nil {
		return schema.GroupVersionResource{}, fmt.Errorf("mapper is nil")
	}
	mapping, err := b.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	return mapping.Resource, nil
}

// GVKFromParts builds schema.GroupVersionKind.
func GVKFromParts(group, version, kind string) schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: group, Version: version, Kind: kind}
}

// EventResourceKey builds Kafka message key for ordered watch events.
func EventResourceKey(clusterID, namespace, group, version, kind, name string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s", clusterID, namespace, group, version, kind, name)
}
