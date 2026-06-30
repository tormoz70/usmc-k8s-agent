package k8s

import (
	"context"
	"fmt"

	"github.com/usmc/k8s-agent/internal/command"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/yaml"
)

type ListOptions struct {
	GVK            command.GVK
	Namespace      string
	LabelSelector  string
	FieldSelector  string
	Limit          int64
	ContinueToken  string
	OutputFormat   string
	StripStatus    bool
	Namespaces     []string
}

type ListItem struct {
	YAML string `json:"yaml,omitempty"`
	JSON string `json:"json,omitempty"`
}

type ListOutput struct {
	ItemsCount    int      `json:"items_count"`
	ContinueToken string   `json:"continue_token,omitempty"`
	Items         []string `json:"items"`
	Truncated     bool     `json:"truncated"`
}

type Lister struct {
	clients *Clients
}

func NewLister(clients *Clients) *Lister {
	return &Lister{clients: clients}
}

func (l *Lister) List(ctx context.Context, opts ListOptions) (*ListOutput, error) {
	gvr, err := l.gvrFor(opts.GVK)
	if err != nil {
		return nil, err
	}

	namespaces := opts.Namespaces
	if len(namespaces) == 0 && opts.Namespace != "" {
		namespaces = []string{opts.Namespace}
	}
	if len(namespaces) == 0 {
		namespaces = []string{""}
	}

	out := &ListOutput{Items: []string{}}
	limit := opts.Limit
	if limit <= 0 {
		limit = 500
	}

	for _, ns := range namespaces {
		items, cont, err := l.listNamespace(ctx, gvr, ns, opts, limit)
		if err != nil {
			return nil, err
		}
		out.Items = append(out.Items, items...)
		out.ItemsCount += len(items)
		if cont != "" {
			out.ContinueToken = cont
			out.Truncated = true
			break
		}
	}

	return out, nil
}

func (l *Lister) listNamespace(ctx context.Context, gvr schema.GroupVersionResource, ns string, opts ListOptions, limit int64) ([]string, string, error) {
	var res dynamic.ResourceInterface
	if ns == "" {
		res = l.clients.Dynamic.Resource(gvr)
	} else {
		res = l.clients.Dynamic.Resource(gvr).Namespace(ns)
	}

	listOpts := metav1.ListOptions{
		LabelSelector: opts.LabelSelector,
		FieldSelector: opts.FieldSelector,
		Limit:         limit,
		Continue:      opts.ContinueToken,
	}

	list, err := res.List(ctx, listOpts)
	if err != nil {
		return nil, "", fmt.Errorf("list resources: %w", err)
	}

	var items []string
	for i := range list.Items {
		obj := SanitizeObject(&list.Items[i], SanitizeOptions{StripStatus: opts.StripStatus})
		var s string
		if opts.OutputFormat == "json" {
			b, err := obj.MarshalJSON()
			if err != nil {
				return nil, "", err
			}
			s = string(b)
		} else {
			b, err := yaml.Marshal(obj.Object)
			if err != nil {
				return nil, "", err
			}
			s = string(b)
		}
		items = append(items, s)
	}
	return items, list.GetContinue(), nil
}

func (l *Lister) gvrFor(gvk command.GVK) (schema.GroupVersionResource, error) {
	l.clients.mapperMu.RLock()
	mapper := l.clients.RESTMapper
	l.clients.mapperMu.RUnlock()

	gk := schema.GroupKind{Group: gvk.Group, Kind: gvk.Kind}
	mapping, err := mapper.RESTMapping(gk, gvk.Version)
	if err != nil {
		l.clients.ResetMapper()
		l.clients.mapperMu.RLock()
		mapper = l.clients.RESTMapper
		l.clients.mapperMu.RUnlock()
		mapping, err = mapper.RESTMapping(gk, gvk.Version)
		if err != nil {
			return schema.GroupVersionResource{}, fmt.Errorf("resolve GVR for %s: %w", gvk.Kind, err)
		}
	}
	return mapping.Resource, nil
}

func (l *Lister) ListAllPages(ctx context.Context, opts ListOptions) ([]unstructured.Unstructured, error) {
	gvr, err := l.gvrFor(opts.GVK)
	if err != nil {
		return nil, err
	}

	namespaces := opts.Namespaces
	if len(namespaces) == 0 && opts.Namespace != "" {
		namespaces = []string{opts.Namespace}
	}
	if len(namespaces) == 0 {
		namespaces = []string{""}
	}

	var all []unstructured.Unstructured
	for _, ns := range namespaces {
		var res dynamic.ResourceInterface
		if ns == "" {
			res = l.clients.Dynamic.Resource(gvr)
		} else {
			res = l.clients.Dynamic.Resource(gvr).Namespace(ns)
		}

		cont := ""
		for {
			listOpts := metav1.ListOptions{
				LabelSelector: opts.LabelSelector,
				FieldSelector: opts.FieldSelector,
				Limit:         500,
				Continue:      cont,
			}
			list, err := res.List(ctx, listOpts)
			if err != nil {
				return nil, fmt.Errorf("list all pages: %w", err)
			}
			all = append(all, list.Items...)
			cont = list.GetContinue()
			if cont == "" {
				break
			}
		}
	}
	return all, nil
}
