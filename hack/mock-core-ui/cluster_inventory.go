package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type clusterDeployment struct {
	Name            string `json:"name"`
	ReadyReplicas   int32  `json:"ready_replicas"`
	DesiredReplicas int32  `json:"desired_replicas"`
}

type clusterPod struct {
	Name   string `json:"name"`
	Phase  string `json:"phase"`
	Ready  bool   `json:"ready"`
	Node   string `json:"node,omitempty"`
}

type clusterNamespace struct {
	Name        string              `json:"name"`
	Phase       string              `json:"phase"`
	Deployments []clusterDeployment `json:"deployments"`
	Pods        []clusterPod        `json:"pods"`
}

type clusterInventory struct {
	K8sAvailable bool               `json:"k8s_available"`
	K8sError     string             `json:"k8s_error,omitempty"`
	Namespaces   []clusterNamespace `json:"namespaces"`
	Counts       struct {
		Namespaces  int `json:"namespaces"`
		Deployments int `json:"deployments"`
		Pods        int `json:"pods"`
	} `json:"counts"`
}

var systemNamespacePrefixes = []string{
	"kube-",
	"local-",
}

func isSystemNamespace(name string) bool {
	if name == "default" || name == "kube-node-lease" || name == "kube-public" || name == "kube-system" {
		return true
	}
	for _, p := range systemNamespacePrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

func (c *agentModeController) inventory(ctx context.Context, includeSystem bool) (*clusterInventory, error) {
	inv := &clusterInventory{
		K8sAvailable: c.k8s != nil,
		Namespaces:   []clusterNamespace{},
	}
	if !inv.K8sAvailable {
		inv.K8sError = c.kubeUnavailableMessage()
		return inv, nil
	}

	nsList, err := c.k8s.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		inv.K8sError = fmt.Sprintf("list namespaces: %v", err)
		return inv, nil
	}

	names := make([]string, 0, len(nsList.Items))
	phases := map[string]string{}
	for _, ns := range nsList.Items {
		if !includeSystem && isSystemNamespace(ns.Name) {
			continue
		}
		names = append(names, ns.Name)
		phases[ns.Name] = string(ns.Status.Phase)
	}
	sort.Strings(names)

	for _, name := range names {
		item := clusterNamespace{
			Name:        name,
			Phase:       phases[name],
			Deployments: []clusterDeployment{},
			Pods:        []clusterPod{},
		}

		deploys, err := c.k8s.AppsV1().Deployments(name).List(ctx, metav1.ListOptions{})
		if err != nil {
			inv.K8sError = fmt.Sprintf("list deployments in %s: %v", name, err)
			return inv, nil
		}
		for _, d := range deploys.Items {
			desired := int32(0)
			if d.Spec.Replicas != nil {
				desired = *d.Spec.Replicas
			}
			item.Deployments = append(item.Deployments, clusterDeployment{
				Name:            d.Name,
				ReadyReplicas:   d.Status.ReadyReplicas,
				DesiredReplicas: desired,
			})
		}
		sort.Slice(item.Deployments, func(i, j int) bool {
			return item.Deployments[i].Name < item.Deployments[j].Name
		})

		pods, err := c.k8s.CoreV1().Pods(name).List(ctx, metav1.ListOptions{})
		if err != nil {
			inv.K8sError = fmt.Sprintf("list pods in %s: %v", name, err)
			return inv, nil
		}
		for _, p := range pods.Items {
			item.Pods = append(item.Pods, clusterPod{
				Name:  p.Name,
				Phase: string(p.Status.Phase),
				Ready: podReady(p),
				Node:  p.Spec.NodeName,
			})
		}
		sort.Slice(item.Pods, func(i, j int) bool {
			return item.Pods[i].Name < item.Pods[j].Name
		})

		inv.Namespaces = append(inv.Namespaces, item)
		inv.Counts.Namespaces++
		inv.Counts.Deployments += len(item.Deployments)
		inv.Counts.Pods += len(item.Pods)
	}

	return inv, nil
}

func podReady(p corev1.Pod) bool {
	if p.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
