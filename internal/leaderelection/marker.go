package leaderelection

import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const leaderLabelKey = "k8s-agent/leader"

// PatchLeaderLabel sets or clears the leader label on the current pod.
func PatchLeaderLabel(ctx context.Context, client kubernetes.Interface, namespace, podName string, leader bool) error {
	if client == nil || namespace == "" || podName == "" {
		return fmt.Errorf("leader label patch requires client, namespace and pod name")
	}
	value := "false"
	if leader {
		value = "true"
	}
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": map[string]string{
				leaderLabelKey: value,
			},
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = client.CoreV1().Pods(namespace).Patch(ctx, podName, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}
