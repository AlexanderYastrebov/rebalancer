package main

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Various helpers adapted from https://github.com/kubernetes/kubernetes/blob/master/pkg/controller/controller_utils.go

const (
	podControllerField = ".metadata.controller"
)

// podControllerIndexKey returns the index key to locate pods with the specified controller ownerReference.
func podControllerIndexKey(ownerReference *metav1.OwnerReference) string {
	return ownerReference.Kind + "/" + ownerReference.Name + "/" + string(ownerReference.UID)
}

func addPodControllerIndexer(mgr ctrl.Manager) error {
	return mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, podControllerField, func(obj client.Object) []string {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return nil
		}
		controller := metav1.GetControllerOf(pod)
		if controller == nil {
			return nil
		}
		return []string{podControllerIndexKey(controller)}
	})
}

// controllerFilterPodsByOwner gets the Pods managed by an owner in the owner's namespace.
func controllerFilterPodsByOwner(ctx context.Context, cli client.Client, owner *metav1.ObjectMeta, ownerKind string) ([]*corev1.Pod, error) {
	key := podControllerIndexKey(&metav1.OwnerReference{Name: owner.Name, Kind: ownerKind, UID: owner.UID})

	// List pods using the indexer
	var pods corev1.PodList
	err := cli.List(ctx, &pods,
		client.InNamespace(owner.Namespace),
		client.MatchingFields{podControllerField: key},
	)
	if err != nil {
		return nil, err
	}
	return ptrList(pods.Items, nil), nil
}

func ptrList[T any](items []T, accept func(*T) bool) []*T {
	result := make([]*T, 0, len(items))
	for i := range items {
		item := &items[i]
		if accept == nil || accept(item) {
			result = append(result, item)
		}
	}
	return result
}
