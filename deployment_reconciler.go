package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// DeploymentReconciler reconciles Deployments that carry EnabledLabel.
type DeploymentReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	Config                Config
	DefaultWorkloadConfig WorkloadConfig
}

func (r *DeploymentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var deploy appsv1.Deployment
	if err := r.Get(ctx, req.NamespacedName, &deploy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Update config
	cfg, err := r.DefaultWorkloadConfig.applyAnnotation(deploy.Annotations[r.Config.ConfigAnnotation])
	if err != nil {
		logger.Error(err, "Invalid config annotation, using default")
		cfg = r.DefaultWorkloadConfig
	}

	disabledUntil, err := parseDisabledUntil(deploy.Annotations[r.Config.DisabledUntilAnnotation])
	if err != nil {
		logger.Error(err, "Invalid disabled until annotation, ignoring")
	}
	disabled := !disabledUntil.IsZero() && time.Now().Before(disabledUntil)

	untilNextCheck := func() (ctrl.Result, error) {
		return ctrl.Result{RequeueAfter: cfg.CheckInterval}, nil
	}

	// Collect pods
	podsByRs, err := r.getPodsByReplicaSets(ctx, deploy)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(podsByRs) == 0 {
		logger.V(1).Info("No ReplicaSets, skipping")
		return untilNextCheck()
	}

	// Remove gates
	var gatesTotal, labeledTotal int
	for rs, pods := range podsByRs {
		gates, labeled, err := r.removeGates(ctx, pods, cfg, disabled)
		if err != nil {
			logger.Error(err, "Failed to remove gates", "rs", rs)
		}
		gatesTotal += gates
		labeledTotal += labeled
	}

	if gatesTotal > 0 {
		logger.V(1).Info("Removed gates", "gates", gatesTotal, "labeled", labeledTotal)
		return untilNextCheck()
	}

	if len(podsByRs) > 1 {
		logger.V(1).Info("Rollout in progress, skipping")
		return untilNextCheck()
	}

	// Reconcile
	var pods []*corev1.Pod
	for _, pods = range podsByRs {
		break
	}

	_, labeled, unlabeled := partitionPods(pods, cfg.Labels, r.Config)

	// Fallback
	// Active fallback: Evict pending labeled pods
	if disabled {
		toEvict := pendingPods(labeled)
		if len(toEvict) == 0 {
			logger.V(1).Info("Disabled", "until", disabledUntil)
			return ctrl.Result{RequeueAfter: time.Until(disabledUntil)}, nil
		}

		logger.Info("Evicting pods due to fallback", "evicting", len(toEvict))

		if err := r.evictPods(ctx, toEvict); err != nil {
			logger.Error(err, "Failed to evict pending pods")
		}
		return untilNextCheck()
	}

	// Activate fallback: disable if has long pending labeled pods
	if hasPendingSince(labeled, time.Now().Add(-cfg.ScheduleTimeout)) {
		until := time.Now().Add(cfg.FallbackInterval)

		logger.Info("Labeled pods pending for too long, disabling", "until", until)

		if err := r.disableDeployment(ctx, &deploy, until); err != nil {
			logger.Error(err, "Failed to disable")
		}
		return untilNextCheck()
	}

	// Rebalance
	pending := len(pendingPods(pods))
	if pending > 0 {
		logger.V(1).Info("Still progressing, skipping", "total", len(pods), "pending", pending)
		return untilNextCheck()
	}

	total := len(pods)
	target := targetLabeledCount(total, cfg)
	actual := len(labeled)

	if actual == target {
		logger.V(1).Info("Balanced, skipping")
		return untilNextCheck()
	}

	var (
		toEvict []*corev1.Pod
		which   string
	)
	if actual > target {
		toEvict = labeled[target:]
		which = "labeled"
	} else {
		toEvict = unlabeled[:target-actual]
		which = "unlabeled"
	}
	if len(toEvict) > 0 {
		logger.Info("Evicting pods to rebalance", "total", total,
			"actual", actual, "target", target, "evicting", len(toEvict), "which", which)

		if err := r.evictPods(ctx, toEvict); err != nil {
			logger.Error(err, "Failed to evict pods for rebalancing")
		}
	}
	return untilNextCheck()
}

// removeGates removes scheduling gates and labels pods to up to the computed target count.
func (r *DeploymentReconciler) removeGates(ctx context.Context, pods []*corev1.Pod, cfg WorkloadConfig, disabled bool) (int, int, error) {
	gated, labeled, _ := partitionPods(pods, cfg.Labels, r.Config)
	if len(gated) == 0 {
		return 0, 0, nil
	}

	// Remove gates and add labels, node selectors and tolerations
	total := len(pods)
	target := targetLabeledCount(total, cfg)
	actual := len(labeled)

	toLabel := 0
	if !disabled {
		maxToLabel := max(0, target-actual)
		toLabel = min(len(gated), maxToLabel)
	}

	var errs []error
	for i, pod := range gated {
		patch := client.MergeFrom(pod.DeepCopy())

		pod.Spec.SchedulingGates = slices.DeleteFunc(pod.Spec.SchedulingGates, func(g corev1.PodSchedulingGate) bool {
			return g.Name == r.Config.SchedulingGateName
		})

		if i < toLabel {
			pod.Labels = mapsAppend(pod.Labels, cfg.Labels)
			pod.Spec.NodeSelector = mapsAppend(pod.Spec.NodeSelector, cfg.NodeSelector)
			pod.Spec.Tolerations = append(pod.Spec.Tolerations, cfg.Tolerations...)
		}

		if err := r.Patch(ctx, pod, patch); err != nil {
			errs = append(errs, fmt.Errorf("failed to patch pod %s: %w", pod.Name, err))
		}
	}
	return len(gated), toLabel, errors.Join(errs...)
}

func mapsAppend(m, v map[string]string) map[string]string {
	if m == nil {
		return maps.Clone(v)
	}
	maps.Copy(m, v)
	return m
}

func (r *DeploymentReconciler) getPodsByReplicaSets(ctx context.Context, deploy appsv1.Deployment) (map[types.UID][]*corev1.Pod, error) {
	rsList, err := r.getReplicaSetsForDeployment(ctx, &deploy)
	if err != nil {
		return nil, err
	}

	return r.getPodMapForDeployment(ctx, rsList)
}

// getReplicaSetsForDeployment returns the list of ReplicaSets that this Deployment manages.
func (r *DeploymentReconciler) getReplicaSetsForDeployment(ctx context.Context, deploy *appsv1.Deployment) ([]*appsv1.ReplicaSet, error) {
	deploymentSelector, err := metav1.LabelSelectorAsSelector(deploy.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("deployment %s/%s has invalid label selector: %w", deploy.Namespace, deploy.Name, err)
	}

	var rsList appsv1.ReplicaSetList
	if err := r.List(ctx, &rsList,
		client.InNamespace(deploy.Namespace),
		client.MatchingLabelsSelector{Selector: deploymentSelector},
	); err != nil {
		return nil, err
	}
	return ptrList(rsList.Items, func(rs *appsv1.ReplicaSet) bool {
		controllerRef := metav1.GetControllerOfNoCopy(rs)
		return controllerRef != nil && controllerRef.UID == deploy.GetUID()
	}), nil
}

// getPodMapForDeployment returns the Pods managed by a Deployment.
//
// It returns a map from ReplicaSet UID to a non-empty list of Pods controlled by that RS.
// The pod pointers returned by this method point the pod objects in the cache and thus
// shouldn't be modified in any way.
func (r *DeploymentReconciler) getPodMapForDeployment(ctx context.Context, rsList []*appsv1.ReplicaSet) (map[types.UID][]*corev1.Pod, error) {
	// Group Pods by their controller (if it's in rsList).
	podMap := make(map[types.UID][]*corev1.Pod, len(rsList))
	for _, rs := range rsList {
		// list all pods managed by this ReplicaSet using the pod indexer
		pods, err := controllerFilterPodsByOwner(ctx, r.Client, &rs.ObjectMeta, "ReplicaSet")
		if err != nil {
			return nil, err
		}
		if len(pods) > 0 {
			podMap[rs.UID] = pods
		}
	}
	return podMap, nil
}

// disableDeployment sets disabled annotation on the Deployment using a patch.
func (r *DeploymentReconciler) disableDeployment(ctx context.Context, deploy *appsv1.Deployment, until time.Time) error {
	disabledUntil := until.UTC().Format(time.RFC3339)
	patch := client.MergeFrom(deploy.DeepCopy())
	deploy.Annotations = mapsAppend(deploy.Annotations, map[string]string{r.Config.DisabledUntilAnnotation: disabledUntil})
	return r.Patch(ctx, deploy, patch)
}

// evictPods evicts a list of pods.
func (r *DeploymentReconciler) evictPods(ctx context.Context, pods []*corev1.Pod) error {
	var errs []error
	for _, pod := range pods {
		if err := r.evictPod(ctx, pod); err != nil {
			errs = append(errs, fmt.Errorf("failed to evict pod %s: %w", pod.Name, err))
		}
	}
	return errors.Join(errs...)
}

// evictPod evicts a pod via the Eviction subresource so PodDisruptionBudgets are respected.
func (r *DeploymentReconciler) evictPod(ctx context.Context, pod *corev1.Pod) error {
	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
	}
	return r.SubResource("eviction").Create(ctx, pod, eviction)
}

func (r *DeploymentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := addPodControllerIndexer(mgr); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Deployment{},
			builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
				return obj.GetLabels()[r.Config.EnabledLabel] == "true"
			})),
		).
		Complete(r)
}

// partitionPods splits pods into three buckets:
//   - gated: scheduling gate is present
//   - labeled: no gate, and all labels are present on the pod
//   - unlabeled: no gate, but at least one label is missing
func partitionPods(pods []*corev1.Pod, labels map[string]string, c Config) (gated, labeled, unlabeled []*corev1.Pod) {
	for _, pod := range pods {
		if c.hasSchedulingGate(pod) {
			gated = append(gated, pod)
		} else if hasAllLabels(pod, labels) {
			labeled = append(labeled, pod)
		} else {
			unlabeled = append(unlabeled, pod)
		}
	}
	return
}

// hasAllLabels reports whether pod carries every key/value in desired.
func hasAllLabels(pod *corev1.Pod, desired map[string]string) bool {
	for k, v := range desired {
		if pod.Labels[k] != v {
			return false
		}
	}
	return true
}

// parseDisabledUntil parses the value of DisabledUntilAnnotation as an RFC3339
// timestamp. Returns the zero Time and a non-nil error if the value is empty or
// malformed.
func parseDisabledUntil(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, value)
}

// hasPendingSince returns true if there are pods that are in Pending phase since deadline.
func hasPendingSince(pods []*corev1.Pod, deadline time.Time) bool {
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodPending && pod.CreationTimestamp.Time.Before(deadline) {
			return true
		}
	}
	return false
}

// pendingPods returns all pods that are in Pending phase.
func pendingPods(pods []*corev1.Pod) []*corev1.Pod {
	var pending []*corev1.Pod
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodPending {
			pending = append(pending, pod)
		}
	}
	return pending
}

// targetLabeledCount computes the target number of labeled pods satisfying config.
func targetLabeledCount(total int, cfg WorkloadConfig) int {
	labeled := int(float64(total) * float64(cfg.LabeledPercentage) / 100.0)
	maxLabeled := max(0, total-cfg.MinUnlabeled)
	return min(labeled, maxLabeled)
}

// countByPhase returns a map of pod phase to count for the given pod slice.
func countByPhase(pods []*corev1.Pod) map[corev1.PodPhase]int {
	counts := make(map[corev1.PodPhase]int)
	for _, pod := range pods {
		counts[pod.Status.Phase]++
	}
	return counts
}
