package main

import (
	"encoding/json"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// WorkloadConfig holds per-workload scheduling configuration.
type WorkloadConfig struct {
	// LabeledPercentage is the desired percentage of pods that should have the
	// node selector, tolerations, and labels applied.
	LabeledPercentage int `json:"labeledPercentage"`
	// MinUnlabeled is the minimum number of pods that must remain without the
	// node selector applied (i.e. unlabeled/gated), regardless of percentage.
	MinUnlabeled int `json:"minUnlabeled"`
	// Labels to apply to the labeled pod subset.
	Labels map[string]string `json:"labels,omitempty"`
	// NodeSelector to apply to the labeled pod subset.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// Tolerations to apply to the labeled pod subset.
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// CheckInterval controls how often the workload state is re-evaluated.
	CheckInterval time.Duration `json:"checkInterval,omitempty"`
	// ScheduleTimeout is how long a labeled pod may remain Pending before the
	// controller marks the Deployment as disabled-until and evicts pending pods.
	ScheduleTimeout time.Duration `json:"scheduleTimeout,omitempty"`
	// FallbackInterval is added to the disabled-until timestamp to determine
	// when the reconciler should next attempt normal reconciliation.
	FallbackInterval time.Duration `json:"fallbackInterval,omitempty"`
	// RebalanceStabilizationPeriod is the minimum duration to wait between rebalance evictions.
	RebalanceStabilizationPeriod time.Duration `json:"rebalanceStabilizationPeriod,omitempty"`
}

// Config holds the annotation and label key names used by the controller.
type Config struct {
	// ConfigAnnotation is the annotation key for per-workload JSON config.
	ConfigAnnotation string
	// EnabledLabel marks a Deployment as managed by this rebalancer.
	EnabledLabel string
	// DisabledUntilAnnotation is set on a Deployment when labeled pods fail to
	// schedule. Its value is an RFC3339 timestamp.
	DisabledUntilAnnotation string
	// SchedulingGateName is the scheduling gate added to pods by the webhook.
	SchedulingGateName string
}

// hasSchedulingGate reports whether pod still carries the rebalancer gate.
func (c Config) hasSchedulingGate(pod *corev1.Pod) bool {
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == c.SchedulingGateName {
			return true
		}
	}
	return false
}

// Controller holds runtime configuration and wires up the reconciler and webhook.
type Controller struct {
	Config                Config
	DefaultWorkloadConfig WorkloadConfig
}

// NewController returns a Controller with default values.
func NewController() *Controller {
	return &Controller{
		Config: Config{
			ConfigAnnotation:        "rebalancer/config",
			EnabledLabel:            "rebalancer/enabled",
			DisabledUntilAnnotation: "rebalancer/disabled-until",
			SchedulingGateName:      "rebalancer/gate",
		},
		DefaultWorkloadConfig: WorkloadConfig{
			Labels: map[string]string{
				"rebalancer/labeled": "true",
			},
			LabeledPercentage:            100,
			MinUnlabeled:                 0,
			CheckInterval:                30 * time.Second,
			ScheduleTimeout:              5 * time.Minute,
			FallbackInterval:             1 * time.Minute,
			RebalanceStabilizationPeriod: 10 * time.Second,
		},
	}
}

// SetupWithManager registers the DeploymentReconciler and the mutating pod
// webhook with mgr.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	if err := (&DeploymentReconciler{
		Client:                mgr.GetClient(),
		Scheme:                mgr.GetScheme(),
		Config:                c.Config,
		DefaultWorkloadConfig: c.DefaultWorkloadConfig,
	}).SetupWithManager(mgr); err != nil {
		return err
	}

	mgr.GetWebhookServer().Register("/mutate-v1-pod", &webhook.Admission{
		Handler: &PodWebhook{
			Reader:  mgr.GetClient(),
			Decoder: admission.NewDecoder(mgr.GetScheme()),
			Config:  c.Config,
		},
	})
	return nil
}

// applyAnnotation applies parsed annotation JSON on top of defaults.
func (cfg WorkloadConfig) applyAnnotation(annotation string) (WorkloadConfig, error) {
	if annotation == "" {
		return cfg, nil
	}
	if err := json.Unmarshal([]byte(annotation), &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
