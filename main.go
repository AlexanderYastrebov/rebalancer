package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(policyv1.AddToScheme(scheme))
}

// labelsFlag implements flag.Value for a comma-separated list of key=value pairs.
// Example: -default-labels=app=myapp,env=prod
type labelsFlag map[string]string

func (f labelsFlag) String() string {
	pairs := make([]string, 0, len(f))
	for k, v := range f {
		pairs = append(pairs, k+"="+v)
	}
	slices.Sort(pairs)
	return strings.Join(pairs, ",")
}

func (f labelsFlag) Set(s string) error {
	if s == "" {
		return nil
	}
	for pair := range strings.SplitSeq(s, ",") {
		k, v, ok := strings.Cut(pair, "=")
		if !ok || k == "" {
			return fmt.Errorf("invalid label %q: must be key=value", pair)
		}
		f[k] = v
	}
	return nil
}

func main() {
	c := NewController()

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: %s [flags]\n\nFlags:\n", os.Args[0])
		fs.PrintDefaults()
	}

	// Flags to override DefaultWorkloadConfig.
	fs.IntVar(&c.DefaultWorkloadConfig.LabeledPercentage, "default-labeled-percentage", c.DefaultWorkloadConfig.LabeledPercentage,
		"Default percentage of pods to label with the node selector (0-100)")
	fs.IntVar(&c.DefaultWorkloadConfig.MinUnlabeled, "default-min-unlabeled", c.DefaultWorkloadConfig.MinUnlabeled,
		"Default minimum number of unlabeled pods to maintain at all times")
	fs.Var(labelsFlag(c.DefaultWorkloadConfig.Labels), "default-labels",
		"Default labels applied to the labeled pod subset, as a comma-separated key=value list (e.g. app=myapp,env=prod)")
	// TODO: NodeSelector, Tolerations
	fs.DurationVar(&c.DefaultWorkloadConfig.CheckInterval, "default-check-interval", c.DefaultWorkloadConfig.CheckInterval,
		"Default interval between workload state re-evaluations")
	fs.DurationVar(&c.DefaultWorkloadConfig.ScheduleTimeout, "default-schedule-timeout", c.DefaultWorkloadConfig.ScheduleTimeout,
		"Default time a labeled pod may remain Pending before the rebalancer is disabled and pending pods are evicted")
	fs.DurationVar(&c.DefaultWorkloadConfig.FallbackInterval, "default-fallback-interval", c.DefaultWorkloadConfig.FallbackInterval,
		"Default extra delay added to disabled-until before the reconciler resumes normal operation")

	// Flags to override Config.
	fs.StringVar(&c.Config.ConfigAnnotation, "config-annotation", c.Config.ConfigAnnotation,
		"Annotation key for per-workload JSON config")
	fs.StringVar(&c.Config.EnabledLabel, "enabled-label", c.Config.EnabledLabel,
		"Label key that marks a Deployment as managed by this rebalancer")
	fs.StringVar(&c.Config.DisabledUntilAnnotation, "disabled-until-annotation", c.Config.DisabledUntilAnnotation,
		"Annotation key set on a Deployment when the rebalancer gate is temporarily disabled")
	fs.StringVar(&c.Config.SchedulingGateName, "scheduling-gate", c.Config.SchedulingGateName,
		"Name of the scheduling gate added to pods by the webhook")

	var (
		webhookPort    int
		webhookCertDir string
		healthAddr     string
	)
	fs.IntVar(&webhookPort, "webhook-port", 9443, "Port the mutating webhook server listens on")
	fs.StringVar(&webhookCertDir, "webhook-cert-dir", os.Getenv("WEBHOOK_CERT_DIR"),
		"Directory containing TLS cert and key for the webhook server")
	fs.StringVar(&healthAddr, "health-address",
		":8081", "TCP address for serving health probes. Use /readyz path for readiness check.")

	config.RegisterFlags(fs)

	var opts zap.Options
	opts.BindFlags(fs)

	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	logger := ctrl.Log.WithName("main")

	if err := run(ctrl.SetupSignalHandler(), ctrl.GetConfigOrDie(), webhookPort, webhookCertDir, healthAddr, c); err != nil {
		logger.Error(err, "Failed to run")
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg *rest.Config, webhookPort int, webhookCertDir string, healthAddr string, c *Controller) error {
	logger := ctrl.Log.WithName("main")

	srv := webhook.NewServer(webhook.Options{
		Port:    webhookPort,
		CertDir: webhookCertDir,
	})
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		WebhookServer:          srv,
		HealthProbeBindAddress: healthAddr,
	})
	if err != nil {
		return fmt.Errorf("unable to create manager: %w", err)
	}

	if err := mgr.AddReadyzCheck("webhook", srv.StartedChecker()); err != nil {
		return fmt.Errorf("unable to set up webhook readiness check: %w", err)
	}

	if err := c.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to set up workload controller: %w", err)
	}

	logger.Info("Starting manager")
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("problem running manager: %w", err)
	}
	return nil
}
