package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"maps"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"k8s.io/client-go/dynamic"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"

	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	kindv1alpha4 "sigs.k8s.io/kind/pkg/apis/config/v1alpha4"
	"sigs.k8s.io/kind/pkg/cluster"

	"sigs.k8s.io/yaml"
)

// ControllerSuite holds shared state for all controller integration tests.
type ControllerSuite struct {
	suite.Suite

	// cluster lifecycle
	provider    *cluster.Provider
	clusterName string

	// controller under test
	c *Controller

	// admin/rest config from kind kubeconfig
	restCfg *rest.Config
	// shared k8s clients / state
	directClient client.Client
	reservedNode string

	// manager lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	mgrDone chan error

	// per-test namespace (set in SetupTest)
	ns string
}

// TestController is the single Go test entry-point that runs the suite.
func TestController(t *testing.T) {
	suite.Run(t, new(ControllerSuite))
}

// Write redirects test namespace logs to the current test output.
func (s *ControllerSuite) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte(`"`+s.ns+`"`)) {
		return s.T().Output().Write(p)
	}
	return len(p), nil
}

// SetupSuite starts the kind cluster, the controller manager,
// and registers the mutating webhook.
func (s *ControllerSuite) SetupSuite() {
	t := s.T()
	ctrl.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(s)))

	s.clusterName = fmt.Sprintf("rebalancer-test-%d", time.Now().UnixNano())
	s.provider = cluster.NewProvider()

	t.Logf("Creating kind cluster %q", s.clusterName)
	kindCfg := &kindv1alpha4.Cluster{
		Nodes: []kindv1alpha4.Node{
			{Role: kindv1alpha4.ControlPlaneRole},
			{Role: kindv1alpha4.WorkerRole},
			{
				Role:   kindv1alpha4.WorkerRole,
				Labels: map[string]string{"nodepool": "reserved"},
			},
		},
	}
	require.NoError(t, s.provider.Create(s.clusterName,
		cluster.CreateWithV1Alpha4Config(kindCfg),
		cluster.CreateWithWaitForReady(5*time.Minute),
	), "Failed to create kind cluster")

	kubeconfigRaw, err := s.provider.KubeConfig(s.clusterName, false)
	require.NoError(t, err, "Failed to get kubeconfig")

	s.restCfg, err = clientcmd.RESTConfigFromKubeConfig([]byte(kubeconfigRaw))
	require.NoError(t, err, "Failed to build REST config")

	hostIP := kindHostIP(t, s.clusterName)
	t.Logf("Host IP reachable from kind: %s", hostIP)

	certDir := t.TempDir()
	caBundle := generateWebhookCert(t, hostIP, certDir)

	webhookPort := freePort(t)
	t.Logf("Webhook server port: %d", webhookPort)

	s.c = NewController()
	s.c.DefaultWorkloadConfig.LabeledPercentage = 60
	s.c.DefaultWorkloadConfig.MinUnlabeled = 4
	s.c.DefaultWorkloadConfig.NodeSelector = map[string]string{"nodepool": "reserved"}
	s.c.DefaultWorkloadConfig.Tolerations = []corev1.Toleration{{
		Key:      "nodepool",
		Operator: corev1.TolerationOpEqual,
		Value:    "reserved",
		Effect:   corev1.TaintEffectNoSchedule,
	}}
	s.c.DefaultWorkloadConfig.CheckInterval = 5 * time.Second
	s.c.DefaultWorkloadConfig.ScheduleTimeout = 20 * time.Second

	s.configureRBAC()

	// Start the manager using a specific ServiceAccount from the RBAC manifest.
	cfg := rest.CopyConfig(s.restCfg)
	cfg.Impersonate = rest.ImpersonationConfig{
		UserName: "system:serviceaccount:default:rebalancer",
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		WebhookServer: webhook.NewServer(webhook.Options{
			Port:    webhookPort,
			CertDir: certDir,
		}),
	})
	require.NoError(t, err, "Failed to create manager")
	require.NoError(t, s.c.SetupWithManager(mgr), "Failed to set up controller")

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.mgrDone = make(chan error, 1)
	go func() {
		s.mgrDone <- mgr.Start(s.ctx)
	}()

	require.True(t, mgr.GetCache().WaitForCacheSync(s.ctx), "Cache never synced")
	waitForWebhook(t, s.ctx, hostIP, webhookPort)

	clientScheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(clientScheme))
	require.NoError(t, appsv1.AddToScheme(clientScheme))
	require.NoError(t, admissionregistrationv1.AddToScheme(clientScheme))

	s.directClient, err = client.New(s.restCfg, client.Options{Scheme: clientScheme})
	require.NoError(t, err, "Failed to create direct client")

	registerWebhook(t, s.ctx, s.directClient, hostIP, webhookPort, caBundle)

	s.reservedNode = setupNodes(t, s.ctx, s.directClient)
}

// TearDownSuite stops the manager and deletes the kind cluster.
func (s *ControllerSuite) TearDownSuite() {
	t := s.T()
	s.cancel()
	if err := <-s.mgrDone; err != nil && s.ctx.Err() == nil {
		t.Errorf("Manager exited with error: %v", err)
	}
	t.Logf("Deleting kind cluster %q", s.clusterName)
	if err := s.provider.Delete(s.clusterName, ""); err != nil {
		t.Errorf("Failed to delete kind cluster: %v", err)
	}
}

// SetupTest creates a fresh namespace for the current test case.
func (s *ControllerSuite) SetupTest() {
	t := s.T()
	s.ns = testNamespace(t)
	t.Logf("Namespace: %s", s.ns)
	require.NoError(t, s.directClient.Create(s.ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: s.ns},
	}), "Failed to create namespace")
}

// listPods returns all pods in the current test namespace matching the given options.
func (s *ControllerSuite) listPods(opts ...client.ListOption) []*corev1.Pod {
	s.T().Helper()
	var podList corev1.PodList
	opts = append([]client.ListOption{client.InNamespace(s.ns)}, opts...)
	require.NoError(s.T(), s.directClient.List(s.ctx, &podList, opts...), "Failed to list pods")
	return ptrList(podList.Items, nil)
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

func (s *ControllerSuite) requireEventuallyHasPhaseCounts(expected map[corev1.PodPhase]int, opts ...client.ListOption) {
	t := s.T()
	t.Helper()
	require.Eventually(t, func() bool {
		pods := s.listPods(client.MatchingLabels{"deployment": "nginx"})
		byPhase := countByPhase(pods)
		t.Logf("Pods: %v", byPhase)
		return maps.Equal(expected, byPhase)
	}, 1*time.Minute, 1*time.Second)
}

// --- Test cases ---

func (s *ControllerSuite) TestEnabledDeploymentIsLabeled() {
	t := s.T()
	const (
		deployName    = "nginx"
		replicas      = 10
		wantLabeled   = 6
		wantUnlabeled = 4
	)

	deploy := s.newDeployment(deployName, replicas)
	deploy.Labels[s.c.Config.EnabledLabel] = "true"

	require.NoError(t, s.directClient.Create(s.ctx, deploy))

	// Step 1: wait for all pods to be Running.
	s.requireEventuallyHasPhaseCounts(map[corev1.PodPhase]int{corev1.PodRunning: replicas}, client.MatchingLabels{"deployment": "nginx"})

	// Step 2: assert labeled / unlabeled split.
	pods := s.listPods(client.MatchingLabels{"deployment": "nginx"})

	labeled, unlabeled := s.partitionLabeled(pods)

	assert.Len(t, labeled, wantLabeled, "wrong number of labeled pods")
	assert.Len(t, unlabeled, wantUnlabeled, "wrong number of unlabeled pods")

	// Step 3: assert node placement.
	for _, pod := range labeled {
		assert.Equal(t, s.reservedNode, pod.Spec.NodeName,
			"labeled pod %s should be on reserved node", pod.Name)
	}
	for _, pod := range unlabeled {
		assert.NotEqual(t, s.reservedNode, pod.Spec.NodeName,
			"unlabeled pod %s should NOT be on reserved node", pod.Name)
	}
}

func (s *ControllerSuite) TestEnabledDeploymentRollout() {
	t := s.T()
	const (
		deployName    = "nginx"
		replicas      = 10
		wantLabeled   = 6
		wantUnlabeled = 4
	)

	deploy := s.newDeployment(deployName, replicas)
	deploy.Labels[s.c.Config.EnabledLabel] = "true"

	require.NoError(t, s.directClient.Create(s.ctx, deploy))

	// Step 1: wait for all pods to be Running.
	s.requireEventuallyHasPhaseCounts(map[corev1.PodPhase]int{corev1.PodRunning: replicas}, client.MatchingLabels{"deployment": "nginx"})

	// Step 2: rollout by patching the deployment pod spec template.
	t.Logf("Rollout")
	rolloutPatch := client.MergeFrom(deploy.DeepCopy())
	if deploy.Spec.Template.Annotations == nil {
		deploy.Spec.Template.Annotations = make(map[string]string)
	}
	deploy.Spec.Template.Annotations["test/rollout-at"] = time.Now().UTC().Format(time.RFC3339)
	require.NoError(t, s.directClient.Patch(s.ctx, deploy, rolloutPatch))

	// Wait for rollout to complete: all pods running.
	s.requireEventuallyHasPhaseCounts(map[corev1.PodPhase]int{corev1.PodRunning: replicas}, client.MatchingLabels{"deployment": "nginx"})

	// Step 3: assert labeled / unlabeled split.
	pods := s.listPods(client.MatchingLabels{"deployment": "nginx"})

	labeled, unlabeled := s.partitionLabeled(pods)

	assert.Len(t, labeled, wantLabeled, "wrong number of labeled pods")
	assert.Len(t, unlabeled, wantUnlabeled, "wrong number of unlabeled pods")
}

func (s *ControllerSuite) TestDeploymentEnable() {
	t := s.T()
	const (
		deployName    = "nginx"
		replicas      = 10
		wantLabeled   = 6
		wantUnlabeled = 4
	)

	deploy := s.newDeployment(deployName, replicas)
	require.NoError(t, s.directClient.Create(s.ctx, deploy))

	// Step 1: wait for all pods to be Running.
	s.requireEventuallyHasPhaseCounts(map[corev1.PodPhase]int{corev1.PodRunning: replicas}, client.MatchingLabels{"deployment": "nginx"})

	// Step 2: enable deployment
	t.Logf("Enabling deployment")
	labelPatch := client.MergeFrom(deploy.DeepCopy())
	if deploy.Labels == nil {
		deploy.Labels = make(map[string]string)
	}
	deploy.Labels[s.c.Config.EnabledLabel] = "true"
	require.NoError(t, s.directClient.Patch(s.ctx, deploy, labelPatch))

	// Step 3: assert labeled / unlabeled split.
	require.Eventually(t, func() bool {
		pods := s.listPods(client.MatchingLabels{"deployment": "nginx"})
		byPhase := countByPhase(pods)
		t.Logf("Pods: %v", byPhase)
		if byPhase[corev1.PodRunning] != replicas {
			return false
		}

		labeled, unlabeled := s.partitionLabeled(pods)

		t.Logf("unlabeled: %d, labeled: %d", len(unlabeled), len(labeled))
		return len(labeled) == wantLabeled && len(unlabeled) == wantUnlabeled
	}, 1*time.Minute, 1*time.Second)
}

func (s *ControllerSuite) TestDeploymentEnableFallback() {
	t := s.T()
	const (
		deployName    = "nginx"
		replicas      = 10
		wantLabeled   = 6
		wantUnlabeled = 4
	)

	deploy := s.newDeployment(deployName, replicas)
	require.NoError(t, s.directClient.Create(s.ctx, deploy))

	// Step 1: wait for all pods to be Running.
	s.requireEventuallyHasPhaseCounts(map[corev1.PodPhase]int{corev1.PodRunning: replicas}, client.MatchingLabels{"deployment": "nginx"})

	// Step 2: cordon reserved node
	s.cordonReservedNode()

	// Step 3: enable deployment, rebalancing will evict pods that will end up pending
	t.Logf("Enabling deployment")
	labelPatch := client.MergeFrom(deploy.DeepCopy())
	if deploy.Labels == nil {
		deploy.Labels = make(map[string]string)
	}
	deploy.Labels[s.c.Config.EnabledLabel] = "true"
	require.NoError(t, s.directClient.Patch(s.ctx, deploy, labelPatch))

	// Step 4: assert fallback (zero labeled)
	require.Eventually(t, func() bool {
		var d appsv1.Deployment
		require.NoError(t, s.directClient.Get(s.ctx, client.ObjectKeyFromObject(deploy), &d))

		if ts, ok := d.Annotations[s.c.Config.DisabledUntilAnnotation]; !ok {
			return false
		} else {
			t.Logf("Disabled until: %s", ts)
		}

		pods := s.listPods(client.MatchingLabels{"deployment": "nginx"})
		byPhase := countByPhase(pods)
		t.Logf("Pods: %v", byPhase)
		if byPhase[corev1.PodRunning] != replicas {
			return false
		}

		labeled, unlabeled := s.partitionLabeled(pods)

		t.Logf("unlabeled: %d, labeled: %d", len(unlabeled), len(labeled))
		return len(labeled) == 0 && len(unlabeled) == replicas
	}, 2*time.Minute, 2*time.Second)

	// Step 5: uncordon reserved node
	s.uncordonReservedNode()

	// Step 6: assert rebalanced
	require.Eventually(t, func() bool {
		pods := s.listPods(client.MatchingLabels{"deployment": "nginx"})
		byPhase := countByPhase(pods)
		t.Logf("Pods: %v", byPhase)
		if byPhase[corev1.PodRunning] != replicas {
			return false
		}

		labeled, unlabeled := s.partitionLabeled(pods)

		t.Logf("unlabeled: %d, labeled: %d", len(unlabeled), len(labeled))
		return len(labeled) == wantLabeled && len(unlabeled) == wantUnlabeled
	}, 2*time.Minute, 2*time.Second)
}

func (s *ControllerSuite) newDeployment(name string, replicas int32) *appsv1.Deployment {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: s.ns,
			Labels:    map[string]string{},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"deployment": name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"deployment": name},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "pause",
						Image: "registry.k8s.io/pause:3.10.1",
					}},
				},
			},
		},
	}
	return deploy
}

// cordonReservedNode marks the reserved node as unschedulable (kubectl cordon).
func (s *ControllerSuite) cordonReservedNode() {
	s.T().Helper()
	var node corev1.Node
	require.NoError(s.T(), s.directClient.Get(s.ctx, client.ObjectKey{Name: s.reservedNode}, &node))
	patch := client.MergeFrom(node.DeepCopy())
	node.Spec.Unschedulable = true
	require.NoError(s.T(), s.directClient.Patch(s.ctx, &node, patch), "Failed to cordon node %s", s.reservedNode)
	s.T().Logf("Cordoned node %s", s.reservedNode)
}

// uncordonReservedNode marks the reserved node as schedulable again (kubectl uncordon).
func (s *ControllerSuite) uncordonReservedNode() {
	s.T().Helper()
	var node corev1.Node
	require.NoError(s.T(), s.directClient.Get(s.ctx, client.ObjectKey{Name: s.reservedNode}, &node))
	patch := client.MergeFrom(node.DeepCopy())
	node.Spec.Unschedulable = false
	require.NoError(s.T(), s.directClient.Patch(s.ctx, &node, patch), "Failed to uncordon node %s", s.reservedNode)
	s.T().Logf("Uncordoned node %s", s.reservedNode)
}

func (s *ControllerSuite) partitionLabeled(pods []*corev1.Pod) ([]*corev1.Pod, []*corev1.Pod) {
	var labeled, unlabeled []*corev1.Pod
	for _, pod := range pods {
		if isLabeled(pod, s.c.DefaultWorkloadConfig) {
			labeled = append(labeled, pod)
		} else {
			unlabeled = append(unlabeled, pod)
		}
	}
	return labeled, unlabeled
}

// isLabeled reports whether pod has all controller-applied labels, node
// selector, and tolerations (i.e. it was fully processed as a "labeled" pod).
func isLabeled(pod *corev1.Pod, cfg WorkloadConfig) bool {
	for k, v := range cfg.Labels {
		if pod.Labels[k] != v {
			return false
		}
	}
	for k, v := range cfg.NodeSelector {
		if pod.Spec.NodeSelector[k] != v {
			return false
		}
	}
outer:
	for _, want := range cfg.Tolerations {
		for _, got := range pod.Spec.Tolerations {
			if got.Key == want.Key &&
				got.Operator == want.Operator &&
				got.Value == want.Value &&
				got.Effect == want.Effect {
				continue outer
			}
		}
		return false
	}
	return true
}

// kindHostIP returns the IPv4 address that kind nodes can use to reach the
// host. On macOS Docker Desktop, host.docker.internal resolves to the correct
// host IP from within containers; on Linux the docker-network gateway works.
func kindHostIP(t *testing.T, clusterName string) string {
	t.Helper()

	// Try resolving host.docker.internal from inside the kind control-plane
	// node (available in Docker Desktop for Mac and Windows).
	node := clusterName + "-control-plane"
	out, err := exec.Command("docker", "exec", node, "sh", "-c",
		"getent hosts host.docker.internal 2>/dev/null | awk '{print $1}'").Output()
	if err == nil {
		if ip := strings.TrimSpace(string(out)); ip != "" {
			if parsed := net.ParseIP(ip); parsed != nil && parsed.To4() != nil {
				t.Logf("Host IP via host.docker.internal: %s", ip)
				return ip
			}
		}
	}

	// Fall back to the IPv4 gateway of the "kind" docker network (Linux).
	out, err = exec.Command("docker", "network", "inspect", "kind",
		"--format", "{{range .IPAM.Config}}{{.Gateway}} {{end}}").Output()
	require.NoError(t, err, "Failed to inspect kind network")
	for _, addr := range strings.Fields(string(out)) {
		if ip := net.ParseIP(addr); ip != nil && ip.To4() != nil {
			t.Logf("Host IP via kind network gateway: %s", addr)
			return addr
		}
	}
	require.Fail(t, "No IPv4 host IP found reachable from kind nodes")
	return ""
}

// freePort returns a free TCP port on the local machine.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err, "Failed to find a free port")
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// generateWebhookCert creates a self-signed TLS certificate whose SAN contains
// hostIP, writes tls.crt and tls.key into dir, and returns the PEM-encoded
// certificate (the CA bundle to put in the MutatingWebhookConfiguration).
func generateWebhookCert(t *testing.T, hostIP string, dir string) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err, "Failed to generate ECDSA key")

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"rebalancer-test"}},
		IPAddresses:           []net.IP{net.ParseIP(hostIP)},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err, "Failed to create certificate")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err, "Failed to marshal EC private key")
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	require.NoError(t, os.WriteFile(filepath.Join(dir, "tls.crt"), certPEM, 0o600), "Failed to write tls.crt")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tls.key"), keyPEM, 0o600), "Failed to write tls.key")
	return certPEM
}

// waitForWebhook blocks until the webhook server is accepting TCP connections
// on the local machine on the given port, or until ctx is cancelled.
// (The server binds to all interfaces, so 127.0.0.1 always works on the host.)
func waitForWebhook(t *testing.T, ctx context.Context, _ string, port int) {
	t.Helper()
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	t.Logf("Waiting for webhook server at %s", addr)
	for {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			t.Logf("Webhook server is ready at %s", addr)
			// Brief pause to let the TLS handshaker fully initialise.
			time.Sleep(500 * time.Millisecond)
			return
		}
		select {
		case <-ctx.Done():
			require.Fail(t, "Context cancelled while waiting for webhook server")
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// registerWebhook creates a MutatingWebhookConfiguration that routes pod
// CREATE calls to the in-process webhook server running on ip:port.
func registerWebhook(t *testing.T, ctx context.Context, c client.Client, ip string, port int, caBundle []byte) {
	t.Helper()

	url := fmt.Sprintf("https://%s/mutate-v1-pod", net.JoinHostPort(ip, strconv.Itoa(port)))
	sideEffects := admissionregistrationv1.SideEffectClassNone
	failurePolicy := admissionregistrationv1.Fail

	whc := &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "rebalancer-test-pod-webhook"},
		Webhooks: []admissionregistrationv1.MutatingWebhook{{
			Name:                    "rebalancer.test.pod",
			AdmissionReviewVersions: []string{"v1"},
			SideEffects:             &sideEffects,
			FailurePolicy:           &failurePolicy,
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				URL:      &url,
				CABundle: caBundle,
			},
			Rules: []admissionregistrationv1.RuleWithOperations{{
				Operations: []admissionregistrationv1.OperationType{
					admissionregistrationv1.OperationAll,
				},
				Rule: admissionregistrationv1.Rule{
					APIGroups:   []string{""},
					APIVersions: []string{"v1"},
					Resources:   []string{"pods"},
				},
			}},
		}},
	}

	require.NoError(t, c.Create(ctx, whc), "Failed to create MutatingWebhookConfiguration")
	t.Cleanup(func() {
		_ = c.Delete(context.Background(), whc)
	})
	t.Logf("Registered MutatingWebhookConfiguration pointing to %s", url)
}

// setupNodes applies the rebalancer/pool=reserved:NoSchedule taint to the
// worker node that has the rebalancer/pool=reserved label.
// It returns the name of the reserved (worker) node.
func setupNodes(t *testing.T, ctx context.Context, c client.Client) string {
	t.Helper()

	var nodeList corev1.NodeList
	require.NoError(t, c.List(ctx, &nodeList))

	var reservedNodeName string
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if node.Labels["nodepool"] != "reserved" {
			continue
		}
		patch := client.MergeFrom(node.DeepCopy())
		reservedNodeName = node.Name
		node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{
			Key:    "nodepool",
			Value:  "reserved",
			Effect: corev1.TaintEffectNoSchedule,
		})
		require.NoError(t, c.Patch(ctx, node, patch), "Failed to patch node %s", node.Name)
		t.Logf("Patched node %s (taints: %v)", node.Name, node.Spec.Taints)
	}

	require.NotEmpty(t, reservedNodeName, "reserved worker node with rebalancer/pool=reserved label not found")
	return reservedNodeName
}

// testNamespace derives a valid Kubernetes namespace name from the current
// test name by lower-casing it and replacing non-alphanumeric runs with "-".
func testNamespace(t *testing.T) string {
	t.Helper()
	name := strings.ToLower(t.Name())
	name = regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	if len(name) > 63 {
		name = name[:63]
	}
	return name
}

// configureRBAC creates ServiceAccount, ClusterRole and ClusterRoleBinding objects.
func (s *ControllerSuite) configureRBAC() {
	t := s.T()
	t.Helper()

	b, err := os.ReadFile("testdata/rbac.yaml")
	require.NoError(t, err)

	docs := strings.Split(string(b), "\n---\n")

	dc, err := dynamic.NewForConfig(s.restCfg)
	require.NoError(t, err)
	httpClient, err := rest.HTTPClientFor(s.restCfg)
	require.NoError(t, err)
	mapper, err := apiutil.NewDynamicRESTMapper(s.restCfg, httpClient)
	require.NoError(t, err)

	for _, d := range docs {
		d = strings.TrimSpace(d)
		if d == "" {
			continue
		}
		j, err := yaml.YAMLToJSON([]byte(d))
		require.NoError(t, err)

		var u unstructured.Unstructured
		err = u.UnmarshalJSON(j)
		require.NoError(t, err)

		gvk := u.GroupVersionKind()
		mapping, err := mapper.RESTMapping(schema.GroupKind{Group: gvk.Group, Kind: gvk.Kind}, gvk.Version)
		require.NoError(t, err)

		resClient := dc.Resource(mapping.Resource)
		if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
			ns := u.GetNamespace()
			if ns == "" {
				ns = "default"
			}
			_, err := resClient.Namespace(ns).Create(t.Context(), &u, metav1.CreateOptions{})
			require.NoError(t, err)

			t.Logf("Created %s %s/%s", gvk.Kind, ns, u.GetName())
		} else {
			_, err := resClient.Create(t.Context(), &u, metav1.CreateOptions{})
			require.NoError(t, err)

			t.Logf("Created %s %s", gvk.Kind, u.GetName())
		}
	}
}
