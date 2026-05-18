package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// PodWebhook is a mutating admission webhook that adds a scheduling gate to
// every pod whose Deployment labels include EnabledLabel="true", i.e. pods
// created by a managed Deployment.
type PodWebhook struct {
	// Reader is the manager's cache-backed client reader.
	// Deployments are already cached because DeploymentReconciler watches them.
	Reader  client.Reader
	Decoder admission.Decoder
	Config  Config
}

func (w *PodWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	if req.Operation != admissionv1.Create {
		return admission.Allowed("operation is not CREATE")
	}

	pod := &corev1.Pod{}
	if err := w.Decoder.DecodeRaw(req.Object, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	if w.Config.hasSchedulingGate(pod) {
		return admission.Allowed("gate already present")
	}

	_, deployName := deploymentName(pod)
	if deployName == "" {
		return admission.Allowed("cannot derive Deployment name from pod")
	}

	var deploy appsv1.Deployment
	if err := w.Reader.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: deployName}, &deploy); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	if deploy.Labels[w.Config.EnabledLabel] != "true" {
		return admission.Allowed("owning Deployment is not managed")
	}

	if t, err := parseDisabledUntil(deploy.Annotations[w.Config.DisabledUntilAnnotation]); err == nil && !t.IsZero() && time.Now().Before(t) {
		return admission.Allowed("rebalancer disabled until " + deploy.Annotations[w.Config.DisabledUntilAnnotation])
	}

	pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{
		Name: w.Config.SchedulingGateName,
	})

	marshaled, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
}
