# Rebalancer

This controller maintains a configured ratio of labeled to unlabeled pods per Deployment.
Labeled pods may also have configured node selector and tolerations.

## How it works

- A mutating Pod webhook adds a scheduling gate to newly created Pods of enabled Deployment labeled `rebalancer/enabled: "true"`.
- The controller watches enabled Deployments and merges per-Deployment config from the `rebalancer/config` JSON-encoded annotation with controller defaults.
- During reconcilation the controller groups Pods by ReplicaSet and partitions them into: gated (have the scheduling gate), labeled (carry all desired labels), and unlabeled (missing labels).
- To label Pods the controller removes the scheduling gate and patches Pods to add the configured labels, nodeSelector and tolerations up to the computed target count: `target = min(floor(total * labeledPercentage/100), total - minUnlabeled)`.
- If labeled Pods remain Pending longer than `scheduleTimeout` (e.g. due to nodeSelector), the controller temporary disables Deployment (fallback) by setting the `rebalancer/disabled-until: <timestamp>` annotation and evicts pending labeled Pods. The webhook does not add gates and controller does not rebalance disabled Deployment Pods.
- To rebalance enabled Deployment, the controller evicts Pods so replacements end up labeled or unlabeled as needed.

## Configuration

The controller default configuration values may be set by command line flags and overriden per-Deployment via annotation.

## Example

Example enabled Deployment with JSON config overriding defaults:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx
  labels:
    app: nginx
    rebalancer/enabled: "true"
  annotations:
    rebalancer/config: |
      {"labeledPercentage": 60, "minUnlabeled": 2}
spec:
  replicas: 10
  selector:
    matchLabels:
      deployment: nginx
  template:
    metadata:
      labels:
        app: nginx
        deployment: nginx
    spec:
      containers:
        - name: nginx
          image: registry.k8s.io/pause:3.10.1

```

Six pods will be labeled and the remaining four will be unlabeled:

```console
$ kubectl get pods -lapp=nginx -Lrebalancer/labeled
NAME                     READY   STATUS    RESTARTS   AGE   LABELED
nginx-547c4b8d5d-2nzfs   1/1     Running   0          11m   true
nginx-547c4b8d5d-4nmcl   1/1     Running   0          11m
nginx-547c4b8d5d-846fr   1/1     Running   0          11m   true
nginx-547c4b8d5d-8w6kw   1/1     Running   0          11m   true
nginx-547c4b8d5d-grcnd   1/1     Running   0          11m
nginx-547c4b8d5d-h6pmk   1/1     Running   0          11m   true
nginx-547c4b8d5d-mnmtj   1/1     Running   0          11m   true
nginx-547c4b8d5d-sqfrh   1/1     Running   0          11m
nginx-547c4b8d5d-vq2x6   1/1     Running   0          11m
nginx-547c4b8d5d-xcs28   1/1     Running   0          11m   true
```

## Test in kind cluster

```sh
kind create cluster

kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.20.2/cert-manager.yaml

kubectl apply -f testdata/cert.yaml

docker build -t rebalancer .

kind load docker-image rebalancer:latest

kubectl apply -f testdata/rebalancer.yaml
```

```sh
kubectl apply -f testdata/nginx.yaml

kubectl get pods -lapp=nginx -Lrebalancer/labeled

kubectl get pods -l app=nginx -o custom-columns="NAME:.metadata.name,LABELED:.metadata.labels['rebalancer/labeled'],NODESELECTOR:.spec.nodeSelector,TOLERATIONS:.spec.tolerations[?(@.key=='nodepool')]"
```
