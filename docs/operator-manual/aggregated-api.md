# Aggregated API (views.argoproj.io)

> [!WARNING]
> **Alpha Feature (Since v3.7.0)**
>
> This is an experimental, [alpha-quality](https://github.com/argoproj/argoproj/blob/main/community/feature-status.md#alpha)
> feature. It may be removed in future releases or modified in backwards-incompatible ways.

Argo CD computes state that is not stored in any Kubernetes resource: the live resource tree of each Application and
the diff between the live and the desired state of its resources. Usually, this state is only available through the
Argo CD API, CLI and UI.

The optional `argocd-aggregated-apiserver` component serves this state as read-only Kubernetes resources, through the
[Kubernetes API aggregation layer](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/apiserver-aggregation/):

| Resource           | Kind              | Short name | Content                                                                                      |
|--------------------|-------------------|------------|----------------------------------------------------------------------------------------------|
| `applicationtrees` | `ApplicationTree` | `apptree`  | The resource tree of the Application, as returned by `argocd app resources`.                 |
| `applicationdiffs` | `ApplicationDiff` | `appdiff`  | The diff of each managed resource, as used by `argocd app diff`. Hook resources are omitted. |

This lets Kubernetes-native tools such as `kubectl`, client-go informers and other controllers read and watch the
state of Applications with their Kubernetes credentials, and records the requests in the Kubernetes audit log.

Every Application has exactly one `ApplicationTree` and one `ApplicationDiff`, with the same namespace and name. They
exist as long as the Application exists. Only the `get`, `list` and `watch` verbs are supported.

> [!NOTE]
> The `views.argoproj.io` API group is always excluded from Argo CD's resource tracking, like `metrics.k8s.io`.
> `ApplicationTree` and `ApplicationDiff` resources never appear in the resource tree of an Application, and are never
> considered for pruning.

## Considerations

> [!WARNING]
> When the `argocd-aggregated-apiserver` pods are unavailable, the `v1alpha1.views.argoproj.io` APIService becomes
> unavailable. Kubernetes clients which discover all API groups of the cluster, such as `kubectl api-resources`, Helm or
> controllers, then report discovery errors for the `views.argoproj.io` group until the pods are available again.
> Run at least two replicas (the default) and keep the PodDisruptionBudget.

* The component reads the state computed by the application controller from Redis. It never triggers a refresh. If
  the controller has not computed the state of an Application yet, the resource is still returned, with the `Ready`
  condition set to `False`.
* The resources are served by the cluster Argo CD is installed in. For Applications deploying to other clusters, they
  describe resources of those clusters.
* Secret data is masked in `ApplicationDiff` resources, like in the Argo CD API.

## Installation

The component is a [Kustomize component](https://kubectl.docs.kubernetes.io/guides/config_management/components/)
which you add to the kustomization installing Argo CD. It must be installed in the `argocd` namespace.

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: argocd
resources:
- https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
components:
- https://github.com/argoproj/argo-cd/manifests/aggregated-api?ref=stable
```

The component adds:

* the `argocd-aggregated-apiserver` Deployment, Service, PodDisruptionBudget and NetworkPolicy, and an ingress rule
  allowing it to reach Redis
* the `v1alpha1.views.argoproj.io` APIService
* the RBAC needed to read Applications, AppProjects, `argocd-cm` and `argocd-rbac-cm`, and to delegate authentication
  and authorization to kube-apiserver (`system:auth-delegator`)
* the `argocd-views-reader` ClusterRole, which you bind to the users who may read the resources (see below)

Check that the API is available:

```shell
kubectl get apiservice v1alpha1.views.argoproj.io
kubectl api-resources --api-group=views.argoproj.io
```

### Serving certificate

By default, the aggregated API server generates a self-signed certificate and the APIService sets
`insecureSkipTLSVerify: true`, like [metrics-server](https://github.com/kubernetes-sigs/metrics-server) does.
kube-apiserver still authenticates itself to the aggregated API server with its front proxy client certificate.

To have kube-apiserver verify the serving certificate, issue a certificate for
`argocd-aggregated-apiserver.argocd.svc`, for instance with [cert-manager](https://cert-manager.io), and patch the
component:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: argocd
resources:
- https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
- certificate.yaml
components:
- https://github.com/argoproj/argo-cd/manifests/aggregated-api?ref=stable
patches:
- target:
    kind: APIService
    name: v1alpha1.views.argoproj.io
  patch: |-
    - op: replace
      path: /spec/insecureSkipTLSVerify
      value: false
    - op: add
      path: /metadata/annotations
      value:
        cert-manager.io/inject-ca-from: argocd/argocd-aggregated-apiserver-tls
- target:
    kind: Deployment
    name: argocd-aggregated-apiserver
  patch: |-
    - op: add
      path: /spec/template/spec/containers/0/args/-
      value: --tls-cert-file=/app/config/tls/tls.crt
    - op: add
      path: /spec/template/spec/containers/0/args/-
      value: --tls-private-key-file=/app/config/tls/tls.key
    - op: add
      path: /spec/template/spec/containers/0/volumeMounts/-
      value:
        name: tls
        mountPath: /app/config/tls
    - op: add
      path: /spec/template/spec/volumes/-
      value:
        name: tls
        secret:
          secretName: argocd-aggregated-apiserver-tls
```

`certificate.yaml` contains the cert-manager `Certificate` (and its issuer):

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: argocd-aggregated-apiserver-tls
spec:
  secretName: argocd-aggregated-apiserver-tls
  dnsNames:
  - argocd-aggregated-apiserver.argocd.svc
  issuerRef:
    name: my-issuer
    kind: Issuer
```

## Access control

Every request is authorized twice:

1. **Kubernetes RBAC**, enforced by kube-apiserver: the user needs the `get`, `list` or `watch` verb on
   `applicationtrees` or `applicationdiffs` in the namespace of the Application.
2. **Argo CD RBAC**, enforced by the aggregated API server: the user needs the `applications, get` permission on the
   Application in `argocd-rbac-cm` or in an AppProject role. Applications the user may not read are reported as not
   found, and are omitted from lists and watches.

For Argo CD RBAC, the Kubernetes user name and groups are prefixed with `k8s:`, so that a Kubernetes identity can
never be mistaken for an Argo CD local user (such as `admin`) or an SSO group. The prefix can be changed with the
`aggregatedapiserver.rbac.subject.prefix` key of `argocd-cmd-params-cm`.

For example, to let the `deployer` ServiceAccount of the `ci` namespace read the trees and diffs of the Applications of
the `web` project, bind the `argocd-views-reader` ClusterRole in the namespace of the Applications:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ci-deployer-views
  namespace: argocd
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: argocd-views-reader
subjects:
- kind: ServiceAccount
  name: deployer
  namespace: ci
```

And grant it access in `argocd-rbac-cm`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: argocd-rbac-cm
  namespace: argocd
data:
  policy.csv: |
    p, k8s:system:serviceaccount:ci:deployer, applications, get, web/*, allow
    # Kubernetes groups are prefixed too, e.g. all ServiceAccounts of the ci namespace:
    g, k8s:system:serviceaccounts:ci, role:readonly
```

> [!NOTE]
> The default role (`policy.default` in `argocd-rbac-cm`) applies to Kubernetes users too. With
> `policy.default: role:readonly`, every Kubernetes user granted access by Kubernetes RBAC can read all Applications.

## Usage

```shell
kubectl get applicationtrees -n argocd
kubectl get apptree guestbook -n argocd -o yaml
kubectl get appdiff guestbook -n argocd -w
```

```text
NAME        PROJECT   NODES   ORPHANED   READY   AGE
guestbook   default   4       0          True    12d
```

### Discovery

```http
GET /apis/views.argoproj.io/v1alpha1
```

```json
{
  "kind": "APIResourceList",
  "apiVersion": "v1",
  "groupVersion": "views.argoproj.io/v1alpha1",
  "resources": [
    {"name": "applicationdiffs", "singularName": "applicationdiff", "namespaced": true, "kind": "ApplicationDiff", "verbs": ["get", "list", "watch"], "shortNames": ["appdiff"]},
    {"name": "applicationtrees", "singularName": "applicationtree", "namespaced": true, "kind": "ApplicationTree", "verbs": ["get", "list", "watch"], "shortNames": ["apptree"]}
  ]
}
```

### Get

```http
GET /apis/views.argoproj.io/v1alpha1/namespaces/argocd/applicationtrees/guestbook
```

```yaml
apiVersion: views.argoproj.io/v1alpha1
kind: ApplicationTree
metadata:
  name: guestbook
  namespace: argocd
  uid: 6f1c2e7a-0d4b-4c3e-9a51-2f0e1b7c9d10 # the UID of the Application
  resourceVersion: 9c2e4f1a7b3d5e60     # opaque hash of the content
  creationTimestamp: "2026-10-01T12:00:00Z"
  labels:
    team: web                           # the labels of the Application, except the tracking label
tree:
  nodes:
  - group: apps
    version: v1
    kind: Deployment
    namespace: default
    name: guestbook-ui
    uid: 0e8f5b8c-6f1d-4d0c-8f2b-6f4f4b1f2a33
    resourceVersion: "1234"
    health:
      status: Healthy
  - group: apps
    version: v1
    kind: ReplicaSet
    namespace: default
    name: guestbook-ui-5d8f7c9b6
    parentRefs:
    - group: apps
      kind: Deployment
      namespace: default
      name: guestbook-ui
status:
  project: default
  conditions:
  - type: Ready
    status: "True"
    reason: Reconciled
```

```http
GET /apis/views.argoproj.io/v1alpha1/namespaces/argocd/applicationdiffs/guestbook
```

```yaml
apiVersion: views.argoproj.io/v1alpha1
kind: ApplicationDiff
metadata:
  name: guestbook
  namespace: argocd
  resourceVersion: 41aa07c2e9d8b311
resources:
- group: apps
  kind: Deployment
  namespace: default
  name: guestbook-ui
  modified: true
  normalizedLiveState: '{"apiVersion":"apps/v1","kind":"Deployment","spec":{"replicas":1}}'
  predictedLiveState: '{"apiVersion":"apps/v1","kind":"Deployment","spec":{"replicas":3}}'
status:
  project: default
  conditions:
  - type: Ready
    status: "True"
    reason: Reconciled
```

| Situation                                                       | Response                                                                                 |
|-----------------------------------------------------------------|------------------------------------------------------------------------------------------|
| Kubernetes RBAC denies the request                              | `403 Forbidden`, returned by kube-apiserver                                              |
| The Application does not exist, or Argo CD RBAC denies access   | `404 Not Found`                                                                          |
| The application controller has not computed the state yet       | `200 OK`, with the `Ready` condition `False` and reason `NotReconciled`                  |
| The application controller failed to compute the state          | `200 OK`, with the `Ready` condition `False` and reason `ComparisonError`, and a message  |
| Redis is unavailable                                            | `503 Service Unavailable`                                                                |

### List

Lists support label selectors (on the labels of the Application) and the `metadata.name` and `metadata.namespace`
field selectors. `kubectl get` requests lists as tables:

```http
GET /apis/views.argoproj.io/v1alpha1/namespaces/argocd/applicationdiffs?labelSelector=team%3Dweb
Accept: application/json;as=Table;v=v1;g=meta.k8s.io
```

### Watch

`kubectl get apptree guestbook -n argocd -w` watches a single Application:

```http
GET /apis/views.argoproj.io/v1alpha1/namespaces/argocd/applicationtrees?watch=true&fieldSelector=metadata.name%3Dguestbook&resourceVersion=9c2e4f1a7b3d5e60&allowWatchBookmarks=true
```

```json
{"type":"MODIFIED","object":{"kind":"ApplicationTree","apiVersion":"views.argoproj.io/v1alpha1","metadata":{"name":"guestbook","namespace":"argocd","resourceVersion":"a03b6c1d2e4f5a69"},"tree":{"nodes":[]},"status":{"project":"default","conditions":[{"type":"Ready","status":"True","reason":"Reconciled"}]}}}
{"type":"BOOKMARK","object":{"kind":"ApplicationTree","apiVersion":"views.argoproj.io/v1alpha1","metadata":{"resourceVersion":"l5b2c7d0e9f1a3b48"},"tree":{},"status":{"project":""}}}
```

Watches with `sendInitialEvents=true` (watch lists) receive an `ADDED` event for every Application, followed by a
`BOOKMARK` event with the `k8s.io/initial-events-end: "true"` annotation:

```http
GET /apis/views.argoproj.io/v1alpha1/namespaces/argocd/applicationdiffs?watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true
```

Afterwards, a watch receives:

* `MODIFIED` when the tree or diff of an Application changes. Reconciliations which do not change the state do not
  produce events.
* `ADDED` when an Application is created, or becomes visible to the user (e.g. after a label or RBAC change).
* `DELETED` when an Application is deleted, or stops being visible to the user.

Semantics to be aware of when writing clients:

* `resourceVersion`s are opaque content hashes, identical on every replica. They are not ordered.
* A watch resumed from the `resourceVersion` of a list or bookmark served by the same replica receives exactly the
  changes made since. A watch resumed from any other `resourceVersion` receives the current state of every object as
  `MODIFIED` events: clients may receive an event for an object they already have, but never miss a change.
* A client which does not keep up with the events receives a `410 Gone` error and must list again.
* Changes are detected through the Redis notifications of the application controller, and every 3 minutes
  (`aggregatedapiserver.watch.resync.period`) to catch lost notifications.

With client-go:

```go
package main

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	config, err := clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	if err != nil {
		panic(err)
	}
	client := dynamic.NewForConfigOrDie(config)
	trees := client.Resource(schema.GroupVersionResource{Group: "views.argoproj.io", Version: "v1alpha1", Resource: "applicationtrees"}).Namespace("argocd")

	tree, err := trees.Get(context.Background(), "guestbook", metav1.GetOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Println(tree.GetResourceVersion())

	w, err := trees.Watch(context.Background(), metav1.ListOptions{FieldSelector: "metadata.name=guestbook"})
	if err != nil {
		panic(err)
	}
	defer w.Stop()
	for event := range w.ResultChan() {
		fmt.Println(event.Type)
	}
}
```

## Configuration

The following keys of `argocd-cmd-params-cm` configure the component. The Redis settings (`redis.*`) and
`application.namespaces` are shared with the other components.

| Key                                            | Default | Description                                                                         |
|------------------------------------------------|---------|-------------------------------------------------------------------------------------|
| `aggregatedapiserver.log.format`               | `json`  | Log format, `json` or `text`.                                                       |
| `aggregatedapiserver.log.level`                | `info`  | Log level.                                                                          |
| `aggregatedapiserver.rbac.subject.prefix`      | `k8s:`  | Prefix of Kubernetes user and group names in the Argo CD RBAC policy.               |
| `aggregatedapiserver.watch.bookmark.interval`  | `1m`    | Interval between watch bookmarks, for clients requesting them.                      |
| `aggregatedapiserver.watch.buffer.size`        | `100`   | Number of events buffered per watch before a slow client is disconnected.           |
| `aggregatedapiserver.watch.resync.period`      | `3m`    | Interval at which watched projections are recomputed.                               |

See [argocd-aggregated-apiserver](server-commands/argocd-aggregated-apiserver.md) for all the command line flags.
