# Multi-Cluster Aggregator (`argocd-aggregator`)

The `argocd-aggregator` is a Kubernetes [aggregated API server](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/apiserver-aggregation/)
that presents a single, unified view of Argo CD resources (`Application`, `ApplicationSet`,
`AppProject`) across **every cluster registered in Argo CD**. It is intended for UI/API
centralization: point one Argo CD UI at a central cluster and browse and edit resources that
actually live on many downstream clusters.

It registers an `APIService` for the `argoproj.io/v1alpha1` group-version. On reads it fans
out to every downstream cluster and aggregates the results; on writes it forwards the change
to the cluster the resource came from.

> [!WARNING]
> The central cluster running the aggregator **must not** have the `argoproj.io` CRDs
> installed (`manifests/crds`). The `APIService` owns the entire `argoproj.io/v1alpha1`
> group-version, and a CRD for the same group-version would conflict with it.

## How it works

```
                         Central cluster (NO argoproj.io CRDs)
  argocd-server (UI) ──AppClientset──> kube-apiserver ──APIService v1alpha1.argoproj.io──┐
                                                                                          v
                                                                          argocd-aggregator
                                                                                          │  fan-out
                    ┌─────────────────────────────┬───────────────────────────┬──────────┘
                    v                             v                           v
         downstream cluster A           downstream cluster B          downstream cluster C
       (real argoproj.io CRDs)         (real argoproj.io CRDs)       (real argoproj.io CRDs)
```

* **Downstream discovery** reuses the existing Argo CD cluster secrets (the same ones managed
  with `argocd cluster add`). No new credential model is introduced.
* **Reads** with no namespace fan out to all clusters in parallel; reads scoped to a namespace
  resolve to a single downstream cluster and namespace.
* **Writes** are routed back to the originating cluster and namespace.

### Synthetic namespaces

Because the same `Application` name can exist in different namespaces on different clusters,
the aggregator presents each `(cluster, downstream namespace)` pair as a **synthetic
namespace** of the form `<cluster>-<hash(namespace)>`. The downstream namespace is hashed so
the composite always stays within the 63-character RFC1123 limit, regardless of how long the
real namespace name is.

The true cluster and namespace are preserved verbatim on labels:

| Label | Meaning |
| ----- | ------- |
| `aggregator.argoproj.io/source-cluster` | downstream cluster name |
| `aggregator.argoproj.io/source-namespace` | real downstream namespace |

> [!NOTE]
> The synthetic namespace is opaque — do not try to parse it. Use the
> `aggregator.argoproj.io/source-cluster` and `aggregator.argoproj.io/source-namespace`
> labels to discover the real location of a resource. On writes, these labels are the
> authoritative routing key.

## Installation

Install the aggregator on the central cluster:

```bash
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/master/manifests/aggregator-install.yaml
```

Then register your Argo CD clusters as secrets in the `argocd` namespace (for example with
`argocd cluster add`), exactly as you would for a normal Argo CD installation.

The bundled `APIService` uses `insecureSkipTLSVerify: true` so the aggregator can serve a
self-signed certificate out of the box.

> [!NOTE]
> For production, provision a serving certificate (for example with cert-manager) and set
> `spec.caBundle` on the `APIService` instead of `insecureSkipTLSVerify`.

## Verifying

```bash
# List applications aggregated across all clusters.
kubectl get applications.argoproj.io -A

# Inspect the real location of a resource.
kubectl get applications.argoproj.io -A \
  -o custom-columns=NAME:.metadata.name,CLUSTER:'.metadata.labels.aggregator\.argoproj\.io/source-cluster',NAMESPACE:'.metadata.labels.aggregator\.argoproj\.io/source-namespace'

# Watch changes streaming in from every cluster.
kubectl get applications.argoproj.io -A -w
```

## Authentication and authorization

The aggregator delegates authentication and authorization to the **central cluster's**
kube-apiserver (TokenReview/SubjectAccessReview). Access is therefore governed by the central
cluster's Kubernetes RBAC for the `argoproj.io` resources.

> [!WARNING]
> Because the aggregator talks directly to downstream `argoproj.io` CRDs, the per-project RBAC
> enforced by each downstream `argocd-server` (the `argocd-rbac-cm` Casbin policy) is **not**
> applied. Restrict access to the aggregated API using central-cluster RBAC.
