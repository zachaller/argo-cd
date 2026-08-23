# Multiple Destinations

> [!NOTE]
> This feature is disabled by default. An operator must set `application.destinations.enabled: "true"`
> in `argocd-cm` before an Application may declare additional destinations.

An Application normally deploys every resource it manages to a single cluster and namespace, given by
`spec.destination`. Declaring additional named destinations lets individual manifests within the same
Application go somewhere else, so one Application can span clusters while remaining a single unit
with one status, one sync operation and one resource tree.

## Declaring destinations

Add a `destinations` list to the Application spec. Each entry needs a `name` that is unique within
the Application, and identifies a cluster by either `server` or `clusterName`:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: guestbook
spec:
  project: default
  source:
    repoURL: https://github.com/argoproj/argocd-example-apps.git
    path: guestbook
    targetRevision: HEAD
  # Resources without the annotation below are deployed here.
  destination:
    server: https://kubernetes.default.svc
    namespace: guestbook
  destinations:
    - name: shared-services
      server: https://shared.example.com
      namespace: platform
    - name: eu-west
      clusterName: eu-west-prod
      namespace: guestbook
```

A destination name must not contain `/` or `@`, because RBAC object strings use those as separators.

**Each destination must be a different cluster.** Two destinations on the same cluster are rejected
even when their namespaces differ.

Argo CD reads a cluster's live objects once per cluster and attributes them to an Application by its
tracking annotation, which names the Application and not the destination. Two destinations sharing a
cluster therefore receive the same set of live objects, and each treats the other's resources as
extras: with pruning enabled they would delete each other's resources, and without it both would
report `OutOfSync` forever.

Use a single destination with namespaced manifests if you want to deploy into several namespaces of
one cluster — that has always worked and needs none of this.

## Declaring destinations from the CLI

`argocd app create` and `argocd app set` take a repeatable `--dest` flag of comma separated
`key=value` pairs. The recognised keys are `name`, `server`, `namespace` and `clusterName`:

```bash
argocd app set my-app \
  --dest name=prod,server=https://prod.example.com,namespace=web \
  --dest name=shared,clusterName=minikube,namespace=infra
```

`name` is required — it is what a manifest's annotation selects. Give either `server` or
`clusterName`, not both, exactly as for `spec.destination`.

Each `argocd app set --dest` call replaces the whole list, so passing every destination you want to
keep is how one is removed as well as added. `--dest` never touches the primary `spec.destination`,
which keeps its own `--dest-server`, `--dest-name` and `--dest-namespace` flags.

`argocd app get` lists the named destinations and, for an Application that declares any, adds a
`DESTINATION` column to its resources. The primary destination shows as `(primary)`.

## Routing a manifest

Annotate a manifest with the name of the destination it belongs to:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: shared-config
  annotations:
    argocd.argoproj.io/destination: shared-services
data:
  key: value
```

A manifest with no annotation is deployed to `spec.destination`.

> [!WARNING]
> A manifest naming a destination the Application does not declare is **not** deployed, and the
> Application is marked with an `InvalidSpecError` and an `Unknown` sync status. It is never sent to
> `spec.destination` instead: deploying a resource to the wrong cluster is worse than not deploying
> it.

## Sync waves and hooks

Sync waves order across destinations, not just within one. Every destination is held at a shared
frontier, so wave 1 does not start in any cluster until wave 0 has finished in all of them. Phases
order first and waves within a phase, exactly as they do for a single-destination Application.

Prune and prune-last operations are the exception. Argo CD rewrites their waves internally, so they
order within a destination but not across them.

If any destination fails, the Application's sync fails, and every destination that has `SyncFail`
hooks runs them — not just the one that failed. Without this a hook meant to clean up after a failed
release would never fire in the clusters that had already applied successfully.

A destination with no `SyncFail` hooks is left alone: it keeps whatever result it reached, so a
destination that applied cleanly is not reported as failed.

> [!NOTE]
> A destination that has not yet finished keeps the whole operation `Running`, even after another
> destination has failed. The operation reports `Failed` once every destination has settled.

## Project permissions

Every destination an Application uses must be permitted by its `AppProject`, not just
`spec.destination`. A destination the project does not allow produces an `InvalidSpecError` naming
that destination.

## RBAC

Each named destination can additionally be made a separate authorization axis, so that a role allowed
to sync an Application is not thereby allowed to sync it into every cluster the Application reaches.
The check is off by default and, when on, applies only to the names in `spec.destinations` -- the
primary `spec.destination` is unaffected. See
[the `destinations` resource](../operator-manual/rbac.md#the-destinations-resource).

## Testing

The end-to-end tests for this feature need two genuinely separate clusters, for the same reason two
destinations may not share one. CI creates a second cluster with [k3d](https://k3d.io) at the same
Kubernetes version as the first. To run them locally:

```bash
make start-e2e-second-cluster
ARGOCD_E2E_SECOND_CLUSTER_KUBECONFIG=$HOME/.kube/second-cluster.config make test-e2e-local
make stop-e2e-second-cluster
```

Without the second cluster those tests skip; the tests for the validation rules run either way.

## Deletion

Deleting an Application removes its resources from every destination it deploys to. A destination is
only considered done when nothing is left in it, so the Application's finalizer stays in place until
all of them are clear.

A destination whose cluster can no longer be resolved is skipped with a warning rather than blocking
the deletion — its resources went with the cluster, and refusing to proceed would strand the
resources still reachable elsewhere.

`PreDelete` and `PostDelete` hooks are routed by the `argocd.argoproj.io/destination` annotation like
any other manifest: each hook is created, waited on and cleaned up in the destination it names, and a
hook without the annotation runs in `spec.destination`. A hook manifest that declares no namespace
takes the namespace of the destination it is routed to.

The deletion waits for every destination's hooks before it continues, so a hook still running in one
cluster holds the whole deletion. A failed hook is deleted in each destination it failed in, so the
next attempt starts clean everywhere.

> [!NOTE]
> A hook annotated for a destination whose cluster can no longer be resolved is skipped with a
> warning. It is never run in another destination -- a delete hook written for one cluster is not
> safe to run against a different one -- and it does not block the deletion of the destinations that
> are still reachable.

## Opening a terminal into a pod

A terminal opens in the cluster the pod actually runs in. The UI sends the destination of the pod you
selected in the resource tree, so nothing has to be chosen by hand.

If a pod with the same name and namespace exists in more than one of the Application's destinations and
the request does not name one, it is refused rather than resolved to whichever was found first --
opening a shell in the wrong cluster is worse than refusing to open one.

The `exec` check and the [`destinations` check](../operator-manual/rbac.md#the-destinations-resource) must
both allow it, the latter against the destination the pod is in.

## Addressing a resource that exists in two destinations

Argo CD identifies a live resource by its group, kind, namespace and name. When an Application's
destinations hold the same four values in more than one cluster, name the destination as well:

```bash
argocd app get-resource my-app --kind ConfigMap --resource-name shared-cm --destination second
argocd app patch-resource my-app --kind ConfigMap --resource-name shared-cm --destination second \
  --patch '{"data":{"key":"value"}}' --patch-type application/merge-patch+json
```

`--destination` takes a name from `spec.destinations`, or `@primary` for `spec.destination`. It is
accepted by `get-resource`, `patch-resource`, `delete-resource`, `actions list` and `actions run`, and
the UI sends it automatically for the resource you selected in the tree.

Without it, a request that matches the same resource in more than one destination is rejected as
ambiguous rather than acting on whichever copy was found first.

Everything that treats the Application as a whole is unaffected: sync, diff, health, the resource tree
and pod logs all handle such resources normally, each against its own cluster. Resource events are
matched by UID, which is already unique per cluster, so they need no destination.

## Limitation: manifests are generated once

Manifests are generated a single time, against the cluster named by `spec.destination`. Helm's
`.Capabilities.KubeVersion` and `.Capabilities.APIVersions`, and kustomize's API-version detection,
therefore describe **the primary destination only**.

If your destinations run different Kubernetes versions, or have different CRDs installed, do not rely
on `.Capabilities` to branch: a chart may render a resource whose API does not exist in the cluster
the resource is routed to. Argo CD raises a `MultipleDestinationsWarning` condition when it detects
that the destinations report different Kubernetes versions.

`.Release.Namespace` likewise reflects the primary destination's namespace. The namespace actually
applied to a resource is still its own destination's namespace; only the value Helm sees while
rendering comes from the primary.
