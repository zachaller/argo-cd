---
title: Multiple destinations for an Application
authors:
  - "@zachaller"
sponsors:
  - TBD
reviewers:
  - TBD
approvers:
  - TBD

creation-date: 2026-08-23
last-updated: 2026-08-23
---

# Multiple destinations for an Application

Let a single Application deploy its resources to more than one cluster, with each manifest
selecting its destination by name.

Related Issues:
* TBD -- an approved issue is required before this is proposed upstream.

## Summary

An Application deploys to exactly one cluster. `spec.destination` is a single
`ApplicationDestination`, and that value is threaded through manifest generation, the live state
cache, the diff, the sync context's REST config, impersonation and sharding.

This proposal adds a named destination list to the Application spec and an annotation that selects
one of them per manifest, so one Application reconciles and syncs across several clusters as a
single unit:

```yaml
spec:
  destination:
    server: https://kubernetes.default.svc
    namespace: workloads
  destinations:
    - name: shared-services
      server: https://shared.example.com
      namespace: platform
```

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: shared-config
  annotations:
    argocd.argoproj.io/destination: shared-services
```

Manifests without the annotation go to `spec.destination`, so an existing Application behaves
exactly as it does today.

## Motivation

The only supported way to deploy one set of manifests to several clusters is an ApplicationSet with
the cluster generator, which produces N separate Application CRs. Each has its own status, its own
sync operation, its own resource tree and its own row in the UI.

That works when the N clusters get the *same* content. It does not express "this bundle deploys as
one unit, but these three objects go to the shared-services cluster and the rest go to the workload
cluster", and it cannot order a sync across those clusters -- there is no way to say that a
migration Job in cluster A finishes before a Deployment in cluster B starts.

### Goals

* One Application, one status, one sync operation, spanning several clusters.
* Per-manifest destination selection that is inert for Applications that do not use it.
* Sync wave ordering that holds *across* destinations, not just within one.
* AppProject validation and RBAC that cover every destination an Application touches.

### Non-goals

* Replacing the ApplicationSet cluster generator. Fanning the same content out to many clusters
  remains its job; this is for one bundle whose parts belong in different clusters.
* Per-destination manifest generation. See "Manifest generation" below.
* Changing the resource tracking annotation.

## Proposal

### Core choice: partition by destination, do not add a cluster dimension to the resource key

`kube.ResourceKey` is `{Group, Kind, Namespace, Name}` and is the map key for the live object cache,
`GetManagedLiveObjs`, `sync.Reconcile` and the resource tree maps. Adding a `Cluster` field would
touch the cluster cache, the diff engine and every consumer of those maps.

Instead, target objects are grouped by resolved destination and the existing single-cluster pipeline
runs once per destination, each with its own flat keyspace, and the per-destination results are
merged with a destination tag on each resource. Two objects with the same GVK, namespace and name in
different clusters never share a map.

Two existing facts make this cheap. `argo.GetDestinationCluster` is already a pure
`(ApplicationDestination, db) -> *Cluster` function rather than being Application-bound. And
`LiveStateCache` is already multi-cluster: every method takes a `*Cluster` and the internal map is
keyed by server URL.

### Destinations must resolve to distinct clusters

No two of an Application's destinations may resolve to the same cluster. This is validated and the
Application is rejected otherwise.

The reason is not tidiness. `GetManagedLiveObjs` returns every object in a cluster whose tracking
annotation names the Application, regardless of which manifests were passed in -- the tracking ID has
no cluster component. Two destinations backed by one API server therefore each see the other's
resources as live-but-untargeted, and each prunes the other's. The result is that every resource is
applied and then deleted within a single sync.

Adding a cluster component to the tracking ID would fix that hazard differently, but it would break
`ParseAppInstanceValue`'s three-part contract and rewrite the annotation on every resource in every
existing installation on upgrade -- a fleet-wide spurious diff and re-apply. Requiring distinct
clusters is the cheaper rule, and it is what users want anyway: two destinations on one cluster are
better expressed as two namespaces in one destination.

### Partitioning happens before target normalization

Manifests are routed to destinations *before* `NormalizeTargetObjects`. That function applies the
destination namespace and writes the tracking annotation, both of which are destination-specific.
Partitioning afterwards would stamp the primary destination's namespace onto objects bound elsewhere;
at sync time `isSelfReferencedObj` then rejects them and the resource is silently skipped with no
error reported anywhere.

A manifest naming a destination the Application does not declare produces an
`InvalidSpecError` and the Application does not sync. It is not routed to the primary destination:
deploying a resource to a cluster nobody asked for is worse than not deploying it.

### Sync

`SyncAppState` builds one sync context per destination, each with its own REST config, its own
impersonation service account chosen by that destination's namespace, and its own permission
validator closure capturing that destination's cluster -- so the existing per-object
`validateSyncPermissions` becomes correct across clusters for free.

Sync state round-trips through `OperationState` between reconcile passes, because
`syncCtx.Sync(ctx)` processes exactly one `(phase, wave)` and the controller rebuilds a fresh sync
context every pass. `ResourceResult` therefore carries a destination, without which prior results
cannot be routed back to the right context and every pass re-applies everything.

### Global wave ordering

Waves are ordered across destinations: wave N finishes everywhere before wave N+1 starts anywhere.

This is a frontier filter applied between passes, not a rendezvous between concurrent contexts. The
contexts do not co-exist across passes, so a barrier built on `WithSyncWaveHook` could never
rendezvous and would deadlock on disjoint wave sets. The frontier is the sorted union of
`(phase, wave)` pairs across destinations, persisted on `SyncOperationResult`, and advanced only once
every destination's tasks at the current frontier are terminal.

This needs one gitops-engine addition, `WithSyncTaskFilter`, which filters tasks after they are built
-- `WithResourcesFilter` is consulted per resource before tasks exist and never sees the phase, so it
cannot hold back "wave 2 of PostSync" while letting "wave 0 of Sync" through.

A second engine addition, `WithForceSyncFailPhase`, makes a failure in one destination run the
`SyncFail` hooks in the others; `executeSyncFailPhase` otherwise fires only when a context's own
tasks failed.

### Deletion and delete hooks

Deleting an Application removes its resources from every destination it deploys to, and the finalizer
stays in place until all of them are clear. A destination whose cluster no longer resolves is skipped
with a warning: its resources went with the cluster, and refusing to proceed would strand the
resources that are still reachable elsewhere.

`PreDelete` and `PostDelete` hooks are manifests like any other, so they are routed by the same
annotation and are created, waited on and cleaned up in the destination they name. A hook that
declares no namespace takes that destination's namespace, and its tracking annotation records that
namespace -- matching what sync wrote when its siblings were applied there. The stage completes only
when every destination's hooks have completed, since an Application is deleted as one unit, and a
failed hook is removed in each destination it failed in so a retry starts clean everywhere.

A hook whose destination no longer resolves is skipped rather than run somewhere else: a delete hook
written for one cluster is not safe to run against a different one. Skipping it also keeps an
unreachable destination from stranding the Application behind a hook that can never complete.

Cleanup judges each destination's hooks by their own outcome. `HookSucceeded` and `HookFailed`
describe what happened to that hook, not to the Application, so a hook that failed in one cluster
does not change what the policy does to a hook in another.

### Manifest generation stays single

Manifests are generated once, against `spec.destination`'s cluster. Generating per destination would
mean N repo-server round trips and would break value-file sharing between `spec.sources`.

The cost is that Helm `.Capabilities` and kustomize API-version detection reflect the primary
destination. A warning condition is raised when the destinations' server versions differ, so a user
whose clusters have diverged is told rather than left to discover it through a failed apply.

### RBAC

A new project-scoped `destinations` Casbin resource, with object `<project>/<destination-name>`,
enforced *in addition to* the existing application check, behind an `argocd-cm` opt-in.

The `applications` object string is deliberately unchanged. `globMatchFunc` uses `glob.Match` with no
separator runes, so a literal policy `p, role:dev, applications, sync, myproj/myapp` would stop
matching a three-segment object entirely: prefixing or suffixing the application object would
silently revoke per-application grants on upgrade.

Only the *named* destinations are checked. The primary has no name, so its object would be
`<project>/`, and checking it would make the flag unsafe to enable fleet-wide.

### Addressing a resource by destination

A resource-level request -- read the live object, patch it, delete it, run an action on it -- names a
group, kind, namespace and name. With one destination that identifies a resource. With several it may
not: the same object can exist in two clusters.

Those requests carry a destination, and the tree lookup narrows to it. Rejecting an ambiguous lookup
is still the default, because a request that says nothing about the destination must not be resolved
to whichever cluster happened to be searched first.

The primary destination needs a name here. An empty destination already means "not given, resolve it
if you can" -- that is what every existing client sends -- so it cannot also mean the primary. It is
addressed as `@primary`: a destination name may not contain `@`, so the value can never collide with
one a user chose, and unlike a proto field that distinguishes unset from empty it survives query
parameters, JSON and shell quoting, which is where this value actually travels.

The event query deliberately does not take a destination. It matches on UID, which is already unique
per cluster, so the field would be dead API surface.

The same filter has to apply before a client decides a request is ambiguous. The CLI builds its
target list from an Application's managed resources and refuses more than one match; without
filtering by destination first it would report that the inputs match several resources and suggest
acting on all of them, which would act on the copy in every cluster rather than the one meant.

### Terminals and cluster nodes

Opening a terminal resolves the pod in the resource tree first, and uses that pod's destination for
both the cluster the shell opens in and the `destinations` check -- with the `create` action, the verb
the `exec` check itself uses. A pod that exists in several destinations, in a request that names none,
is refused: opening a shell in the wrong cluster is worse than refusing to open one.

Cluster nodes are attributed as well, because node names are unique only within a cluster. The pod
view groups pods by node, and keying those groups by name alone merges two clusters' identically
named nodes -- `node-1` on kind or k3d, or a repeated instance name on a cloud provider -- into one
group holding one cluster's capacity figures and both clusters' pods. `HostInfo` therefore carries the
destination it was read from, and the grouping keys on the pair.

### Sharding

The shard is still chosen from `spec.destination`, so an Application has exactly one owning shard.
That shard may open live state caches for every cluster the Application references. The accepted
trade-off is that a cluster may now be watched by more than one shard, raising controller memory and
API server load.

## Use cases

**Use case 1.** A team's bundle needs an ExternalSecret and a ServiceMonitor in the platform cluster
and its workloads in the tenant cluster. Today that is two Applications with no ordering between
them and two rows in the UI; here it is one Application whose status covers both.

**Use case 2.** A schema migration Job must run in the database cluster before the Deployment that
depends on it rolls out in the workload cluster. Global wave ordering expresses that; two
Applications cannot.

## Security considerations

The feature is off by default. `application.destinations.enabled` in `argocd-cm` gates it, and an
Application declaring `spec.destinations` while it is off is rejected rather than silently deploying
everything to the primary destination.

Every destination goes through `AppProject` validation and, when the RBAC gate is on, through a
`destinations` check in addition to the application check. Impersonation is fail-closed: one
destination lacking a matching service account fails the whole operation.

## Risks and mitigations

**A wider blast radius per Application.** One Application can now write to several clusters, so a
compromised or mistaken Application reaches further. Mitigated by the AppProject destination list,
which already constrains where an Application may deploy, and by the optional `destinations` RBAC
resource for operators who want an explicit second axis.

**Cross-cluster pruning if destinations collapse to one cluster.** Discussed above; validated
against, and the e2e harness additionally asserts that its second cluster is not the same API server
as the first.

**Divergent cluster capabilities.** Manifests are generated against the primary destination, so a
chart whose output depends on `.Capabilities` may be wrong for another destination. A warning
condition is raised when server versions differ.

## Upgrade / downgrade strategy

An Application that does not set `spec.destinations` behaves exactly as before: the field is
optional, the annotation is ignored when absent, the tracking annotation is unchanged, the
`applications` RBAC object string is unchanged, and the feature gate defaults to off.

Downgrading with `spec.destinations` set leaves the field unrecognised, and annotated manifests would
be deployed to the primary destination. Operators should remove the field before downgrading.

## Drawbacks

* A cluster may be watched by more than one controller shard, costing memory and API server load.
* Manifest generation reflects one destination's capabilities.
* Every resource-level API request, and its CLI and UI callers, gained a destination. A resource that
  exists in two destinations cannot be addressed without one, so a client that omits it still gets an
  error -- deliberately, since the alternative is acting on the wrong cluster.
