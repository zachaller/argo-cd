---
title: Read-only aggregated API for Application trees and diffs
authors:
  - TBD
sponsors:
  - TBD
reviewers:
  - TBD
approvers:
  - TBD

creation-date: 2026-10-16
last-updated: 2026-10-16
---

# Read-only aggregated API for Application trees and diffs

## Open Questions

* Is `views.argoproj.io` the right name for the API group?
* Should the component eventually be part of the default installation, or remain an opt-in component?
* Should action subresources (sync, refresh, rollback, resource actions) follow in a later proposal?

## Summary

Add an optional component, `argocd-aggregated-apiserver`, which serves read-only views of computed Application state
as Kubernetes resources through the Kubernetes API aggregation layer:

* `applicationtrees.views.argoproj.io`: the live resource tree of an Application.
* `applicationdiffs.views.argoproj.io`: the diff between the live and the desired state of its managed resources.

Both support `get`, `list` and `watch`, and are protected by both Kubernetes RBAC and Argo CD RBAC.

## Motivation

The resource tree and the diffs are computed by the application controller and stored in Redis. They cannot be
served by CRDs, and are only available through the Argo CD API, which requires an Argo CD token and the Argo CD CLI or
gRPC/REST clients. Kubernetes-native consumers (kubectl, client-go informers, other controllers, policy engines) cannot
read or watch them with the credentials they already have, and their reads do not appear in the Kubernetes audit log.

### Goals

* `kubectl get applicationtrees`, `kubectl get applicationdiffs -w` and client-go informers work with regular
  Kubernetes credentials.
* Access is never broader than what the Argo CD RBAC policy grants: AppProject restrictions cannot be bypassed.
* The feature is opt-in. Installations which do not enable it are unaffected.
* An outage of the component does not affect the other Argo CD components.

### Non-Goals

* Write access, action subresources (sync, refresh, rollback, resource actions).
* Rendered manifests.
* Replacing the Argo CD API, CLI or UI.

## Proposal

### Use cases

#### Use case 1:
As a platform engineer, I want a controller to watch the health of the resources of an Application using its
ServiceAccount, without provisioning an Argo CD token.

#### Use case 2:
As a developer, I want to inspect the diff of my Application with `kubectl`, using my existing cluster access, and have
the access recorded in the Kubernetes audit log.

### Implementation Details/Notes/Constraints

* A new subcommand of the `argocd` binary, `argocd-aggregated-apiserver`, built on `k8s.io/apiserver`, deployed by an
  opt-in Kustomize component (`manifests/aggregated-api`) with its own Deployment, Service and APIService.
* Each Application has exactly one projection of each kind, with the same namespace and name, labels (except the
  tracking label) and UID. Projections are built from the Application informer and the application controller's
  Redis cache. The component never triggers a refresh.
* `resourceVersion`s are content hashes, identical on every replica. Lists and bookmarks remember the
  `resourceVersion`s of their items, so that watches resume exactly; otherwise watches replay the current state.
* One watch hub per replica subscribes once to the controller's Redis notifications (with a pattern subscription) and
  once to the Application informer, computes each changed projection once per kind and fans it out to the watchers it
  is visible to. Unchanged projections do not produce events. A periodic resync catches lost notifications.
* The `views.argoproj.io` API group is always excluded from resource tracking, so that the projections never appear in
  resource trees and cannot cause reconciliation loops.

### Detailed examples

See the [documentation](../operator-manual/aggregated-api.md).

### Security Considerations

* Authentication and authorization are delegated to kube-apiserver (TokenReview/SubjectAccessReview, front proxy
  client certificates), like any aggregated API server.
* The handler additionally enforces the Argo CD RBAC policy (`applications, get`), including AppProject roles. The
  Kubernetes user name and groups are prefixed (`k8s:` by default), so that a Kubernetes identity cannot be mistaken for
  an Argo CD local user such as `admin`, an SSO group or a project token subject.
* Denied Applications are reported as not found, like in the Argo CD API, so that their existence is not revealed.
* Secret data in diffs is masked by the application controller before it is stored in Redis.
* The component only needs read access to Applications, AppProjects and ConfigMaps of the Argo CD namespace, and no
  access to Secrets (the Redis password is provided through the environment).

### Risks and Mitigations

* **Discovery availability**: when the APIService is unavailable, clients performing full discovery report errors.
  Mitigation: opt-in, two replicas with a PodDisruptionBudget by default, and a prominent warning in the documentation.
* **Load on Redis**: each change is read once per kind per replica, not once per watcher.
* **Slow clients**: watches have a bounded buffer and are terminated with `410 Gone` instead of blocking the hub.

### Upgrade / Downgrade Strategy

The component is opt-in and stateless. Removing the component removes the APIService.

## Drawbacks

* A new component to operate, with its own serving certificate and the dependencies of `k8s.io/apiserver`.
* Two authorization layers to configure.

## Alternatives

* Exposing the state in the status of the Application CRD: the tree and diffs are too large and change too often to be
  stored in etcd.
* A separate CRD written by the controller: same size and write amplification concerns.
* Serving the API from argocd-server: couples the availability of the APIService to argocd-server rollouts.
