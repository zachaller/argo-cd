package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/sync/common"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/sync/hook"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/argoproj/argo-cd/v3/util/lua"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	argoutil "github.com/argoproj/argo-cd/v3/util/argo"
	traceutil "github.com/argoproj/argo-cd/v3/util/trace"
)

type HookType string

const (
	PreDeleteHookType  HookType = "PreDelete"
	PostDeleteHookType HookType = "PostDelete"
)

var hookTypeAnnotations = map[HookType]map[string]string{
	PreDeleteHookType: {
		"argocd.argoproj.io/hook": string(PreDeleteHookType),
		"helm.sh/hook":            "pre-delete",
	},
	PostDeleteHookType: {
		"argocd.argoproj.io/hook": string(PostDeleteHookType),
		"helm.sh/hook":            "post-delete",
	},
}

func isHookOfType(obj *unstructured.Unstructured, hookType HookType) bool {
	if obj == nil || obj.GetAnnotations() == nil {
		return false
	}

	for k, v := range hookTypeAnnotations[hookType] {
		if val, ok := obj.GetAnnotations()[k]; ok {
			if slices.ContainsFunc(strings.Split(val, ","), func(item string) bool {
				return strings.TrimSpace(item) == v
			}) {
				return true
			}
		}
	}
	return false
}

func isHook(obj *unstructured.Unstructured) bool {
	if hook.IsHook(obj) {
		return true
	}

	for hookType := range hookTypeAnnotations {
		if isHookOfType(obj, hookType) {
			return true
		}
	}
	return false
}

func isPreDeleteHook(obj *unstructured.Unstructured) bool {
	return isHookOfType(obj, PreDeleteHookType)
}

func isPostDeleteHook(obj *unstructured.Unstructured) bool {
	return isHookOfType(obj, PostDeleteHookType)
}

// hasGitOpsEngineSyncPhaseHook is true when gitops-engine would run the resource during a sync
// phase (PreSync, Sync, PostSync, SyncFail). PreDelete/PostDelete are not sync phases;
// without this check, state reconciliation drops such resources
// entirely because isPreDeleteHook/isPostDeleteHook match any comma-separated value.
// HookTypeSkip is omitted as it is not a sync phase.
func hasGitOpsEngineSyncPhaseHook(obj *unstructured.Unstructured) bool {
	for _, t := range hook.Types(obj) {
		switch t {
		case common.HookTypePreSync, common.HookTypeSync, common.HookTypePostSync, common.HookTypeSyncFail:
			return true
		}
	}
	return false
}

// hookName names a hook for a log or error message, prefixing the destination it lives in when that
// is not the primary one -- the same shape as ResourceNode.FullName.
func hookName(destination, namespace, name string) string {
	if destination == argoutil.PrimaryDestinationName {
		return fmt.Sprintf("%s/%s", namespace, name)
	}
	return fmt.Sprintf("%s/%s/%s", destination, namespace, name)
}

// hookLogger returns a logger that names the destination a hook stage is acting in, so a message
// about a hook is not ambiguous once an application deploys to more than one cluster.
func hookLogger(logCtx *log.Entry, destination string) *log.Entry {
	if destination == argoutil.PrimaryDestinationName {
		return logCtx
	}
	return logCtx.WithField("destination", destination)
}

// executeHooks is a generic function to execute hooks of a specified type.
//
// Hooks are routed to the destination their annotation names, exactly as reconcile routes every
// other manifest, and are created, health-checked and retried in that destination's cluster. The
// stage completes only when every destination's hooks have completed: an application is deleted as
// one unit, so a hook still running in one cluster holds the whole deletion.
func (ctrl *ApplicationController) executeHooks(ctx context.Context, hookType HookType, app *appv1.Application, proj *appv1.AppProject, dests []deletionDestination, liveObjs map[string]map[kube.ResourceKey]*unstructured.Unstructured, logCtx *log.Entry) (completed bool, retErr error) {
	ctx, span := tracer.Start(ctx, "controller.executeHooks")
	setAppTraceAttrs(span, app, attribute.String("argocd.hook.type", string(hookType)))
	defer func() { traceutil.EndSpan(span, retErr) }()
	appLabelKey, err := ctrl.settingsMgr.GetAppInstanceLabelKey()
	if err != nil {
		return false, err
	}
	trackingMethodStr, err := ctrl.settingsMgr.GetTrackingMethod()
	if err != nil {
		return false, err
	}
	installationID, err := ctrl.settingsMgr.GetInstallationID()
	if err != nil {
		return false, err
	}
	trackingMethod := appv1.TrackingMethod(trackingMethodStr)
	resourceTracking := argoutil.NewResourceTracking()

	var revisions []string
	for _, src := range app.Spec.GetSources() {
		revisions = append(revisions, src.TargetRevision)
	}

	// Fetch target objects from Git to know which hooks should exist
	targets, _, _, err := ctrl.appStateManager.GetRepoObjs(ctx, app, app.Spec.GetSources(), appLabelKey, revisions, false, false, nil, proj, true)
	if err != nil {
		return false, err
	}

	// Route each hook to the destination its annotation names. A hook naming a destination that did
	// not resolve is skipped rather than created in the primary destination: running a delete hook
	// against the wrong cluster is worse than not running it at all.
	targetHooks := make(map[string][]*unstructured.Unstructured, len(dests))
	for _, dest := range dests {
		targetHooks[dest.name] = nil
	}
	for _, obj := range targets {
		if !isHookOfType(obj, hookType) {
			continue
		}
		destName := argoutil.DestinationNameForObject(obj)
		if _, declared := targetHooks[destName]; !declared {
			logCtx.Warnf("Skipping %s hook %s/%s: destination %q is unavailable", hookType, obj.GetKind(), obj.GetName(), destName)
			continue
		}
		targetHooks[destName] = append(targetHooks[destName], obj)
	}

	// Find existing hooks of the specified type, in each destination's own cluster
	runningHooks := make(map[string]map[kube.ResourceKey]*unstructured.Unstructured, len(dests))
	for _, dest := range dests {
		running := map[kube.ResourceKey]*unstructured.Unstructured{}
		for key, obj := range liveObjs[dest.name] {
			if isHookOfType(obj, hookType) {
				running[key] = obj
			}
		}
		runningHooks[dest.name] = running
	}

	// Create hooks that don't exist yet
	createdCnt := 0
	for _, dest := range dests {
		destLog := hookLogger(logCtx, dest.name)

		// Find expected hooks that need to be created
		expectedHook := map[kube.ResourceKey]*unstructured.Unstructured{}
		for _, obj := range targetHooks[dest.name] {
			if obj.GetNamespace() == "" {
				obj.SetNamespace(dest.namespace)
			}
			if _, alreadyExists := runningHooks[dest.name][kube.GetResourceKey(obj)]; !alreadyExists {
				expectedHook[kube.GetResourceKey(obj)] = obj
			}
		}

		for key, obj := range expectedHook {
			// Apply app instance tracking metadata so the hook can be tracked and cleaned up.
			// Use the same code path as regular sync resources so the configured
			// tracking method (label, annotation, annotation+label) is honored.
			// When the configured tracking method writes a label, this also ensures
			// the label value is truncated to fit Kubernetes' 63-character label
			// limit (see https://github.com/argoproj/argo-cd/issues/27527).
			// The tracking annotation records the destination's own namespace, matching what sync
			// wrote when the hook's siblings were applied there.
			if err := resourceTracking.SetAppInstance(obj, appLabelKey, app.InstanceName(ctrl.namespace), dest.namespace, trackingMethod, installationID); err != nil {
				return false, fmt.Errorf("failed to set app instance tracking on %s hook %s: %w", hookType, key, err)
			}

			destLog.Infof("Creating %s hook resource: %s", hookType, key)
			_, err = ctrl.kubectl.CreateResource(ctx, dest.config, obj.GroupVersionKind(), obj.GetName(), obj.GetNamespace(), obj, metav1.CreateOptions{})
			if err != nil {
				if apierrors.IsAlreadyExists(err) {
					destLog.Warnf("Hook resource %s already exists, skipping", key)
					continue
				}
				return false, err
			}
			createdCnt++
		}
	}

	if createdCnt > 0 {
		logCtx.Infof("Created %d %s hooks", createdCnt, hookType)
		return false, nil
	}

	// Check health of running hooks
	resourceOverrides, err := ctrl.settingsMgr.GetResourceOverrides()
	if err != nil {
		return false, err
	}
	healthOverrides := lua.ResourceHealthOverrides(resourceOverrides)

	progressingHooksCount := 0
	var failedHooks []string

	for _, dest := range dests {
		destLog := hookLogger(logCtx, dest.name)
		var failedHookObjects []*unstructured.Unstructured

		for key, obj := range runningHooks[dest.name] {
			hookHealth, err := health.GetResourceHealth(obj, healthOverrides)
			if err != nil {
				return false, err
			}
			if hookHealth == nil {
				destLog.WithFields(log.Fields{
					"group":     obj.GroupVersionKind().Group,
					"version":   obj.GroupVersionKind().Version,
					"kind":      obj.GetKind(),
					"name":      obj.GetName(),
					"namespace": obj.GetNamespace(),
				}).Info("No health check defined for resource, considering it healthy")
				hookHealth = &health.HealthStatus{
					Status: health.HealthStatusHealthy,
				}
			}

			switch hookHealth.Status {
			case health.HealthStatusProgressing:
				destLog.Debugf("Hook %s is progressing", key)
				progressingHooksCount++
			case health.HealthStatusDegraded:
				destLog.Warnf("Hook %s is degraded: %s", key, hookHealth.Message)
				failedHooks = append(failedHooks, hookName(dest.name, obj.GetNamespace(), obj.GetName()))
				failedHookObjects = append(failedHookObjects, obj)
			case health.HealthStatusHealthy:
				destLog.Debugf("Hook %s is healthy", key)
			}
		}

		// Delete failed hooks to allow retry with potentially fixed hook definitions. Every
		// destination is cleaned up before the error is returned, so a retry does not find one
		// cluster's failed hooks still in place.
		if len(failedHookObjects) > 0 {
			destLog.Infof("Deleting %d failed %s hook(s) to allow retry", len(failedHookObjects), hookType)
			for _, obj := range failedHookObjects {
				err = ctrl.kubectl.DeleteResource(ctx, dest.config, obj.GroupVersionKind(), obj.GetName(), obj.GetNamespace(), metav1.DeleteOptions{})
				if err != nil && !apierrors.IsNotFound(err) {
					destLog.WithError(err).Warnf("Failed to delete failed hook %s/%s", obj.GetNamespace(), obj.GetName())
				}
			}
		}
	}

	if len(failedHooks) > 0 {
		return false, fmt.Errorf("%s hook(s) failed: %s", hookType, strings.Join(failedHooks, ", "))
	}

	if progressingHooksCount > 0 {
		logCtx.Infof("Waiting for %d %s hooks to complete", progressingHooksCount, hookType)
		return false, nil
	}

	return true, nil
}

// cleanupHooks is a generic function to clean up hooks of a specified type, in every destination the
// application deploys to.
//
// Each destination's hooks are judged by their own outcome: a hook that failed in one cluster does
// not change what a delete policy does to a hook in another, because the policies describe what
// happened to that hook, not to the application as a whole.
func (ctrl *ApplicationController) cleanupHooks(ctx context.Context, hookType HookType, dests []deletionDestination, liveObjs map[string]map[kube.ResourceKey]*unstructured.Unstructured, logCtx *log.Entry) (completed bool, retErr error) {
	ctx, span := tracer.Start(ctx, "controller.cleanupHooks")
	span.SetAttributes(attribute.String("argocd.hook.type", string(hookType)))
	defer func() { traceutil.EndSpan(span, retErr) }()
	resourceOverrides, err := ctrl.settingsMgr.GetResourceOverrides()
	if err != nil {
		return false, err
	}
	healthOverrides := lua.ResourceHealthOverrides(resourceOverrides)

	pendingDeletionCount := 0

	for _, dest := range dests {
		destLog := hookLogger(logCtx, dest.name)
		aggregatedHealth := health.HealthStatusHealthy
		var hooks []*unstructured.Unstructured

		// Collect hooks and determine overall health
		for _, obj := range liveObjs[dest.name] {
			if !isHookOfType(obj, hookType) {
				continue
			}
			hookHealth, err := health.GetResourceHealth(obj, healthOverrides)
			if err != nil {
				return false, err
			}
			if hookHealth == nil {
				hookHealth = &health.HealthStatus{
					Status: health.HealthStatusHealthy,
				}
			}
			if health.IsWorse(aggregatedHealth, hookHealth.Status) {
				aggregatedHealth = hookHealth.Status
			}
			hooks = append(hooks, obj)
		}

		if len(hooks) == 0 {
			continue
		}

		// Process hooks for deletion
		for _, obj := range hooks {
			deletePolicies := hook.DeletePolicies(obj)
			shouldDelete := false

			if len(deletePolicies) == 0 {
				// If no delete policy is specified, always delete hooks during cleanup phase
				shouldDelete = true
			} else {
				// Check if any delete policy matches the current hook state
				for _, policy := range deletePolicies {
					if (policy == common.HookDeletePolicyHookFailed && aggregatedHealth == health.HealthStatusDegraded) ||
						(policy == common.HookDeletePolicyHookSucceeded && aggregatedHealth == health.HealthStatusHealthy) {
						shouldDelete = true
						break
					}
				}
			}

			if shouldDelete {
				pendingDeletionCount++
				if obj.GetDeletionTimestamp() != nil {
					continue
				}
				destLog.Infof("Deleting %s hook %s/%s", hookType, obj.GetNamespace(), obj.GetName())
				err = ctrl.kubectl.DeleteResource(ctx, dest.config, obj.GroupVersionKind(), obj.GetName(), obj.GetNamespace(), metav1.DeleteOptions{})
				if err != nil && !apierrors.IsNotFound(err) {
					return false, err
				}
			}
		}
	}

	if pendingDeletionCount > 0 {
		logCtx.Infof("Waiting for %d %s hooks to be deleted", pendingDeletionCount, hookType)
		return false, nil
	}

	return true, nil
}

// Execute and cleanup hooks for pre-delete and post-delete operations

func (ctrl *ApplicationController) executePreDeleteHooks(ctx context.Context, app *appv1.Application, proj *appv1.AppProject, dests []deletionDestination, liveObjs map[string]map[kube.ResourceKey]*unstructured.Unstructured, logCtx *log.Entry) (bool, error) {
	return ctrl.executeHooks(ctx, PreDeleteHookType, app, proj, dests, liveObjs, logCtx)
}

func (ctrl *ApplicationController) cleanupPreDeleteHooks(ctx context.Context, dests []deletionDestination, liveObjs map[string]map[kube.ResourceKey]*unstructured.Unstructured, logCtx *log.Entry) (bool, error) {
	return ctrl.cleanupHooks(ctx, PreDeleteHookType, dests, liveObjs, logCtx)
}

func (ctrl *ApplicationController) executePostDeleteHooks(ctx context.Context, app *appv1.Application, proj *appv1.AppProject, dests []deletionDestination, liveObjs map[string]map[kube.ResourceKey]*unstructured.Unstructured, logCtx *log.Entry) (bool, error) {
	return ctrl.executeHooks(ctx, PostDeleteHookType, app, proj, dests, liveObjs, logCtx)
}

func (ctrl *ApplicationController) cleanupPostDeleteHooks(ctx context.Context, dests []deletionDestination, liveObjs map[string]map[kube.ResourceKey]*unstructured.Unstructured, logCtx *log.Entry) (bool, error) {
	return ctrl.cleanupHooks(ctx, PostDeleteHookType, dests, liveObjs, logCtx)
}
