package controller

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/strategicpatch"

	cdcommon "github.com/argoproj/argo-cd/v3/common"

	gitopsDiff "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/diff"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/sync"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/sync/common"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/sync/syncwaves"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	jsonpatch "github.com/evanphx/json-patch"
	log "github.com/sirupsen/logrus"
	otel_codes "go.opentelemetry.io/otel/codes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/util/openapi"

	"github.com/argoproj/argo-cd/v3/controller/metrics"
	"github.com/argoproj/argo-cd/v3/controller/syncid"
	"github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	applog "github.com/argoproj/argo-cd/v3/util/app/log"
	"github.com/argoproj/argo-cd/v3/util/argo"
	"github.com/argoproj/argo-cd/v3/util/argo/diff"
	kubeutil "github.com/argoproj/argo-cd/v3/util/kube"
	logutils "github.com/argoproj/argo-cd/v3/util/log"
	"github.com/argoproj/argo-cd/v3/util/lua"
	"github.com/argoproj/argo-cd/v3/util/settings"
)

const (
	// EnvVarSyncWaveDelay is an environment variable which controls the delay in seconds between
	// each sync-wave
	EnvVarSyncWaveDelay = "ARGOCD_SYNC_WAVE_DELAY"
)

func (m *appStateManager) getOpenAPISchema(server *v1alpha1.Cluster) (openapi.Resources, error) {
	cluster, err := m.liveStateCache.GetClusterCache(server)
	if err != nil {
		return nil, err
	}
	return cluster.GetOpenAPISchema(), nil
}

func (m *appStateManager) getGVKParser(server *v1alpha1.Cluster) (*managedfields.GvkParser, error) {
	cluster, err := m.liveStateCache.GetClusterCache(server)
	if err != nil {
		return nil, err
	}
	return cluster.GetGVKParser(), nil
}

// getServerSideDiffDryRunApplier will return the kubectl implementation of the KubeApplier
// interface that provides functionality to dry run apply kubernetes resources. Returns a
// cleanup function that must be called to remove the generated kube config for this
// server.
func (m *appStateManager) getServerSideDiffDryRunApplier(cluster *v1alpha1.Cluster, project *v1alpha1.AppProject, app *v1alpha1.Application) (gitopsDiff.KubeApplier, func(), error) {
	rawConfig, err := cluster.RawRestConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("error getting cluster REST config: %w", err)
	}
	// When impersonation is enabled, the dry-run apply is executed as the ServiceAccount
	// derived from the AppProject's destinationServiceAccounts, matching the identity used
	// by the sync operation.
	if err := m.applyDiffImpersonationConfig(rawConfig, project, app, cluster); err != nil {
		return nil, nil, err
	}
	ops, cleanup, err := kubeutil.ManageServerSideDiffDryRuns(rawConfig, m.onKubectlRun)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating kubectl ResourceOperations: %w", err)
	}
	return ops, cleanup, nil
}

// applyDiffImpersonationConfig sets the impersonation headers on the given REST config
// so that the server-side diff dry-run runs as the same ServiceAccount used by sync.
// It mirrors the sync path's behaviour, including honoring the impersonation-enforced
// setting: when impersonation is enabled but no matching destinationServiceAccount is
// found, enforcement causes the diff to fail rather than silently fall back to the
// controller credential (the dry-run apply is authorized by the API server as a patch,
// so falling back would require the standing cluster credential to hold patch authority).
func (m *appStateManager) applyDiffImpersonationConfig(config *rest.Config, project *v1alpha1.AppProject, app *v1alpha1.Application, destCluster *v1alpha1.Cluster) error {
	logEntry := log.WithFields(applog.GetAppLogFields(app))

	impersonationEnabled, err := m.settingsMgr.IsImpersonationEnabled()
	if err != nil {
		return fmt.Errorf("error getting impersonation setting: %w", err)
	}
	if !impersonationEnabled {
		return nil
	}
	serviceAccountToImpersonate, err := settings.DeriveServiceAccountToImpersonate(project, app, destCluster, app.Spec.Destination.Namespace)
	if err != nil {
		return fmt.Errorf("error deriving service account to impersonate: %w", err)
	}
	if serviceAccountToImpersonate == "" {
		// No matching service account found - check enforcement.
		impersonationEnforced, err := m.settingsMgr.IsImpersonationEnforced()
		if err != nil {
			return fmt.Errorf("error getting impersonation enforcement setting: %w", err)
		}
		if impersonationEnforced {
			return fmt.Errorf("no matching service account found for destination server %s and namespace %s", destCluster.Server, app.Spec.Destination.Namespace)
		}
		// Non-enforced mode: fall back to the controller credential, consistent with sync.
		logEntry.Debugf("server-side diff: no matching service account found for impersonation (project: %s, server: %s, namespace: %s), falling back to controller service account", project.Name, destCluster.Server, app.Spec.Destination.Namespace)
		return nil
	}
	logEntry.Debugf("server-side diff: impersonating service account %q, matching sync", serviceAccountToImpersonate)
	config.Impersonate = rest.ImpersonationConfig{
		UserName: serviceAccountToImpersonate,
	}
	return nil
}

func NewOperationState(operation v1alpha1.Operation) *v1alpha1.OperationState {
	return &v1alpha1.OperationState{
		Phase:     common.OperationRunning,
		Operation: operation,
		StartedAt: metav1.Now(),
	}
}

func newSyncOperationResult(app *v1alpha1.Application, op v1alpha1.SyncOperation) *v1alpha1.SyncOperationResult {
	syncRes := &v1alpha1.SyncOperationResult{}

	if len(op.Sources) > 0 || op.Source != nil {
		// specific source specified in the SyncOperation
		if op.Source != nil {
			syncRes.Source = *op.Source
		}
		syncRes.Sources = op.Sources
	} else {
		// normal sync case, get sources from the spec
		syncRes.Sources = app.Spec.Sources
		syncRes.Source = app.Spec.GetSource()
	}

	// Sync requests might be requested with ambiguous revisions (e.g. master, HEAD, v1.2.3).
	// This can change meaning when resuming operations (e.g a hook sync). After calculating a
	// concrete git commit SHA, the revision of the SyncOperationResult will be updated with the SHA
	syncRes.Revision = op.Revision
	syncRes.Revisions = op.Revisions
	return syncRes
}

func (m *appStateManager) SyncAppState(ctx context.Context, app *v1alpha1.Application, project *v1alpha1.AppProject, state *v1alpha1.OperationState) {
	ctx, span := tracer.Start(ctx, "controller.SyncAppState")
	setAppTraceAttrs(span, app)
	// SyncAppState is void; it signals failure through state.Phase rather than a return value, so
	// map a terminal failed phase onto the span status at exit (mirroring traceutil.EndSpan).
	defer func() {
		if state.Phase.Failed() {
			span.SetStatus(otel_codes.Error, state.Message)
		}
		span.End()
	}()
	syncId, err := syncid.Generate()
	if err != nil {
		state.Phase = common.OperationError
		state.Message = fmt.Sprintf("Failed to generate sync ID: %v", err)
		return
	}
	logEntry := log.WithFields(applog.GetAppLogFields(app)).WithField("syncId", syncId)

	if state.Operation.Sync == nil {
		state.Phase = common.OperationError
		state.Message = "Invalid operation request: no operation specified"
		return
	}

	syncOp := *state.Operation.Sync

	if state.SyncResult == nil {
		state.SyncResult = newSyncOperationResult(app, syncOp)
	}

	if isBlocked, err := syncWindowPreventsSync(app, project); isBlocked {
		// If the operation is currently running, simply let the user know the sync is blocked by a current sync window
		if state.Phase == common.OperationRunning {
			state.Message = "Sync operation blocked by sync window"
			if err != nil {
				state.Message = fmt.Sprintf("%s: %v", state.Message, err)
			}
		}
		return
	}

	revisions := state.SyncResult.Revisions
	sources := state.SyncResult.Sources
	isMultiSourceSync := len(sources) > 0
	if !isMultiSourceSync {
		sources = []v1alpha1.ApplicationSource{state.SyncResult.Source}
		revisions = []string{state.SyncResult.Revision}
	}

	// ignore error if CompareStateRepoError, this shouldn't happen as noRevisionCache is true
	compareResult, err := m.CompareAppState(ctx, app, project, revisions, sources, false, true, syncOp.Manifests, isMultiSourceSync)
	if err != nil && !stderrors.Is(err, ErrCompareStateRepo) {
		state.Phase = common.OperationError
		state.Message = err.Error()
		return
	}

	// We are now guaranteed to have a concrete commit SHA. Save this in the sync result revision so that we remember
	// what we should be syncing to when resuming operations.
	state.SyncResult.Revision = compareResult.syncStatus.Revision
	state.SyncResult.Revisions = compareResult.syncStatus.Revisions

	// validates if it should fail the sync on that revision if it finds shared resources
	hasSharedResource, sharedResourceMessage := hasSharedResourceCondition(app)
	if syncOp.SyncOptions.HasOption("FailOnSharedResource=true") && hasSharedResource {
		state.Phase = common.OperationFailed
		state.Message = "Shared resource found: " + sharedResourceMessage
		return
	}

	// If there are any comparison or spec errors error conditions do not perform the operation
	if errConditions := app.Status.GetConditions(map[v1alpha1.ApplicationConditionType]bool{
		v1alpha1.ApplicationConditionComparisonError:  true,
		v1alpha1.ApplicationConditionInvalidSpecError: true,
	}); len(errConditions) > 0 {
		state.Phase = common.OperationError
		state.Message = argo.FormatAppConditions(errConditions)
		return
	}

	resourceOverrides, err := m.settingsMgr.GetResourceOverrides()
	if err != nil {
		state.Phase = common.OperationError
		state.Message = fmt.Sprintf("Failed to load resource overrides: %v", err)
		return
	}

	initialResourcesRes := make([]common.ResourceSyncResult, len(state.SyncResult.Resources))
	for i, res := range state.SyncResult.Resources {
		key := kube.ResourceKey{Group: res.Group, Kind: res.Kind, Namespace: res.Namespace, Name: res.Name}
		initialResourcesRes[i] = common.ResourceSyncResult{
			ResourceKey: key,
			Message:     res.Message,
			Status:      res.Status,
			HookPhase:   res.HookPhase,
			HookType:    res.HookType,
			SyncPhase:   res.SyncPhase,
			Version:     res.Version,
			Images:      res.Images,
			Order:       i + 1,
		}
	}

	prunePropagationPolicy := metav1.DeletePropagationForeground
	switch {
	case syncOp.SyncOptions.HasOption("PrunePropagationPolicy=background"):
		prunePropagationPolicy = metav1.DeletePropagationBackground
	case syncOp.SyncOptions.HasOption("PrunePropagationPolicy=foreground"):
		prunePropagationPolicy = metav1.DeletePropagationForeground
	case syncOp.SyncOptions.HasOption("PrunePropagationPolicy=orphan"):
		prunePropagationPolicy = metav1.DeletePropagationOrphan
	}

	clientSideApplyManager := common.DefaultClientSideApplyMigrationManager
	// Check for custom field manager from application annotation
	if managerValue := app.GetAnnotation(cdcommon.AnnotationClientSideApplyMigrationManager); managerValue != "" {
		clientSideApplyManager = managerValue
	}

	installationID, err := m.settingsMgr.GetInstallationID()
	if err != nil {
		log.Errorf("Could not get installation ID: %v", err)
		return
	}
	trackingMethod, err := m.settingsMgr.GetTrackingMethod()
	if err != nil {
		log.Errorf("Could not get trackingMethod: %v", err)
		return
	}

	start := time.Now()

	// Group the results of the previous pass by destination. Nothing survives in memory between
	// passes -- the controller rebuilds every sync context each reconcile -- so a destination's
	// progress is recovered solely from what it recorded on the operation last time. Handing a
	// context another destination's results would make it re-apply everything.
	initialByDest := make(map[string][]common.ResourceSyncResult, len(compareResult.destOrder))
	for i, res := range state.SyncResult.Resources {
		initialByDest[res.Destination] = append(initialByDest[res.Destination], initialResourcesRes[i])
	}

	state.SyncResult.Resources = nil

	if app.Spec.SyncPolicy != nil {
		state.SyncResult.ManagedNamespaceMetadata = app.Spec.SyncPolicy.ManagedNamespaceMetadata
	}

	var (
		mergedPhase    common.OperationPhase
		mergedMessages []string
	)

	// The phase and message the operation entered this pass with. Every destination starts from
	// these; syncDestination writes its result back into the operation, so reading them there would
	// hand each destination the phase the previous one happened to end on.
	entryPhase, entryMessage := state.Phase, state.Message

	// Sync waves have to hold across destinations, not just within one: a resource in wave 1 of one
	// cluster must not apply before wave 0 has finished in every cluster. Each destination is
	// therefore held at a shared frontier and only allowed to work at or before it.
	//
	// A single-destination application takes no frontier at all, so its behaviour is untouched.
	var (
		schedule     []syncStep
		frontier     syncStep
		haveFrontier bool
	)
	if len(compareResult.destOrder) > 1 {
		schedule = syncSchedule(compareResult.perDestination, compareResult.destOrder)
		frontier, haveFrontier = frontierStep(state.SyncResult, schedule)
	}
	// Whether any destination still had work at or before the frontier this pass. While that is
	// true the frontier stays put; once no destination does, every one of them has finished it.
	frontierHasPendingWork := false

	// Each destination's result this pass is kept rather than merged straight away, because a
	// destination may have to be run a second time -- with its SyncFail hooks forced -- once some
	// other destination has failed.
	type destinationOutcome struct {
		phase   common.OperationPhase
		message string
		sync    destinationSyncOutcome
	}
	outcomes := make(map[string]destinationOutcome, len(compareResult.destOrder))

	// runDestination syncs one destination and records what it produced. A non-empty
	// forceSyncFailReason makes it apply nothing and run its SyncFail hooks instead. It reports
	// false when syncDestination has already recorded a terminal state and the caller must stop.
	runDestination := func(destName, forceSyncFailReason string, initial []common.ResourceSyncResult) bool {
		dest := compareResult.resolvedDests[destName]
		dc := compareResult.perDestination[destName]

		ss := syncSettings{
			resourceOverrides:      resourceOverrides,
			prunePropagationPolicy: prunePropagationPolicy,
			clientSideApplyManager: clientSideApplyManager,
			installationID:         installationID,
			trackingMethod:         trackingMethod,
			initialResourcesRes:    initial,
			initialPhase:           entryPhase,
			initialMessage:         entryMessage,
			forceSyncFailReason:    forceSyncFailReason,
		}
		// A forced destination applies nothing, so it has no work for the frontier to hold back,
		// and counting it as pending would stall every destination behind it.
		if haveFrontier && forceSyncFailReason == "" {
			ss.syncTaskFilter = func(phase common.SyncPhase, wave int) bool {
				// SyncFail runs only when something has already failed; holding it back would
				// prevent cleanup rather than order it.
				if phase == common.SyncPhaseSyncFail {
					return true
				}
				step := syncStep{phase: phase, wave: wave}
				if frontier.before(step) {
					return false
				}
				frontierHasPendingWork = true
				return true
			}
		}

		outcome, ok := m.syncDestination(ctx, app, project, state, compareResult, dest, dc, syncOp, ss, logEntry)
		if !ok {
			// syncDestination has already recorded a terminal state on the operation.
			return false
		}
		logEntry = outcome.logEntry
		outcomes[destName] = destinationOutcome{phase: state.Phase, message: state.Message, sync: outcome}
		return true
	}

	multiDest := len(compareResult.destOrder) > 1
	// Set once some destination has failed. Passed to the destinations that have SyncFail hooks of
	// their own, so that those hooks run even though nothing in that destination failed. Only the
	// first failure is reported: it is the one that caused the rest.
	forceReason := ""
	forced := map[string]bool{}

	for _, destName := range compareResult.destOrder {
		if _, ok := compareResult.resolvedDests[destName]; !ok {
			continue
		}
		dc, ok := compareResult.perDestination[destName]
		if !ok {
			continue
		}
		// Only a destination with SyncFail hooks has anything to gain from being forced. Forcing
		// one without them would report it as failed when it in fact applied cleanly.
		reason := ""
		if forceReason != "" && hasSyncFailHooks(dc) {
			reason = forceReason
			forced[destName] = true
		}
		if !runDestination(destName, reason, initialByDest[destName]) {
			return
		}
		if multiDest && forceReason == "" && destinationFailed(outcomes[destName].phase) {
			forceReason = forcedSyncFailReason(destName)
		}
	}

	// A destination that fails late leaves the ones before it already applied, with no failure of
	// their own for the engine to trigger their SyncFail hooks on. Run those again with the failure
	// forced, seeded with what they just did so that nothing is applied twice.
	if forceReason != "" {
		for _, destName := range compareResult.destOrder {
			o, ok := outcomes[destName]
			if !ok || forced[destName] || destinationFailed(o.phase) {
				continue
			}
			if !hasSyncFailHooks(compareResult.perDestination[destName]) {
				continue
			}
			if !runDestination(destName, forceReason, o.sync.resources) {
				return
			}
		}
	}

	for _, destName := range compareResult.destOrder {
		o, ok := outcomes[destName]
		if !ok {
			continue
		}
		destCluster := o.sync.cluster

		mergedPhase = mergeOperationPhase(mergedPhase, o.phase)
		if o.message != "" {
			if multiDest && destName != argo.PrimaryDestinationName {
				mergedMessages = append(mergedMessages, fmt.Sprintf("[%s] %s", destName, o.message))
			} else {
				mergedMessages = append(mergedMessages, o.message)
			}
		}

		var apiVersion []kube.APIResourceInfo
		for _, res := range o.sync.resources {
			augmentedMsg, err := argo.AugmentSyncMsg(res, func() ([]kube.APIResourceInfo, error) {
				if apiVersion == nil {
					_, apiVersion, err = m.liveStateCache.GetVersionsInfo(destCluster)
					if err != nil {
						return nil, fmt.Errorf("failed to get version info from the target cluster %q", destCluster.Server)
					}
				}
				return apiVersion, nil
			})

			if err != nil {
				log.Errorf("using the original message since: %v", err)
			} else {
				res.Message = augmentedMsg
			}

			state.SyncResult.Resources = append(state.SyncResult.Resources, &v1alpha1.ResourceResult{
				HookType:    res.HookType,
				Group:       res.ResourceKey.Group,
				Kind:        res.ResourceKey.Kind,
				Namespace:   res.ResourceKey.Namespace,
				Name:        res.ResourceKey.Name,
				Version:     res.Version,
				SyncPhase:   res.SyncPhase,
				HookPhase:   res.HookPhase,
				Status:      res.Status,
				Message:     res.Message,
				Images:      res.Images,
				Destination: destName,
			})
		}
	}

	state.Phase = mergedPhase
	state.Message = strings.Join(mergedMessages, "; ")

	if haveFrontier {
		// Advance only when every destination has finished the current step. Whatever the merged
		// phase says, an operation with steps still ahead of it is not finished.
		if !frontierHasPendingWork {
			if next, ok := nextFrontier(schedule, frontier); ok {
				frontier = next
			}
		}
		if _, more := nextFrontier(schedule, frontier); more || frontierHasPendingWork {
			if state.Phase == common.OperationSucceeded {
				state.Phase = common.OperationRunning
			}
		}
		state.SyncResult.WaveFrontier = &v1alpha1.SyncWaveFrontier{
			Phase: string(frontier.phase),
			Wave:  int64(frontier.wave),
		}
	}

	logEntry.WithField("duration", time.Since(start)).Info("sync/terminate complete")

	if !syncOp.DryRun && len(syncOp.Resources) == 0 && state.Phase.Successful() {
		err := m.persistRevisionHistory(app, compareResult.syncStatus.Revision, compareResult.syncStatus.ComparedTo.Source, compareResult.syncStatus.Revisions, compareResult.syncStatus.ComparedTo.Sources, isMultiSourceSync, state.StartedAt, state.Operation.InitiatedBy)
		if err != nil {
			state.Phase = common.OperationError
			state.Message = fmt.Sprintf("failed to record sync to history: %v", err)
		}
	}
}

// normalizeTargetResources modifies target resources to ensure ignored fields are not touched during synchronization:
//   - applies normalization to the target resources based on the live resources
//   - copies ignored fields from the matching live resources: apply normalizer to the live resource,
//     calculates the patch performed by normalizer and applies the patch to the target resource
//
// operationPhaseRank orders phases from most to least successful, so that merging keeps the worst.
func operationPhaseRank(p common.OperationPhase) int {
	switch p {
	case common.OperationError:
		return 5
	case common.OperationFailed:
		return 4
	case common.OperationTerminating:
		return 3
	case common.OperationRunning:
		return 2
	case common.OperationSucceeded:
		return 1
	default:
		return 0
	}
}

// mergeOperationPhase combines one destination's phase into the operation's overall phase, keeping
// the least successful of the two. An application is only Succeeded when every destination is.
func mergeOperationPhase(a, b common.OperationPhase) common.OperationPhase {
	if a == "" {
		return b
	}
	// A destination that has not finished keeps the whole operation unfinished, even when another
	// has already failed. A terminal phase says nothing about a destination that is still working,
	// and reporting the operation terminal would abandon that work -- including the SyncFail hooks
	// it has already started. The failed destination stays failed on the next pass, so the
	// operation still ends up failed once every destination has settled.
	if a.Completed() != b.Completed() {
		if a.Completed() {
			return b
		}
		return a
	}
	if operationPhaseRank(b) > operationPhaseRank(a) {
		return b
	}
	return a
}

// destinationFailed reports whether a destination has finished and did not succeed.
func destinationFailed(phase common.OperationPhase) bool {
	return phase.Completed() && !phase.Successful()
}

// forcedSyncFailReason is the failure message given to the destinations that have to run their
// SyncFail hooks because a different destination failed.
func forcedSyncFailReason(destName string) string {
	if destName == argo.PrimaryDestinationName {
		return "the primary destination failed to sync"
	}
	return fmt.Sprintf("destination %q failed to sync", destName)
}

// hasSyncFailHooks reports whether a destination has any SyncFail hook. Only such a destination has
// anything to gain from having another destination's failure forced onto it.
func hasSyncFailHooks(dc *destinationComparison) bool {
	if dc == nil {
		return false
	}
	for _, obj := range dc.reconciliationResult.Hooks {
		if obj == nil {
			continue
		}
		if slices.Contains(sync.SyncPhases(obj), common.SyncPhaseSyncFail) {
			return true
		}
	}
	return false
}

func normalizeTargetResources(openAPISchema openapi.Resources, cr *destinationComparison) ([]*unstructured.Unstructured, error) {
	// Normalize live and target resources (cleaning or aligning them)
	normalized, err := diff.Normalize(cr.reconciliationResult.Live, cr.reconciliationResult.Target, cr.diffConfig)
	if err != nil {
		return nil, err
	}

	patchedTargets := []*unstructured.Unstructured{}

	for idx, live := range cr.reconciliationResult.Live {
		normalizedTarget := normalized.Targets[idx]
		if normalizedTarget == nil {
			patchedTargets = append(patchedTargets, nil)
			continue
		}
		gvk := normalizedTarget.GroupVersionKind()

		originalTarget := cr.reconciliationResult.Target[idx]
		if live == nil {
			// No live resource, just use target
			patchedTargets = append(patchedTargets, originalTarget)
			continue
		}

		var (
			lookupPatchMeta strategicpatch.LookupPatchMeta
			versionedObject any
		)

		// Load patch meta struct or OpenAPI schema for CRDs
		if versionedObject, err = scheme.Scheme.New(gvk); err == nil {
			if lookupPatchMeta, err = strategicpatch.NewPatchMetaFromStruct(versionedObject); err != nil {
				return nil, err
			}
		} else if crdSchema := openAPISchema.LookupResource(gvk); crdSchema != nil {
			lookupPatchMeta = strategicpatch.NewPatchMetaFromOpenAPI(crdSchema)
		}

		// RespectIgnoreDifferences preserves ignored fields by copying their live
		// values into the target that is applied during sync. `status` must be
		// excluded from that copy: it is owned by the resource's own controller,
		// never by the sync. Merging live `status` into the apply makes the sync
		// field manager (ArgoCDSSAManager, "argocd-controller") a co-owner of
		// `status` under server-side apply. For resources without a /status
		// subresource (e.g. argoproj.io/Application) this freezes a stale
		// status.operationState.phase that the controller can no longer correct.
		liveForPatch, normalizedLiveForPatch := live, normalized.Lives[idx]
		liveForPatch = liveForPatch.DeepCopy()
		unstructured.RemoveNestedField(liveForPatch.Object, "status")

		if normalizedLiveForPatch != nil {
			normalizedLiveForPatch = normalizedLiveForPatch.DeepCopy()
			unstructured.RemoveNestedField(normalizedLiveForPatch.Object, "status")
		}

		// Calculate live patch
		livePatch, err := getMergePatch(normalizedLiveForPatch, liveForPatch, lookupPatchMeta)
		if err != nil {
			return nil, err
		}

		patchedTarget, err := applyMergePatch(normalizedTarget, livePatch, versionedObject, lookupPatchMeta)
		if err != nil {
			return nil, err
		}

		// Restore non-ignored fields that may have been overwritten due to
		// patchStrategy:"replace" on a parent field (e.g. policy/v1 PDB selector).
		// Strategic merge patch treats "replace" fields as atomic, so patching in
		// one ignored sub-field pulls the entire parent from live, clobbering
		// non-ignored sibling fields. We detect and undo this here.
		var normalizedLiveObj map[string]any
		if normalized.Lives[idx] != nil {
			normalizedLiveObj = normalized.Lives[idx].Object
		}
		restoreNonIgnoredFields(patchedTarget.Object, originalTarget.Object, normalizedTarget.Object, normalizedLiveObj)

		patchedTargets = append(patchedTargets, patchedTarget)
	}

	return patchedTargets, nil
}

// getMergePatch calculates and returns the patch between the original and the
// modified unstructures.
func getMergePatch(original, modified *unstructured.Unstructured, lookupPatchMeta strategicpatch.LookupPatchMeta) ([]byte, error) {
	originalJSON, err := original.MarshalJSON()
	if err != nil {
		return nil, err
	}
	modifiedJSON, err := modified.MarshalJSON()
	if err != nil {
		return nil, err
	}
	if lookupPatchMeta != nil {
		return strategicpatch.CreateThreeWayMergePatch(modifiedJSON, modifiedJSON, originalJSON, lookupPatchMeta, true)
	}

	return jsonpatch.CreateMergePatch(originalJSON, modifiedJSON)
}

// applyMergePatch will apply the given patch in the obj and return the patched unstructure.
func applyMergePatch(obj *unstructured.Unstructured, patch []byte, versionedObject any, meta strategicpatch.LookupPatchMeta) (*unstructured.Unstructured, error) {
	originalJSON, err := obj.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var patchedJSON []byte
	switch {
	case versionedObject != nil:
		patchedJSON, err = strategicpatch.StrategicMergePatch(originalJSON, patch, versionedObject)
	case meta != nil:
		var originalMap, patchMap map[string]any
		if err := json.Unmarshal(originalJSON, &originalMap); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(patch, &patchMap); err != nil {
			return nil, err
		}

		patchedMap, err := strategicpatch.StrategicMergeMapPatchUsingLookupPatchMeta(originalMap, patchMap, meta)
		if err != nil {
			return nil, err
		}
		patchedJSON, err = json.Marshal(patchedMap)
		if err != nil {
			return nil, err
		}
	default:
		patchedJSON, err = jsonpatch.MergePatch(originalJSON, patch)
	}
	if err != nil {
		return nil, err
	}

	patchedObj := &unstructured.Unstructured{}
	_, _, err = unstructured.UnstructuredJSONScheme.Decode(patchedJSON, nil, patchedObj)
	if err != nil {
		return nil, err
	}
	return patchedObj, nil
}

// restoreNonIgnoredFields walks patched, original, normalized (target), and
// normalizedLive in parallel. It corrects three classes of collateral damage
// caused by patchStrategy:"replace" treating a parent field as atomic:
//
//  1. Overwrite — a non-ignored value was replaced with the live value.
//  2. Drop — a non-ignored key was removed because live lacks it.
//  3. Add — a non-ignored live-only key leaked into the patched target.
//
// A field is considered "not ignored" when normalizedTarget == originalTarget
// for that field (the normalizer left it alone). For live-only keys (pass 2),
// a key is non-ignored if it exists in normalizedLive (the normalizer did not
// strip it from live).
func restoreNonIgnoredFields(patched, original, normalizedTarget, normalizedLive map[string]any) {
	// Pass 1: restore non-ignored fields that were overwritten or dropped.
	for key, originalVal := range original {
		patchedVal, inPatched := patched[key]
		normalizedVal, inNormalized := normalizedTarget[key]

		patchedMap, patchedIsMap := patchedVal.(map[string]any)
		originalMap, originalIsMap := originalVal.(map[string]any)
		normalizedMap, normalizedIsMap := normalizedVal.(map[string]any)

		if inPatched && patchedIsMap && originalIsMap && normalizedIsMap {
			var normalizedLiveMap map[string]any
			if v, ok := normalizedLive[key].(map[string]any); ok {
				normalizedLiveMap = v
			}
			restoreNonIgnoredFields(patchedMap, originalMap, normalizedMap, normalizedLiveMap)
			continue
		}

		// Leaf, type-changed, or missing field.
		// If normalized == original, the normalizer did not touch this field,
		// so it is not ignored and should keep the original (target) value.
		if inNormalized && reflect.DeepEqual(normalizedVal, originalVal) && (!inPatched || !reflect.DeepEqual(patchedVal, originalVal)) {
			patched[key] = originalVal
		}
	}

	// Pass 2: remove non-ignored keys that were introduced into patched from
	// the live object via replace-strategy collateral but do not exist in the
	// original target. A key is non-ignored if it exists in normalizedLive
	// (the normalizer did not strip it). Ignored live-only keys are kept —
	// they were intentionally copied by the livePatch.
	for key := range patched {
		if _, inOriginal := original[key]; inOriginal {
			continue
		}
		if _, inNormalizedLive := normalizedLive[key]; inNormalizedLive {
			delete(patched, key)
		}
	}
}

// hasSharedResourceCondition will check if the Application has any resource that has already
// been synced by another Application. If the resource is found in another Application it returns
// true along with a human readable message of which specific resource has this condition.
func hasSharedResourceCondition(app *v1alpha1.Application) (bool, string) {
	for _, condition := range app.Status.Conditions {
		if condition.Type == v1alpha1.ApplicationConditionSharedResourceWarning {
			return true, condition.Message
		}
	}
	return false, ""
}

// delayBetweenSyncWaves is a gitops-engine SyncWaveHook which introduces an artificial delay
// between each sync wave. We introduce an artificial delay in order give other controllers a
// _chance_ to react to the spec change that we just applied. This is important because without
// this, Argo CD will likely assess resource health too quickly (against the stale object), causing
// hooks to fire prematurely. See: https://github.com/argoproj/argo-cd/issues/4669.
// Note, this is not foolproof, since a proper fix would require the CRD record
// status.observedGeneration coupled with a health.lua that verifies
// status.observedGeneration == metadata.generation
func delayBetweenSyncWaves(_ common.SyncPhase, _ int, finalWave bool) error {
	if !finalWave {
		delaySec := 2
		if delaySecStr := os.Getenv(EnvVarSyncWaveDelay); delaySecStr != "" {
			if val, err := strconv.Atoi(delaySecStr); err == nil {
				delaySec = val
			}
		}
		duration := time.Duration(delaySec) * time.Second
		time.Sleep(duration)
	}
	return nil
}

func syncWindowPreventsSync(app *v1alpha1.Application, proj *v1alpha1.AppProject) (bool, error) {
	window := proj.Spec.SyncWindows.Matches(app)
	isManual := false
	var operationStartTime *time.Time
	if app.Status.OperationState != nil {
		isManual = !app.Status.OperationState.Operation.InitiatedBy.Automated
		if !app.Status.OperationState.StartedAt.IsZero() {
			t := app.Status.OperationState.StartedAt.Time
			operationStartTime = &t
		}
	}
	canSync, err := window.CanSync(isManual, operationStartTime)
	if err != nil {
		// prevents sync because sync window has an error
		return true, err
	}
	return !canSync, nil
}

// validateSyncPermissions checks whether the given resource is permitted by the project's
// allow/deny lists and destination rules. It returns an error if the API resource info is nil
// (preventing a nil-pointer panic), if the resource's group/kind is not permitted, or if
// the resource's namespace is not an allowed destination.
func validateSyncPermissions(
	project *v1alpha1.AppProject,
	destCluster *v1alpha1.Cluster,
	getProjectClusters func(string) ([]*v1alpha1.Cluster, error),
	un *unstructured.Unstructured,
	res *metav1.APIResource,
) error {
	if res == nil {
		return fmt.Errorf("failed to get API resource info for %s/%s: unable to verify permissions", un.GroupVersionKind().Group, un.GroupVersionKind().Kind)
	}
	if !project.IsGroupKindNamePermitted(un.GroupVersionKind().GroupKind(), un.GetName(), res.Namespaced) {
		return fmt.Errorf("resource %s:%s is not permitted in project %s", un.GroupVersionKind().Group, un.GroupVersionKind().Kind, project.Name)
	}
	if res.Namespaced {
		permitted, err := project.IsDestinationPermitted(destCluster, un.GetNamespace(), getProjectClusters)
		if err != nil {
			return err
		}

		if !permitted {
			return fmt.Errorf("namespace %v is not permitted in project '%s'", un.GetNamespace(), project.Name)
		}
	}
	return nil
}

// syncSettings carries the application-level inputs shared by every destination's sync context.
type syncSettings struct {
	resourceOverrides      map[string]v1alpha1.ResourceOverride
	prunePropagationPolicy metav1.DeletionPropagation
	clientSideApplyManager string
	installationID         string
	trackingMethod         string
	initialResourcesRes    []common.ResourceSyncResult
	// initialPhase and initialMessage are the operation's state on entry to this pass. They are
	// carried explicitly because every destination's sync context has to start from the same
	// state: syncDestination writes its result back into the operation, so reading it there would
	// hand each destination the phase the previous one happened to end on.
	initialPhase   common.OperationPhase
	initialMessage string
	// forceSyncFailReason, when set, makes this destination apply nothing and run its SyncFail
	// hooks instead, failing with this reason. It is set when a different destination has failed.
	forceSyncFailReason string
	// syncTaskFilter, when set, holds back tasks beyond the shared wave frontier. It is nil for a
	// single-destination application, which needs no coordination.
	syncTaskFilter func(phase common.SyncPhase, wave int) bool
}

// destinationSyncOutcome is what one destination's sync pass produced.
type destinationSyncOutcome struct {
	resources []common.ResourceSyncResult
	// cluster is the resolved destination cluster, needed to augment sync messages afterwards.
	cluster *v1alpha1.Cluster
	// logEntry may carry impersonation fields added while building the context.
	logEntry *log.Entry
}

// syncDestination builds a sync context for one destination and runs a single pass against it.
//
// Everything here is destination-specific: the REST configs, the service account impersonated, the
// permission validator, the default namespace and the resources the engine may touch. Gathering
// them behind one call is what will let an application sync to more than one cluster.
//
// It reports false when it has already recorded a terminal state on the operation and the caller
// should stop. The returned log entry may carry impersonation fields added while building the
// context.
func (m *appStateManager) syncDestination(
	ctx context.Context,
	app *v1alpha1.Application,
	project *v1alpha1.AppProject,
	state *v1alpha1.OperationState,
	compareResult *comparisonResult,
	dest argo.ResolvedDestination,
	dc *destinationComparison,
	syncOp v1alpha1.SyncOperation,
	ss syncSettings,
	logEntry *log.Entry,
) (destinationSyncOutcome, bool) {
	resourceOverrides := ss.resourceOverrides
	prunePropagationPolicy := ss.prunePropagationPolicy
	clientSideApplyManager := ss.clientSideApplyManager
	installationID := ss.installationID
	trackingMethod := ss.trackingMethod
	initialResourcesRes := ss.initialResourcesRes

	destCluster := dest.Cluster
	// The destination's own namespace, not the application's: a resource routed to a named
	// destination is applied into that destination's namespace and its service account is selected
	// by it.
	destNamespace := dest.Destination.Namespace

	rawConfig, err := destCluster.RawRestConfig()
	if err != nil {
		state.Phase = common.OperationError
		state.Message = err.Error()
		return destinationSyncOutcome{}, false
	}

	clusterRESTConfig, err := destCluster.RESTConfig()
	if err != nil {
		state.Phase = common.OperationError
		state.Message = err.Error()
		return destinationSyncOutcome{}, false
	}
	restConfig := metrics.AddMetricsTransportWrapper(m.metricsServer, app, clusterRESTConfig)

	// Which destination this context is, which cluster it will apply to, and how many objects it
	// carries. With more than one destination the cluster a resource reaches is the thing most
	// worth being able to confirm from a log, and it is not otherwise recoverable: the engine logs
	// the API server it applied to but never the destination that chose it.
	logEntry = logEntry.WithFields(log.Fields{
		"destination":       dest.Name,
		"destinationServer": destCluster.Server,
	})

	reconciliationResult := dc.reconciliationResult
	logEntry.Infof("syncing destination %q to cluster %s in namespace %q: %d target, %d live",
		dest.Name, destCluster.Server, destNamespace, len(reconciliationResult.Target), len(reconciliationResult.Live))

	// if RespectIgnoreDifferences is enabled, it should normalize the target
	// resources which in this case applies the live values in the configured
	// ignore differences fields.
	if syncOp.SyncOptions.HasOption("RespectIgnoreDifferences=true") {
		openAPISchema, err := m.getOpenAPISchema(destCluster)
		if err != nil {
			state.Phase = common.OperationError
			state.Message = fmt.Sprintf("failed to load openAPISchema: %v", err)
			return destinationSyncOutcome{}, false
		}

		patchedTargets, err := normalizeTargetResources(openAPISchema, dc)
		if err != nil {
			state.Phase = common.OperationError
			state.Message = fmt.Sprintf("Failed to normalize target resources: %s", err)
			return destinationSyncOutcome{}, false
		}
		reconciliationResult.Target = patchedTargets
	}

	impersonationEnabled, err := m.settingsMgr.IsImpersonationEnabled()
	if err != nil {
		log.Errorf("could not get impersonation feature flag: %v", err)
		return destinationSyncOutcome{}, false
	}
	if impersonationEnabled {
		serviceAccountToImpersonate, err := settings.DeriveServiceAccountToImpersonate(project, app, destCluster, destNamespace)
		if err != nil {
			state.Phase = common.OperationError
			state.Message = fmt.Sprintf("failed to derive service account to impersonate: %v", err)
			return destinationSyncOutcome{}, false
		}

		if serviceAccountToImpersonate == "" {
			// No matching service account found - check enforcement
			impersonationEnforced, enforcedErr := m.settingsMgr.IsImpersonationEnforced()
			if enforcedErr != nil {
				log.Errorf("could not get impersonation enforcement flag: %v", enforcedErr)
				state.Phase = common.OperationError
				state.Message = fmt.Sprintf("failed to check impersonation enforcement setting: %v", enforcedErr)
				return destinationSyncOutcome{}, false
			}

			if impersonationEnforced {
				state.Phase = common.OperationError
				state.Message = fmt.Sprintf("no matching service account found for destination server %s and namespace %s", destCluster.Server, destNamespace)
				return destinationSyncOutcome{}, false
			}

			// Non-enforced mode: log info and continue with controller SA
			logEntry.Infof("no matching service account found for impersonation (project: %s, server: %s, namespace: %s), falling back to controller service account", project.Name, destCluster.Server, destNamespace)
		} else {
			logEntry = logEntry.WithFields(log.Fields{"impersonationEnabled": "true", "serviceAccount": serviceAccountToImpersonate})
			// set the impersonation headers.
			rawConfig.Impersonate = rest.ImpersonationConfig{
				UserName: serviceAccountToImpersonate,
			}
			restConfig.Impersonate = rest.ImpersonationConfig{
				UserName: serviceAccountToImpersonate,
			}
		}
	}

	opts := []sync.SyncOpt{
		sync.WithLogr(logutils.NewLogrusLogger(logEntry)),
		sync.WithHealthOverride(lua.ResourceHealthOverrides(resourceOverrides)),
		sync.WithPermissionValidator(func(un *unstructured.Unstructured, res *metav1.APIResource) error {
			return validateSyncPermissions(project, destCluster, func(proj string) ([]*v1alpha1.Cluster, error) {
				return m.db.GetProjectClusters(ctx, proj)
			}, un, res)
		}),
		sync.WithOperationSettings(syncOp.DryRun, syncOp.Prune, syncOp.SyncStrategy.Force(), syncOp.IsApplyStrategy() || len(syncOp.Resources) > 0),
		sync.WithInitialState(ss.initialPhase, ss.initialMessage, initialResourcesRes, state.StartedAt),
		sync.WithResourcesFilter(func(key kube.ResourceKey, target *unstructured.Unstructured, live *unstructured.Unstructured) bool {
			return (len(syncOp.Resources) == 0 ||
				isPostDeleteHook(target) ||
				isPreDeleteHook(target) ||
				argo.ContainsSyncResource(key.Name, key.Namespace, schema.GroupVersionKind{Kind: key.Kind, Group: key.Group}, syncOp.Resources)) &&
				m.isSelfReferencedObj(live, target, app.GetName(), v1alpha1.TrackingMethod(trackingMethod), installationID)
		}),
		sync.WithManifestValidation(!syncOp.SyncOptions.HasOption(common.SyncOptionsDisableValidation)),
		sync.WithSyncWaveHook(delayBetweenSyncWaves),
		sync.WithPruneLast(syncOp.SyncOptions.HasOption(common.SyncOptionPruneLast)),
		sync.WithResourceModificationChecker(syncOp.SyncOptions.HasOption("ApplyOutOfSyncOnly=true"), dc.diffResultList),
		sync.WithPrunePropagationPolicy(&prunePropagationPolicy),
		sync.WithReplace(syncOp.SyncOptions.HasOption(common.SyncOptionReplace)),
		sync.WithServerSideApply(syncOp.SyncOptions.HasOption(common.SyncOptionServerSideApply)),
		sync.WithServerSideApplyManager(cdcommon.ArgoCDSSAManager),
		sync.WithClientSideApplyMigration(
			!syncOp.SyncOptions.HasOption(common.SyncOptionDisableClientSideApplyMigration),
			clientSideApplyManager,
		),
		sync.WithPruneConfirmed(app.IsDeletionConfirmed(state.StartedAt.Time)),
		sync.WithDefaultPruneOption(syncOp.SyncOptions.GetOptionValue(common.SyncOptionPrune)),
		sync.WithSkipDryRunOnMissingResource(syncOp.SyncOptions.HasOption(common.SyncOptionSkipDryRunOnMissingResource)),
	}

	if ss.syncTaskFilter != nil {
		opts = append(opts, sync.WithSyncTaskFilter(ss.syncTaskFilter))
	}

	if ss.forceSyncFailReason != "" {
		opts = append(opts, sync.WithForceSyncFailPhase(ss.forceSyncFailReason))
	}

	if syncOp.SyncOptions.HasOption("CreateNamespace=true") {
		opts = append(opts, sync.WithNamespaceModifier(syncNamespace(app.Spec.SyncPolicy)))
	}

	syncCtx, cleanup, err := sync.NewSyncContext(
		compareResult.syncStatus.Revision,
		reconciliationResult,
		restConfig,
		rawConfig,
		m.kubectl,
		destNamespace,
		opts...,
	)
	if err != nil {
		state.Phase = common.OperationError
		state.Message = fmt.Sprintf("failed to initialize sync context: %v", err)
		return destinationSyncOutcome{}, false
	}

	defer cleanup()

	if ss.initialPhase == common.OperationTerminating {
		syncCtx.Terminate(ctx)
	} else {
		syncCtx.Sync(ctx)
	}
	var resState []common.ResourceSyncResult
	state.Phase, state.Message, resState = syncCtx.GetState()

	return destinationSyncOutcome{resources: resState, cluster: destCluster, logEntry: logEntry}, true
}

// phaseOrder ranks sync phases in the order the engine executes them. SyncFail is deliberately
// absent: it runs only on failure and must never be held back by the frontier.
func phaseOrder(p common.SyncPhase) int {
	switch p {
	case common.SyncPhasePreSync:
		return 0
	case common.SyncPhaseSync:
		return 1
	case common.SyncPhasePostSync:
		return 2
	default:
		return 3
	}
}

// syncStep is one point in the sync-wave schedule shared by an application's destinations.
type syncStep struct {
	phase common.SyncPhase
	wave  int
}

// before reports whether s comes strictly before other in execution order.
func (s syncStep) before(other syncStep) bool {
	if po, oo := phaseOrder(s.phase), phaseOrder(other.phase); po != oo {
		return po < oo
	}
	return s.wave < other.wave
}

// destinationSteps returns every step a destination's objects will produce tasks in.
//
// Waves come from the sync-wave annotation and phases from the engine's own SyncPhases, so this
// matches how the engine schedules ordinary work. Prune and prune-last tasks are the exception: the
// engine rewrites their waves internally, so they cannot be predicted here and are left out. That is
// deliberate -- the frontier exists to order applying resources across clusters, and admitting a
// wave that never actually arrives would stall every destination behind it.
func destinationSteps(dc *destinationComparison) map[syncStep]bool {
	steps := map[syncStep]bool{}
	collect := func(obj *unstructured.Unstructured) {
		if obj == nil {
			return
		}
		wave := syncwaves.Wave(obj)
		for _, phase := range sync.SyncPhases(obj) {
			if phase == common.SyncPhaseSyncFail {
				continue
			}
			steps[syncStep{phase: phase, wave: wave}] = true
		}
	}
	for _, obj := range dc.reconciliationResult.Target {
		collect(obj)
	}
	for _, obj := range dc.reconciliationResult.Hooks {
		collect(obj)
	}
	return steps
}

// syncSchedule is the ordered union of the steps of every destination: the sequence a
// multi-destination sync advances through, one step at a time, in lockstep.
func syncSchedule(perDestination map[string]*destinationComparison, destOrder []string) []syncStep {
	all := map[syncStep]bool{}
	for _, name := range destOrder {
		dc, ok := perDestination[name]
		if !ok {
			continue
		}
		for step := range destinationSteps(dc) {
			all[step] = true
		}
	}
	schedule := make([]syncStep, 0, len(all))
	for step := range all {
		schedule = append(schedule, step)
	}
	sort.Slice(schedule, func(i, j int) bool { return schedule[i].before(schedule[j]) })
	return schedule
}

// frontierStep reads the frontier recorded on the operation, defaulting to the first step of the
// schedule when a sync has not started advancing yet.
func frontierStep(result *v1alpha1.SyncOperationResult, schedule []syncStep) (syncStep, bool) {
	if len(schedule) == 0 {
		return syncStep{}, false
	}
	if result == nil || result.WaveFrontier == nil {
		return schedule[0], true
	}
	return syncStep{
		phase: common.SyncPhase(result.WaveFrontier.Phase),
		wave:  int(result.WaveFrontier.Wave),
	}, true
}

// nextFrontier returns the step after the given one, and whether one exists.
func nextFrontier(schedule []syncStep, current syncStep) (syncStep, bool) {
	for _, step := range schedule {
		if current.before(step) {
			return step, true
		}
	}
	return syncStep{}, false
}
