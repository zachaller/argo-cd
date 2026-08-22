package sync

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/sync/common"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/sync/hook"
)

// SyncPhases returns the phases a resource participates in: the sync phase for ordinary resources,
// and whichever of PreSync, Sync, PostSync and SyncFail a hook declares. A resource marked to be
// skipped participates in none.
//
// It is exported so that a caller coordinating several sync contexts can work out which phases an
// object will produce tasks in before running them.
func SyncPhases(obj *unstructured.Unstructured) []common.SyncPhase {
	if hook.Skip(obj) {
		return nil
	} else if hook.IsHook(obj) {
		phasesMap := make(map[common.SyncPhase]bool)
		for _, hookType := range hook.Types(obj) {
			switch hookType {
			case common.HookTypePreSync, common.HookTypeSync, common.HookTypePostSync, common.HookTypeSyncFail:
				phasesMap[common.SyncPhase(hookType)] = true
			}
		}
		var phases []common.SyncPhase
		for phase := range phasesMap {
			phases = append(phases, phase)
		}
		return phases
	}
	return []common.SyncPhase{common.SyncPhaseSync}
}

func syncPhases(obj *unstructured.Unstructured) []common.SyncPhase {
	return SyncPhases(obj)
}
