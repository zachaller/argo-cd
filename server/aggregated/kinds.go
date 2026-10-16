package aggregated

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/duration"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	views "github.com/argoproj/argo-cd/v3/pkg/apis/views/v1alpha1"
)

// kind describes one of the resources served by the aggregated API server.
type kind struct {
	resource   string
	singular   string
	shortNames []string
	// project builds the projection of an Application.
	project func(p *projector, app *appv1.Application) (runtime.Object, error)
	// newObject returns an empty object with only its metadata set.
	newObject func(meta metav1.ObjectMeta) runtime.Object
	// newList returns a list of the given items, which must be of this kind.
	newList      func(items []runtime.Object, resourceVersion string) runtime.Object
	columns      []metav1.TableColumnDefinition
	cells        func(obj runtime.Object) []any
	extractItems func(list runtime.Object) []runtime.Object
}

var nameColumn = metav1.TableColumnDefinition{Name: "Name", Type: "string", Format: "name", Description: "Name of the Application"}

var projectColumn = metav1.TableColumnDefinition{Name: "Project", Type: "string", Description: "AppProject of the Application"}

var readyColumn = metav1.TableColumnDefinition{Name: "Ready", Type: "string", Description: "Whether the application controller has computed the state of the Application"}

var ageColumn = metav1.TableColumnDefinition{Name: "Age", Type: "string", Description: "Age of the Application"}

func readyStatus(status views.ProjectionStatus) string {
	for _, condition := range status.Conditions {
		if condition.Type == views.ConditionTypeReady {
			return string(condition.Status)
		}
	}
	return string(metav1.ConditionUnknown)
}

func age(meta metav1.ObjectMeta) string {
	if meta.CreationTimestamp.IsZero() {
		return "<unknown>"
	}
	return duration.HumanDuration(time.Since(meta.CreationTimestamp.Time))
}

var treeKind = &kind{
	resource:   views.ApplicationTreeResource,
	singular:   views.ApplicationTreeSingular,
	shortNames: []string{views.ApplicationTreeShort},
	project: func(p *projector, app *appv1.Application) (runtime.Object, error) {
		return p.tree(app)
	},
	newObject: func(meta metav1.ObjectMeta) runtime.Object {
		return &views.ApplicationTree{ObjectMeta: meta}
	},
	newList: func(items []runtime.Object, resourceVersion string) runtime.Object {
		list := &views.ApplicationTreeList{ResourceVersion: resourceVersion, Items: make([]views.ApplicationTree, 0, len(items))}
		for _, item := range items {
			list.Items = append(list.Items, *item.(*views.ApplicationTree))
		}
		return list
	},
	columns: []metav1.TableColumnDefinition{
		nameColumn,
		projectColumn,
		{Name: "Nodes", Type: "integer", Description: "Number of resources in the tree"},
		{Name: "Orphaned", Type: "integer", Description: "Number of orphaned resources"},
		readyColumn,
		ageColumn,
	},
	cells: func(obj runtime.Object) []any {
		tree := obj.(*views.ApplicationTree)
		return []any{tree.Name, tree.Status.Project, int64(len(tree.Tree.Nodes)), int64(len(tree.Tree.OrphanedNodes)), readyStatus(tree.Status), age(tree.ObjectMeta)}
	},
	extractItems: func(list runtime.Object) []runtime.Object {
		trees := list.(*views.ApplicationTreeList)
		items := make([]runtime.Object, 0, len(trees.Items))
		for i := range trees.Items {
			items = append(items, &trees.Items[i])
		}
		return items
	},
}

var diffKind = &kind{
	resource:   views.ApplicationDiffResource,
	singular:   views.ApplicationDiffSingular,
	shortNames: []string{views.ApplicationDiffShort},
	project: func(p *projector, app *appv1.Application) (runtime.Object, error) {
		return p.diff(app)
	},
	newObject: func(meta metav1.ObjectMeta) runtime.Object {
		return &views.ApplicationDiff{ObjectMeta: meta}
	},
	newList: func(items []runtime.Object, resourceVersion string) runtime.Object {
		list := &views.ApplicationDiffList{ResourceVersion: resourceVersion, Items: make([]views.ApplicationDiff, 0, len(items))}
		for _, item := range items {
			list.Items = append(list.Items, *item.(*views.ApplicationDiff))
		}
		return list
	},
	columns: []metav1.TableColumnDefinition{
		nameColumn,
		projectColumn,
		{Name: "Resources", Type: "integer", Description: "Number of managed resources"},
		{Name: "Modified", Type: "integer", Description: "Number of managed resources whose live state differs from the desired state"},
		readyColumn,
		ageColumn,
	},
	cells: func(obj runtime.Object) []any {
		diff := obj.(*views.ApplicationDiff)
		modified := int64(0)
		for _, res := range diff.Resources {
			if res.Modified {
				modified++
			}
		}
		return []any{diff.Name, diff.Status.Project, int64(len(diff.Resources)), modified, readyStatus(diff.Status), age(diff.ObjectMeta)}
	},
	extractItems: func(list runtime.Object) []runtime.Object {
		diffs := list.(*views.ApplicationDiffList)
		items := make([]runtime.Object, 0, len(diffs.Items))
		for i := range diffs.Items {
			items = append(items, &diffs.Items[i])
		}
		return items
	},
}
