package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// Group is the API group served by argocd-aggregated-apiserver.
	Group = "views.argoproj.io"
	// Version is the only version of the views API group.
	Version = "v1alpha1"

	ApplicationTreeKind     = "ApplicationTree"
	ApplicationTreeResource = "applicationtrees"
	ApplicationTreeSingular = "applicationtree"
	ApplicationTreeShort    = "apptree"

	ApplicationDiffKind     = "ApplicationDiff"
	ApplicationDiffResource = "applicationdiffs"
	ApplicationDiffSingular = "applicationdiff"
	ApplicationDiffShort    = "appdiff"
)

// SchemeGroupVersion is group version used to register these objects
var SchemeGroupVersion = schema.GroupVersion{Group: Group, Version: Version}

// Resource takes an unqualified resource and returns a Group-qualified GroupResource.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

// addKnownTypes adds the set of types defined in this package to the supplied scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ApplicationTree{},
		&ApplicationTreeList{},
		&ApplicationDiff{},
		&ApplicationDiffList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
