package aggregated

import (
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	views "github.com/argoproj/argo-cd/v3/pkg/apis/views/v1alpha1"
)

var (
	// Scheme contains the types served by the aggregated API server.
	Scheme = runtime.NewScheme()
	// Codecs provides the serializers for Scheme.
	Codecs = serializer.NewCodecFactory(Scheme)
)

func init() {
	utilruntime.Must(views.AddToScheme(Scheme))
	// The views API has no separate internal representation. Registering the versioned types as the internal version
	// lets the generic API machinery serve them without any conversion functions.
	Scheme.AddKnownTypes(schema.GroupVersion{Group: views.Group, Version: runtime.APIVersionInternal},
		&views.ApplicationTree{},
		&views.ApplicationTreeList{},
		&views.ApplicationDiff{},
		&views.ApplicationDiffList{},
	)
	utilruntime.Must(metainternalversion.AddToScheme(Scheme))
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})
	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	Scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
}
