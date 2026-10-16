package aggregated

import (
	"maps"
	"strings"

	apiextensionsopenapi "k8s.io/apiextensions-apiserver/pkg/generated/openapi"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/util"
	"k8s.io/kube-openapi/pkg/validation/spec"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	views "github.com/argoproj/argo-cd/v3/pkg/apis/views/v1alpha1"
)

// getOpenAPIDefinitions returns the OpenAPI definitions of the views API, of the Argo CD types it embeds and of the
// Kubernetes meta types.
//
// The definitions of the Argo CD types reference Kubernetes types by Go package path, whereas the Kubernetes
// definitions are keyed by OpenAPI model name. References by package path are aliased to the matching model name.
// References to Kubernetes types without generated definitions (e.g. core/v1 types nested deep in the resource
// tree) are described as free-form objects, rather than compiling in the definitions of all Kubernetes APIs.
func getOpenAPIDefinitions(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
	defs := map[string]common.OpenAPIDefinition{}
	maps.Copy(defs, apiextensionsopenapi.GetOpenAPIDefinitions(ref))
	maps.Copy(defs, appv1.GetOpenAPIDefinitions(ref))
	maps.Copy(defs, views.GetOpenAPIDefinitions(ref))

	for missing := missingDependencies(defs); len(missing) > 0; missing = missingDependencies(defs) {
		for _, name := range missing {
			if alias, ok := defs[util.ToRESTFriendlyName(name)]; ok {
				defs[name] = alias
				continue
			}
			defs[name] = common.OpenAPIDefinition{Schema: spec.Schema{
				SchemaProps: spec.SchemaProps{
					Description: "See the " + name + " Go type.",
					Type:        []string{"object"},
				},
				VendorExtensible: spec.VendorExtensible{Extensions: spec.Extensions{"x-kubernetes-preserve-unknown-fields": true}},
			}}
		}
	}
	return defs
}

func missingDependencies(defs map[string]common.OpenAPIDefinition) []string {
	var missing []string
	seen := map[string]bool{}
	for _, def := range defs {
		for _, dep := range def.Dependencies {
			if _, ok := defs[dep]; !ok && !seen[dep] {
				seen[dep] = true
				missing = append(missing, dep)
			}
		}
	}
	return missing
}

// definitionNamer names definitions after the GroupVersionKinds of the views API, and gives all other definitions
// the REST friendly form of their name (e.g. io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta), so that aliased
// definitions collapse into one.
type definitionNamer struct {
	namer *openapinamer.DefinitionNamer
}

func newDefinitionNamer() *definitionNamer {
	// Scheme also registers the types as the internal version, which must not appear in the OpenAPI GVK extensions.
	external := runtime.NewScheme()
	utilruntime.Must(views.AddToScheme(external))
	return &definitionNamer{namer: openapinamer.NewDefinitionNamer(external)}
}

func (d *definitionNamer) GetDefinitionName(name string) (string, spec.Extensions) {
	friendly, extensions := d.namer.GetDefinitionName(name)
	if extensions == nil && strings.Contains(friendly, "/") {
		friendly = util.ToRESTFriendlyName(friendly)
	}
	return friendly, extensions
}
