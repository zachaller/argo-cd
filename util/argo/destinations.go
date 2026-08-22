package argo

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/argoproj/argo-cd/v3/common"
	argoappv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
)

// PrimaryDestinationName is the name of the implicit destination backed by spec.destination.
// Manifests without the argocd.argoproj.io/destination annotation are routed to it.
const PrimaryDestinationName = ""

// destinationNameDisallowedCharSet lists characters that may not appear in a destination name.
// '/' and '@' are excluded because RBAC object strings use them as separators.
const destinationNameDisallowedCharSet = "/@"

// ResolvedDestination pairs a destination declared on an Application with the cluster it resolves to.
type ResolvedDestination struct {
	// Name is the destination's name, or PrimaryDestinationName for spec.destination.
	Name string
	// Destination is the declared destination, in the form accepted by AppProject permission checks.
	Destination argoappv1.ApplicationDestination
	// Cluster is the cluster the destination resolves to.
	Cluster *argoappv1.Cluster
}

// ResolveDestinations resolves spec.destination and every entry in spec.destinations to a cluster.
// The primary destination is always present under PrimaryDestinationName. The returned slice of
// names gives a stable iteration order: the primary first, then the named destinations in the order
// they are declared.
//
// An error is returned if any destination fails to resolve, so that a partially resolved application
// is never reconciled.
func ResolveDestinations(ctx context.Context, spec *argoappv1.ApplicationSpec, db ClusterGetter) (map[string]ResolvedDestination, []string, error) {
	resolved := make(map[string]ResolvedDestination, len(spec.Destinations)+1)
	order := make([]string, 0, len(spec.Destinations)+1)

	primary, err := GetDestinationCluster(ctx, spec.Destination, db)
	if err != nil {
		return nil, nil, err
	}
	resolved[PrimaryDestinationName] = ResolvedDestination{
		Name:        PrimaryDestinationName,
		Destination: spec.Destination,
		Cluster:     primary,
	}
	order = append(order, PrimaryDestinationName)

	for _, named := range spec.Destinations {
		dest := named.ToApplicationDestination()
		cluster, err := GetDestinationCluster(ctx, dest, db)
		if err != nil {
			return nil, nil, fmt.Errorf("destination %q: %w", named.Name, err)
		}
		resolved[named.Name] = ResolvedDestination{
			Name:        named.Name,
			Destination: dest,
			Cluster:     cluster,
		}
		order = append(order, named.Name)
	}

	return resolved, order, nil
}

// ValidateMultiDestinationDisabled returns a condition when an application declares named
// destinations while the multi-destination feature is switched off in argocd-cm. Returning a
// condition rather than ignoring the field means the spec is visibly rejected instead of silently
// deploying everything to the primary destination.
func ValidateMultiDestinationDisabled(spec *argoappv1.ApplicationSpec, enabled bool) []argoappv1.ApplicationCondition {
	if enabled || !spec.HasMultipleDestinations() {
		return nil
	}
	return []argoappv1.ApplicationCondition{{
		Type:    argoappv1.ApplicationConditionInvalidSpecError,
		Message: "spec.destinations is set but multiple destinations are not enabled; set application.destinations.enabled to \"true\" in argocd-cm",
	}}
}

// ValidateDestinationNames checks that every named destination has a usable, unique name. It does
// not resolve clusters, so it is safe to call before the cluster database is reachable.
func ValidateDestinationNames(destinations []argoappv1.NamedDestination) []string {
	var errs []string
	seen := make(map[string]bool, len(destinations))
	for i, d := range destinations {
		switch {
		case d.Name == "":
			errs = append(errs, fmt.Sprintf("spec.destinations[%d]: name must not be empty", i))
		case strings.ContainsAny(d.Name, destinationNameDisallowedCharSet):
			errs = append(errs, fmt.Sprintf("spec.destinations[%d]: name %q must not contain any of %q", i, d.Name, destinationNameDisallowedCharSet))
		case seen[d.Name]:
			errs = append(errs, fmt.Sprintf("spec.destinations[%d]: duplicate destination name %q", i, d.Name))
		default:
			seen[d.Name] = true
		}
	}
	return errs
}

// ValidateDistinctDestinations checks that no two resolved destinations refer to the same cluster
// and namespace.
//
// Overlapping destinations would share a single cluster cache, so the same live object would be
// handed to both partitions and both would claim ownership of it, producing a repeated apply and an
// oscillating diff. Rejecting the spec is the only safe outcome.
func ValidateDistinctDestinations(resolved map[string]ResolvedDestination, order []string) []string {
	var errs []string
	type target struct{ server, namespace string }
	seen := make(map[target]string, len(order))
	for _, name := range order {
		d, ok := resolved[name]
		if !ok || d.Cluster == nil {
			continue
		}
		key := target{server: d.Cluster.Server, namespace: d.Destination.Namespace}
		if previous, dup := seen[key]; dup {
			errs = append(errs, fmt.Sprintf("destinations %s and %s both target cluster %q namespace %q; destinations must be distinct",
				describeDestination(previous), describeDestination(name), key.server, key.namespace))
			continue
		}
		seen[key] = name
	}
	return errs
}

func describeDestination(name string) string {
	if name == PrimaryDestinationName {
		return "spec.destination"
	}
	return fmt.Sprintf("%q", name)
}

// DestinationNameForObject returns the destination the manifest is annotated for, or
// PrimaryDestinationName when it carries no destination annotation.
func DestinationNameForObject(un *unstructured.Unstructured) string {
	if un == nil {
		return PrimaryDestinationName
	}
	return un.GetAnnotations()[common.AnnotationKeyDestination]
}

// PartitionByDestination groups target objects by the destination they are annotated for. Every
// destination in resolved is present in the result, possibly with no objects.
//
// Objects annotated for a destination the application does not declare are dropped and reported.
// They are never routed to the primary destination: silently deploying a resource to the wrong
// cluster is worse than not deploying it at all.
func PartitionByDestination(objs []*unstructured.Unstructured, resolved map[string]ResolvedDestination) (map[string][]*unstructured.Unstructured, []string) {
	partitions := make(map[string][]*unstructured.Unstructured, len(resolved))
	for name := range resolved {
		partitions[name] = nil
	}

	unknown := make(map[string][]string)
	for _, obj := range objs {
		name := DestinationNameForObject(obj)
		if _, ok := resolved[name]; !ok {
			unknown[name] = append(unknown[name], fmt.Sprintf("%s/%s", obj.GetKind(), obj.GetName()))
			continue
		}
		partitions[name] = append(partitions[name], obj)
	}

	if len(unknown) == 0 {
		return partitions, nil
	}

	names := make([]string, 0, len(unknown))
	for name := range unknown {
		names = append(names, name)
	}
	sort.Strings(names)

	errs := make([]string, 0, len(names))
	for _, name := range names {
		errs = append(errs, fmt.Sprintf("%s reference undeclared destination %q; add it to spec.destinations",
			strings.Join(unknown[name], ", "), name))
	}
	return partitions, errs
}
