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

// PrimaryDestinationName is re-exported from the API package, where FindNode and the destination
// selectors also need it, so that both refer to one definition.
const PrimaryDestinationName = argoappv1.PrimaryDestinationName

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

// MultiDestinationEnabledChecker reads the multi-destination feature gate. It is satisfied by
// *settings.SettingsManager.
type MultiDestinationEnabledChecker interface {
	IsMultiDestinationEnabled() (bool, error)
}

// ValidateMultiDestinationGate reports whether an application may declare named destinations.
//
// The setting is only read when the application actually declares destinations, so an application
// that does not use the feature never touches the settings ConfigMap and cannot be affected by a
// transient failure reading it. A read failure for an application that does use the feature is
// surfaced rather than silently treated as "disabled", which would reject a valid spec.
func ValidateMultiDestinationGate(spec *argoappv1.ApplicationSpec, checker MultiDestinationEnabledChecker) []argoappv1.ApplicationCondition {
	if !spec.HasMultipleDestinations() {
		return nil
	}
	enabled, err := checker.IsMultiDestinationEnabled()
	if err != nil {
		return []argoappv1.ApplicationCondition{{
			Type:    argoappv1.ApplicationConditionUnknownError,
			Message: fmt.Sprintf("could not determine whether multiple destinations are enabled: %v", err),
		}}
	}
	return ValidateMultiDestinationDisabled(spec, enabled)
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

// ValidateDistinctDestinations checks that no two resolved destinations refer to the same cluster.
//
// Two destinations sharing a cluster cannot be told apart, even with different namespaces. Live
// objects are fetched per cluster and attributed to an application by its tracking annotation, which
// names the application and not the destination, so both partitions receive the same live set. Each
// then sees the other's resources as unmatched extras: with pruning on they delete each other's
// resources, and without it both report OutOfSync forever.
//
// Attributing a live object to a destination would mean putting the destination in the tracking
// annotation, which would rewrite it on every resource of every existing application on upgrade.
// Requiring a cluster per destination is the cheaper correct answer, and matches what the feature is
// for: an application deploying into more than one cluster.
func ValidateDistinctDestinations(resolved map[string]ResolvedDestination, order []string) []string {
	var errs []string
	seen := make(map[string]string, len(order))
	for _, name := range order {
		d, ok := resolved[name]
		if !ok || d.Cluster == nil {
			continue
		}
		if previous, dup := seen[d.Cluster.Server]; dup {
			errs = append(errs, fmt.Sprintf("destinations %s and %s both resolve to cluster %q; each destination must be a different cluster",
				describeDestination(previous), describeDestination(name), d.Cluster.Server))
			continue
		}
		seen[d.Cluster.Server] = name
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
