package clusters

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// LabelSourceCluster records the verbatim downstream cluster name an aggregated
	// object originates from. It is the authoritative routing key for write operations.
	LabelSourceCluster = "aggregator.argoproj.io/source-cluster"
	// LabelSourceNamespace records the verbatim downstream namespace an aggregated
	// object originates from.
	LabelSourceNamespace = "aggregator.argoproj.io/source-namespace"

	// clusterPrefixMaxLen bounds the human-readable cluster portion of a synthetic
	// namespace so that "<prefix>-<hash>" always fits within the RFC1123 63-char limit
	// (prefix + '-' + 8 hex chars = at most 52).
	clusterPrefixMaxLen = 43
	// hashLen is the number of hex characters of the disambiguating suffix hash.
	hashLen = 8
)

// dnsLabelInvalid matches any run of characters that are not valid inside an RFC1123
// DNS label (lowercase alphanumeric and '-').
var dnsLabelInvalid = regexp.MustCompile(`[^a-z0-9-]+`)

// Target identifies a concrete downstream destination for an aggregated resource.
type Target struct {
	// Cluster is the downstream cluster name (matches the Argo cluster secret name).
	Cluster string
	// Server is the downstream cluster API server URL.
	Server string
	// Namespace is the real downstream namespace.
	Namespace string
}

// SyntheticNamespace deterministically maps a (cluster, downstream namespace) pair to a
// single synthetic namespace presented by the aggregator. The cluster name is kept as a
// human-readable prefix for grouping, and a short hash of the full (cluster, namespace)
// pair is appended to guarantee uniqueness and a bounded, valid RFC1123 result. The
// namespace itself is hashed (not embedded) so the composite never exceeds the 63-char
// limit regardless of downstream namespace length.
func SyntheticNamespace(cluster, namespace string) string {
	prefix := sanitizeDNSLabel(cluster)
	if len(prefix) > clusterPrefixMaxLen {
		prefix = strings.Trim(prefix[:clusterPrefixMaxLen], "-")
	}
	if prefix == "" {
		prefix = "cluster"
	}
	sum := sha256.Sum256([]byte(cluster + "/" + namespace))
	return prefix + "-" + hex.EncodeToString(sum[:])[:hashLen]
}

// sanitizeDNSLabel lowercases the input and replaces any character that is not valid in an
// RFC1123 DNS label with '-', trimming leading/trailing dashes.
func sanitizeDNSLabel(s string) string {
	s = strings.ToLower(s)
	s = dnsLabelInvalid.ReplaceAllString(s, "-")
	return strings.Trim(s, "-")
}

// Tag rewrites an aggregated object's metadata so that it appears to live in the synthetic
// namespace, while preserving the true downstream cluster and namespace on labels. It also
// records the synthetic -> Target mapping so reads can later be resolved without parsing.
func (r *Registry) Tag(obj metav1.Object, t Target) {
	synthetic := SyntheticNamespace(t.Cluster, t.Namespace)
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[LabelSourceCluster] = t.Cluster
	labels[LabelSourceNamespace] = t.Namespace
	obj.SetLabels(labels)
	obj.SetNamespace(synthetic)
	r.reverse.Store(synthetic, t)
}

// Untag restores the real downstream namespace on an object that is about to be written to a
// downstream cluster and strips the aggregator's bookkeeping labels.
func Untag(obj metav1.Object, t Target) {
	obj.SetNamespace(t.Namespace)
	labels := obj.GetLabels()
	if labels != nil {
		delete(labels, LabelSourceCluster)
		delete(labels, LabelSourceNamespace)
		obj.SetLabels(labels)
	}
}

// ResolveForWrite determines the downstream Target for a write operation. The
// source-cluster/source-namespace labels on the submitted object are authoritative; the
// reverse map (populated by prior reads) is used as a fallback when labels are absent.
func (r *Registry) ResolveForWrite(ctx context.Context, synthetic string, obj metav1.Object) (Target, error) {
	if obj != nil {
		labels := obj.GetLabels()
		cluster := labels[LabelSourceCluster]
		namespace := labels[LabelSourceNamespace]
		if cluster != "" && namespace != "" {
			server, err := r.serverForCluster(ctx, cluster)
			if err != nil {
				return Target{}, err
			}
			return Target{Cluster: cluster, Server: server, Namespace: namespace}, nil
		}
	}
	if t, ok := r.Resolve(synthetic); ok {
		return t, nil
	}
	return Target{}, fmt.Errorf("cannot resolve downstream target for namespace %q: set the %s and %s labels", synthetic, LabelSourceCluster, LabelSourceNamespace)
}

// Resolve returns the downstream Target previously recorded for a synthetic namespace.
func (r *Registry) Resolve(synthetic string) (Target, bool) {
	v, ok := r.reverse.Load(synthetic)
	if !ok {
		return Target{}, false
	}
	return v.(Target), true
}
