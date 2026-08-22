package argo

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/argoproj/argo-cd/v3/common"
	argoappv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	dbmocks "github.com/argoproj/argo-cd/v3/util/db/mocks"
)

func testObj(kind, name, destination string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       kind,
		"metadata":   map[string]any{"name": name},
	}}
	if destination != "" {
		obj.SetAnnotations(map[string]string{common.AnnotationKeyDestination: destination})
	}
	return obj
}

func TestDestinationNameForObject(t *testing.T) {
	t.Parallel()

	assert.Equal(t, PrimaryDestinationName, DestinationNameForObject(nil))
	assert.Equal(t, PrimaryDestinationName, DestinationNameForObject(testObj("Service", "svc", "")))
	assert.Equal(t, "prod", DestinationNameForObject(testObj("Service", "svc", "prod")))
}

func TestValidateDestinationNames(t *testing.T) {
	t.Parallel()

	t.Run("valid", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, ValidateDestinationNames([]argoappv1.NamedDestination{
			{Name: "prod", Server: "https://prod"},
			{Name: "staging", Server: "https://staging"},
		}))
	})

	t.Run("empty name", func(t *testing.T) {
		t.Parallel()
		errs := ValidateDestinationNames([]argoappv1.NamedDestination{{Server: "https://prod"}})
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0], "must not be empty")
	})

	t.Run("duplicate name", func(t *testing.T) {
		t.Parallel()
		errs := ValidateDestinationNames([]argoappv1.NamedDestination{
			{Name: "prod", Server: "https://a"},
			{Name: "prod", Server: "https://b"},
		})
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0], "duplicate destination name")
	})

	t.Run("separators reserved by RBAC object strings are rejected", func(t *testing.T) {
		t.Parallel()
		errs := ValidateDestinationNames([]argoappv1.NamedDestination{
			{Name: "prod/eu", Server: "https://a"},
			{Name: "prod@eu", Server: "https://b"},
		})
		assert.Len(t, errs, 2)
	})
}

func TestResolveDestinations(t *testing.T) {
	t.Parallel()

	t.Run("primary plus named, in declaration order", func(t *testing.T) {
		t.Parallel()
		spec := &argoappv1.ApplicationSpec{
			Destination:  argoappv1.ApplicationDestination{Server: "https://primary", Namespace: "default"},
			Destinations: []argoappv1.NamedDestination{{Name: "prod", Server: "https://prod", Namespace: "apps"}},
		}
		db := &dbmocks.ArgoDB{}
		db.EXPECT().GetCluster(mock.Anything, "https://primary").Return(&argoappv1.Cluster{Server: "https://primary"}, nil).Maybe()
		db.EXPECT().GetCluster(mock.Anything, "https://prod").Return(&argoappv1.Cluster{Server: "https://prod"}, nil).Maybe()

		resolved, order, err := ResolveDestinations(t.Context(), spec, db)
		require.NoError(t, err)
		assert.Equal(t, []string{PrimaryDestinationName, "prod"}, order)
		assert.Equal(t, "https://primary", resolved[PrimaryDestinationName].Cluster.Server)
		assert.Equal(t, "https://prod", resolved["prod"].Cluster.Server)
	})

	t.Run("an unresolvable named destination fails the whole resolution", func(t *testing.T) {
		t.Parallel()
		spec := &argoappv1.ApplicationSpec{
			Destination:  argoappv1.ApplicationDestination{Server: "https://primary", Namespace: "default"},
			Destinations: []argoappv1.NamedDestination{{Name: "prod"}},
		}
		db := &dbmocks.ArgoDB{}
		db.EXPECT().GetCluster(mock.Anything, "https://primary").Return(&argoappv1.Cluster{Server: "https://primary"}, nil).Maybe()

		_, _, err := ResolveDestinations(t.Context(), spec, db)
		require.ErrorContains(t, err, "prod")
	})
}

func TestValidateDistinctDestinations(t *testing.T) {
	t.Parallel()

	t.Run("distinct clusters are allowed", func(t *testing.T) {
		t.Parallel()
		resolved := map[string]ResolvedDestination{
			PrimaryDestinationName: {Destination: argoappv1.ApplicationDestination{Namespace: "a"}, Cluster: &argoappv1.Cluster{Server: "https://one"}},
			"second":               {Name: "second", Destination: argoappv1.ApplicationDestination{Namespace: "b"}, Cluster: &argoappv1.Cluster{Server: "https://two"}},
		}
		assert.Empty(t, ValidateDistinctDestinations(resolved, []string{PrimaryDestinationName, "second"}))
	})

	t.Run("the same cluster is rejected even with different namespaces", func(t *testing.T) {
		t.Parallel()
		// Live objects are fetched per cluster and attributed by an annotation that names the
		// application, not the destination, so two destinations on one cluster receive the same
		// live set and each treats the other's resources as extras.
		resolved := map[string]ResolvedDestination{
			PrimaryDestinationName: {Destination: argoappv1.ApplicationDestination{Namespace: "a"}, Cluster: &argoappv1.Cluster{Server: "https://one"}},
			"second":               {Name: "second", Destination: argoappv1.ApplicationDestination{Namespace: "b"}, Cluster: &argoappv1.Cluster{Server: "https://one"}},
		}
		errs := ValidateDistinctDestinations(resolved, []string{PrimaryDestinationName, "second"})
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0], "spec.destination")
		assert.Contains(t, errs[0], "second")
	})

	t.Run("two named destinations resolving to the same cluster by different means", func(t *testing.T) {
		t.Parallel()
		// One declared by server URL, one by cluster name, both landing on the same cluster.
		resolved := map[string]ResolvedDestination{
			PrimaryDestinationName: {Destination: argoappv1.ApplicationDestination{Namespace: "x"}, Cluster: &argoappv1.Cluster{Server: "https://other"}},
			"by-url":               {Name: "by-url", Destination: argoappv1.ApplicationDestination{Namespace: "apps"}, Cluster: &argoappv1.Cluster{Server: "https://one"}},
			"by-name":              {Name: "by-name", Destination: argoappv1.ApplicationDestination{Name: "one", Namespace: "apps"}, Cluster: &argoappv1.Cluster{Server: "https://one"}},
		}
		errs := ValidateDistinctDestinations(resolved, []string{PrimaryDestinationName, "by-url", "by-name"})
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0], "must be a different cluster")
	})
}

func TestPartitionByDestination(t *testing.T) {
	t.Parallel()

	resolved := map[string]ResolvedDestination{
		PrimaryDestinationName: {},
		"prod":                 {Name: "prod"},
	}

	t.Run("routes by annotation, defaulting to the primary", func(t *testing.T) {
		t.Parallel()
		objs := []*unstructured.Unstructured{
			testObj("Service", "svc", ""),
			testObj("ConfigMap", "cm", "prod"),
			testObj("Secret", "sec", ""),
		}
		partitions, errs := PartitionByDestination(objs, resolved)
		assert.Empty(t, errs)
		assert.Len(t, partitions[PrimaryDestinationName], 2)
		assert.Len(t, partitions["prod"], 1)
	})

	t.Run("every declared destination is present even with no objects", func(t *testing.T) {
		t.Parallel()
		partitions, errs := PartitionByDestination(nil, resolved)
		assert.Empty(t, errs)
		require.Contains(t, partitions, PrimaryDestinationName)
		require.Contains(t, partitions, "prod")
	})

	t.Run("undeclared destination is reported and never routed to the primary", func(t *testing.T) {
		t.Parallel()
		objs := []*unstructured.Unstructured{
			testObj("Service", "svc", ""),
			testObj("ConfigMap", "cm", "staging"),
		}
		partitions, errs := PartitionByDestination(objs, resolved)
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0], "staging")
		assert.Contains(t, errs[0], "ConfigMap/cm")
		// The misrouted object must not silently land on the primary destination.
		assert.Len(t, partitions[PrimaryDestinationName], 1)
		assert.Empty(t, partitions["prod"])
	})
}

func TestValidatePermissionsMultipleDestinations(t *testing.T) {
	t.Parallel()

	source := &argoappv1.ApplicationSource{RepoURL: "https://github.com/argoproj/argo-cd", Path: "."}

	t.Run("named destination not permitted by project", func(t *testing.T) {
		t.Parallel()
		spec := argoappv1.ApplicationSpec{
			Source:       source,
			Destination:  argoappv1.ApplicationDestination{Server: "https://allowed", Namespace: "default"},
			Destinations: []argoappv1.NamedDestination{{Name: "prod", Server: "https://denied", Namespace: "apps"}},
		}
		proj := argoappv1.AppProject{Spec: argoappv1.AppProjectSpec{
			SourceRepos:  []string{"*"},
			Destinations: []argoappv1.ApplicationDestination{{Server: "https://allowed", Namespace: "*"}},
		}}
		db := &dbmocks.ArgoDB{}
		db.EXPECT().GetCluster(mock.Anything, "https://allowed").Return(&argoappv1.Cluster{Server: "https://allowed"}, nil).Maybe()
		db.EXPECT().GetCluster(mock.Anything, "https://denied").Return(&argoappv1.Cluster{Server: "https://denied"}, nil).Maybe()

		conditions, err := ValidatePermissions(t.Context(), &spec, &proj, db)
		require.NoError(t, err)
		require.Len(t, conditions, 1)
		assert.Equal(t, argoappv1.ApplicationConditionInvalidSpecError, conditions[0].Type)
		assert.Contains(t, conditions[0].Message, `destination "prod"`)
	})

	t.Run("all destinations permitted", func(t *testing.T) {
		t.Parallel()
		spec := argoappv1.ApplicationSpec{
			Source:       source,
			Destination:  argoappv1.ApplicationDestination{Server: "https://one", Namespace: "default"},
			Destinations: []argoappv1.NamedDestination{{Name: "prod", Server: "https://two", Namespace: "apps"}},
		}
		proj := argoappv1.AppProject{Spec: argoappv1.AppProjectSpec{
			SourceRepos:  []string{"*"},
			Destinations: []argoappv1.ApplicationDestination{{Server: "*", Namespace: "*"}},
		}}
		db := &dbmocks.ArgoDB{}
		db.EXPECT().GetCluster(mock.Anything, "https://one").Return(&argoappv1.Cluster{Server: "https://one"}, nil).Maybe()
		db.EXPECT().GetCluster(mock.Anything, "https://two").Return(&argoappv1.Cluster{Server: "https://two"}, nil).Maybe()

		conditions, err := ValidatePermissions(t.Context(), &spec, &proj, db)
		require.NoError(t, err)
		assert.Empty(t, conditions)
	})

	t.Run("overlapping destinations are rejected", func(t *testing.T) {
		t.Parallel()
		spec := argoappv1.ApplicationSpec{
			Source:       source,
			Destination:  argoappv1.ApplicationDestination{Server: "https://one", Namespace: "default"},
			Destinations: []argoappv1.NamedDestination{{Name: "dupe", Server: "https://one", Namespace: "default"}},
		}
		proj := argoappv1.AppProject{Spec: argoappv1.AppProjectSpec{
			SourceRepos:  []string{"*"},
			Destinations: []argoappv1.ApplicationDestination{{Server: "*", Namespace: "*"}},
		}}
		db := &dbmocks.ArgoDB{}
		db.EXPECT().GetCluster(mock.Anything, "https://one").Return(&argoappv1.Cluster{Server: "https://one"}, nil).Maybe()

		conditions, err := ValidatePermissions(t.Context(), &spec, &proj, db)
		require.NoError(t, err)
		require.Len(t, conditions, 1)
		assert.Contains(t, conditions[0].Message, "must be a different cluster")
	})

	t.Run("invalid destination name short-circuits before resolution", func(t *testing.T) {
		t.Parallel()
		spec := argoappv1.ApplicationSpec{
			Source:       source,
			Destination:  argoappv1.ApplicationDestination{Server: "https://one", Namespace: "default"},
			Destinations: []argoappv1.NamedDestination{{Name: "", Server: "https://two"}},
		}
		proj := argoappv1.AppProject{Spec: argoappv1.AppProjectSpec{
			SourceRepos:  []string{"*"},
			Destinations: []argoappv1.ApplicationDestination{{Server: "*", Namespace: "*"}},
		}}
		// No GetCluster expectations: resolution must not be reached.
		db := &dbmocks.ArgoDB{}

		conditions, err := ValidatePermissions(t.Context(), &spec, &proj, db)
		require.NoError(t, err)
		require.Len(t, conditions, 1)
		assert.Contains(t, conditions[0].Message, "must not be empty")
	})
}

func TestValidateMultiDestinationDisabled(t *testing.T) {
	t.Parallel()

	withDests := &argoappv1.ApplicationSpec{
		Destinations: []argoappv1.NamedDestination{{Name: "prod", Server: "https://prod"}},
	}
	withoutDests := &argoappv1.ApplicationSpec{}

	assert.Empty(t, ValidateMultiDestinationDisabled(withDests, true), "enabled: no condition")
	assert.Empty(t, ValidateMultiDestinationDisabled(withoutDests, false), "no named destinations: no condition")

	conditions := ValidateMultiDestinationDisabled(withDests, false)
	require.Len(t, conditions, 1)
	assert.Equal(t, argoappv1.ApplicationConditionInvalidSpecError, conditions[0].Type)
	assert.Contains(t, conditions[0].Message, "application.destinations.enabled")
}

// failingMultiDestinationChecker fails every read and records whether it was consulted.
type failingMultiDestinationChecker struct{ called bool }

func (c *failingMultiDestinationChecker) IsMultiDestinationEnabled() (bool, error) {
	c.called = true
	return false, errors.New("configmap unavailable")
}

type staticMultiDestinationChecker struct{ enabled bool }

func (c staticMultiDestinationChecker) IsMultiDestinationEnabled() (bool, error) {
	return c.enabled, nil
}

func TestValidateMultiDestinationGate(t *testing.T) {
	t.Parallel()

	withDests := &argoappv1.ApplicationSpec{
		Destinations: []argoappv1.NamedDestination{{Name: "prod", Server: "https://prod"}},
	}

	t.Run("an application without named destinations never reads the setting", func(t *testing.T) {
		t.Parallel()
		// This is the point of the guard: a settings failure must not be able to affect
		// applications that do not use the feature.
		checker := &failingMultiDestinationChecker{}
		assert.Empty(t, ValidateMultiDestinationGate(&argoappv1.ApplicationSpec{}, checker))
		assert.False(t, checker.called, "settings must not be consulted")
	})

	t.Run("enabled", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, ValidateMultiDestinationGate(withDests, staticMultiDestinationChecker{enabled: true}))
	})

	t.Run("disabled", func(t *testing.T) {
		t.Parallel()
		conditions := ValidateMultiDestinationGate(withDests, staticMultiDestinationChecker{enabled: false})
		require.Len(t, conditions, 1)
		assert.Equal(t, argoappv1.ApplicationConditionInvalidSpecError, conditions[0].Type)
		assert.Contains(t, conditions[0].Message, "application.destinations.enabled")
	})

	t.Run("a settings failure is surfaced, not silently read as disabled", func(t *testing.T) {
		t.Parallel()
		checker := &failingMultiDestinationChecker{}
		conditions := ValidateMultiDestinationGate(withDests, checker)
		require.Len(t, conditions, 1)
		assert.Equal(t, argoappv1.ApplicationConditionUnknownError, conditions[0].Type)
		assert.Contains(t, conditions[0].Message, "configmap unavailable")
	})
}
