package settings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
)

func TestDeriveServiceAccountToImpersonate(t *testing.T) {
	t.Parallel()
	t.Run("MatchingServerAndNamespace", func(t *testing.T) {
		t.Parallel()
		project := &v1alpha1.AppProject{
			Spec: v1alpha1.AppProjectSpec{
				DestinationServiceAccounts: []v1alpha1.ApplicationDestinationServiceAccount{
					{Server: "https://cluster-api.example.com", Namespace: "dest-ns", DefaultServiceAccount: "test-sa"},
				},
			},
		}
		app := &v1alpha1.Application{
			Spec: v1alpha1.ApplicationSpec{
				Destination: v1alpha1.ApplicationDestination{
					Server:    "https://cluster-api.example.com",
					Namespace: "dest-ns",
				},
			},
		}
		cluster := &v1alpha1.Cluster{Server: "https://cluster-api.example.com"}

		user, err := DeriveServiceAccountToImpersonate(project, app, cluster, app.Spec.Destination.Namespace)
		require.NoError(t, err)
		assert.Equal(t, "system:serviceaccount:dest-ns:test-sa", user)
	})

	t.Run("MatchingWithGlobPatterns", func(t *testing.T) {
		t.Parallel()
		project := &v1alpha1.AppProject{
			Spec: v1alpha1.AppProjectSpec{
				DestinationServiceAccounts: []v1alpha1.ApplicationDestinationServiceAccount{
					{Server: "*", Namespace: "*", DefaultServiceAccount: "test-sa"},
				},
			},
		}
		app := &v1alpha1.Application{
			Spec: v1alpha1.ApplicationSpec{
				Destination: v1alpha1.ApplicationDestination{
					Server:    "https://cluster-api.example.com",
					Namespace: "any-ns",
				},
			},
		}
		cluster := &v1alpha1.Cluster{Server: "https://cluster-api.example.com"}

		user, err := DeriveServiceAccountToImpersonate(project, app, cluster, app.Spec.Destination.Namespace)
		require.NoError(t, err)
		assert.Equal(t, "system:serviceaccount:any-ns:test-sa", user)
	})

	t.Run("MatchingWithNamespacedServiceAccount", func(t *testing.T) {
		t.Parallel()
		project := &v1alpha1.AppProject{
			Spec: v1alpha1.AppProjectSpec{
				DestinationServiceAccounts: []v1alpha1.ApplicationDestinationServiceAccount{
					{Server: "https://cluster-api.example.com", Namespace: "dest-ns", DefaultServiceAccount: "other-ns:deploy-sa"},
				},
			},
		}
		app := &v1alpha1.Application{
			Spec: v1alpha1.ApplicationSpec{
				Destination: v1alpha1.ApplicationDestination{
					Server:    "https://cluster-api.example.com",
					Namespace: "dest-ns",
				},
			},
		}
		cluster := &v1alpha1.Cluster{Server: "https://cluster-api.example.com"}

		user, err := DeriveServiceAccountToImpersonate(project, app, cluster, app.Spec.Destination.Namespace)
		require.NoError(t, err)
		assert.Equal(t, "system:serviceaccount:other-ns:deploy-sa", user)
	})

	t.Run("FallbackToAppNamespaceWhenDestEmpty", func(t *testing.T) {
		t.Parallel()
		project := &v1alpha1.AppProject{
			Spec: v1alpha1.AppProjectSpec{
				DestinationServiceAccounts: []v1alpha1.ApplicationDestinationServiceAccount{
					// Namespace pattern matches empty string via glob "*"
					{Server: "*", Namespace: "", DefaultServiceAccount: "test-sa"},
				},
			},
		}
		app := &v1alpha1.Application{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app-ns"},
			Spec: v1alpha1.ApplicationSpec{
				Destination: v1alpha1.ApplicationDestination{
					Server:    "https://cluster-api.example.com",
					Namespace: "",
				},
			},
		}
		cluster := &v1alpha1.Cluster{Server: "https://cluster-api.example.com"}

		user, err := DeriveServiceAccountToImpersonate(project, app, cluster, app.Spec.Destination.Namespace)
		require.NoError(t, err)
		// Should use app.Namespace ("app-ns") as the SA namespace since Destination.Namespace is empty
		assert.Equal(t, "system:serviceaccount:app-ns:test-sa", user)
	})

	t.Run("NoMatchingEntry", func(t *testing.T) {
		t.Parallel()
		project := &v1alpha1.AppProject{
			Spec: v1alpha1.AppProjectSpec{
				DestinationServiceAccounts: []v1alpha1.ApplicationDestinationServiceAccount{
					{Server: "https://other-server.com", Namespace: "other-ns", DefaultServiceAccount: "test-sa"},
				},
			},
		}
		app := &v1alpha1.Application{
			Spec: v1alpha1.ApplicationSpec{
				Destination: v1alpha1.ApplicationDestination{
					Server:    "https://cluster-api.example.com",
					Namespace: "dest-ns",
				},
			},
		}
		cluster := &v1alpha1.Cluster{Server: "https://cluster-api.example.com"}

		user, err := DeriveServiceAccountToImpersonate(project, app, cluster, app.Spec.Destination.Namespace)
		require.NoError(t, err)
		assert.Empty(t, user)
	})

	t.Run("EmptyDestinationServiceAccounts", func(t *testing.T) {
		t.Parallel()
		project := &v1alpha1.AppProject{
			Spec: v1alpha1.AppProjectSpec{
				DestinationServiceAccounts: []v1alpha1.ApplicationDestinationServiceAccount{},
			},
		}
		app := &v1alpha1.Application{
			Spec: v1alpha1.ApplicationSpec{
				Destination: v1alpha1.ApplicationDestination{
					Server:    "https://cluster-api.example.com",
					Namespace: "dest-ns",
				},
			},
		}
		cluster := &v1alpha1.Cluster{Server: "https://cluster-api.example.com"}

		user, err := DeriveServiceAccountToImpersonate(project, app, cluster, app.Spec.Destination.Namespace)
		require.NoError(t, err)
		assert.Empty(t, user)
	})

	t.Run("InvalidServiceAccountChars", func(t *testing.T) {
		t.Parallel()
		project := &v1alpha1.AppProject{
			Spec: v1alpha1.AppProjectSpec{
				DestinationServiceAccounts: []v1alpha1.ApplicationDestinationServiceAccount{
					{Server: "*", Namespace: "*", DefaultServiceAccount: "bad*sa"},
				},
			},
		}
		app := &v1alpha1.Application{
			Spec: v1alpha1.ApplicationSpec{
				Destination: v1alpha1.ApplicationDestination{
					Server:    "https://cluster-api.example.com",
					Namespace: "dest-ns",
				},
			},
		}
		cluster := &v1alpha1.Cluster{Server: "https://cluster-api.example.com"}

		user, err := DeriveServiceAccountToImpersonate(project, app, cluster, app.Spec.Destination.Namespace)
		assert.Empty(t, user)
		assert.ErrorContains(t, err, "default service account contains invalid chars")
	})

	t.Run("BlankServiceAccount", func(t *testing.T) {
		t.Parallel()
		project := &v1alpha1.AppProject{
			Spec: v1alpha1.AppProjectSpec{
				DestinationServiceAccounts: []v1alpha1.ApplicationDestinationServiceAccount{
					{Server: "*", Namespace: "*", DefaultServiceAccount: "  "},
				},
			},
		}
		app := &v1alpha1.Application{
			Spec: v1alpha1.ApplicationSpec{
				Destination: v1alpha1.ApplicationDestination{
					Server:    "https://cluster-api.example.com",
					Namespace: "dest-ns",
				},
			},
		}
		cluster := &v1alpha1.Cluster{Server: "https://cluster-api.example.com"}

		user, err := DeriveServiceAccountToImpersonate(project, app, cluster, app.Spec.Destination.Namespace)
		assert.Empty(t, user)
		assert.ErrorContains(t, err, "default service account contains invalid chars")
	})

	t.Run("InvalidServerGlobPattern", func(t *testing.T) {
		t.Parallel()
		project := &v1alpha1.AppProject{
			Spec: v1alpha1.AppProjectSpec{
				DestinationServiceAccounts: []v1alpha1.ApplicationDestinationServiceAccount{
					{Server: "[", Namespace: "dest-ns", DefaultServiceAccount: "test-sa"},
				},
			},
		}
		app := &v1alpha1.Application{
			Spec: v1alpha1.ApplicationSpec{
				Destination: v1alpha1.ApplicationDestination{
					Server:    "https://cluster-api.example.com",
					Namespace: "dest-ns",
				},
			},
		}
		cluster := &v1alpha1.Cluster{Server: "https://cluster-api.example.com"}

		user, err := DeriveServiceAccountToImpersonate(project, app, cluster, app.Spec.Destination.Namespace)
		assert.Empty(t, user)
		assert.ErrorContains(t, err, "invalid glob pattern for destination server")
	})

	t.Run("InvalidNamespaceGlobPattern", func(t *testing.T) {
		t.Parallel()
		project := &v1alpha1.AppProject{
			Spec: v1alpha1.AppProjectSpec{
				DestinationServiceAccounts: []v1alpha1.ApplicationDestinationServiceAccount{
					{Server: "*", Namespace: "[", DefaultServiceAccount: "test-sa"},
				},
			},
		}
		app := &v1alpha1.Application{
			Spec: v1alpha1.ApplicationSpec{
				Destination: v1alpha1.ApplicationDestination{
					Server:    "https://cluster-api.example.com",
					Namespace: "dest-ns",
				},
			},
		}
		cluster := &v1alpha1.Cluster{Server: "https://cluster-api.example.com"}

		user, err := DeriveServiceAccountToImpersonate(project, app, cluster, app.Spec.Destination.Namespace)
		assert.Empty(t, user)
		assert.ErrorContains(t, err, "invalid glob pattern for destination namespace")
	})

	t.Run("FirstMatchWins", func(t *testing.T) {
		t.Parallel()
		project := &v1alpha1.AppProject{
			Spec: v1alpha1.AppProjectSpec{
				DestinationServiceAccounts: []v1alpha1.ApplicationDestinationServiceAccount{
					{Server: "*", Namespace: "dest-ns", DefaultServiceAccount: "first-sa"},
					{Server: "*", Namespace: "*", DefaultServiceAccount: "second-sa"},
				},
			},
		}
		app := &v1alpha1.Application{
			Spec: v1alpha1.ApplicationSpec{
				Destination: v1alpha1.ApplicationDestination{
					Server:    "https://cluster-api.example.com",
					Namespace: "dest-ns",
				},
			},
		}
		cluster := &v1alpha1.Cluster{Server: "https://cluster-api.example.com"}

		user, err := DeriveServiceAccountToImpersonate(project, app, cluster, app.Spec.Destination.Namespace)
		require.NoError(t, err)
		assert.Equal(t, "system:serviceaccount:dest-ns:first-sa", user)
	})
}

// The service account is selected by the namespace of the destination being synced, which for an
// application with additional named destinations is not spec.destination.Namespace.
func TestDeriveServiceAccountToImpersonate_UsesDestinationNamespace(t *testing.T) {
	t.Parallel()

	project := &v1alpha1.AppProject{
		Spec: v1alpha1.AppProjectSpec{
			DestinationServiceAccounts: []v1alpha1.ApplicationDestinationServiceAccount{
				{Server: "https://cluster-api.example.com", Namespace: "primary-ns", DefaultServiceAccount: "primary-sa"},
				{Server: "https://cluster-api.example.com", Namespace: "other-ns", DefaultServiceAccount: "other-sa"},
			},
		},
	}
	app := &v1alpha1.Application{
		Spec: v1alpha1.ApplicationSpec{
			Destination: v1alpha1.ApplicationDestination{
				Server:    "https://cluster-api.example.com",
				Namespace: "primary-ns",
			},
			Destinations: []v1alpha1.NamedDestination{
				{Name: "other", Server: "https://cluster-api.example.com", Namespace: "other-ns"},
			},
		},
	}
	cluster := &v1alpha1.Cluster{Server: "https://cluster-api.example.com"}

	primary, err := DeriveServiceAccountToImpersonate(project, app, cluster, "primary-ns")
	require.NoError(t, err)
	assert.Equal(t, "system:serviceaccount:primary-ns:primary-sa", primary)

	// The named destination must pick its own service account, not the primary's.
	other, err := DeriveServiceAccountToImpersonate(project, app, cluster, "other-ns")
	require.NoError(t, err)
	assert.Equal(t, "system:serviceaccount:other-ns:other-sa", other)

	// A namespace no entry matches yields no service account, which is the "not configured" signal
	// the enforcement flag acts on.
	none, err := DeriveServiceAccountToImpersonate(project, app, cluster, "unmatched-ns")
	require.NoError(t, err)
	assert.Empty(t, none)
}
