package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
)

const (
	// ConditionTypeReady is True when the projection contains state computed by the application controller.
	ConditionTypeReady = "Ready"

	// ReasonReconciled means the application controller has computed the state of the application.
	ReasonReconciled = "Reconciled"
	// ReasonNotReconciled means the application controller has not (yet) cached any state for the application.
	ReasonNotReconciled = "NotReconciled"
	// ReasonComparisonError means the application controller failed to compute the state of the application.
	ReasonComparisonError = "ComparisonError"
)

// ApplicationTree is a read-only projection of the live resource tree of an Application, as computed by the
// application controller. Every Application has exactly one ApplicationTree with the same namespace and name.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ApplicationTree struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object metadata. Name, namespace, UID, creation timestamp and labels mirror the Application.
	// ResourceVersion is an opaque hash of the content.
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// Tree is the resource tree of the Application.
	Tree appv1.ApplicationTree `json:"tree"`
	// Status describes the Application this projection was computed from.
	Status ProjectionStatus `json:"status"`
}

// ApplicationTreeList is a list of ApplicationTree resources.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ApplicationTreeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ApplicationTree `json:"items"`
}

// ApplicationDiff is a read-only projection of the difference between the live and the desired state of the
// resources managed by an Application, as computed by the application controller. Every Application has exactly one
// ApplicationDiff with the same namespace and name.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ApplicationDiff struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object metadata. Name, namespace, UID, creation timestamp and labels mirror the Application.
	// ResourceVersion is an opaque hash of the content.
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// Resources holds the diff of each resource managed by the Application. Hook resources are omitted. Secret data is
	// masked by the application controller.
	// +listType=atomic
	Resources []appv1.ResourceDiff `json:"resources"`
	// Status describes the Application this projection was computed from.
	Status ProjectionStatus `json:"status"`
}

// ApplicationDiffList is a list of ApplicationDiff resources.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ApplicationDiffList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ApplicationDiff `json:"items"`
}

// ProjectionStatus describes the Application a projection was computed from.
type ProjectionStatus struct {
	// Project is the AppProject of the Application.
	Project string `json:"project"`
	// Conditions describe whether the projection holds state computed by the application controller.
	// +listType=map
	// +listMapKey=type
	Conditions []ProjectionCondition `json:"conditions,omitempty"`
}

// ProjectionCondition describes the state of a projection. Unlike metav1.Condition it carries no timestamps, so that
// the content (and therefore the resourceVersion) of a projection only changes when the projected state changes.
type ProjectionCondition struct {
	// Type of the condition, e.g. Ready.
	Type string `json:"type"`
	// Status of the condition: True, False or Unknown.
	Status metav1.ConditionStatus `json:"status"`
	// Reason is a CamelCase reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`
	// Message is a human readable message indicating details about the condition.
	Message string `json:"message,omitempty"`
}
