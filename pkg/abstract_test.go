package exporter

import (
	"testing"
)

func TestFilterThroughTags(t *testing.T) {
	// Setup Test

	// Arrange
	expected := true
	resource := resource{}
	filterTags := []Tag{}

	// Act
	actual := resource.filterThroughTags(filterTags)

	// Assert
	if actual != expected {
		t.Fatalf("\nexpected: %t\nactual:  %t", expected, actual)
	}
}
