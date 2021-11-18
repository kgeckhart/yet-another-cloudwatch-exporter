package exporter

import (
	"testing"
)

func TestMigrateTagsToPrometheus(t *testing.T) {
	// Setup Test
	id := "tag_Id"
	namespace := "AWS/Service"
	region := "us-east-1"
	tagItem := Tag{Key: "Name", Value: "tag_Value"}
	tags := []*Tag{&tagItem}
	r := resource{ID: &id, Namespace: &namespace, Region: &region, Tags: tags}
	resources := []*resource{&r}

	// Arrange
	prometheusMetricName := "aws_service_info"
	promLabels := make(map[string]string)
	promLabels["name"] = "tag_Id"
	promLabels["tag_Name"] = "tag_Value"
	var metricValue float64 = 0

	p := PrometheusMetric{
		name:   &prometheusMetricName,
		labels: promLabels,
		value:  &metricValue,
	}
	expected := []*PrometheusMetric{&p}

	// Act
	actual := generateAWSInfoMetrics(resources, false)

	// Assert
	if *actual[0].name != *expected[0].name {
		t.Fatalf("\nexpected: %q\nactual:  %q", len(expected), len(actual))
	}

}
