package appender

import (
	"context"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/maxdimassociator"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type ResourceAssociationStrategy struct {
	Resources        []*model.TaggedResource
	DimensionRegexps []model.DimensionsRegexp
	TagsOnMetrics    []string
}

type ResourceAssociation struct {
	associator     Associator
	staticResource *Resource
}

type Associator interface {
	MetricToResource(cwMetric *model.Metric) *Resource
}

func (ra ResourceAssociationStrategy) new(
	logger logging.Logger,
) metricResourceEnricher {
	var associator Associator
	if len(ra.DimensionRegexps) > 0 && len(ra.Resources) > 0 {
		associator = maxDimAdapter{wrapped: maxdimassociator.NewAssociator(logger, ra.DimensionRegexps, ra.Resources)}
		return NewResourceAssociation(nil, associator)
	}

	return NewResourceAssociation(globalResource, nil)
}

func (ra ResourceAssociationStrategy) resourceTagsOnMetrics() []string {
	return ra.TagsOnMetrics
}

// NewResourceAssociation is an injectable function for testing purposes
func NewResourceAssociation(staticResource *Resource, associator Associator) *ResourceAssociation {
	return &ResourceAssociation{
		staticResource: staticResource,
		associator:     associator,
	}
}

func (rad *ResourceAssociation) Enrich(_ context.Context, metrics []*model.Metric) ([]*model.Metric, Resources) {
	associatedResources := make([]*Resource, len(metrics))
	// Slightly modified version of compact to work cleanly with two arrays (both are taken from https://stackoverflow.com/a/20551116)
	outputI := 0
	for _, metric := range metrics {
		resource := rad.associator.MetricToResource(metric)
		if resource != nil {
			metrics[outputI] = metric
			associatedResources[outputI] = resource
			outputI++
		}
	}
	for i := outputI; i < len(metrics); i++ {
		metrics[i] = nil
	}
	metrics = metrics[:outputI]
	associatedResources = associatedResources[:outputI]

	return metrics, Resources{associatedResources: associatedResources}
}

var globalResource = &Resource{
	Name: "global",
	Tags: nil,
}

type maxDimAdapter struct {
	wrapped maxdimassociator.Associator
}

func (r maxDimAdapter) MetricToResource(cwMetric *model.Metric) *Resource {
	resource, skip := r.wrapped.AssociateMetricToResource(cwMetric)
	if skip {
		return nil
	}
	if resource == nil {
		return globalResource
	}
	return &Resource{
		Name: resource.ARN,
		Tags: resource.Tags,
	}
}
