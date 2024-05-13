package appender

import (
	"context"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/maxdimassociator"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type ResourceAssociation struct {
	Resources        []*model.TaggedResource
	DimensionRegexps []model.DimensionsRegexp
	TagsOnMetrics    []string
}

type ResourceAssociationDecorator struct {
	associator     Associator
	next           *CloudwatchDataAccumulator
	staticResource *Resource
}

type Associator interface {
	MetricToResource(cwMetric *model.Metric) *Resource
}

func (ra ResourceAssociation) new(
	next *CloudwatchDataAccumulator,
	logger logging.Logger,
) Appender {
	var associator Associator
	if len(ra.DimensionRegexps) > 0 && len(ra.Resources) > 0 {
		associator = maxDimAdapter{wrapped: maxdimassociator.NewAssociator(logger, ra.DimensionRegexps, ra.Resources)}
		return NewResourceDecorator(next, nil, associator)
	}

	return NewResourceDecorator(next, globalResource, nil)
}

// NewResourceDecorator is an injectable function for testing purposes
func NewResourceDecorator(next *CloudwatchDataAccumulator, staticResource *Resource, associator Associator) *ResourceAssociationDecorator {
	return &ResourceAssociationDecorator{
		staticResource: staticResource,
		associator:     associator,
		next:           next,
	}
}

func (rad *ResourceAssociationDecorator) Append(ctx context.Context, namespace string, metricConfig *model.MetricConfig, metrics []*model.Metric) {
	if rad.staticResource != nil {
		rad.next.Append(ctx, namespace, metricConfig, metrics, resources{staticResource: rad.staticResource})
		return
	}

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

	rad.next.Append(ctx, namespace, metricConfig, metrics, resources{associatedResources: associatedResources})
}

func (rad *ResourceAssociationDecorator) Done() {
	rad.next.Done()
}

func (rad *ResourceAssociationDecorator) ListAll() []*model.CloudwatchData {
	return rad.next.ListAll()
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
