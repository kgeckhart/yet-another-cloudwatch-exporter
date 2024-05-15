package resourcemetadata

import (
	"context"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/maxdimassociator"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type Association struct {
	Resources        []*model.TaggedResource
	DimensionRegexps []model.DimensionsRegexp
	TagsOnMetrics    []string
}

type Discovery interface {
	Run() ([]*model.TaggedResource, error)
}

func NewResourceAssociation(dimensionRegexps []model.DimensionsRegexp, tagsOnMetrics []string, resources []*model.TaggedResource) Association {
	return Association{
		Resources:        resources,
		DimensionRegexps: dimensionRegexps,
		TagsOnMetrics:    tagsOnMetrics,
	}
}

func (ra Association) Create(logger logging.Logger) MetricResourceEnricher {
	if len(ra.DimensionRegexps) > 0 && len(ra.Resources) > 0 {
		maxDim := maxDimAdapter{wrapped: maxdimassociator.NewAssociator(logger, ra.DimensionRegexps, ra.Resources)}
		return NewResourceAssociationEnricher(nil, maxDim)
	}

	return NewResourceAssociationEnricher(globalResource, nil)
}

type associator interface {
	MetricToResource(cwMetric *model.Metric) *Resource
}

type ResourceAssociationEnricher struct {
	associator     associator
	staticResource *Resource
}

// NewResourceAssociationEnricher is an injectable function for testing purposes
func NewResourceAssociationEnricher(staticResource *Resource, associator associator) *ResourceAssociationEnricher {
	return &ResourceAssociationEnricher{
		staticResource: staticResource,
		associator:     associator,
	}
}

func (rad *ResourceAssociationEnricher) Enrich(_ context.Context, metrics []*model.Metric) ([]*model.Metric, Resources) {
	if rad.staticResource != nil {
		return metrics, Resources{StaticResource: rad.staticResource}
	}
	associatedResources := make([]*Resource, len(metrics))
	// Slightly modified version of compact to work cleanly with two arrays (both are taken from https://stackoverflow.com/a/20551116)
	outputI := 0
	for _, metric := range metrics {
		resource := rad.associator.MetricToResource(metric)
		if resource != nil {
			metrics[outputI] = metric

			// TODO tags on metrics should probably create a new Resource?
			// var tags []model.Tag
			// if len(c.resourceTagsOnMetrics) > 0 {
			// 	tags = make([]model.Tag, 0, len(c.resourceTagsOnMetrics))
			// 	for _, tagName := range c.resourceTagsOnMetrics {
			// 		tag := model.Tag{
			// 			Key: tagName,
			// 		}
			// 		for _, resourceTag := range resource.Tags {
			// 			if resourceTag.Key == tagName {
			// 				tag.Value = resourceTag.Value
			// 				break
			// 			}
			// 		}
			//
			// 		// Always add the tag, even if it's empty, to ensure the same labels are present on all metrics for a single service
			// 		tags = append(tags, tag)
			// 	}
			// }
			associatedResources[outputI] = resource
			outputI++
		}
	}
	for i := outputI; i < len(metrics); i++ {
		metrics[i] = nil
	}
	metrics = metrics[:outputI]
	associatedResources = associatedResources[:outputI]

	return metrics, Resources{AssociatedResources: associatedResources}
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
