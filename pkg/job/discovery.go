package job

import (
	"context"
	"errors"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients/tagging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/listmetrics"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/maxdimassociator"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type listMetricsProcessor interface {
	Run(ctx context.Context, params listmetrics.ProcessingParams, appender listmetrics.Appender) error
}

type getMetricDataProcessor interface {
	Run(ctx context.Context, namespace string, requests []*model.CloudwatchData) ([]*model.CloudwatchData, error)
}

func runDiscoveryJob(
	ctx context.Context,
	logger logging.Logger,
	job model.DiscoveryJob,
	region string,
	clientTag tagging.Client,
	lmProcessor listMetricsProcessor,
	gmdProcessor getMetricDataProcessor,
) ([]*model.TaggedResource, []*model.CloudwatchData) {
	logger.Debug("Get tagged resources")

	resources, err := clientTag.GetResources(ctx, job, region)
	if err != nil {
		if errors.Is(err, tagging.ErrExpectedToFindResources) {
			logger.Error(err, "No tagged resources made it through filtering")
		} else {
			logger.Error(err, "Couldn't describe resources")
		}
		return nil, nil
	}

	if len(resources) == 0 {
		logger.Debug("No tagged resources", "region", region, "namespace", job.Type)
	}

	svc := config.SupportedServices.GetService(job.Type)
	params := listmetrics.ProcessingParams{
		Namespace:                 svc.Namespace,
		Metrics:                   job.Metrics,
		RecentlyActiveOnly:        job.RecentlyActiveOnly,
		DimensionNameRequirements: job.DimensionNameRequirements,
	}
	cwda := NewCloudwatchDataAccumulator(job.ExportedTagsOnMetrics)
	a := NewDefaultResourceDecorator(cwda, logger, job.DimensionsRegexps, resources)
	err = lmProcessor.Run(ctx, params, a)
	if err != nil {
		logger.Error(err, "Failed to list metric data")
		return nil, nil
	}
	getMetricDatas := a.ListAll()
	if len(getMetricDatas) == 0 {
		logger.Info("No metrics data found")
		return resources, nil
	}

	metrics, err := gmdProcessor.Run(ctx, svc.Namespace, getMetricDatas)
	if err != nil {
		logger.Error(err, "Failed to get metric data")
		return nil, nil
	}

	return resources, metrics
}

type ResourceAssociationDecorator struct {
	associator     Associator
	next           MetricResourceAppender
	staticResource *Resource
}

type Associator interface {
	MetricToResource(cwMetric *model.Metric) *Resource
}

func NewDefaultResourceDecorator(
	next MetricResourceAppender,
	logger logging.Logger,
	dimensionRegexps []model.DimensionsRegexp,
	resources []*model.TaggedResource,
) *ResourceAssociationDecorator {
	var associator Associator
	if len(dimensionRegexps) > 0 && len(resources) > 0 {
		associator = maxDimAdapter{wrapped: maxdimassociator.NewAssociator(logger, dimensionRegexps, resources)}
		return NewResourceDecorator(next, nil, associator)
	}

	return NewResourceDecorator(next, globalResource, nil)
}

// NewResourceDecorator is an injectable function for testing purposes
func NewResourceDecorator(next MetricResourceAppender, staticResource *Resource, associator Associator) *ResourceAssociationDecorator {
	return &ResourceAssociationDecorator{
		staticResource: staticResource,
		associator:     associator,
		next:           next,
	}
}

func (rad *ResourceAssociationDecorator) Append(ctx context.Context, namespace string, metricConfig *model.MetricConfig, metrics []*model.Metric) {
	if rad.staticResource != nil {
		rad.next.Append(ctx, namespace, metricConfig, metrics, Resources{staticResource: rad.staticResource})
		return
	}

	resources := make([]*Resource, len(metrics), len(metrics))
	// Slightly modified version of compact to work cleanly with two arrays (both are taken from https://stackoverflow.com/a/20551116)
	outputI := 0
	for _, metric := range metrics {
		resource := rad.associator.MetricToResource(metric)
		if resource != nil {
			metrics[outputI] = metric
			resources[outputI] = resource
			outputI++
		}
	}
	for i := outputI; i < len(metrics); i++ {
		metrics[i] = nil
	}
	metrics = metrics[:outputI]
	resources = resources[:outputI]

	rad.next.Append(ctx, namespace, metricConfig, metrics, Resources{associatedResources: resources})
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
