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

	getMetricDatas, err = gmdProcessor.Run(ctx, svc.Namespace, getMetricDatas)
	if err != nil {
		logger.Error(err, "Failed to get metric data")
		return nil, nil
	}

	return resources, getMetricDatas
}

type ResourceAssociationDecorator struct {
	associator Associator
	next       ResourceAppender
}

type Associator interface {
	MetricToResource(cwMetric *model.Metric) *Resource
}

func NewDefaultResourceDecorator(
	next ResourceAppender,
	logger logging.Logger,
	dimensionRegexps []model.DimensionsRegexp,
	resources []*model.TaggedResource,
) *ResourceAssociationDecorator {
	var associator Associator
	if len(dimensionRegexps) > 0 && len(resources) > 0 {
		associator = maxDimAdapter{wrapped: maxdimassociator.NewAssociator(logger, dimensionRegexps, resources)}
	} else {
		associator = globalNameAssociator{}
	}
	return NewResourceDecorator(next, associator)
}

// NewResourceDecorator is an injectable function for testing purposes
func NewResourceDecorator(next ResourceAppender, associator Associator) *ResourceAssociationDecorator {
	return &ResourceAssociationDecorator{
		associator: associator,
		next:       next,
	}
}

func (rad *ResourceAssociationDecorator) Append(ctx context.Context, namespace string, metricConfig *model.MetricConfig, metric *model.Metric) {
	resource := rad.associator.MetricToResource(metric)
	rad.next.Append(ctx, namespace, metricConfig, metric, resource)
}

func (rad *ResourceAssociationDecorator) Done(ctx context.Context) {
	rad.next.Done(ctx)
}

func (rad *ResourceAssociationDecorator) ListAll() []*model.CloudwatchData {
	return rad.next.ListAll()
}

var globalResource = &Resource{
	Name: "global",
	Tags: nil,
}

type globalNameAssociator struct{}

func (ns globalNameAssociator) MetricToResource(_ *model.Metric) *Resource {
	return globalResource
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
