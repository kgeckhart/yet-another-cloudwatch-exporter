package job

import (
	"context"
	"errors"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients/tagging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/appender"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/listmetrics"
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
	a := appender.New(logger, appender.ResourceAssociationStrategy{
		Resources:        resources,
		DimensionRegexps: job.DimensionsRegexps,
		TagsOnMetrics:    job.ExportedTagsOnMetrics,
	})
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
