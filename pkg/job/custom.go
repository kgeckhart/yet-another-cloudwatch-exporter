package job

import (
	"context"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/appender"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/listmetrics"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

func runCustomNamespaceJob(
	ctx context.Context,
	logger logging.Logger,
	job model.CustomNamespaceJob,
	lmProcessor listMetricsProcessor,
	gmdProcessor getMetricDataProcessor,
) []*model.CloudwatchData {
	params := listmetrics.ProcessingParams{
		Namespace:                 job.Namespace,
		Metrics:                   job.Metrics,
		RecentlyActiveOnly:        job.RecentlyActiveOnly,
		DimensionNameRequirements: job.DimensionNameRequirements,
	}
	a := appender.New(logger, appender.StaticResourceStrategy{Name: job.Name})
	err := lmProcessor.Run(ctx, params, a)
	if err != nil {
		logger.Error(err, "Failed to list metric data")
		return nil
	}
	getMetricDatas := a.ListAll()
	if len(getMetricDatas) == 0 {
		logger.Info("No metrics data found")
		return nil
	}

	metrics, err := gmdProcessor.Run(ctx, job.Namespace, getMetricDatas)
	if err != nil {
		logger.Error(err, "Failed to get metric data")
		return nil
	}

	return metrics
}
