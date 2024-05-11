package job

import (
	"context"

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
	cwda := NewCloudwatchDataAccumulator()
	a := NewResourceNameDecorator(cwda, job.Name)
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

	getMetricDatas, err = gmdProcessor.Run(ctx, job.Namespace, getMetricDatas)
	if err != nil {
		logger.Error(err, "Failed to get metric data")
		return nil
	}

	return getMetricDatas
}

type ResourceNameDecorator struct {
	name string
	next ResourceAppender
}

func NewResourceNameDecorator(
	next ResourceAppender,
	name string,
) *ResourceNameDecorator {
	return &ResourceNameDecorator{
		name: name,
		next: next,
	}
}

func (rad *ResourceNameDecorator) Append(ctx context.Context, namespace string, metricConfig *model.MetricConfig, metric *model.Metric) {
	resource := Resource{Name: rad.name}
	rad.next.Append(ctx, namespace, metricConfig, metric, &resource)
}

func (rad *ResourceNameDecorator) Done(ctx context.Context) {
	rad.next.Done(ctx)
}

func (rad *ResourceNameDecorator) ListAll() []*model.CloudwatchData {
	return rad.next.ListAll()
}
