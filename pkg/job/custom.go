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

	metrics, err := gmdProcessor.Run(ctx, job.Namespace, getMetricDatas)
	if err != nil {
		logger.Error(err, "Failed to get metric data")
		return nil
	}

	return metrics
}

type ResourceNameDecorator struct {
	resource *Resource
	next     MetricResourceAppender
}

func NewResourceNameDecorator(
	next MetricResourceAppender,
	name string,
) *ResourceNameDecorator {
	return &ResourceNameDecorator{
		resource: &Resource{Name: name},
		next:     next,
	}
}

func (rnd *ResourceNameDecorator) Append(ctx context.Context, namespace string, metricConfig *model.MetricConfig, metrics []*model.Metric) {
	rnd.next.Append(ctx, namespace, metricConfig, metrics, Resources{staticResource: rnd.resource})
}

func (rnd *ResourceNameDecorator) Done() {
	rnd.next.Done()
}

func (rnd *ResourceNameDecorator) ListAll() []*model.CloudwatchData {
	return rnd.next.ListAll()
}
