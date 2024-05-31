package appender

import (
	"context"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/resourcemetadata"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type cloudwatchDataMapper struct{}

func (c cloudwatchDataMapper) Map(_ context.Context, namespace string, metricConfig *model.MetricConfig, metrics []*model.Metric, resources resourcemetadata.Resources) []*model.CloudwatchData {
	batch := make([]*model.CloudwatchData, 0, len(metrics)*len(metricConfig.Statistics))
	for i, metric := range metrics {
		var resource *resourcemetadata.Resource
		if resources.StaticResource != nil {
			resource = resources.StaticResource
		} else {
			resource = resources.AssociatedResources[i]
		}

		for _, stat := range metricConfig.Statistics {
			data := &model.CloudwatchData{
				MetricName: metricConfig.Name,
				Namespace:  namespace,
				Dimensions: metric.Dimensions,
				GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
					Period:    metricConfig.Period,
					Length:    metricConfig.Length,
					Delay:     metricConfig.Delay,
					Statistic: stat,
				},
				MetricMigrationParams: model.MetricMigrationParams{
					NilToZero:              metricConfig.NilToZero,
					AddCloudwatchTimestamp: metricConfig.AddCloudwatchTimestamp,
				},
				ResourceName:              resource.Name,
				Tags:                      resource.Tags,
				GetMetricDataResult:       nil,
				GetMetricStatisticsResult: nil,
			}
			batch = append(batch, data)
		}
	}
	return batch
}
