package job

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients/cloudwatch"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

func runCustomNamespaceJob(
	ctx context.Context,
	logger logging.Logger,
	job model.CustomNamespaceJob,
	clientCloudwatch cloudwatch.Client,
	processor GetMetricDataProcessor,
) []*model.CloudwatchData {
	getMetricDatas := getMetricDataForQueriesForCustomNamespace(ctx, job, clientCloudwatch, logger)
	metricDataLength := len(getMetricDatas)
	if metricDataLength == 0 {
		logger.Debug("No metrics data found")
		return nil
	}

	getMetricDatas, err := processor.Run(ctx, logger, job.Namespace, getMetricDatas)
	if err != nil {
		logger.Error(err, "Failed to GetMetricData")
		return nil
	}

	return getMetricDatas
}

func getMetricDataForQueriesForCustomNamespace(
	ctx context.Context,
	customNamespaceJob model.CustomNamespaceJob,
	clientCloudwatch cloudwatch.Client,
	logger logging.Logger,
) []*model.CloudwatchData {
	mux := &sync.Mutex{}
	var getMetricDatas []*model.CloudwatchData

	var wg sync.WaitGroup
	wg.Add(len(customNamespaceJob.Metrics))

	for _, metric := range customNamespaceJob.Metrics {
		// For every metric of the job get the full list of metrics.
		// This includes, for this metric the possible combinations
		// of dimensions and value of dimensions with data.

		go func(metric *model.MetricConfig) {
			defer wg.Done()
			err := clientCloudwatch.ListMetrics(ctx, customNamespaceJob.Namespace, metric, customNamespaceJob.RecentlyActiveOnly, func(page []*model.Metric) {
				var data []*model.CloudwatchData

				for _, cwMetric := range page {
					if len(customNamespaceJob.DimensionNameRequirements) > 0 && !metricDimensionsMatchNames(cwMetric, customNamespaceJob.DimensionNameRequirements) {
						continue
					}

					for _, stat := range metric.Statistics {
						id := fmt.Sprintf("id_%d", rand.Int())
						data = append(data, &model.CloudwatchData{
							ID:        customNamespaceJob.Name,
							Namespace: customNamespaceJob.Namespace,
							GetMetricDataResult: &model.GetMetricDataResult{
								ID:        &id,
								Statistic: stat,
							},
							Dimensions:   cwMetric.Dimensions,
							MetricConfig: metric,
						})
					}
				}

				mux.Lock()
				getMetricDatas = append(getMetricDatas, data...)
				mux.Unlock()
			})
			if err != nil {
				logger.Error(err, "Failed to get full metric list", "metric_name", metric.Name, "namespace", customNamespaceJob.Namespace)
				return
			}
		}(metric)
	}

	wg.Wait()
	return getMetricDatas
}
