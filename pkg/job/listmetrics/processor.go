package listmetrics

import (
	"context"
	"sync"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type Client interface {
	ListMetrics(ctx context.Context, namespace string, metric *model.MetricConfig, recentlyActiveOnly bool, fn func(page []*model.Metric)) error
}

type Appender interface {
	Done(ctx context.Context)
	Append(ctx context.Context, namespace string, metricConfig *model.MetricConfig, metric *model.Metric)
}

type Processor struct {
	logger logging.Logger
	client Client
}

func NewDefaultProcessor(logger logging.Logger, client Client) Processor {
	return NewProcessor(logger, client)
}

func NewProcessor(logger logging.Logger, client Client) Processor {
	return Processor{
		logger: logger,
		client: client,
	}
}

type ProcessingParams struct {
	Namespace                 string
	Metrics                   []*model.MetricConfig
	RecentlyActiveOnly        bool
	DimensionNameRequirements []string
}

func (p Processor) Run(ctx context.Context, params ProcessingParams, appender Appender) error {
	var wg sync.WaitGroup
	wg.Add(len(params.Metrics))

	// For every metric of the job call the ListMetrics API
	// to fetch the existing combinations of dimensions and
	// value of dimensions with data.
	for _, metric := range params.Metrics {
		go func(metric *model.MetricConfig) {
			defer wg.Done()

			err := p.client.ListMetrics(ctx, params.Namespace, metric, params.RecentlyActiveOnly, func(page []*model.Metric) {
				for _, cwMetric := range page {
					if len(params.DimensionNameRequirements) > 0 && !metricDimensionsMatchNames(cwMetric, params.DimensionNameRequirements) {
						continue
					}

					appender.Append(ctx, params.Namespace, metric, cwMetric)
				}
			})
			if err != nil {
				p.logger.Error(err, "Failed to get full metric list", "metric_name", metric.Name, "namespace", params.Namespace)
				return
			}
		}(metric)
	}

	wg.Wait()
	appender.Done(ctx)
	return nil
}

func metricDimensionsMatchNames(metric *model.Metric, dimensionNameRequirements []string) bool {
	if len(dimensionNameRequirements) != len(metric.Dimensions) {
		return false
	}
	for _, dimension := range metric.Dimensions {
		foundMatch := false
		for _, dimensionName := range dimensionNameRequirements {
			if dimension.Name == dimensionName {
				foundMatch = true
				break
			}
		}
		if !foundMatch {
			return false
		}
	}
	return true
}
