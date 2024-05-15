package appender

import (
	"context"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/resourcemetadata"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

func New(enricher resourcemetadata.MetricResourceEnricher) *Appender {
	return &Appender{
		resourceEnricher: enricher,
		mapper:           cloudwatchDataMapper{},
		accumulator:      NewAccumulator(),
	}
}

type Appender struct {
	resourceEnricher resourcemetadata.MetricResourceEnricher
	mapper           cloudwatchDataMapper
	accumulator      *Accumulator
}

func (a Appender) Append(ctx context.Context, namespace string, metricConfig *model.MetricConfig, metrics []*model.Metric) {
	metrics, metricResources := a.resourceEnricher.Enrich(ctx, metrics)
	cloudwatchData := a.mapper.Map(ctx, namespace, metricConfig, metrics, metricResources)
	a.accumulator.Append(ctx, cloudwatchData)
}

func (a Appender) Done() {
	a.accumulator.Done()
}

func (a Appender) ListAll() []*model.CloudwatchData {
	return a.accumulator.ListAll()
}
