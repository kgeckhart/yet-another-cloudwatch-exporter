package appender

import (
	"context"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type StaticResource struct {
	Name string
}

type ResourceNameDecorator struct {
	resource *Resource
	next     *CloudwatchDataAccumulator
}

func (sr StaticResource) new(next *CloudwatchDataAccumulator, _ logging.Logger) Appender {
	return &ResourceNameDecorator{
		resource: &Resource{Name: sr.Name},
		next:     next,
	}
}

func (rnd *ResourceNameDecorator) Append(ctx context.Context, namespace string, metricConfig *model.MetricConfig, metrics []*model.Metric) {
	rnd.next.Append(ctx, namespace, metricConfig, metrics, resources{staticResource: rnd.resource})
}

func (rnd *ResourceNameDecorator) Done() {
	rnd.next.Done()
}

func (rnd *ResourceNameDecorator) ListAll() []*model.CloudwatchData {
	return rnd.next.ListAll()
}
