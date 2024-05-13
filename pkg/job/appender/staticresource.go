package appender

import (
	"context"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type StaticResourceStrategy struct {
	Name string
}

type ResourceName struct {
	resource *Resource
}

func (sr StaticResourceStrategy) new(_ logging.Logger) metricResourceEnricher {
	return &ResourceName{
		resource: &Resource{Name: sr.Name},
	}
}

func (sr StaticResourceStrategy) resourceTagsOnMetrics() []string {
	return nil
}

func (rnd *ResourceName) Enrich(_ context.Context, metrics []*model.Metric) ([]*model.Metric, Resources) {
	return metrics, Resources{staticResource: rnd.resource}
}
