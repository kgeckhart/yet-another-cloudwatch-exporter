package resourcemetadata

import (
	"context"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type StaticResource struct {
	Name string
}

type ResourceName struct {
	resource *Resource
}

func (sr StaticResource) Create(_ logging.Logger) MetricResourceEnricher {
	return &ResourceName{resource: &Resource{Name: sr.Name}}
}

func (rnd *ResourceName) Enrich(_ context.Context, metrics []*model.Metric) ([]*model.Metric, Resources) {
	return metrics, Resources{StaticResource: rnd.resource}
}
