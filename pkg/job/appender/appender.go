package appender

import (
	"context"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type Resource struct {
	// Name is an identifiable value for the resource and is variable dependent on the match made
	//	It will be the AWS ARN (Amazon Resource Name) if a unique resource was found
	//  It will be "global" if a unique resource was not found
	//  CustomNamespaces will have the custom namespace Name
	Name string
	// Tags is a set of tags associated to the resource
	Tags []model.Tag
}

type Resources struct {
	staticResource      *Resource
	associatedResources []*Resource
}

type metricResourceEnricher interface {
	Enrich(ctx context.Context, metrics []*model.Metric) ([]*model.Metric, Resources)
}

type ResourceEnrichmentStrategy interface {
	new(logger logging.Logger) metricResourceEnricher
	resourceTagsOnMetrics() []string
}

type Appender struct {
	resourceEnricher metricResourceEnricher
	accumulator      *Accumulator
}

func (a Appender) Append(ctx context.Context, namespace string, metricConfig *model.MetricConfig, metrics []*model.Metric) {
	metrics, metricResources := a.resourceEnricher.Enrich(ctx, metrics)
	a.accumulator.Append(ctx, namespace, metricConfig, metrics, metricResources)
}

func (a Appender) Done() {
	a.accumulator.Done()
}

func (a Appender) ListAll() []*model.CloudwatchData {
	return a.accumulator.ListAll()
}

func New(logger logging.Logger, strategy ResourceEnrichmentStrategy) *Appender {
	return &Appender{
		resourceEnricher: strategy.new(logger),
		accumulator:      NewAccumulator(strategy.resourceTagsOnMetrics()),
	}
}
