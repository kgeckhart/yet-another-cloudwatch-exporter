package appender

import (
	"context"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type Appender interface {
	Append(ctx context.Context, namespace string, metricConfig *model.MetricConfig, metrics []*model.Metric)
	Done()
	ListAll() []*model.CloudwatchData
}

type Resource struct {
	// Name is an identifiable value for the resource and is variable dependent on the match made
	//	It will be the AWS ARN (Amazon Resource Name) if a unique resource was found
	//  It will be "global" if a unique resource was not found
	//  CustomNamespaces will have the custom namespace Name
	Name string
	// Tags is a set of tags associated to the resource
	Tags []model.Tag
}

type resources struct {
	staticResource      *Resource
	associatedResources []*Resource
}

type ResourceStrategy interface {
	new(next *CloudwatchDataAccumulator, logger logging.Logger) Appender
}

func New(logger logging.Logger, strategy ResourceStrategy) Appender {
	return strategy.new(NewCloudwatchDataAccumulator(), logger)
}
