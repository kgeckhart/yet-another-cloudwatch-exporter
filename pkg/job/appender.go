package job

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type ResourceAppender interface {
	Append(ctx context.Context, namespace string, metricConfig *model.MetricConfig, metric *model.Metric, resource *Resource)
	Done(ctx context.Context)
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

type CloudwatchDataAccumulator struct {
	mux                   sync.Mutex
	cloudwatchData        []*model.CloudwatchData
	done                  atomic.Bool
	resourceTagsOnMetrics []string
}

func NewCloudwatchDataAccumulator(resourceTagsOnMetrics ...[]string) *CloudwatchDataAccumulator {
	var tagsOnMetrics []string
	if len(resourceTagsOnMetrics) == 1 {
		tagsOnMetrics = resourceTagsOnMetrics[0]
	}
	return &CloudwatchDataAccumulator{
		mux:                   sync.Mutex{},
		cloudwatchData:        []*model.CloudwatchData{},
		done:                  atomic.Bool{},
		resourceTagsOnMetrics: tagsOnMetrics,
	}
}

func (a *CloudwatchDataAccumulator) Append(_ context.Context, namespace string, metricConfig *model.MetricConfig, metric *model.Metric, resource *Resource) {
	if a.done.Load() {
		return
	}

	// Skip metrics which are not associated with a resource
	if resource == nil {
		return
	}

	var tags []model.Tag
	if len(a.resourceTagsOnMetrics) > 0 {
		tags = make([]model.Tag, 0, len(a.resourceTagsOnMetrics))
		for _, tagName := range a.resourceTagsOnMetrics {
			tag := model.Tag{
				Key: tagName,
			}
			for _, resourceTag := range resource.Tags {
				if resourceTag.Key == tagName {
					tag.Value = resourceTag.Value
					break
				}
			}

			// Always add the tag, even if it's empty, to ensure the same labels are present on all metrics for a single service
			tags = append(tags, tag)
		}
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
			Tags:                      tags,
			GetMetricDataResult:       nil,
			GetMetricStatisticsResult: nil,
		}
		a.mux.Lock()
		a.cloudwatchData = append(a.cloudwatchData, data)
		a.mux.Unlock()
	}
}

func (a *CloudwatchDataAccumulator) Done(context.Context) {
	a.done.CompareAndSwap(false, true)
}

func (a *CloudwatchDataAccumulator) ListAll() []*model.CloudwatchData {
	if !a.done.Load() {
		return nil
	}

	return a.cloudwatchData
}
