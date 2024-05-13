package appender

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type Accumulator struct {
	mux                   sync.Mutex
	batches               [][]*model.CloudwatchData
	flattened             []*model.CloudwatchData
	done                  atomic.Bool
	resourceTagsOnMetrics []string
}

func NewAccumulator(resourceTagsOnMetrics []string) *Accumulator {
	return &Accumulator{
		mux:                   sync.Mutex{},
		batches:               [][]*model.CloudwatchData{},
		done:                  atomic.Bool{},
		resourceTagsOnMetrics: resourceTagsOnMetrics,
	}
}

func (a *Accumulator) Append(_ context.Context, namespace string, metricConfig *model.MetricConfig, metrics []*model.Metric, resources Resources) {
	if a.done.Load() {
		return
	}

	batch := make([]*model.CloudwatchData, 0, len(metrics)*len(metricConfig.Statistics))
	for i, metric := range metrics {
		var resource *Resource
		if resources.staticResource != nil {
			resource = resources.staticResource
		} else {
			resource = resources.associatedResources[i]
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
			batch = append(batch, data)
		}
	}
	a.mux.Lock()
	defer a.mux.Unlock()
	a.batches = append(a.batches, batch)
}

func (a *Accumulator) Done() {
	a.done.CompareAndSwap(false, true)
	flattenedLength := 0
	for _, batch := range a.batches {
		flattenedLength += len(batch)
	}

	a.flattened = make([]*model.CloudwatchData, 0, flattenedLength)
	for _, batch := range a.batches {
		a.flattened = append(a.flattened, batch...)
	}
}

func (a *Accumulator) ListAll() []*model.CloudwatchData {
	if !a.done.Load() {
		return nil
	}

	return a.flattened
}
