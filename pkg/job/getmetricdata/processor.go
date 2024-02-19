package getmetricdata

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients/cloudwatch"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type Client interface {
	GetMetricData(ctx context.Context, getMetricData []*model.CloudwatchData, namespace string, startTime time.Time, endTime time.Time) []cloudwatch.MetricDataResult
}

type IteratorFactory interface {
	// Build returns an ideal batch iterator based on the provided CloudwatchData
	Build([]*model.CloudwatchData) BatchIterator
}

type BatchIterator interface {
	// Size returns the number of batches in the iterator
	Size() int

	// Next returns the next batch of CloudWatch data be used when calling GetMetricData and the start + end time for
	// the GetMetricData call
	// If called when there are no more batches default values will be returned
	Next() ([]*model.CloudwatchData, time.Time, time.Time)

	// HasMore returns true if there are more batches to iterate otherwise false. Should be used in a loop
	// to govern calls to Next()
	HasMore() bool
}

type Processor struct {
	client      Client
	concurrency int
	logger      logging.Logger
	factory     IteratorFactory
}

func NewDefaultProcessor(logger logging.Logger, client Client, metricsPerQuery int, concurrency int) Processor {
	factory := iteratorFactory{
		metricsPerQuery:  metricsPerQuery,
		windowCalculator: MetricWindowCalculator{clock: TimeClock{}},
	}

	return NewProcessor(logger, client, factory, concurrency)
}

func NewProcessor(logger logging.Logger, client Client, factory IteratorFactory, concurrency int) Processor {
	return Processor{
		logger:      logger,
		factory:     factory,
		client:      client,
		concurrency: concurrency,
	}
}

func (b Processor) Run(ctx context.Context, namespace string, requests []*model.CloudwatchData) ([]*model.CloudwatchData, error) {
	if len(requests) == 0 {
		return requests, nil
	}

	iterator := b.factory.Build(requests)
	output := make([][]cloudwatch.MetricDataResult, 0, iterator.Size())

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(b.concurrency)
	mu := sync.Mutex{}

	for iterator.HasMore() {
		batch, startTime, endTime := iterator.Next()
		g.Go(func() error {
			data := b.client.GetMetricData(gCtx, batch, namespace, startTime, endTime)
			if data != nil {
				mu.Lock()
				output = append(output, data)
				mu.Unlock()
			} else {
				b.logger.Warn("GetMetricData empty result", "start", startTime, "end", endTime)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("GetMetricData work group error: %w", err)
	}

	mapResultsToMetricDatas(output, requests, b.logger)

	// Remove unprocessed/unknown elements in place, if any. Since getMetricDatas
	// is a slice of pointers, the compaction can be easily done in-place.
	requests = compact(requests, func(m *model.CloudwatchData) bool {
		return m.GetMetricDataResult != nil
	})

	return requests, nil
}

func mapResultsToMetricDatas(output [][]cloudwatch.MetricDataResult, datas []*model.CloudwatchData, logger logging.Logger) {
	// queryIDToData is a support structure used to easily find via a QueryID, the corresponding
	// model.CloudatchData.
	queryIDToData := make(map[string]*model.CloudwatchData, len(datas))

	// load the index
	for _, data := range datas {
		queryIDToData[data.GetMetricDataProcessingParams.QueryID] = data
	}

	// Update getMetricDatas slice with values and timestamps from API response.
	// We iterate through the response MetricDataResults and match the result ID
	// with what was sent in the API request.
	// In the event that the API response contains any ID we don't know about
	// (shouldn't really happen) we log a warning and move on. On the other hand,
	// in case the API response does not contain results for all the IDs we've
	// requested, unprocessed elements will be removed later on.
	for _, data := range output {
		if data == nil {
			continue
		}
		for _, metricDataResult := range data {
			// find into index
			metricData, ok := queryIDToData[metricDataResult.ID]
			if !ok {
				logger.Warn("GetMetricData returned unknown metric ID", "metric_id", metricDataResult.ID)
				continue
			}
			// skip elements that have been already mapped but still exist in queryIDToData
			if metricData.GetMetricDataResult != nil {
				continue
			}
			metricData.GetMetricDataResult = &model.GetMetricDataResult{
				Statistic: metricData.GetMetricDataProcessingParams.Statistic,
				Datapoint: metricDataResult.Datapoint,
				Timestamp: metricDataResult.Timestamp,
			}
			// All GetMetricData processing is done clear the params
			metricData.GetMetricDataProcessingParams = nil
		}
	}
}
