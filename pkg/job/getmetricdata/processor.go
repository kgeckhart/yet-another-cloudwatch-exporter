package getmetricdata

import (
	"context"
	"fmt"
	"math"
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

type WindowCalculator interface {
	Calculate(roundingPeriod time.Duration, length time.Duration, delay time.Duration) (time.Time, time.Time)
}

type Processor struct {
	metricsPerQuery int
	client          Client
	concurrency     int
	calculator      WindowCalculator
	logger          logging.Logger
}

func NewProcessor(logger logging.Logger, client Client, metricsPerQuery int, concurrency int, windowCalculator ...WindowCalculator) Processor {
	var usedCalculator WindowCalculator = MetricWindowCalculator{clock: TimeClock{}}
	if len(windowCalculator) == 1 {
		usedCalculator = windowCalculator[0]
	}
	return Processor{
		logger:          logger,
		metricsPerQuery: metricsPerQuery,
		client:          client,
		concurrency:     concurrency,
		calculator:      usedCalculator,
	}
}

func (p Processor) Run(ctx context.Context, namespace string, requests []*model.CloudwatchData) ([]*model.CloudwatchData, error) {
	var getMetricDataOutput [][]cloudwatch.MetricDataResult
	var err error
	if requestHaveConsistentTimeParameters(requests) {
		getMetricDataOutput, err = p.executeWithSimpleBatching(ctx, namespace, requests)
	} else {
		getMetricDataOutput, err = p.executeBatchingByTimeParameters(ctx, namespace, requests)
	}

	if err != nil {
		return nil, err
	}

	mapResultsToMetricDatas(getMetricDataOutput, requests, p.logger)

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

// executeWithSimpleBatching can be used when all GetMetricData requests are going to have the same start and end time.
// It calculates start and end time once before executing all the queries and reuses it for all queriep. It's considered
// simple because the input requests are batched in place using array slicing with no extra logic needed.
func (p Processor) executeWithSimpleBatching(ctx context.Context, namespace string, requests []*model.CloudwatchData) ([][]cloudwatch.MetricDataResult, error) {
	metricDataLength := len(requests)
	if metricDataLength == 0 {
		return nil, nil
	}
	partitionSize := int(math.Ceil(float64(metricDataLength) / float64(p.metricsPerQuery)))
	p.logger.Debug("GetMetricData partitions", "size", partitionSize)
	getMetricDataOutput := make([][]cloudwatch.MetricDataResult, 0, partitionSize)

	metricConfig := requests[0].GetMetricDataProcessingParams
	startTime, endTime := p.calculator.Calculate(
		time.Duration(metricConfig.Period)*time.Second,
		time.Duration(metricConfig.Length)*time.Second,
		time.Duration(metricConfig.Delay)*time.Second)

	if p.logger.IsDebugEnabled() {
		p.logger.Debug("GetMetricData Window for all requests", "start_time", startTime.Format(TimeFormat), "end_time", endTime.Format(TimeFormat))
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(p.concurrency)
	mu := sync.Mutex{}
	count := 0
	for i := 0; i < metricDataLength; i += p.metricsPerQuery {
		start := i
		end := i + p.metricsPerQuery
		if end > metricDataLength {
			end = metricDataLength
		}
		partitionNum := count
		count++

		g.Go(func() error {
			input := requests[start:end]
			data := p.client.GetMetricData(gCtx, input, namespace, startTime, endTime)
			if data != nil {
				mu.Lock()
				getMetricDataOutput = append(getMetricDataOutput, data)
				mu.Unlock()
			} else {
				p.logger.Warn("GetMetricData partition empty result", "start", start, "end", end, "partitionNum", partitionNum)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("GetMetricData work group error: %w", err)
	}

	return getMetricDataOutput, nil
}

// executeBatchingByTimeParameters batches the incoming requests to ensure a strong level of consistency
// for the start and end time of the GetMetricData requestp. This is only necessary when the requests contain different
// values for length, period, and delay.
func (p Processor) executeBatchingByTimeParameters(ctx context.Context, namespace string, requests []*model.CloudwatchData) ([][]cloudwatch.MetricDataResult, error) {
	batchesByStartAndEndTime := make(map[string][]*model.CloudwatchData)
	for _, request := range requests {
		key := fmt.Sprintf("%d|%d|%d", request.GetMetricDataProcessingParams.Period, request.GetMetricDataProcessingParams.Length, request.GetMetricDataProcessingParams.Delay)
		if _, exists := batchesByStartAndEndTime[key]; !exists {
			batchesByStartAndEndTime[key] = make([]*model.CloudwatchData, 0)
		}
		batchesByStartAndEndTime[key] = append(batchesByStartAndEndTime[key], request)
	}

	var results [][]cloudwatch.MetricDataResult
	for _, batch := range batchesByStartAndEndTime {
		bResult, err := p.executeWithSimpleBatching(ctx, namespace, batch)
		if err != nil {
			return nil, err
		}
		results = append(results, bResult...)
	}

	return results, nil
}

func requestHaveConsistentTimeParameters(requests []*model.CloudwatchData) bool {
	var length int64
	var delay int64
	var period int64

	for i, request := range requests {
		// Set initial values on first iteration
		if i == 0 {
			length = request.GetMetricDataProcessingParams.Length
			delay = request.GetMetricDataProcessingParams.Delay
			period = request.GetMetricDataProcessingParams.Period
			continue
		}
		if request.GetMetricDataProcessingParams.Length != length {
			return false
		}
		if request.GetMetricDataProcessingParams.Delay != delay {
			return false
		}
		if request.GetMetricDataProcessingParams.Period != period {
			return false
		}
	}

	return true
}
