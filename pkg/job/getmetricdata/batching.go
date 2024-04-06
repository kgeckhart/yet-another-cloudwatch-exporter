package getmetricdata

import (
	"fmt"
	"math"
	"time"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type WindowCalculator interface {
	Calculate(period time.Duration, length time.Duration, delay time.Duration) (time.Time, time.Time)
}

type iteratorFactory struct {
	metricsPerQuery  int
	windowCalculator WindowCalculator
}

func (b iteratorFactory) Build(data []*model.CloudwatchData, jobMetricLength, jobMetricDelay int64) BatchIterator {
	if len(data) == 0 {
		return nothingToIterate{}
	}

	if dataHasConsistentStartTimeParameters(data) {
		params := data[0].GetMetricDataProcessingParams
		params.Length = jobMetricLength
		params.Delay = jobMetricDelay

		return NewSimpleBatchIterator(b.windowCalculator, b.metricsPerQuery, data, params)
	}

	return NewVaryingTimeParameterBatchingIterator(b.windowCalculator, b.metricsPerQuery, data)
}

func dataHasConsistentStartTimeParameters(cloudwatchData []*model.CloudwatchData) bool {
	// We exclude length from this because historically jobs always took the longest length and changing that
	// 	without warning could break configs that were only working because a longer length extended the time window
	//  for the GetMetricData query
	var delay int64
	var period int64

	for i, data := range cloudwatchData {
		// Set initial values on first iteration
		if i == 0 {
			delay = data.GetMetricDataProcessingParams.Delay
			period = data.GetMetricDataProcessingParams.Period
			continue
		}
		if data.GetMetricDataProcessingParams.Delay != delay {
			return false
		}
		if data.GetMetricDataProcessingParams.Period != period {
			return false
		}
	}

	return true
}

type nothingToIterate struct{}

func (n nothingToIterate) Size() int {
	return 0
}

func (n nothingToIterate) Next() ([]*model.CloudwatchData, time.Time, time.Time) {
	return nil, time.Time{}, time.Time{}
}

func (n nothingToIterate) HasMore() bool {
	return false
}

type simpleBatchingIterator struct {
	size            int
	currentBatch    int
	startTime       time.Time
	endTime         time.Time
	data            []*model.CloudwatchData
	entriesPerBatch int
}

func (s *simpleBatchingIterator) Size() int {
	return s.size
}

func (s *simpleBatchingIterator) Next() ([]*model.CloudwatchData, time.Time, time.Time) {
	// We are out of data return defaults
	if s.currentBatch >= s.size {
		return nil, time.Time{}, time.Time{}
	}

	startingIndex := s.currentBatch * s.entriesPerBatch
	endingIndex := startingIndex + s.entriesPerBatch
	if endingIndex > len(s.data) {
		endingIndex = len(s.data)
	}

	// TODO are we technically doing this https://go.dev/wiki/SliceTricks#batching-with-minimal-allocation and if not
	// would it change allocations to do this ahead of time?
	result := s.data[startingIndex:endingIndex]
	s.currentBatch++
	return result, s.startTime, s.endTime
}

func (s *simpleBatchingIterator) HasMore() bool {
	return s.currentBatch < s.size
}

// NewSimpleBatchIterator returns an iterator which slices the data in place based on the metricsPerQuery.
func NewSimpleBatchIterator(windowCalculator WindowCalculator, metricsPerQuery int, data []*model.CloudwatchData, params *model.GetMetricDataProcessingParams) BatchIterator {
	startTime, endTime := windowCalculator.Calculate(
		time.Duration(params.Period)*time.Second,
		time.Duration(params.Length)*time.Second,
		time.Duration(params.Delay)*time.Second)

	size := int(math.Ceil(float64(len(data)) / float64(metricsPerQuery)))

	return &simpleBatchingIterator{
		size:            size,
		startTime:       startTime,
		endTime:         endTime,
		data:            data,
		entriesPerBatch: metricsPerQuery,
	}
}

type timeParameterBatchingIterator struct {
	computedSize int
	current      BatchIterator
	remaining    []BatchIterator
}

func (t *timeParameterBatchingIterator) Size() int {
	return t.computedSize
}

func (t *timeParameterBatchingIterator) Next() ([]*model.CloudwatchData, time.Time, time.Time) {
	batch, start, end := t.current.Next()

	// Doing this before returning from Next drastically simplifies HasMore because it can depend on
	// t.current.HasMore() being accurate.
	if !t.current.HasMore() {
		// Current iterator is out and there's none left, set current to nothingToIterate
		if len(t.remaining) == 0 {
			t.remaining = nil
			t.current = nothingToIterate{}
		}

		// Pop from https://go.dev/wiki/SliceTricks
		next, remaining := t.remaining[len(t.remaining)-1], t.remaining[:len(t.remaining)-1]
		t.current = next
		t.remaining = remaining
	}

	return batch, start, end
}

func (t *timeParameterBatchingIterator) HasMore() bool {
	return t.current.HasMore()
}

func NewVaryingTimeParameterBatchingIterator(windowCalculator WindowCalculator, metricsPerQuery int, data []*model.CloudwatchData) BatchIterator {
	batchesByPeriodAndDelay := make(map[string][]*model.CloudwatchData)
	// Since we do batching by period + delay we still need to keep track of the longest length so start + endtime can
	// be calculated properly.
	// It seemed less allocation heavy to have two maps vs one map with a custom struct of []*model.CloudwatchData + int64
	longestLengthForBatch := make(map[string]int64)
	for _, datum := range data {
		key := fmt.Sprintf("%d|%d", datum.GetMetricDataProcessingParams.Period, datum.GetMetricDataProcessingParams.Delay)
		if _, exists := batchesByPeriodAndDelay[key]; !exists {
			batchesByPeriodAndDelay[key] = make([]*model.CloudwatchData, 0)
		}
		batchesByPeriodAndDelay[key] = append(batchesByPeriodAndDelay[key], datum)
		if longestLengthForBatch[key] < datum.GetMetricDataProcessingParams.Length {
			longestLengthForBatch[key] = datum.GetMetricDataProcessingParams.Length
		}
	}

	computedSize := 0
	var firstIterator BatchIterator
	iterators := make([]BatchIterator, 0, len(batchesByPeriodAndDelay)-1)
	// We are ranging a map, and we won't have an index to mark the first iterator
	isFirst := true
	for key, batch := range batchesByPeriodAndDelay {
		batchParams := batch[0].GetMetricDataProcessingParams
		// Make sure to set the length to the longest length for the batch
		batchParams.Length = longestLengthForBatch[key]
		iterator := NewSimpleBatchIterator(windowCalculator, metricsPerQuery, batch, batchParams)
		computedSize += iterator.Size()
		if isFirst {
			firstIterator = iterator
			isFirst = false
		} else {
			iterators = append(iterators, iterator)
		}
	}

	return &timeParameterBatchingIterator{
		computedSize: computedSize,
		current:      firstIterator,
		remaining:    iterators,
	}
}
