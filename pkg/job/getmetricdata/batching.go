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

func (b iteratorFactory) Build(data []*model.CloudwatchData) BatchIterator {
	if len(data) == 0 {
		return nothingToIterate{}
	}

	if dataHasConsistentTimeParameters(data) {
		return NewSlicingBatchIterator(b.windowCalculator, b.metricsPerQuery, data)
	}

	return NewVaryingTimeParameterBatchingIterator(b.windowCalculator, b.metricsPerQuery, data)
}

func dataHasConsistentTimeParameters(cloudwatchData []*model.CloudwatchData) bool {
	var length int64
	var delay int64
	var period int64

	for i, data := range cloudwatchData {
		// Set initial values on first iteration
		if i == 0 {
			length = data.GetMetricDataProcessingParams.Length
			delay = data.GetMetricDataProcessingParams.Delay
			period = data.GetMetricDataProcessingParams.Period
			continue
		}
		if data.GetMetricDataProcessingParams.Length != length {
			return false
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
	batchesByStartAndEndTime := make(map[string][]*model.CloudwatchData)
	for _, datum := range data {
		key := fmt.Sprintf("%d|%d|%d", datum.GetMetricDataProcessingParams.Period, datum.GetMetricDataProcessingParams.Length, datum.GetMetricDataProcessingParams.Delay)
		if _, exists := batchesByStartAndEndTime[key]; !exists {
			batchesByStartAndEndTime[key] = make([]*model.CloudwatchData, 0)
		}
		batchesByStartAndEndTime[key] = append(batchesByStartAndEndTime[key], datum)
	}

	computedSize := 0
	var firstIterator BatchIterator
	iterators := make([]BatchIterator, 0, len(batchesByStartAndEndTime)-1)
	// We are ranging a map, and we won't have an index to mark the first iterator
	isFirst := true
	for _, batch := range batchesByStartAndEndTime {
		iterator := NewSlicingBatchIterator(windowCalculator, metricsPerQuery, batch)
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

	result := s.data[startingIndex:endingIndex]
	s.currentBatch++
	return result, s.startTime, s.endTime
}

func (s *simpleBatchingIterator) HasMore() bool {
	return s.currentBatch < s.size
}

// NewSlicingBatchIterator returns an iterator which slices the data in place based on the metricsPerQuery.
// TODO are we technically doing this https://go.dev/wiki/SliceTricks#batching-with-minimal-allocation and if not can we?
func NewSlicingBatchIterator(windowCalculator WindowCalculator, metricsPerQuery int, data []*model.CloudwatchData) BatchIterator {
	params := data[0].GetMetricDataProcessingParams
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
