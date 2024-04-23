package job

import (
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type appendingBatchingIterator struct {
	appendBuffer    []*model.CloudwatchData
	entriesPerBatch int
	done            bool
	roundingPeriod  *int64
	length          int64
	delay           int64
}

func (s *appendingBatchingIterator) Next() ([]*model.CloudwatchData, *model.GetMetricDataProcessingParams) {

}

func (s *appendingBatchingIterator) HasMore() bool {
	return s.done
}

func (s *appendingBatchingIterator) Add(data []*model.CloudwatchData) {

}

func (s *appendingBatchingIterator) Done() {
	s.done = true
}

// NewSimpleBatchIterator returns an iterator which slices the data in place based on the metricsPerQuery.
func NewSimpleBatchIterator(metricsPerQuery int, length, delay int64, roundingPeriod *int64) cloudwatchDataIterator {
	return &appendingBatchingIterator{
		roundingPeriod:  roundingPeriod,
		entriesPerBatch: metricsPerQuery,
		length:          length,
		delay:           delay,
	}
}
