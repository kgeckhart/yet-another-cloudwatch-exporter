package appender

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type Accumulator struct {
	mux       sync.Mutex
	batches   [][]*model.CloudwatchData
	flattened []*model.CloudwatchData
	done      atomic.Bool
}

func NewAccumulator() *Accumulator {
	return &Accumulator{
		mux:     sync.Mutex{},
		batches: [][]*model.CloudwatchData{},
		done:    atomic.Bool{},
	}
}

func (a *Accumulator) Append(_ context.Context, batch []*model.CloudwatchData) {
	if a.done.Load() {
		return
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
