package exporter

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/promutil"
)

var finalMetrics []*promutil.PrometheusMetric

func benchmarkAppend(sliceLength int, b *testing.B) {
	metric := promutil.PrometheusMetric{
		Name:   aws.String("metric1"),
		Labels: map[string]string{"label1": "value1"},
		Value:  aws.Float64(1.0),
	}
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		metrics := make([]*promutil.PrometheusMetric, sliceLength)
		b.StartTimer()
		var metricSlice2 []*promutil.PrometheusMetric
		for i := 0; i < sliceLength; i++ {
			metricSlice2 = append(metricSlice2, &metric)
		}
		finalMetrics = append(metrics, metricSlice2...)
	}
}

func BenchmarkAppend10(b *testing.B)   { benchmarkAppend(10, b) }
func BenchmarkAppend50(b *testing.B)   { benchmarkAppend(50, b) }
func BenchmarkAppend100(b *testing.B)  { benchmarkAppend(100, b) }
func BenchmarkAppend500(b *testing.B)  { benchmarkAppend(500, b) }
func BenchmarkAppend1000(b *testing.B) { benchmarkAppend(1000, b) }

func benchmarkAdd(sliceLength int, b *testing.B) {
	metric := promutil.PrometheusMetric{
		Name:   aws.String("metric1"),
		Labels: map[string]string{"label1": "value1"},
		Value:  aws.Float64(1.0),
	}
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		finalMetrics = make([]*promutil.PrometheusMetric, sliceLength)
		b.StartTimer()

		for i := 0; i < sliceLength; i++ {
			finalMetrics = append(finalMetrics, &metric)
		}
	}
}

func BenchmarkAdd10(b *testing.B)   { benchmarkAdd(10, b) }
func BenchmarkAdd50(b *testing.B)   { benchmarkAdd(50, b) }
func BenchmarkAdd100(b *testing.B)  { benchmarkAdd(100, b) }
func BenchmarkAdd500(b *testing.B)  { benchmarkAdd(500, b) }
func BenchmarkAdd1000(b *testing.B) { benchmarkAdd(1000, b) }
