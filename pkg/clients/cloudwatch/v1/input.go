package v1

import (
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/promutil"
)

func createGetMetricDataInput(getMetricData []*model.CloudwatchData, namespace *string, startTime time.Time, endTime time.Time) *cloudwatch.GetMetricDataInput {
	metricsDataQuery := make([]*cloudwatch.MetricDataQuery, 0, len(getMetricData))
	for _, data := range getMetricData {
		metricStat := &cloudwatch.MetricStat{
			Metric: &cloudwatch.Metric{
				Dimensions: toCloudWatchDimensions(data.Dimensions),
				MetricName: &data.MetricConfig.Name,
				Namespace:  namespace,
			},
			Period: &data.MetricConfig.Period,
			Stat:   &data.GetMetricDataResult.Statistic,
		}
		metricsDataQuery = append(metricsDataQuery, &cloudwatch.MetricDataQuery{
			Id:         data.GetMetricDataResult.ID,
			MetricStat: metricStat,
			ReturnData: aws.Bool(true),
		})
	}

	return &cloudwatch.GetMetricDataInput{
		EndTime:           &endTime,
		StartTime:         &startTime,
		MetricDataQueries: metricsDataQuery,
		ScanBy:            aws.String("TimestampDescending"),
	}
}

func toCloudWatchDimensions(dimensions []*model.Dimension) []*cloudwatch.Dimension {
	cwDim := make([]*cloudwatch.Dimension, 0, len(dimensions))
	for _, dim := range dimensions {
		cwDim = append(cwDim, &cloudwatch.Dimension{
			Name:  &dim.Name,
			Value: &dim.Value,
		})
	}
	return cwDim
}

func createGetMetricStatisticsInput(dimensions []*model.Dimension, namespace *string, metric *model.MetricConfig, logger logging.Logger) *cloudwatch.GetMetricStatisticsInput {
	period := metric.Period
	length := metric.Length
	delay := metric.Delay
	endTime := time.Now().Add(-time.Duration(delay) * time.Second)
	startTime := time.Now().Add(-(time.Duration(length) + time.Duration(delay)) * time.Second)

	var statistics []*string
	var extendedStatistics []*string
	for _, statistic := range metric.Statistics {
		if promutil.Percentile.MatchString(statistic) {
			extendedStatistics = append(extendedStatistics, aws.String(statistic))
		} else {
			statistics = append(statistics, aws.String(statistic))
		}
	}

	output := &cloudwatch.GetMetricStatisticsInput{
		Dimensions:         toCloudWatchDimensions(dimensions),
		Namespace:          namespace,
		StartTime:          &startTime,
		EndTime:            &endTime,
		Period:             &period,
		MetricName:         &metric.Name,
		Statistics:         statistics,
		ExtendedStatistics: extendedStatistics,
	}

	if logger.IsDebugEnabled() {
		logger.Debug("CLI helper - " +
			"aws cloudwatch get-metric-statistics" +
			" --metric-name " + metric.Name +
			" --dimensions " + dimensionsToCliString(dimensions) +
			" --namespace " + *namespace +
			" --statistics " + *statistics[0] +
			" --period " + strconv.FormatInt(period, 10) +
			" --start-time " + startTime.Format(time.RFC3339) +
			" --end-time " + endTime.Format(time.RFC3339))
	}

	return output
}

func dimensionsToCliString(dimensions []*model.Dimension) string {
	out := strings.Builder{}
	for _, dim := range dimensions {
		out.WriteString("Name=")
		out.WriteString(dim.Name)
		out.WriteString(",Value=")
		out.WriteString(dim.Value)
		out.WriteString(" ")
	}
	return out.String()
}
