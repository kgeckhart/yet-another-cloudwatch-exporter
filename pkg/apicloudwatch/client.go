package apicloudwatch

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/promutil"
)

var _ CloudWatchClient = (*Client)(nil)

type CloudWatchClient interface {
	ListMetrics(ctx context.Context, namespace string, metric *config.Metric) (*cloudwatch.ListMetricsOutput, error)
	GetMetricData(ctx context.Context, filter *cloudwatch.GetMetricDataInput) *cloudwatch.GetMetricDataOutput
	GetMetricStatistics(ctx context.Context, filter *cloudwatch.GetMetricStatisticsInput) []*types.Datapoint
}

const timeFormat = "2006-01-02T15:04:05.999999-07:00"

type Client struct {
	logger        logging.Logger
	cloudwatchAPI *cloudwatch.Client
}

func NewClient(logger logging.Logger, cloudwatchAPI *cloudwatch.Client) *Client {
	return &Client{
		logger:        logger,
		cloudwatchAPI: cloudwatchAPI,
	}
}

func (c Client) ListMetrics(ctx context.Context, namespace string, metric *config.Metric) (*cloudwatch.ListMetricsOutput, error) {
	filter := &cloudwatch.ListMetricsInput{
		MetricName: aws.String(metric.Name),
		Namespace:  aws.String(namespace),
	}

	if c.logger.IsDebugEnabled() {
		c.logger.Debug("ListMetrics", "input", filter)
	}

	var res cloudwatch.ListMetricsOutput
	paginator := cloudwatch.NewListMetricsPaginator(c.cloudwatchAPI, filter)
	for paginator.HasMorePages() {
		promutil.CloudwatchAPICounter.Inc()
		page, err := paginator.NextPage(ctx)
		if err != nil {
			promutil.CloudwatchAPIErrorCounter.Inc()
			c.logger.Error(err, "ListMetrics error")
			return nil, err
		}
		res.Metrics = append(res.Metrics, page.Metrics...)
	}

	if c.logger.IsDebugEnabled() {
		c.logger.Debug("ListMetrics", "output", res)
	}
	return &res, nil
}

func (c Client) GetMetricData(ctx context.Context, filter *cloudwatch.GetMetricDataInput) *cloudwatch.GetMetricDataOutput {
	var resp cloudwatch.GetMetricDataOutput

	if c.logger.IsDebugEnabled() {
		c.logger.Debug("GetMetricData", "input", filter)
	}

	paginator := cloudwatch.NewGetMetricDataPaginator(c.cloudwatchAPI, filter)
	for paginator.HasMorePages() {
		promutil.CloudwatchAPICounter.Inc()
		promutil.CloudwatchGetMetricDataAPICounter.Inc()

		page, err := paginator.NextPage(ctx)
		if err != nil {
			c.logger.Error(err, "GetMetricData error")
			return nil
		}
		resp.MetricDataResults = append(resp.MetricDataResults, page.MetricDataResults...)
	}

	if c.logger.IsDebugEnabled() {
		c.logger.Debug("GetMetricData", "output", resp)
	}

	return &resp
}

func (c Client) GetMetricStatistics(ctx context.Context, filter *cloudwatch.GetMetricStatisticsInput) []*types.Datapoint {
	if c.logger.IsDebugEnabled() {
		c.logger.Debug("GetMetricStatistics", "input", filter)
	}

	resp, err := c.cloudwatchAPI.GetMetricStatistics(ctx, filter)

	if c.logger.IsDebugEnabled() {
		c.logger.Debug("GetMetricStatistics", "output", resp)
	}

	promutil.CloudwatchAPICounter.Inc()
	promutil.CloudwatchGetMetricStatisticsAPICounter.Inc()

	if err != nil {
		c.logger.Error(err, "Failed to get metric statistics")
		return nil
	}

	ptrs := make([]*types.Datapoint, 0, len(resp.Datapoints))
	for _, datapoint := range resp.Datapoints {
		ptrs = append(ptrs, &datapoint)
	}

	return ptrs
}

var _ CloudWatchClient = (*LimitedConcurrencyClient)(nil)

type LimitedConcurrencyClient struct {
	client *Client
	sem    chan struct{}
}

func NewLimitedConcurrencyClient(client *Client, maxConcurrency int) *LimitedConcurrencyClient {
	return &LimitedConcurrencyClient{
		client: client,
		sem:    make(chan struct{}, maxConcurrency),
	}
}

func (c LimitedConcurrencyClient) GetMetricStatistics(ctx context.Context, filter *cloudwatch.GetMetricStatisticsInput) []*types.Datapoint {
	c.sem <- struct{}{}
	res := c.client.GetMetricStatistics(ctx, filter)
	<-c.sem
	return res
}

func (c LimitedConcurrencyClient) GetMetricData(ctx context.Context, filter *cloudwatch.GetMetricDataInput) *cloudwatch.GetMetricDataOutput {
	c.sem <- struct{}{}
	res := c.client.GetMetricData(ctx, filter)
	<-c.sem
	return res
}

func (c LimitedConcurrencyClient) ListMetrics(ctx context.Context, namespace string, metric *config.Metric) (*cloudwatch.ListMetricsOutput, error) {
	c.sem <- struct{}{}
	res, err := c.client.ListMetrics(ctx, namespace, metric)
	<-c.sem
	return res, err
}
