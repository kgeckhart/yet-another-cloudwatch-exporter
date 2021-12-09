package exporter

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	CloudwatchAPICounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "yace_cloudwatch_requests_total",
		Help: "Help is not implemented yet.",
	})
	CloudwatchAPIErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "yace_cloudwatch_request_errors",
		Help: "Help is not implemented yet.",
	})
	CloudwatchGetMetricDataAPICounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "yace_cloudwatch_getmetricdata_requests_total",
		Help: "Help is not implemented yet.",
	})
	CloudwatchGetMetricStatisticsAPICounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "yace_cloudwatch_getmetricstatistics_requests_total",
		Help: "Help is not implemented yet.",
	})
	ResourceGroupTaggingAPICounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "yace_cloudwatch_resourcegrouptaggingapi_requests_total",
		Help: "Help is not implemented yet.",
	})
	AutoScalingAPICounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "yace_cloudwatch_autoscalingapi_requests_total",
		Help: "Help is not implemented yet.",
	})
	TargetGroupsAPICounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "yace_cloudwatch_targetgroupapi_requests_total",
		Help: "Help is not implemented yet.",
	})
	ApiGatewayAPICounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "yace_cloudwatch_apigatewayapi_requests_total",
	})
	Ec2APICounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "yace_cloudwatch_ec2api_requests_total",
		Help: "Help is not implemented yet.",
	})
)

func UpdateMetrics(
	config ScrapeConf,
	registry *prometheus.Registry,
	metricsPerQuery int,
	labelsSnakeCase bool,
	cloudwatchSemaphore, tagSemaphore chan struct{},
	cache SessionCache,
) {
	tagsData, cloudwatchData := scrapeAwsData(
		config,
		metricsPerQuery,
		cloudwatchSemaphore,
		tagSemaphore,
		cache,
	)
	var metrics []*PrometheusMetric

	metrics = append(metrics, migrateCloudwatchToPrometheus(cloudwatchData, labelsSnakeCase)...)
	metrics = ensureLabelConsistencyForMetrics(metrics)

	metrics = append(metrics, migrateTagsToPrometheus(tagsData, labelsSnakeCase)...)

	registry.MustRegister(NewPrometheusCollector(metrics))
}
