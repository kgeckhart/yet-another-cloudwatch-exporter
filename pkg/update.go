package exporter

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

func UpdateMetrics(
	config ScrapeConf,
	registry *prometheus.Registry,
	metricsPerQuery int,
	labelsSnakeCase bool,
	cloudwatchSemaphore, tagSemaphore chan struct{},
	cache SessionCache,
	logger log.Logger,
) {
	tagsData, cloudwatchData := scrapeAwsData(
		config,
		metricsPerQuery,
		cloudwatchSemaphore,
		tagSemaphore,
		cache,
		logger,
	)

	promMetrics, err := migrateCloudwatchToPrometheus(cloudwatchData, labelsSnakeCase)
	if err != nil {
		level.Error(logger).Log("msg", "Error migrating cloudwatch metrics to prometheus metrics", "err", err)
		return
	}

	var metrics []*PrometheusMetric
	metrics = append(metrics, promMetrics...)
	metrics = ensureLabelConsistencyForMetrics(metrics)

	metrics = append(metrics, migrateTagsToPrometheus(tagsData, labelsSnakeCase)...)

	err = registry.Register(NewPrometheusCollector(metrics))
	if err != nil {
		level.Error(logger).Log("msg", "Failed to register exported metrics with prometheus", "err", err)
		return
	}
	for _, counter := range []prometheus.Counter{cloudwatchAPICounter, cloudwatchAPIErrorCounter, cloudwatchGetMetricDataAPICounter, cloudwatchGetMetricStatisticsAPICounter, resourceGroupTaggingAPICounter, autoScalingAPICounter, apiGatewayAPICounter, targetGroupsAPICounter} {
		if err := registry.Register(counter); err != nil {
			level.Warn(logger).Log("msg", "Failed to register internal metric with prometheus", "err", err)
		}
	}
}
