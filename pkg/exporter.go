package exporter

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/promutil"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/session"
)

// Metrics is a slice of prometheus metrics specific to the scraping process such API call counters
var Metrics = []prometheus.Collector{
	promutil.CloudwatchAPICounter,
	promutil.CloudwatchAPIErrorCounter,
	promutil.CloudwatchGetMetricDataAPICounter,
	promutil.CloudwatchGetMetricStatisticsAPICounter,
	promutil.ResourceGroupTaggingAPICounter,
	promutil.AutoScalingAPICounter,
	promutil.TargetGroupsAPICounter,
	promutil.APIGatewayAPICounter,
	promutil.Ec2APICounter,
	promutil.DmsAPICounter,
	promutil.StoragegatewayAPICounter,
}

const (
	DefaultMetricsPerQuery          = 500
	DefaultLabelsSnakeCase          = false
	DefaultCloudWatchAPIConcurrency = 5
	DefaultTaggingAPIConcurrency    = 5
)

// featureFlagsMap is a map that contains the enabled feature flags. If a key is not present, it means the feature flag
// is disabled.
type featureFlagsMap map[string]struct{}

type options struct {
	metricsPerQuery          int
	labelsSnakeCase          bool
	cloudWatchAPIConcurrency int
	taggingAPIConcurrency    int
	featureFlags             featureFlagsMap
}

var defaultOptions = options{
	metricsPerQuery:          DefaultMetricsPerQuery,
	labelsSnakeCase:          DefaultLabelsSnakeCase,
	cloudWatchAPIConcurrency: DefaultCloudWatchAPIConcurrency,
	taggingAPIConcurrency:    DefaultTaggingAPIConcurrency,
	featureFlags:             make(featureFlagsMap),
}

// IsFeatureFlag implements the FeatureFlags interface, allowing us to inject the options-configure feature flags in the rest of the code.
func (ff featureFlagsMap) IsFeatureEnabled(flag string) bool {
	_, ok := ff[flag]
	return ok
}

type OptionsFunc func(*options) error

func MetricsPerQuery(metricsPerQuery int) OptionsFunc {
	return func(o *options) error {
		if metricsPerQuery <= 0 {
			return fmt.Errorf("MetricsPerQuery must be a positive value")
		}

		o.metricsPerQuery = metricsPerQuery
		return nil
	}
}

func LabelsSnakeCase(labelsSnakeCase bool) OptionsFunc {
	return func(o *options) error {
		o.labelsSnakeCase = labelsSnakeCase
		return nil
	}
}

func CloudWatchAPIConcurrency(maxConcurrency int) OptionsFunc {
	return func(o *options) error {
		if maxConcurrency <= 0 {
			return fmt.Errorf("CloudWatchAPIConcurrency must be a positive value")
		}

		o.cloudWatchAPIConcurrency = maxConcurrency
		return nil
	}
}

func TaggingAPIConcurrency(maxConcurrency int) OptionsFunc {
	return func(o *options) error {
		if maxConcurrency <= 0 {
			return fmt.Errorf("TaggingAPIConcurrency must be a positive value")
		}

		o.taggingAPIConcurrency = maxConcurrency
		return nil
	}
}

// EnableFeatureFlag is an option that enables a feature flag on the YACE's entrypoint.
func EnableFeatureFlag(flags ...string) OptionsFunc {
	return func(o *options) error {
		for _, flag := range flags {
			o.featureFlags[flag] = struct{}{}
		}
		return nil
	}
}

// UpdateMetrics can be used to scrape metrics from AWS on demand using the provided parameters. Scraped metrics will be added to the provided registry and
// any labels discovered during the scrape will be added to observedMetricLabels with their metric name as the key. Any errors encountered are not returned but
// will be logged and will either fail the scrape or a partial metric result will be added to the registry.
func UpdateMetrics(
	ctx context.Context,
	logger logging.Logger,
	cfg config.ScrapeConf,
	registry *prometheus.Registry,
	cache session.SessionCache,
	observedMetricLabels map[string]model.LabelSet,
	optFuncs ...OptionsFunc,
) error {
	options := defaultOptions
	for _, f := range optFuncs {
		if err := f(&options); err != nil {
			return err
		}
	}

	// add feature flags to context passed down to all other layers
	ctx = config.CtxWithFlags(ctx, options.featureFlags)
	cloudwatchDataReceiver := make(chan []*model.CloudwatchData, options.cloudWatchAPIConcurrency)
	taggedResourcesReceiver := make(chan []*model.TaggedResource, options.taggingAPIConcurrency)
	go func() {
		job.ScrapeAwsData(
			ctx,
			logger,
			cfg,
			cache,
			options.metricsPerQuery,
			options.cloudWatchAPIConcurrency,
			options.taggingAPIConcurrency,
			cloudwatchDataReceiver,
			taggedResourcesReceiver,
		)

		close(cloudwatchDataReceiver)
		close(taggedResourcesReceiver)
	}()

	// TODO buffered or unbuffered here?
	metricsReceiver := make(chan *promutil.PrometheusMetric, 100)
	done := make(chan struct{})
	var g errgroup.Group
	g.Go(func() error {
		err := promutil.BuildMetrics(cloudwatchDataReceiver, done, options.labelsSnakeCase, logger, metricsReceiver)
		if err != nil {
			// Shut it down early
			done <- struct{}{}
			return err
		}
		return nil
	})
	g.Go(func() error {
		promutil.BuildNamespaceInfoMetrics(taggedResourcesReceiver, done, options.labelsSnakeCase, logger, metricsReceiver)
		return nil
	})
	var metrics []*promutil.PrometheusMetric
	g.Go(func() error {
		for {
			select {
			case received := <-metricsReceiver:
				metrics = append(metrics, received)
				if _, exists := observedMetricLabels[*received.Name]; !exists {
					observedMetricLabels[*received.Name] = make(model.LabelSet)
				}
				for key := range received.Labels {
					if _, exists := observedMetricLabels[*received.Name][key]; !exists {
						observedMetricLabels[*received.Name][key] = struct{}{}
					}
				}
			case <-done:
				return nil
			}
		}
	})
	if err := g.Wait(); err != nil {
		logger.Error(err, "Error migrating cloudwatch data to prometheus metrics")
		return nil
	}

	metrics = promutil.EnsureLabelConsistencyForMetrics(metrics, observedMetricLabels)

	registry.MustRegister(promutil.NewPrometheusCollector(metrics))

	return nil
}
