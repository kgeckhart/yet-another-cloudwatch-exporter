package job

import (
	"context"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients/cloudwatch"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/appender"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/getmetricdata"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/listmetrics"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type Config interface {
	Namespace() string
	listMetricsParams() listmetrics.ProcessingParams
	resourceEnrichmentStrategy() appender.ResourceEnrichmentStrategy
}

type Params struct {
	Region                string
	Role                  model.Role
	CloudwatchConcurrency cloudwatch.ConcurrencyConfig
	// TODO do we really need to configure this it's a billing related setting?
	GetMetricDataMetricsPerQuery int
}

type Runner struct {
	logger        logging.Logger
	config        Config
	listMetrics   listMetricsProcessor
	getMetricData getMetricDataProcessor
}

func New(logger logging.Logger, factory clients.Factory, params Params, config Config) *Runner {
	cloudwatchClient := factory.GetCloudwatchClient(params.Region, params.Role, params.CloudwatchConcurrency)
	gmdProcessor := getmetricdata.NewDefaultProcessor(logger, cloudwatchClient, params.GetMetricDataMetricsPerQuery, params.CloudwatchConcurrency.GetMetricData)
	lmProcessor := listmetrics.NewDefaultProcessor(logger, cloudwatchClient)
	return internalNew(logger, lmProcessor, gmdProcessor, config)
}

// internalNew allows an injection point for interfaces
func internalNew(logger logging.Logger, listMetrics listMetricsProcessor, getMetricData getMetricDataProcessor, config Config) *Runner {
	return &Runner{
		logger:        logger,
		config:        config,
		listMetrics:   listMetrics,
		getMetricData: getMetricData,
	}
}

func (r *Runner) Run(ctx context.Context) ([]*model.TaggedResource, []*model.CloudwatchData) {
	r.logger.Debug("Get tagged resources")

	// resources, err := clientTag.GetResources(ctx, job, region)
	// if err != nil {
	// 	if errors.Is(err, tagging.ErrExpectedToFindResources) {
	// 		logger.Error(err, "No tagged resources made it through filtering")
	// 	} else {
	// 		logger.Error(err, "Couldn't describe resources")
	// 	}
	// 	return nil, nil
	// }
	//
	// if len(resources) == 0 {
	// 	logger.Debug("No tagged resources", "region", region, "namespace", job.Type)
	// }

	a := appender.New(r.logger, r.config.resourceEnrichmentStrategy())
	err := r.listMetrics.Run(ctx, r.config.listMetricsParams(), a)
	if err != nil {
		r.logger.Error(err, "Failed to list metric data")
		return nil, nil
	}
	getMetricDatas := a.ListAll()
	if len(getMetricDatas) == 0 {
		r.logger.Info("No metrics data found")
		// return resources, nil
	}

	metrics, err := r.getMetricData.Run(ctx, r.config.Namespace(), getMetricDatas)
	if err != nil {
		r.logger.Error(err, "Failed to get metric data")
		return nil, nil
	}

	return nil, metrics
}
