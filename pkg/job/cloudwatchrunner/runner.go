package cloudwatchrunner

import (
	"context"
	"fmt"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients/cloudwatch"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/appender"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/getmetricdata"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/listmetrics"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/resourcemetadata"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type ResourceEnrichment interface {
	Create(logger logging.Logger) resourcemetadata.MetricResourceEnricher
}

type Job interface {
	Namespace() string
	CustomTags() []model.Tag
	listMetricsParams() listmetrics.ProcessingParams
	resourceEnrichment() ResourceEnrichment
}

type getMetricDataProcessor interface {
	Run(ctx context.Context, namespace string, requests []*model.CloudwatchData) ([]*model.CloudwatchData, error)
}

type listMetricsProcessor interface {
	Run(ctx context.Context, params listmetrics.ProcessingParams, appender listmetrics.Appender) error
}

type Params struct {
	Region                       string
	Role                         model.Role
	CloudwatchConcurrency        cloudwatch.ConcurrencyConfig
	GetMetricDataMetricsPerQuery int
}

func NewDefault(logger logging.Logger, factory clients.Factory, params Params, job Job) *Runner {
	cloudwatchClient := factory.GetCloudwatchClient(params.Region, params.Role, params.CloudwatchConcurrency)
	lmProcessor := listmetrics.NewDefaultProcessor(logger, cloudwatchClient)
	gmdProcessor := getmetricdata.NewDefaultProcessor(logger, cloudwatchClient, params.GetMetricDataMetricsPerQuery, params.CloudwatchConcurrency.GetMetricData)
	a := appender.New(job.resourceEnrichment().Create(logger))

	return New(logger, lmProcessor, gmdProcessor, a, params, job)
}

type Runner struct {
	logger        logging.Logger
	job           Job
	listMetrics   listMetricsProcessor
	getMetricData getMetricDataProcessor
	params        Params
	appender      *appender.Appender
}

// New allows an injection point for interfaces
func New(logger logging.Logger, listMetrics listMetricsProcessor, getMetricData getMetricDataProcessor, a *appender.Appender, params Params, job Job) *Runner {
	return &Runner{
		logger:        logger,
		job:           job,
		listMetrics:   listMetrics,
		getMetricData: getMetricData,
		appender:      a,
		params:        params,
	}
}

func (r *Runner) Run(ctx context.Context) ([]*model.CloudwatchData, error) {
	err := r.listMetrics.Run(ctx, r.job.listMetricsParams(), r.appender)
	if err != nil {
		return nil, fmt.Errorf("failed to list metric data: %w", err)
	}

	getMetricDatas := r.appender.ListAll()
	if len(getMetricDatas) == 0 {
		return nil, nil
	}

	metrics, err := r.getMetricData.Run(ctx, r.job.Namespace(), getMetricDatas)
	if err != nil {
		return nil, fmt.Errorf("failed to get metric data: %w", err)
	}

	return metrics, nil
}
