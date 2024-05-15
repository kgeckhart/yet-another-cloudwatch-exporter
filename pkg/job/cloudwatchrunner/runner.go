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
	AccountID                    string
	CloudwatchConcurrency        cloudwatch.ConcurrencyConfig
	GetMetricDataMetricsPerQuery int
}

func New(logger logging.Logger, factory clients.Factory, params Params, job Job) *Runner {
	cloudwatchClient := factory.GetCloudwatchClient(params.Region, params.Role, params.CloudwatchConcurrency)
	lmProcessor := listmetrics.NewDefaultProcessor(logger, cloudwatchClient)
	gmdProcessor := getmetricdata.NewDefaultProcessor(logger, cloudwatchClient, params.GetMetricDataMetricsPerQuery, params.CloudwatchConcurrency.GetMetricData)
	return internalNew(logger, lmProcessor, gmdProcessor, params, job)
}

type Runner struct {
	logger        logging.Logger
	job           Job
	listMetrics   listMetricsProcessor
	getMetricData getMetricDataProcessor
	params        Params
}

// internalNew allows an injection point for interfaces
func internalNew(logger logging.Logger, listMetrics listMetricsProcessor, getMetricData getMetricDataProcessor, params Params, job Job) *Runner {
	return &Runner{
		logger:        logger,
		job:           job,
		listMetrics:   listMetrics,
		getMetricData: getMetricData,
		params:        params,
	}
}

func (r *Runner) Run(ctx context.Context) (*model.CloudwatchMetricResult, error) {
	a := appender.New(r.job.resourceEnrichment().Create(r.logger))

	err := r.listMetrics.Run(ctx, r.job.listMetricsParams(), a)
	if err != nil {
		return nil, fmt.Errorf("failed to list metric data: %w", err)
	}

	getMetricDatas := a.ListAll()
	if len(getMetricDatas) == 0 {
		return nil, nil
	}

	metrics, err := r.getMetricData.Run(ctx, r.job.Namespace(), getMetricDatas)
	if err != nil {
		return nil, fmt.Errorf("failed to get metric data: %w", err)
	}

	result := &model.CloudwatchMetricResult{
		Context: &model.ScrapeContext{
			Region:     r.params.Region,
			AccountID:  r.params.AccountID,
			CustomTags: r.job.CustomTags(),
		},
		Data: metrics,
	}
	return result, nil
}
