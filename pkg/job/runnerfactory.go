package job

import (
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients/account"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients/cloudwatch"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/cloudwatchrunner"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/resourcemetadata"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type RunnerFactory struct {
	clientFactory                clients.Factory
	resourceMetadataConcurrency  int
	cloudwatchConcurrency        cloudwatch.ConcurrencyConfig
	getMetricDataMetricsPerQuery int
}

func (r *RunnerFactory) GetAccountClient(region string, role model.Role) account.Client {
	return r.clientFactory.GetAccountClient(region, role)
}

func (r *RunnerFactory) NewResourceMetadataRunner(logger logging.Logger, region string, role model.Role) ResourceMetadataRunner {
	return resourcemetadata.NewDefaultRunner(logger, r.clientFactory, region, role, r.resourceMetadataConcurrency)
}

func (r *RunnerFactory) NewCloudWatchRunner(logger logging.Logger, region string, role model.Role, job cloudwatchrunner.Job) CloudwatchRunner {
	params := cloudwatchrunner.Params{
		Region:                       region,
		Role:                         role,
		CloudwatchConcurrency:        r.cloudwatchConcurrency,
		GetMetricDataMetricsPerQuery: r.getMetricDataMetricsPerQuery,
	}
	return cloudwatchrunner.NewDefault(logger, r.clientFactory, params, job)
}
