package job

import (
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/cloudwatchrunner"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/resourcemetadata"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type RunnerFactory struct{}

func (r RunnerFactory) NewResourceMetadataRunner(logger logging.Logger, clientFactory clients.Factory, region string, role model.Role, concurrency int) *resourcemetadata.Runner {
	return resourcemetadata.NewDefaultRunner(logger, clientFactory, region, role, concurrency)
}

func (r RunnerFactory) NewCloudWatchRunner(logger logging.Logger, factory clients.Factory, params cloudwatchrunner.Params, job cloudwatchrunner.Job) *cloudwatchrunner.Runner {
	return cloudwatchrunner.NewDefault(logger, factory, params, job)
}
