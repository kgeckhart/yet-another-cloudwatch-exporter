package job

import (
	"context"
	"fmt"
	"sync"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients/cloudwatch"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/cloudwatchrunner"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/resourcemetadata"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type ScrapeRunner struct {
	clientFactory         clients.Factory
	jobsCfg               model.JobsConfig
	metricsPerQuery       int
	cloudwatchConcurrency cloudwatch.ConcurrencyConfig
	taggingAPIConcurrency int

	roleRegionToAccount map[model.Role]map[string]string
	logger              logging.Logger
	runnerFactory       runnerFactory
}

type runnerFactory interface {
	NewResourceMetadataRunner(logger logging.Logger, clientFactory clients.Factory, region string, role model.Role, concurrency int) *resourcemetadata.Runner
	NewCloudWatchRunner(logger logging.Logger, factory clients.Factory, params cloudwatchrunner.Params, job cloudwatchrunner.Job) *cloudwatchrunner.Runner
}

func NewScrapeRunner(logger logging.Logger,
	jobsCfg model.JobsConfig,
	clientFactory clients.Factory,
	runnerFactory runnerFactory,
	metricsPerQuery int,
	cloudwatchConcurrency cloudwatch.ConcurrencyConfig,
	taggingAPIConcurrency int,
) *ScrapeRunner {
	roleRegionToAccount := map[model.Role]map[string]string{}
	jobConfigVisitor(jobsCfg, func(_ any, role model.Role, region string) {
		if _, exists := roleRegionToAccount[role]; !exists {
			roleRegionToAccount[role] = map[string]string{}
		}
		roleRegionToAccount[role] = map[string]string{region: ""}
	})

	return &ScrapeRunner{
		clientFactory:         clientFactory,
		runnerFactory:         runnerFactory,
		logger:                logger,
		jobsCfg:               jobsCfg,
		metricsPerQuery:       metricsPerQuery,
		cloudwatchConcurrency: cloudwatchConcurrency,
		taggingAPIConcurrency: taggingAPIConcurrency,
		roleRegionToAccount:   roleRegionToAccount,
	}
}

func (sr ScrapeRunner) Run(ctx context.Context) ([]model.TaggedResourceResult, []model.CloudwatchMetricResult) {
	var wg sync.WaitGroup
	mux := &sync.Mutex{}
	sr.logger.Debug("Starting account initialization")
	for role, regions := range sr.roleRegionToAccount {
		for region := range regions {
			wg.Add(1)
			go func(role model.Role, region string) {
				defer wg.Done()
				accountID, err := sr.clientFactory.GetAccountClient(region, role).GetAccount(ctx)
				if err != nil {
					sr.logger.Error(err, "Failed to get Account", "region", region, "role_arn", role.RoleArn)
				} else {
					sr.roleRegionToAccount[role][region] = accountID
				}
			}(role, region)
		}
	}
	wg.Wait()
	sr.logger.Debug("Finished account initialization")

	metricResults := make([]model.CloudwatchMetricResult, 0)
	resourceResults := make([]model.TaggedResourceResult, 0)
	sr.logger.Debug("Starting job runs")
	jobConfigVisitor(sr.jobsCfg, func(job any, role model.Role, region string) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var namespace string
			jobAction(sr.logger, job, func(job model.DiscoveryJob) {
				namespace = job.Type
			}, func(job model.CustomNamespaceJob) {
				namespace = job.Namespace
			})
			jobLogger := sr.logger.With("namespace", namespace, "region", region, "arn", role.RoleArn)

			accountID := sr.roleRegionToAccount[role][region]
			if accountID == "" {
				jobLogger.Error(nil, "Account for job was not found see previous errors")
				return
			}
			jobLogger = jobLogger.With("account", accountID)

			var jobToRun cloudwatchrunner.Job
			jobAction(jobLogger, job,
				func(job model.DiscoveryJob) {
					rmProcessor := sr.runnerFactory.NewResourceMetadataRunner(jobLogger, sr.clientFactory, region, role, sr.taggingAPIConcurrency)

					jobLogger.Debug("Starting resource discovery")
					resources, err := rmProcessor.Run(ctx, region, job)
					if err != nil {
						jobLogger.Error(err, "Resource metadata processor failed")
						return
					}
					if len(resources) > 0 {
						result := model.TaggedResourceResult{
							Context: &model.ScrapeContext{
								Region:     region,
								AccountID:  accountID,
								CustomTags: job.CustomTags,
							},
							Data: resources,
						}
						mux.Lock()
						resourceResults = append(resourceResults, result)
						mux.Unlock()
					} else {
						jobLogger.Debug("No tagged resources")
					}
					jobLogger.Debug("Resource discovery finished", "number_of_discovered_resources", len(resources))

					jobToRun = cloudwatchrunner.DiscoveryJob{Job: job, Resources: resources}
				}, func(job model.CustomNamespaceJob) {
					jobToRun = cloudwatchrunner.CustomNamespaceJob{Job: job}
				},
			)
			jobLogger.Debug("Starting cloudwatch metrics runner")
			runnerParams := cloudwatchrunner.Params{
				Region:                       region,
				Role:                         role,
				CloudwatchConcurrency:        sr.cloudwatchConcurrency,
				GetMetricDataMetricsPerQuery: sr.metricsPerQuery,
			}
			runner := sr.runnerFactory.NewCloudWatchRunner(jobLogger, sr.clientFactory, runnerParams, jobToRun)
			metricResult, err := runner.Run(ctx)
			if err != nil {
				jobLogger.Error(err, "Failed to run job")
				return
			}

			if metricResult == nil {
				jobLogger.Debug("No metrics data found")
				return
			}

			jobLogger.Debug("Job run finished", "number_of_metrics", len(metricResult))

			result := model.CloudwatchMetricResult{
				Context: &model.ScrapeContext{
					Region:     region,
					AccountID:  accountID,
					CustomTags: jobToRun.CustomTags(),
				},
				Data: metricResult,
			}

			mux.Lock()
			defer mux.Unlock()
			metricResults = append(metricResults, result)
		}()
	})
	wg.Wait()
	sr.logger.Debug("Finished job runs", "resource_results", len(resourceResults), "metric_results", len(metricResults))
	return resourceResults, metricResults
}

// Walk through each custom namespace and discovery jobs and take an action
func jobConfigVisitor(jobsCfg model.JobsConfig, action func(job any, role model.Role, region string)) {
	for _, job := range jobsCfg.DiscoveryJobs {
		for _, role := range job.Roles {
			for _, region := range job.Regions {
				action(job, role, region)
			}
		}
	}

	for _, job := range jobsCfg.CustomNamespaceJobs {
		for _, role := range job.Roles {
			for _, region := range job.Regions {
				action(job, role, region)
			}
		}
	}
}

// Take an action depending on the job type, only supports discovery and custom job types
func jobAction(logger logging.Logger, job any, discovery func(job model.DiscoveryJob), custom func(job model.CustomNamespaceJob)) {
	// Type switches are free https://stackoverflow.com/a/28027945
	switch typedJob := job.(type) {
	case model.DiscoveryJob:
		discovery(typedJob)
	case model.CustomNamespaceJob:
		custom(typedJob)
	default:
		logger.Error(fmt.Errorf("config type of %T is not supported", typedJob), "Unexpected job type")
		return
	}
}
