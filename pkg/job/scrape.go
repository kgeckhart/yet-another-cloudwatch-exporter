package job

import (
	"context"
	"fmt"
	"sync"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients/cloudwatch"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/cloudwatchrunner"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/getmetricdata"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/resourcemetadata"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

func ScrapeAwsData(
	ctx context.Context,
	logger logging.Logger,
	jobsCfg model.JobsConfig,
	factory clients.Factory,
	metricsPerQuery int,
	cloudwatchConcurrency cloudwatch.ConcurrencyConfig,
	taggingAPIConcurrency int,
) ([]model.TaggedResourceResult, []model.CloudwatchMetricResult) {
	if config.FlagsFromCtx(ctx).IsFeatureEnabled(config.UnifiedJobRunner) {
		if len(jobsCfg.StaticJobs) > 0 {
			logger.Error(nil, "Static jobs are not supported by the unified job runner at this time")
			return nil, nil
		}
		runner := NewScrapeRunner(logger, jobsCfg, factory, metricsPerQuery, cloudwatchConcurrency, taggingAPIConcurrency)
		return runner.Run(ctx)
	}

	mux := &sync.Mutex{}
	cwData := make([]model.CloudwatchMetricResult, 0)
	awsInfoData := make([]model.TaggedResourceResult, 0)
	var wg sync.WaitGroup

	for _, job := range jobsCfg.DiscoveryJobs {
		for _, role := range job.Roles {
			for _, region := range job.Regions {
				wg.Add(1)
				go func(discoveryJob model.DiscoveryJob, region string, role model.Role) {
					defer wg.Done()
					jobLogger := logger.With("job_type", discoveryJob.Type, "region", region, "arn", role.RoleArn)
					accountID, err := factory.GetAccountClient(region, role).GetAccount(ctx)
					if err != nil {
						jobLogger.Error(err, "Couldn't get account Id")
						return
					}
					jobLogger = jobLogger.With("account", accountID)

					cloudwatchClient := factory.GetCloudwatchClient(region, role, cloudwatchConcurrency)
					gmdProcessor := getmetricdata.NewDefaultProcessor(logger, cloudwatchClient, metricsPerQuery, cloudwatchConcurrency.GetMetricData)
					resources, metrics := runDiscoveryJob(ctx, jobLogger, discoveryJob, region, factory.GetTaggingClient(region, role, taggingAPIConcurrency), cloudwatchClient, gmdProcessor)
					addDataToOutput := len(metrics) != 0
					if config.FlagsFromCtx(ctx).IsFeatureEnabled(config.AlwaysReturnInfoMetrics) {
						addDataToOutput = addDataToOutput || len(resources) != 0
					}
					if addDataToOutput {
						sc := &model.ScrapeContext{
							Region:     region,
							AccountID:  accountID,
							CustomTags: discoveryJob.CustomTags,
						}
						metricResult := model.CloudwatchMetricResult{
							Context: sc,
							Data:    metrics,
						}
						resourceResult := model.TaggedResourceResult{
							Data: resources,
						}
						if discoveryJob.IncludeContextOnInfoMetrics {
							resourceResult.Context = sc
						}

						mux.Lock()
						awsInfoData = append(awsInfoData, resourceResult)
						cwData = append(cwData, metricResult)
						mux.Unlock()
					}
				}(job, region, role)
			}
		}
	}

	for _, staticJob := range jobsCfg.StaticJobs {
		for _, role := range staticJob.Roles {
			for _, region := range staticJob.Regions {
				wg.Add(1)
				go func(staticJob model.StaticJob, region string, role model.Role) {
					defer wg.Done()
					jobLogger := logger.With("static_job_name", staticJob.Name, "region", region, "arn", role.RoleArn)
					accountID, err := factory.GetAccountClient(region, role).GetAccount(ctx)
					if err != nil {
						jobLogger.Error(err, "Couldn't get account Id")
						return
					}
					jobLogger = jobLogger.With("account", accountID)

					metrics := runStaticJob(ctx, jobLogger, staticJob, factory.GetCloudwatchClient(region, role, cloudwatchConcurrency))
					metricResult := model.CloudwatchMetricResult{
						Context: &model.ScrapeContext{
							Region:     region,
							AccountID:  accountID,
							CustomTags: staticJob.CustomTags,
						},
						Data: metrics,
					}
					mux.Lock()
					cwData = append(cwData, metricResult)
					mux.Unlock()
				}(staticJob, region, role)
			}
		}
	}

	for _, job := range jobsCfg.CustomNamespaceJobs {
		for _, role := range job.Roles {
			for _, region := range job.Regions {
				wg.Add(1)
				go func(customNamespaceJob model.CustomNamespaceJob, region string, role model.Role) {
					defer wg.Done()
					jobLogger := logger.With("custom_metric_namespace", customNamespaceJob.Namespace, "region", region, "arn", role.RoleArn)
					accountID, err := factory.GetAccountClient(region, role).GetAccount(ctx)
					if err != nil {
						jobLogger.Error(err, "Couldn't get account Id")
						return
					}
					jobLogger = jobLogger.With("account", accountID)

					cloudwatchClient := factory.GetCloudwatchClient(region, role, cloudwatchConcurrency)
					gmdProcessor := getmetricdata.NewDefaultProcessor(logger, cloudwatchClient, metricsPerQuery, cloudwatchConcurrency.GetMetricData)
					metrics := runCustomNamespaceJob(ctx, jobLogger, customNamespaceJob, cloudwatchClient, gmdProcessor)
					metricResult := model.CloudwatchMetricResult{
						Context: &model.ScrapeContext{
							Region:     region,
							AccountID:  accountID,
							CustomTags: customNamespaceJob.CustomTags,
						},
						Data: metrics,
					}
					mux.Lock()
					cwData = append(cwData, metricResult)
					mux.Unlock()
				}(job, region, role)
			}
		}
	}
	wg.Wait()
	return awsInfoData, cwData
}

type ScrapeRunner struct {
	factory               clients.Factory
	jobsCfg               model.JobsConfig
	metricsPerQuery       int
	cloudwatchConcurrency cloudwatch.ConcurrencyConfig
	taggingAPIConcurrency int

	roleRegionToAccount map[model.Role]map[string]string
	logger              logging.Logger
}

func NewScrapeRunner(logger logging.Logger,
	jobsCfg model.JobsConfig,
	factory clients.Factory,
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
		factory:               factory,
		logger:                logger,
		jobsCfg:               jobsCfg,
		metricsPerQuery:       metricsPerQuery,
		cloudwatchConcurrency: cloudwatchConcurrency,
		taggingAPIConcurrency: taggingAPIConcurrency,
		roleRegionToAccount:   roleRegionToAccount,
	}
}

func (sr ScrapeRunner) Run(ctx context.Context) ([]model.TaggedResourceResult, []model.CloudwatchMetricResult) {
	sr.logger.Debug("Starting account initialization")
	for role, regions := range sr.roleRegionToAccount {
		for region := range regions {
			accountID, err := sr.factory.GetAccountClient(region, role).GetAccount(ctx)
			if err != nil {
				sr.logger.Error(err, "Failed to get Account", "region", region, "role_arn", role.RoleArn)
			} else {
				sr.roleRegionToAccount[role][region] = accountID
			}
		}
	}
	sr.logger.Debug("Finished account initialization")

	mux := &sync.Mutex{}
	metricResults := make([]model.CloudwatchMetricResult, 0)
	resourceResults := make([]model.TaggedResourceResult, 0)
	var wg sync.WaitGroup

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

			var jobToRun cloudwatchrunner.Job
			jobAction(jobLogger, job,
				func(job model.DiscoveryJob) {
					taggingClient := sr.factory.GetTaggingClient(region, role, sr.taggingAPIConcurrency)
					rmProcessor := resourcemetadata.NewProcessor(jobLogger, taggingClient)

					jobLogger.Debug("Starting resource discovery")
					resourceResult, err := rmProcessor.Run(ctx, region, accountID, job)
					if err != nil {
						jobLogger.Error(err, "Resource metadata processor failed")
						return
					}
					if len(resourceResult.Data) > 0 {
						mux.Lock()
						resourceResults = append(resourceResults, *resourceResult)
						mux.Unlock()
					}
					jobLogger.Debug("Resource discovery finished starting cloudwatch metrics runner", "discovered_resources", len(resourceResult.Data))

					jobToRun = cloudwatchrunner.DiscoveryJob{Job: job, Resources: resourceResult.Data}
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
			runner := cloudwatchrunner.New(jobLogger, sr.factory, runnerParams, jobToRun)
			metricResult, err := runner.Run(ctx)
			if err != nil {
				jobLogger.Error(err, "Failed to run job")
				return
			}

			if metricResult == nil {
				jobLogger.Info("No metrics data found")
			}

			jobLogger.Debug("Job run finished", "number_of_metrics", len(metricResult.Data))

			mux.Lock()
			defer mux.Unlock()
			metricResults = append(metricResults, *metricResult)
		}()
	})
	sr.logger.Debug("Finished job runs", "resource_results", len(resourceResults), "metric_results", len(metricResults))
	wg.Wait()
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
