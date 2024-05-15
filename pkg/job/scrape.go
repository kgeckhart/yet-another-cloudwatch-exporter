package job

import (
	"context"
	"sync"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients/cloudwatch"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/getmetricdata"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/listmetrics"
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
		return runWithUnifiedRunner(ctx, logger, jobsCfg, factory, metricsPerQuery, cloudwatchConcurrency, taggingAPIConcurrency)
	}

	mux := &sync.Mutex{}
	cwData := make([]model.CloudwatchMetricResult, 0)
	awsInfoData := make([]model.TaggedResourceResult, 0)
	var wg sync.WaitGroup

	for _, discoveryJob := range jobsCfg.DiscoveryJobs {
		for _, role := range discoveryJob.Roles {
			for _, region := range discoveryJob.Regions {
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
				}(discoveryJob, region, role)
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

	for _, customNamespaceJob := range jobsCfg.CustomNamespaceJobs {
		for _, role := range customNamespaceJob.Roles {
			for _, region := range customNamespaceJob.Regions {
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
				}(customNamespaceJob, region, role)
			}
		}
	}
	wg.Wait()
	return awsInfoData, cwData
}

func runWithUnifiedRunner(
	ctx context.Context,
	logger logging.Logger,
	jobsCfg model.JobsConfig,
	factory clients.Factory,
	metricsPerQuery int,
	cloudwatchConcurrency cloudwatch.ConcurrencyConfig,
	taggingAPIConcurrency int,
) ([]model.TaggedResourceResult, []model.CloudwatchMetricResult) {
	if len(jobsCfg.StaticJobs) > 0 {
		logger.Error(nil, "Static jobs are not supported by the unified job runner at this time")
	}

	mux := &sync.Mutex{}
	metricResults := make([]model.CloudwatchMetricResult, 0)
	resourceResults := make([]model.TaggedResourceResult, 0)
	var wg sync.WaitGroup
	roleToRegionToAccount := map[model.Role]map[string]string{}
	for _, job := range jobsCfg.DiscoveryJobs {
		for _, role := range job.Roles {
			if _, exists := roleToRegionToAccount[role]; !exists {
				roleToRegionToAccount[role] = map[string]string{}
			}
			for _, region := range job.Regions {
				jobLogger := logger.With("namespace", job.Type, "region", region, "arn", role.RoleArn)
				var accountID string
				if _, exists := roleToRegionToAccount[role][region]; !exists {
					var err error
					accountID, err = factory.GetAccountClient(region, role).GetAccount(ctx)
					if err != nil {
						logger.Error(err, "Couldn't get account Id")
						continue
					}
					roleToRegionToAccount[role][region] = accountID
				} else {
					accountID = roleToRegionToAccount[role][region]
				}
				jobLogger = jobLogger.With("account", accountID)
				// Capture important loop variables before starting goroutine
				runnerParams := Params{
					Region:                       region,
					Role:                         role,
					CloudwatchConcurrency:        cloudwatchConcurrency,
					GetMetricDataMetricsPerQuery: metricsPerQuery,
				}

				wg.Add(1)
				go func() {
					defer wg.Done()
					taggingClient := factory.GetTaggingClient(runnerParams.Region, runnerParams.Role, taggingAPIConcurrency)
					rmProcessor := resourcemetadata.NewProcessor(jobLogger, taggingClient)

					jobLogger.Debug("Starting resource discovery")
					resourceResult, err := rmProcessor.Run(ctx, runnerParams.Region, runnerParams.AccountID, job)
					if err != nil {
						logger.Error(err, "Resource metadata processor failed")
						return
					}
					if len(resourceResult.Data) > 0 {
						mux.Lock()
						resourceResults = append(resourceResults, *resourceResult)
						mux.Unlock()
					}

					jobLogger.Debug("Resource discovery finished starting cloudwatch metrics runner", "discovered_resources", len(resourceResult.Data))

					runner := NewRunner(jobLogger, factory, runnerParams, discoveryJob{job: job, resources: resourceResult.Data})
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
			}
		}
	}

	for _, job := range jobsCfg.CustomNamespaceJobs {
		for _, role := range job.Roles {
			if _, exists := roleToRegionToAccount[role]; !exists {
				roleToRegionToAccount[role] = map[string]string{}
			}
			for _, region := range job.Regions {
				jobLogger := logger.With("namespace", job.Namespace, "region", region, "arn", role.RoleArn)
				var accountID string
				if _, exists := roleToRegionToAccount[role][region]; !exists {
					var err error
					accountID, err = factory.GetAccountClient(region, role).GetAccount(ctx)
					if err != nil {
						logger.Error(err, "Couldn't get account Id")
						continue
					}
					roleToRegionToAccount[role][region] = accountID
				} else {
					accountID = roleToRegionToAccount[role][region]
				}
				jobLogger = jobLogger.With("account", accountID)

				runnerParams := Params{
					Region:                       region,
					Role:                         role,
					CloudwatchConcurrency:        cloudwatchConcurrency,
					GetMetricDataMetricsPerQuery: metricsPerQuery,
				}

				wg.Add(1)
				go func() {
					defer wg.Done()
					jobLogger.Debug("Starting cloudwatch metrics runner")

					runner := NewRunner(jobLogger, factory, runnerParams, customNamespaceJob{job: job})
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
			}
		}
	}

	wg.Wait()
	return resourceResults, metricResults
}

type discoveryJob struct {
	job       model.DiscoveryJob
	resources []*model.TaggedResource
}

func (d discoveryJob) Namespace() string {
	return d.job.Type
}

func (d discoveryJob) CustomTags() []model.Tag {
	return d.job.CustomTags
}

func (d discoveryJob) listMetricsParams() listmetrics.ProcessingParams {
	return listmetrics.ProcessingParams{
		Namespace:                 d.job.Type,
		Metrics:                   d.job.Metrics,
		RecentlyActiveOnly:        d.job.RecentlyActiveOnly,
		DimensionNameRequirements: d.job.DimensionNameRequirements,
	}
}

func (d discoveryJob) resourceEnrichment() ResourceEnrichment {
	return resourcemetadata.NewResourceAssociation(d.job.DimensionsRegexps, d.job.ExportedTagsOnMetrics, nil)
}

type customNamespaceJob struct {
	job model.CustomNamespaceJob
}

func (c customNamespaceJob) Namespace() string {
	return c.job.Namespace
}

func (c customNamespaceJob) listMetricsParams() listmetrics.ProcessingParams {
	return listmetrics.ProcessingParams{
		Namespace:                 c.job.Namespace,
		Metrics:                   c.job.Metrics,
		RecentlyActiveOnly:        c.job.RecentlyActiveOnly,
		DimensionNameRequirements: c.job.DimensionNameRequirements,
	}
}

func (c customNamespaceJob) resourceEnrichment() ResourceEnrichment {
	return resourcemetadata.StaticResource{Name: c.job.Name}
}

func (c customNamespaceJob) CustomTags() []model.Tag {
	return c.job.CustomTags
}
