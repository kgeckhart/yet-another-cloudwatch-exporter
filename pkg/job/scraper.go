package job

import (
	"context"
	"fmt"
	"sync"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients/account"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/cloudwatchrunner"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/resourcemetadata"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type Scraper struct {
	jobsCfg             model.JobsConfig
	roleRegionToAccount map[model.Role]map[string]string
	logger              logging.Logger
	runnerFactory       runnerFactory
}

type runnerFactory interface {
	GetAccountClient(region string, role model.Role) account.Client
	NewResourceMetadataRunner(logger logging.Logger, region string, role model.Role) *resourcemetadata.Runner
	NewCloudWatchRunner(logger logging.Logger, region string, role model.Role, job cloudwatchrunner.Job) *cloudwatchrunner.Runner
}

func NewScraper(logger logging.Logger,
	jobsCfg model.JobsConfig,
	runnerFactory runnerFactory,
) *Scraper {
	roleRegionToAccount := map[model.Role]map[string]string{}
	jobConfigVisitor(jobsCfg, func(_ any, role model.Role, region string) {
		if _, exists := roleRegionToAccount[role]; !exists {
			roleRegionToAccount[role] = map[string]string{}
		}
		roleRegionToAccount[role] = map[string]string{region: ""}
	})

	return &Scraper{
		runnerFactory:       runnerFactory,
		logger:              logger,
		jobsCfg:             jobsCfg,
		roleRegionToAccount: roleRegionToAccount,
	}
}

func (s Scraper) Scrape(ctx context.Context) ([]model.TaggedResourceResult, []model.CloudwatchMetricResult) {
	var wg sync.WaitGroup
	mux := &sync.Mutex{}
	s.logger.Debug("Starting account initialization")
	for role, regions := range s.roleRegionToAccount {
		for region := range regions {
			wg.Add(1)
			go func(role model.Role, region string) {
				defer wg.Done()
				accountID, err := s.runnerFactory.GetAccountClient(region, role).GetAccount(ctx)
				if err != nil {
					s.logger.Error(err, "Failed to get Account", "region", region, "role_arn", role.RoleArn)
				} else {
					s.roleRegionToAccount[role][region] = accountID
				}
			}(role, region)
		}
	}
	wg.Wait()
	s.logger.Debug("Finished account initialization")

	metricResults := make([]model.CloudwatchMetricResult, 0)
	resourceResults := make([]model.TaggedResourceResult, 0)
	s.logger.Debug("Starting job runs")
	jobConfigVisitor(s.jobsCfg, func(job any, role model.Role, region string) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var namespace string
			jobAction(s.logger, job, func(job model.DiscoveryJob) {
				namespace = job.Type
			}, func(job model.CustomNamespaceJob) {
				namespace = job.Namespace
			})
			jobLogger := s.logger.With("namespace", namespace, "region", region, "arn", role.RoleArn)

			accountID := s.roleRegionToAccount[role][region]
			if accountID == "" {
				jobLogger.Error(nil, "Account for job was not found see previous errors")
				return
			}
			jobLogger = jobLogger.With("account", accountID)

			var jobToRun cloudwatchrunner.Job
			jobAction(jobLogger, job,
				func(job model.DiscoveryJob) {
					jobLogger.Debug("Starting resource discovery")
					rmRunner := s.runnerFactory.NewResourceMetadataRunner(jobLogger, region, role)
					resources, err := rmRunner.Run(ctx, region, job)
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
			cwRunner := s.runnerFactory.NewCloudWatchRunner(jobLogger, region, role, jobToRun)
			metricResult, err := cwRunner.Run(ctx)
			if err != nil {
				jobLogger.Error(err, "Failed to run job")
				return
			}

			if len(metricResult) == 0 {
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
	s.logger.Debug("Finished job runs", "resource_results", len(resourceResults), "metric_results", len(metricResults))
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
