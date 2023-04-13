package job

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/service/sts"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/session"
)

func ScrapeAwsData(
	ctx context.Context,
	logger logging.Logger,
	cfg config.ScrapeConf,
	cache session.SessionCache,
	metricsPerQuery int,
	cloudWatchAPIConcurrency int,
	taggingAPIConcurrency int,
	cloudWatchData chan<- []*model.CloudwatchData,
	taggedResources chan<- []*model.TaggedResource,
) {
	var wg sync.WaitGroup

	// since we have called refresh, we have loaded all the credentials
	// into the clients and it is now safe to call concurrently. Defer the
	// clearing, so we always clear credentials before the next scrape
	cache.Refresh()
	defer cache.Clear()

	for _, discoveryJob := range cfg.Discovery.Jobs {
		for _, role := range discoveryJob.Roles {
			for _, region := range discoveryJob.Regions {
				wg.Add(1)
				go func(discoveryJob *config.Job, region string, role config.Role) {
					defer wg.Done()
					jobLogger := logger.With("job_type", discoveryJob.Type, "region", region, "arn", role.RoleArn)
					result, err := cache.GetSTS(role).GetCallerIdentityWithContext(ctx, &sts.GetCallerIdentityInput{})
					if err != nil || result.Account == nil {
						jobLogger.Error(err, "Couldn't get account Id")
						return
					}
					jobLogger = jobLogger.With("account", *result.Account)

					runDiscoveryJob(ctx, jobLogger, cache, metricsPerQuery, discoveryJob, region, role, result.Account, cfg.Discovery.ExportedTagsOnMetrics, taggingAPIConcurrency, cloudWatchAPIConcurrency, cloudWatchData, taggedResources)
				}(discoveryJob, region, role)
			}
		}
	}

	for _, staticJob := range cfg.Static {
		for _, role := range staticJob.Roles {
			for _, region := range staticJob.Regions {
				wg.Add(1)
				go func(staticJob *config.Static, region string, role config.Role) {
					defer wg.Done()
					jobLogger := logger.With("static_job_name", staticJob.Name, "region", region, "arn", role.RoleArn)
					result, err := cache.GetSTS(role).GetCallerIdentityWithContext(ctx, &sts.GetCallerIdentityInput{})
					if err != nil || result.Account == nil {
						jobLogger.Error(err, "Couldn't get account Id")
						return
					}
					jobLogger = jobLogger.With("account", *result.Account)

					runStaticJob(ctx, jobLogger, cache, region, role, staticJob, result.Account, cloudWatchAPIConcurrency, cloudWatchData)
				}(staticJob, region, role)
			}
		}
	}

	for _, customNamespaceJob := range cfg.CustomNamespace {
		logger.Warn("Jobs of type 'customNamespace' are deprecated and will be removed soon", "job", customNamespaceJob.Name)

		for _, role := range customNamespaceJob.Roles {
			for _, region := range customNamespaceJob.Regions {
				wg.Add(1)
				go func(customNamespaceJob *config.CustomNamespace, region string, role config.Role) {
					defer wg.Done()
					jobLogger := logger.With("custom_metric_namespace", customNamespaceJob.Namespace, "region", region, "arn", role.RoleArn)
					result, err := cache.GetSTS(role).GetCallerIdentityWithContext(ctx, &sts.GetCallerIdentityInput{})
					if err != nil || result.Account == nil {
						jobLogger.Error(err, "Couldn't get account Id")
						return
					}
					jobLogger = jobLogger.With("account", *result.Account)

					runCustomNamespaceJob(ctx, jobLogger, cache, metricsPerQuery, customNamespaceJob, region, role, result.Account, cloudWatchAPIConcurrency, cloudWatchData)
				}(customNamespaceJob, region, role)
			}
		}
	}
	wg.Wait()
}
