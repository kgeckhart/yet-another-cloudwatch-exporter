package exporter

import (
	"math"
	"sync"

	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func scrapeAwsData(
	config ScrapeConf,
	metricsPerQuery int,
	cloudwatchSemaphore, tagSemaphore chan struct{},
	cache SessionCache,
	logger log.Logger,
) ([]*taggedResource, []*cloudwatchData) {
	mux := &sync.Mutex{}

	cwData := make([]*cloudwatchData, 0)
	awsInfoData := make([]*taggedResource, 0)
	var wg sync.WaitGroup

	// since we have called refresh, we have loaded all the credentials
	// into the clients and it is now safe to call concurrently. Defer the
	// clearing, so we always clear credentials before the next scrape
	cache.Refresh()
	defer cache.Clear()

	for _, discoveryJob := range config.Discovery.Jobs {
		for _, role := range discoveryJob.Roles {
			for _, region := range discoveryJob.Regions {
				wg.Add(1)
				go func(discoveryJob *Job, region string, role Role) {
					defer wg.Done()
					result, err := cache.GetSTS(role).GetCallerIdentity(&sts.GetCallerIdentityInput{})
					if err != nil || result.Account == nil {
						level.Error(logger).Log("msg", "Couldn't get account Id", "role", role.RoleArn, "err", err)
						return
					}

					clientCloudwatch := cloudwatchInterface{
						client: cache.GetCloudwatch(&region, role),
					}

					clientTag := tagsInterface{
						account:          *result.Account,
						client:           cache.GetTagging(&region, role),
						apiGatewayClient: cache.GetAPIGateway(&region, role),
						asgClient:        cache.GetASG(&region, role),
						ec2Client:        cache.GetEC2(&region, role),
					}

					resources, metrics := scrapeDiscoveryJobUsingMetricData(discoveryJob, region, result.Account, config.Discovery.ExportedTagsOnMetrics, clientTag, clientCloudwatch, metricsPerQuery, discoveryJob.RoundingPeriod, tagSemaphore, logger)
					mux.Lock()
					awsInfoData = append(awsInfoData, resources...)
					cwData = append(cwData, metrics...)
					mux.Unlock()
				}(discoveryJob, region, role)
			}
		}
	}

	for _, staticJob := range config.Static {
		for _, role := range staticJob.Roles {
			for _, region := range staticJob.Regions {
				wg.Add(1)
				go func(staticJob *Static, region string, role Role) {
					defer wg.Done()
					result, err := cache.GetSTS(role).GetCallerIdentity(&sts.GetCallerIdentityInput{})
					if err != nil || result.Account == nil {
						level.Error(logger).Log("msg", "Couldn't get account Id", "role", role.RoleArn, "err", err)
						return
					}

					clientCloudwatch := cloudwatchInterface{
						client: cache.GetCloudwatch(&region, role),
					}

					metrics := scrapeStaticJob(staticJob, region, result.Account, clientCloudwatch, cloudwatchSemaphore, logger)

					mux.Lock()
					cwData = append(cwData, metrics...)
					mux.Unlock()
				}(staticJob, region, role)
			}
		}
	}
	wg.Wait()
	return awsInfoData, cwData
}

func scrapeStaticJob(resource *Static, region string, accountId *string, clientCloudwatch cloudwatchInterface, cloudwatchSemaphore chan struct{}, logger log.Logger) (cw []*cloudwatchData) {
	mux := &sync.Mutex{}
	var wg sync.WaitGroup

	for j := range resource.Metrics {
		metric := resource.Metrics[j]
		wg.Add(1)
		go func() {
			defer wg.Done()

			cloudwatchSemaphore <- struct{}{}
			defer func() {
				<-cloudwatchSemaphore
			}()

			id := resource.Name
			data := cloudwatchData{
				ID:                     &id,
				Metric:                 &metric.Name,
				Namespace:              &resource.Namespace,
				Statistics:             metric.Statistics,
				NilToZero:              metric.NilToZero,
				AddCloudwatchTimestamp: metric.AddCloudwatchTimestamp,
				CustomTags:             resource.CustomTags,
				Dimensions:             createStaticDimensions(resource.Dimensions),
				Region:                 &region,
				AccountId:              accountId,
			}

			filter := createGetMetricStatisticsInput(
				data.Dimensions,
				&resource.Namespace,
				metric,
				logger,
			)

			data.Points = clientCloudwatch.get(filter)

			if data.Points != nil {
				mux.Lock()
				cw = append(cw, &data)
				mux.Unlock()
			}
		}()
	}
	wg.Wait()
	return cw
}

func GetMetricDataInputLength(job *Job) int64 {
	length := defaultLengthSeconds

	if job.Length > 0 {
		length = job.Length
	}
	for _, metric := range job.Metrics {
		if metric.Length > length {
			length = metric.Length
		}
	}
	return length
}

func getMetricDataForQueries(
	discoveryJob *Job,
	svc *serviceFilter,
	region string,
	accountId *string,
	tagsOnMetrics exportedTagsOnMetrics,
	clientCloudwatch cloudwatchInterface,
	resources []*taggedResource,
	tagSemaphore chan struct{},
	logger log.Logger) []cloudwatchData {
	var getMetricDatas []cloudwatchData

	// For every metric of the job
	for _, metric := range discoveryJob.Metrics {
		// Get the full list of metrics
		// This includes, for this metric the possible combinations
		// of dimensions and value of dimensions with data
		tagSemaphore <- struct{}{}

		metricsList, err := getFullMetricsList(svc.Namespace, metric, clientCloudwatch)
		<-tagSemaphore

		if err != nil {
			level.Error(logger).Log("msg", "Failed to get full metric list", "err", err, "metric_name", metric.Name, "namespace", svc.Namespace, "region", region)
			continue
		}

		if len(resources) == 0 {
			level.Debug(logger).Log("msg", "No resources for metric", "err", err, "metric_name", metric.Name, "namespace", svc.Namespace, "region", region)
		}
		getMetricDatas = append(getMetricDatas, getFilteredMetricDatas(region, accountId, discoveryJob.Type, discoveryJob.CustomTags, tagsOnMetrics, svc.DimensionRegexps, resources, metricsList.Metrics, metric)...)
	}
	return getMetricDatas
}

func scrapeDiscoveryJobUsingMetricData(
	job *Job,
	region string,
	accountId *string,
	tagsOnMetrics exportedTagsOnMetrics,
	clientTag tagsInterface,
	clientCloudwatch cloudwatchInterface,
	metricsPerQuery int,
	roundingPeriod *int64,
	tagSemaphore chan struct{},
	logger log.Logger) (resources []*taggedResource, cw []*cloudwatchData) {

	// Add the info tags of all the resources
	tagSemaphore <- struct{}{}
	resources, err := clientTag.get(job, region)
	<-tagSemaphore
	if err != nil {
		level.Error(logger).Log("msg", "Couldn't get account Id", "region", region, "err", err)
		return
	}

	svc := SupportedServices.GetService(job.Type)
	getMetricDatas := getMetricDataForQueries(job, svc, region, accountId, tagsOnMetrics, clientCloudwatch, resources, tagSemaphore, logger)
	metricDataLength := len(getMetricDatas)
	if metricDataLength == 0 {
		level.Debug(logger).Log("msg", "No metric data found", "job_type", job.Type)
		return
	}

	maxMetricCount := metricsPerQuery
	length := GetMetricDataInputLength(job)
	partition := int(math.Ceil(float64(metricDataLength) / float64(maxMetricCount)))

	mux := &sync.Mutex{}
	var wg sync.WaitGroup
	wg.Add(partition)

	for i := 0; i < metricDataLength; i += maxMetricCount {
		go func(i int) {
			defer wg.Done()
			end := i + maxMetricCount
			if end > metricDataLength {
				end = metricDataLength
			}
			input := getMetricDatas[i:end]
			filter := createGetMetricDataInput(input, &svc.Namespace, length, job.Delay, roundingPeriod, logger)
			data := clientCloudwatch.getMetricData(filter)
			if data != nil {
				output := make([]*cloudwatchData, 0)
				for _, MetricDataResult := range data.MetricDataResults {
					getMetricData, err := findGetMetricDataById(input, *MetricDataResult.Id)
					if err == nil {
						if len(MetricDataResult.Values) != 0 {
							getMetricData.GetMetricDataPoint = MetricDataResult.Values[0]
							getMetricData.GetMetricDataTimestamps = MetricDataResult.Timestamps[0]
						}
						output = append(output, &getMetricData)
					}
				}
				mux.Lock()
				cw = append(cw, output...)
				mux.Unlock()
			}
		}(i)
	}

	wg.Wait()
	return resources, cw
}
