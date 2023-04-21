package session

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	aws_config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	"github.com/aws/aws-sdk-go-v2/service/storagegateway"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	aws_logging "github.com/aws/smithy-go/logging"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
)

// AWSClientCache is used to store AWS client instances that are created once and shared
// over the lifetime of the AWS Scrape to reduce the number of AWS API calls
type AWSClientCache interface {
	GetSTS(string, config.Role) *sts.Client
	GetCloudwatch(*string, config.Role) *cloudwatch.Client
	GetTagging(*string, config.Role) *resourcegroupstaggingapi.Client
	GetASG(*string, config.Role) *autoscaling.Client
	GetEC2(*string, config.Role) *ec2.Client
	GetDMS(*string, config.Role) *databasemigrationservice.Client
	GetAPIGateway(*string, config.Role) *apigateway.Client
	GetStorageGateway(*string, config.Role) *storagegateway.Client
	GetPrometheus(*string, config.Role) *amp.Client
	Refresh()
	Clear()
}

type awsRegion = string

type awsClientCache struct {
	sts           *sts.Client
	logger        logging.Logger
	configOptions []func(*aws_config.LoadOptions) error
	clients       map[config.Role]map[awsRegion]*clientCacheV2
	mu            sync.Mutex
	refreshed     bool
	cleared       bool
}

type optionalClient[T any] struct {
	necessaryForConfig bool
	client             T
}

type clientCacheV2 struct {
	awsConfig      *aws.Config
	sts            *sts.Client
	cloudwatch     *cloudwatch.Client
	tagging        optionalClient[*resourcegroupstaggingapi.Client]
	asg            optionalClient[*autoscaling.Client]
	ec2            optionalClient[*ec2.Client]
	dms            optionalClient[*databasemigrationservice.Client]
	apiGateway     optionalClient[*apigateway.Client]
	storageGateway optionalClient[*storagegateway.Client]
	prometheus     optionalClient[*amp.Client]
}

func NewAwsClientCache(cfg config.ScrapeConf, fips bool, logger logging.Logger) (AWSClientCache, error) {
	var options []func(*aws_config.LoadOptions) error
	options = append(options, aws_config.WithLogger(aws_logging.LoggerFunc(func(classification aws_logging.Classification, format string, v ...interface{}) {
		if classification == aws_logging.Debug && logger.IsDebugEnabled() {
			logger.Debug(fmt.Sprintf(format, v...))
		} else if classification == aws_logging.Warn {
			logger.Warn(fmt.Sprintf(format, v...))
		} else { // AWS logging only supports debug or warn, log everything else as error
			logger.Error(fmt.Errorf("unexected aws error classification: %s", classification), fmt.Sprintf(format, v...))
		}
	})))

	endpointURLOverride := os.Getenv("AWS_ENDPOINT_URL")
	if endpointURLOverride != "" {
		options = append(options, aws_config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: endpointURLOverride,
			}, nil
		})))
	}

	if fips {
		options = append(options, aws_config.WithUseFIPSEndpoint(aws.FIPSEndpointStateEnabled))
	}

	c, err := aws_config.LoadDefaultConfig(context.TODO(), options...)
	if err != nil {
		return nil, err
	}

	stsClient := sts.NewFromConfig(c)
	clients := map[config.Role]map[awsRegion]*clientCacheV2{}
	for _, discoveryJob := range cfg.Discovery.Jobs {
		serviceDefinition := config.SupportedServices.GetService(discoveryJob.Type)
		jobRequiresTagging := serviceDefinition.DiscoveredByTagging()
		requiresMetadataAPIs := true
		// TODO make this work
		//_, exists := apitagging.ServiceFilters[serviceDefinition.Namespace]
		//if !exists {
		//	requiresMetadataAPIs = false
		//}
		for _, role := range discoveryJob.Roles {
			if _, ok := clients[role]; !ok {
				clients[role] = map[awsRegion]*clientCacheV2{}
			}
			for _, region := range discoveryJob.Regions {
				regionConfig, regionStsClient := awsConfigAndStsForRegion(role, &c, stsClient, region, role)
				clients[role][region] = &clientCacheV2{
					awsConfig:      regionConfig,
					sts:            regionStsClient,
					cloudwatch:     nil,
					tagging:        optionalClient[*resourcegroupstaggingapi.Client]{necessaryForConfig: jobRequiresTagging},
					asg:            optionalClient[*autoscaling.Client]{necessaryForConfig: requiresMetadataAPIs},
					ec2:            optionalClient[*ec2.Client]{necessaryForConfig: requiresMetadataAPIs},
					dms:            optionalClient[*databasemigrationservice.Client]{necessaryForConfig: requiresMetadataAPIs},
					apiGateway:     optionalClient[*apigateway.Client]{necessaryForConfig: requiresMetadataAPIs},
					storageGateway: optionalClient[*storagegateway.Client]{necessaryForConfig: requiresMetadataAPIs},
					prometheus:     optionalClient[*amp.Client]{necessaryForConfig: requiresMetadataAPIs},
				}
			}
		}
	}

	for _, staticJob := range cfg.Static {
		for _, role := range staticJob.Roles {
			if _, ok := clients[role]; !ok {
				clients[role] = map[awsRegion]*clientCacheV2{}
			}
			for _, region := range staticJob.Regions {
				// Discovery job client definitions have precedence
				if _, exists := clients[role][region]; !exists {
					regionConfig, regionStsClient := awsConfigAndStsForRegion(role, &c, stsClient, region, role)
					clients[role][region] = &clientCacheV2{
						awsConfig:      regionConfig,
						sts:            regionStsClient,
						cloudwatch:     nil,
						tagging:        optionalClient[*resourcegroupstaggingapi.Client]{necessaryForConfig: false},
						asg:            optionalClient[*autoscaling.Client]{necessaryForConfig: false},
						ec2:            optionalClient[*ec2.Client]{necessaryForConfig: false},
						dms:            optionalClient[*databasemigrationservice.Client]{necessaryForConfig: false},
						apiGateway:     optionalClient[*apigateway.Client]{necessaryForConfig: false},
						storageGateway: optionalClient[*storagegateway.Client]{necessaryForConfig: false},
						prometheus:     optionalClient[*amp.Client]{necessaryForConfig: false},
					}
				}
			}
		}
	}

	for _, customNamespaceJob := range cfg.CustomNamespace {
		for _, role := range customNamespaceJob.Roles {
			if _, ok := clients[role]; !ok {
				clients[role] = map[awsRegion]*clientCacheV2{}
			}
			for _, region := range customNamespaceJob.Regions {
				// Discovery job client definitions have precedence
				if _, exists := clients[role][region]; !exists {
					regionConfig, regionStsClient := awsConfigAndStsForRegion(role, &c, stsClient, region, role)
					clients[role][region] = &clientCacheV2{
						awsConfig:      regionConfig,
						sts:            regionStsClient,
						cloudwatch:     nil,
						tagging:        optionalClient[*resourcegroupstaggingapi.Client]{necessaryForConfig: false},
						asg:            optionalClient[*autoscaling.Client]{necessaryForConfig: false},
						ec2:            optionalClient[*ec2.Client]{necessaryForConfig: false},
						dms:            optionalClient[*databasemigrationservice.Client]{necessaryForConfig: false},
						apiGateway:     optionalClient[*apigateway.Client]{necessaryForConfig: false},
						storageGateway: optionalClient[*storagegateway.Client]{necessaryForConfig: false},
						prometheus:     optionalClient[*amp.Client]{necessaryForConfig: false},
					}
				}
			}
		}
	}

	return &awsClientCache{
		sts:           sts.NewFromConfig(c),
		logger:        logger,
		clients:       clients,
		configOptions: options,
	}, nil
}

func (acc *awsClientCache) GetSTS(region string, role config.Role) *sts.Client {
	return acc.clients[role][region].sts
}

func (acc *awsClientCache) GetCloudwatch(region *string, role config.Role) *cloudwatch.Client {
	if !acc.refreshed {
		acc.mu.Lock()
		defer acc.mu.Unlock()
	}

	if client := acc.clients[role][*region].cloudwatch; client != nil {
		return client
	}

	//Double check it wasn't created while waiting for write lock
	if client := acc.clients[role][*region].cloudwatch; client != nil {
		return client
	}
	client := acc.createCloudwatchClient(acc.clients[role][*region].awsConfig)
	acc.clients[role][*region].cloudwatch = client
	return client
}

func (acc *awsClientCache) createCloudwatchClient(regionConfig *aws.Config) *cloudwatch.Client {
	return cloudwatch.NewFromConfig(*regionConfig, func(options *cloudwatch.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}
		options.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
			options.MaxAttempts = 5
			options.MaxBackoff = 3 * time.Second

			// existing settings
			// TODO how to tell the difference between throttle and non-throttle errors now?
			//	NumMaxRetries: 5
			//	//MaxThrottleDelay and MinThrottleDelay used for throttle errors
			//	MaxThrottleDelay: 10 * time.Second
			//	MinThrottleDelay: 1 * time.Second
			//	// For other errors
			//	MaxRetryDelay: 3 * time.Second
			//	MinRetryDelay: 1 * time.Second
		})
	})
}

func (acc *awsClientCache) GetTagging(region *string, role config.Role) *resourcegroupstaggingapi.Client {
	if !acc.refreshed {
		acc.mu.Lock()
		defer acc.mu.Unlock()
	}

	if oc := acc.clients[role][*region].tagging; oc.client != nil {
		return oc.client
	}

	client := acc.createTaggingClient(acc.clients[role][*region].awsConfig)
	acc.clients[role][*region].tagging = optionalClient[*resourcegroupstaggingapi.Client]{true, client}
	return client
}

func (acc *awsClientCache) createTaggingClient(regionConfig *aws.Config) *resourcegroupstaggingapi.Client {
	return resourcegroupstaggingapi.NewFromConfig(*regionConfig, func(options *resourcegroupstaggingapi.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) GetASG(region *string, role config.Role) *autoscaling.Client {
	if !acc.refreshed {
		acc.mu.Lock()
		defer acc.mu.Unlock()
	}

	if oc := acc.clients[role][*region].asg; oc.client != nil {
		return oc.client
	}

	client := acc.createAutoScalingClient(acc.clients[role][*region].awsConfig)
	acc.clients[role][*region].asg = optionalClient[*autoscaling.Client]{true, client}
	return client
}

func (acc *awsClientCache) createAutoScalingClient(assumedConfig *aws.Config) *autoscaling.Client {
	return autoscaling.NewFromConfig(*assumedConfig, func(options *autoscaling.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) GetEC2(region *string, role config.Role) *ec2.Client {
	if !acc.refreshed {
		acc.mu.Lock()
		defer acc.mu.Unlock()
	}

	if oc := acc.clients[role][*region].ec2; oc.client != nil {
		return oc.client
	}

	client := acc.createEC2Client(acc.clients[role][*region].awsConfig)
	acc.clients[role][*region].ec2 = optionalClient[*ec2.Client]{true, client}
	return client
}

func (acc *awsClientCache) createEC2Client(assumedConfig *aws.Config) *ec2.Client {
	return ec2.NewFromConfig(*assumedConfig, func(options *ec2.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) GetDMS(region *string, role config.Role) *databasemigrationservice.Client {
	if !acc.refreshed {
		acc.mu.Lock()
		defer acc.mu.Unlock()
	}

	if oc := acc.clients[role][*region].dms; oc.client != nil {
		return oc.client
	}

	client := acc.createDMSClient(acc.clients[role][*region].awsConfig)
	acc.clients[role][*region].dms = optionalClient[*databasemigrationservice.Client]{true, client}
	return client
}

func (acc *awsClientCache) createDMSClient(assumedConfig *aws.Config) *databasemigrationservice.Client {
	return databasemigrationservice.NewFromConfig(*assumedConfig, func(options *databasemigrationservice.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) GetAPIGateway(region *string, role config.Role) *apigateway.Client {
	if !acc.refreshed {
		acc.mu.Lock()
		defer acc.mu.Unlock()
	}

	if oc := acc.clients[role][*region].apiGateway; oc.client != nil {
		return oc.client
	}

	client := acc.createAPIGatewayClient(acc.clients[role][*region].awsConfig)
	acc.clients[role][*region].apiGateway = optionalClient[*apigateway.Client]{true, client}
	return client
}

func (acc *awsClientCache) createAPIGatewayClient(assumedConfig *aws.Config) *apigateway.Client {
	return apigateway.NewFromConfig(*assumedConfig, func(options *apigateway.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) GetStorageGateway(region *string, role config.Role) *storagegateway.Client {
	if !acc.refreshed {
		acc.mu.Lock()
		defer acc.mu.Unlock()
	}

	if oc := acc.clients[role][*region].storageGateway; oc.client != nil {
		return oc.client
	}

	client := acc.createStorageGatewayClient(acc.clients[role][*region].awsConfig)
	acc.clients[role][*region].storageGateway = optionalClient[*storagegateway.Client]{true, client}
	return client
}

func (acc *awsClientCache) createStorageGatewayClient(assumedConfig *aws.Config) *storagegateway.Client {
	return storagegateway.NewFromConfig(*assumedConfig, func(options *storagegateway.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) GetPrometheus(region *string, role config.Role) *amp.Client {
	if !acc.refreshed {
		acc.mu.Lock()
		defer acc.mu.Unlock()
	}

	if oc := acc.clients[role][*region].prometheus; oc.client != nil {
		return oc.client
	}

	client := acc.createPrometheusClient(acc.clients[role][*region].awsConfig)
	acc.clients[role][*region].prometheus = optionalClient[*amp.Client]{true, client}
	return client
}

func (acc *awsClientCache) createPrometheusClient(assumedConfig *aws.Config) *amp.Client {
	return amp.NewFromConfig(*assumedConfig, func(options *amp.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) Refresh() {
	if acc.refreshed {
		return
	}
	acc.mu.Lock()
	defer acc.mu.Unlock()
	// Avoid double refresh in the event Refresh() is called concurrently
	if acc.refreshed {
		return
	}

	for _, regionClients := range acc.clients {
		for _, cache := range regionClients {
			cache.cloudwatch = acc.createCloudwatchClient(cache.awsConfig)

			if cache.tagging.necessaryForConfig {
				cache.tagging.client = acc.createTaggingClient(cache.awsConfig)
			}

			if cache.asg.necessaryForConfig {
				cache.asg.client = acc.createAutoScalingClient(cache.awsConfig)
			}

			if cache.ec2.necessaryForConfig {
				cache.ec2.client = acc.createEC2Client(cache.awsConfig)
			}

			if cache.dms.necessaryForConfig {
				cache.dms.client = acc.createDMSClient(cache.awsConfig)
			}

			if cache.apiGateway.necessaryForConfig {
				cache.apiGateway.client = acc.createAPIGatewayClient(cache.awsConfig)
			}

			if cache.storageGateway.necessaryForConfig {
				cache.storageGateway.client = acc.createStorageGatewayClient(cache.awsConfig)
			}

			if cache.prometheus.necessaryForConfig {
				cache.prometheus.client = acc.createPrometheusClient(cache.awsConfig)
			}
		}
	}

	acc.refreshed = true
	acc.cleared = false
}

func (acc *awsClientCache) Clear() {
	if acc.cleared {
		return
	}
	//Prevent concurrent reads/write if clear is called during execution
	acc.mu.Lock()
	defer acc.mu.Unlock()
	// Avoid double clear in the event Refresh() is called concurrently
	if acc.cleared {
		return
	}

	for _, regions := range acc.clients {
		for _, cache := range regions {
			cache.cloudwatch = nil
			cache.tagging.client = nil
			cache.asg.client = nil
			cache.ec2.client = nil
			cache.dms.client = nil
			cache.apiGateway.client = nil
			cache.storageGateway.client = nil
			cache.prometheus.client = nil
		}

	}

	acc.refreshed = false
	acc.cleared = true
}

var defaultRole = config.Role{}

func awsConfigAndStsForRegion(r config.Role, c *aws.Config, stsClient *sts.Client, region awsRegion, role config.Role) (*aws.Config, *sts.Client) {
	if r == defaultRole {
		//We are not using delegated access so return the original config
		return c, stsClient
	}

	//TODO is this actually safe?
	delegatedConfig := c.Copy()
	delegatedConfig.Region = region

	// based on https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/credentials/stscreds#hdr-Assume_Role
	// found via https://github.com/aws/aws-sdk-go-v2/issues/1382
	credentials := stscreds.NewAssumeRoleProvider(stsClient, role.RoleArn, func(options *stscreds.AssumeRoleOptions) {
		if role.ExternalID != "" {
			options.ExternalID = aws.String(role.ExternalID)
		}
	})
	delegatedConfig.Credentials = aws.NewCredentialsCache(credentials)

	delegatedStsClient := sts.NewFromConfig(delegatedConfig, func(options *sts.Options) {
		options.RetryMaxAttempts = 5
	})

	return &delegatedConfig, delegatedStsClient
}
