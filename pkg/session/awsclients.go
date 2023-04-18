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

type awsClientCache struct {
	sts                    *sts.Client
	logger                 logging.Logger
	configOptions          []func(*aws_config.LoadOptions) error
	assumedRoleConfigCache map[config.Role]map[string]*aws.Config
	mu                     sync.RWMutex
}

func NewAwsClientCache(cfg config.ScrapeConf, fips bool, logger logging.Logger) (AWSClientCache, error) {
	var options []func(*aws_config.LoadOptions) error
	options = append(options, aws_config.WithLogger(aws_logging.LoggerFunc(func(classification aws_logging.Classification, format string, v ...interface{}) {
		if classification == aws_logging.Debug && logger.IsDebugEnabled() {
			logger.Debug(fmt.Sprintf(format, v...))
		} else if classification == aws_logging.Warn {
			logger.Warn(fmt.Sprintf(format, v...))
		} else { // AWS logging only support debug or warn log everything else as error with an error just in case
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

	assumedRoleConfigs := map[config.Role]map[string]*aws.Config{}
	emptyRole := config.Role{}
	for _, discoveryJob := range cfg.Discovery.Jobs {
		for _, role := range discoveryJob.Roles {
			if _, ok := assumedRoleConfigs[role]; !ok {
				assumedRoleConfigs[role] = map[string]*aws.Config{}
			}
			for _, region := range discoveryJob.Regions {
				// Nothing to assume so original config is fine
				if role == emptyRole {
					assumedRoleConfigs[role][region] = &c
				} else {
					assumedRoleConfigs[role][region] = nil
				}
			}
		}
	}

	for _, staticJob := range cfg.Static {
		for _, role := range staticJob.Roles {
			if _, ok := assumedRoleConfigs[role]; !ok {
				assumedRoleConfigs[role] = map[string]*aws.Config{}
			}
			for _, region := range staticJob.Regions {
				// Nothing to assume so original config is fine
				if role == emptyRole {
					assumedRoleConfigs[role][region] = &c
				} else {
					assumedRoleConfigs[role][region] = nil
				}
			}
		}
	}

	for _, customNamespaceJob := range cfg.CustomNamespace {
		for _, role := range customNamespaceJob.Roles {
			if _, ok := assumedRoleConfigs[role]; !ok {
				assumedRoleConfigs[role] = map[string]*aws.Config{}
			}
			for _, region := range customNamespaceJob.Regions {
				// Nothing to assume so original config is fine
				if role == emptyRole {
					assumedRoleConfigs[role][region] = &c
				} else {
					assumedRoleConfigs[role][region] = nil
				}
			}
		}
	}

	return &awsClientCache{
		sts:                    sts.NewFromConfig(c),
		logger:                 logger,
		assumedRoleConfigCache: assumedRoleConfigs,
		configOptions:          options,
	}, nil
}

func (acc *awsClientCache) GetSTS(region string, role config.Role) *sts.Client {
	assumedConfig, err := acc.getAssumedConfig(region, role)
	if err != nil {
		// TODO this is bad
		acc.logger.Error(err, "failed to create assumed config")
		return nil
	}

	return sts.NewFromConfig(assumedConfig, func(options *sts.Options) {
		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) GetCloudwatch(region *string, role config.Role) *cloudwatch.Client {
	assumedConfig, err := acc.getAssumedConfig(*region, role)
	if err != nil {
		// TODO this is bad
		acc.logger.Error(err, "failed to create assumed config")
		return nil
	}

	return cloudwatch.NewFromConfig(assumedConfig, func(options *cloudwatch.Options) {
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
	assumedConfig, err := acc.getAssumedConfig(*region, role)
	if err != nil {
		// TODO this is bad
		acc.logger.Error(err, "failed to create assumed config")
		return nil
	}

	return resourcegroupstaggingapi.NewFromConfig(assumedConfig, func(options *resourcegroupstaggingapi.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) GetASG(region *string, role config.Role) *autoscaling.Client {
	assumedConfig, err := acc.getAssumedConfig(*region, role)
	if err != nil {
		// TODO this is bad
		acc.logger.Error(err, "failed to create assumed config")
		return nil
	}

	return autoscaling.NewFromConfig(assumedConfig, func(options *autoscaling.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) GetEC2(region *string, role config.Role) *ec2.Client {
	assumedConfig, err := acc.getAssumedConfig(*region, role)
	if err != nil {
		// TODO this is bad
		acc.logger.Error(err, "failed to create assumed config")
		return nil
	}

	return ec2.NewFromConfig(assumedConfig, func(options *ec2.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) GetDMS(region *string, role config.Role) *databasemigrationservice.Client {
	assumedConfig, err := acc.getAssumedConfig(*region, role)
	if err != nil {
		// TODO this is bad
		acc.logger.Error(err, "failed to create assumed config")
		return nil
	}

	return databasemigrationservice.NewFromConfig(assumedConfig, func(options *databasemigrationservice.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) GetAPIGateway(region *string, role config.Role) *apigateway.Client {
	assumedConfig, err := acc.getAssumedConfig(*region, role)
	if err != nil {
		// TODO this is bad
		acc.logger.Error(err, "failed to create assumed config")
		return nil
	}

	return apigateway.NewFromConfig(assumedConfig, func(options *apigateway.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) GetStorageGateway(region *string, role config.Role) *storagegateway.Client {
	assumedConfig, err := acc.getAssumedConfig(*region, role)
	if err != nil {
		// TODO this is bad
		acc.logger.Error(err, "failed to create assumed config")
		return nil
	}

	return storagegateway.NewFromConfig(assumedConfig, func(options *storagegateway.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) GetPrometheus(region *string, role config.Role) *amp.Client {
	assumedConfig, err := acc.getAssumedConfig(*region, role)
	if err != nil {
		// TODO this is bad
		acc.logger.Error(err, "failed to create assumed config")
		return nil
	}

	return amp.NewFromConfig(assumedConfig, func(options *amp.Options) {
		if acc.logger.IsDebugEnabled() {
			options.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
		}

		options.RetryMaxAttempts = 5
	})
}

func (acc *awsClientCache) Refresh() {
	// TODO actually build a client cache
}

func (acc *awsClientCache) Clear() {
	// TODO actually clear a client cache
}

func (acc *awsClientCache) getAssumedConfig(region string, role config.Role) (aws.Config, error) {
	acc.mu.RLock()
	assumedConfig := acc.assumedRoleConfigCache[role][region]
	acc.mu.RUnlock()
	if assumedConfig == nil {
		acc.mu.Lock()

		var err error
		assumedConfig, err = acc.awsConfigWithRoleDelegationProvider(region, role)
		if err != nil {
			return aws.Config{}, err
		}
		acc.assumedRoleConfigCache[role][region] = assumedConfig

		acc.mu.Unlock()
	}
	return *assumedConfig, nil
}

func (acc *awsClientCache) awsConfigWithRoleDelegationProvider(region string, role config.Role) (*aws.Config, error) {
	// based on https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/credentials/stscreds#hdr-Assume_Role
	// found via https://github.com/aws/aws-sdk-go-v2/issues/1382
	credentials := stscreds.NewAssumeRoleProvider(acc.sts, role.RoleArn, func(options *stscreds.AssumeRoleOptions) {
		if role.ExternalID != "" {
			options.ExternalID = aws.String(role.ExternalID)
		}
	})

	cfg, err := aws_config.LoadDefaultConfig(context.TODO(),
		aws_config.WithRegion(region),
		aws_config.WithCredentialsProvider(aws.NewCredentialsCache(credentials)),
		// TODO why doesn't this work?
		/* acc.configOptions...*/)

	return &cfg, err
}
