package apitagging

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	"github.com/aws/aws-sdk-go-v2/service/storagegateway"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/promutil"
)

type TaggingClient interface {
	GetResources(ctx context.Context, job *config.Job, region string) ([]*model.TaggedResource, error)
}

var _ TaggingClient = (*Client)(nil)

var ErrExpectedToFindResources = errors.New("expected to discover resources but none were found")

type Client struct {
	logger            logging.Logger
	taggingAPI        *resourcegroupstaggingapi.Client
	autoscalingAPI    *autoscaling.Client
	apiGatewayAPI     *apigateway.Client
	ec2API            *ec2.Client
	dmsAPI            *databasemigrationservice.Client
	prometheusSvcAPI  *amp.Client
	storageGatewayAPI *storagegateway.Client
}

func NewClient(
	logger logging.Logger,
	taggingAPI *resourcegroupstaggingapi.Client,
	autoscalingAPI *autoscaling.Client,
	apiGatewayAPI *apigateway.Client,
	ec2API *ec2.Client,
	dmsClient *databasemigrationservice.Client,
	prometheusClient *amp.Client,
	storageGatewayAPI *storagegateway.Client,
) *Client {
	return &Client{
		logger:            logger,
		taggingAPI:        taggingAPI,
		autoscalingAPI:    autoscalingAPI,
		apiGatewayAPI:     apiGatewayAPI,
		ec2API:            ec2API,
		dmsAPI:            dmsClient,
		prometheusSvcAPI:  prometheusClient,
		storageGatewayAPI: storageGatewayAPI,
	}
}

func (c Client) GetResources(ctx context.Context, job *config.Job, region string) ([]*model.TaggedResource, error) {
	svc := config.SupportedServices.GetService(job.Type)
	var resources []*model.TaggedResource
	shouldHaveDiscoveredResources := false

	if len(svc.ResourceFilters) > 0 {
		shouldHaveDiscoveredResources = true
		inputparams := &resourcegroupstaggingapi.GetResourcesInput{
			ResourceTypeFilters: svc.ResourceFilters,
			ResourcesPerPage:    aws.Int32(int32(100)), // max allowed value according to API docs
		}

		paginator := resourcegroupstaggingapi.NewGetResourcesPaginator(c.taggingAPI, inputparams)
		for paginator.HasMorePages() {
			promutil.ResourceGroupTaggingAPICounter.Inc()
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, err
			}

			for _, resourceTagMapping := range page.ResourceTagMappingList {
				resource := model.TaggedResource{
					ARN:       *resourceTagMapping.ResourceARN,
					Namespace: job.Type,
					Region:    region,
					Tags:      make([]model.Tag, 0, len(resourceTagMapping.Tags)),
				}

				for _, t := range resourceTagMapping.Tags {
					resource.Tags = append(resource.Tags, model.Tag{Key: *t.Key, Value: *t.Value})
				}

				if resource.FilterThroughTags(job.SearchTags) {
					resources = append(resources, &resource)
				} else {
					c.logger.Debug("Skipping resource because search tags do not match", "arn", resource.ARN)
				}
			}
		}

		c.logger.Debug("GetResourcesPages finished", "total", len(resources))
	}

	if ext, ok := serviceFilters[svc.Namespace]; ok {
		if ext.ResourceFunc != nil {
			shouldHaveDiscoveredResources = true
			newResources, err := ext.ResourceFunc(ctx, c, job, region)
			if err != nil {
				return nil, fmt.Errorf("failed to apply ResourceFunc for %s, %w", svc.Namespace, err)
			}
			resources = append(resources, newResources...)
			c.logger.Debug("ResourceFunc finished", "total", len(resources))
		}

		if ext.FilterFunc != nil {
			filteredResources, err := ext.FilterFunc(ctx, c, resources)
			if err != nil {
				return nil, fmt.Errorf("failed to apply FilterFunc for %s, %w", svc.Namespace, err)
			}
			resources = filteredResources
			c.logger.Debug("FilterFunc finished", "total", len(resources))
		}
	}

	if shouldHaveDiscoveredResources && len(resources) == 0 {
		return nil, ErrExpectedToFindResources
	}

	return resources, nil
}

var _ TaggingClient = (*LimitedConcurrencyClient)(nil)

type LimitedConcurrencyClient struct {
	client *Client
	sem    chan struct{}
}

func NewLimitedConcurrencyClient(client *Client, maxConcurrency int) *LimitedConcurrencyClient {
	return &LimitedConcurrencyClient{
		client: client,
		sem:    make(chan struct{}, maxConcurrency),
	}
}

func (c LimitedConcurrencyClient) GetResources(ctx context.Context, job *config.Job, region string) ([]*model.TaggedResource, error) {
	c.sem <- struct{}{}
	res, err := c.client.GetResources(ctx, job, region)
	<-c.sem
	return res, err
}
