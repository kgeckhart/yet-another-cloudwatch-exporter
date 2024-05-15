package resourcemetadata

import (
	"context"
	"errors"
	"fmt"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/clients/tagging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/logging"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type Processor struct {
	client tagging.Client
	logger logging.Logger
}

func NewProcessor(logger logging.Logger, client tagging.Client) *Processor {
	return &Processor{
		logger: logger,
		client: client,
	}
}

func (p Processor) Run(ctx context.Context, region string, account string, job model.DiscoveryJob) (*model.TaggedResourceResult, error) {
	resources, err := p.client.GetResources(ctx, job, region)
	if err != nil {
		if errors.Is(err, tagging.ErrExpectedToFindResources) {
			return nil, fmt.Errorf("no tagged resources made it through filtering: %w", err)
		}
		return nil, fmt.Errorf("failed to describe resources: %w", err)
	}

	if len(resources) == 0 {
		p.logger.Debug("No tagged resources", "region", region, "namespace", job.Type)
	}

	result := &model.TaggedResourceResult{
		Context: &model.ScrapeContext{
			Region:     region,
			AccountID:  account,
			CustomTags: job.CustomTags,
		},
		Data: resources,
	}

	return result, nil
}
