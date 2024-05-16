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

func (p Processor) Run(ctx context.Context, region string, job model.DiscoveryJob) ([]*model.TaggedResource, error) {
	resources, err := p.client.GetResources(ctx, job, region)
	if err != nil {
		if errors.Is(err, tagging.ErrExpectedToFindResources) {
			return nil, fmt.Errorf("no tagged resources made it through filtering: %w", err)
		}
		return nil, fmt.Errorf("failed to describe resources: %w", err)
	}

	return resources, nil
}
