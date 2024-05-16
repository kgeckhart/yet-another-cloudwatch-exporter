package cloudwatchrunner

import (
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/listmetrics"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/job/resourcemetadata"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

type Discovery struct {
	Job       model.DiscoveryJob
	Resources []*model.TaggedResource
}

func (d Discovery) Namespace() string {
	return d.Job.Type
}

func (d Discovery) CustomTags() []model.Tag {
	return d.Job.CustomTags
}

func (d Discovery) listMetricsParams() listmetrics.ProcessingParams {
	return listmetrics.ProcessingParams{
		Namespace:                 d.Job.Type,
		Metrics:                   d.Job.Metrics,
		RecentlyActiveOnly:        d.Job.RecentlyActiveOnly,
		DimensionNameRequirements: d.Job.DimensionNameRequirements,
	}
}

func (d Discovery) resourceEnrichment() ResourceEnrichment {
	return resourcemetadata.NewResourceAssociation(d.Job.DimensionsRegexps, d.Job.ExportedTagsOnMetrics, nil)
}
