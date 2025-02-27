package maxdimassociator

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/grafana/regexp"
	"github.com/stretchr/testify/require"

	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/nerdswords/yet-another-cloudwatch-exporter/pkg/model"
)

var ec2Instance1 = &model.TaggedResource{
	ARN:       "arn:aws:ec2:us-east-1:123456789012:instance/i-abc123",
	Namespace: "AWS/EC2",
}

var ec2Instance2 = &model.TaggedResource{
	ARN:       "arn:aws:ec2:us-east-1:123456789012:instance/i-def456",
	Namespace: "AWS/EC2",
}

var ec2Resources = []*model.TaggedResource{
	ec2Instance1,
	ec2Instance2,
}

func TestAssociatorEC2(t *testing.T) {
	type args struct {
		dimensionRegexps []*regexp.Regexp
		resources        []*model.TaggedResource
		metric           *cloudwatch.Metric
	}

	type testCase struct {
		name             string
		args             args
		expectedSkip     bool
		expectedResource *model.TaggedResource
	}

	testcases := []testCase{
		{
			name: "should match with InstanceId dimension",
			args: args{
				dimensionRegexps: config.SupportedServices.GetService("AWS/EC2").DimensionRegexps,
				resources:        ec2Resources,
				metric: &cloudwatch.Metric{
					Namespace:  aws.String("AWS/EC2"),
					MetricName: aws.String("CPUUtilization"),
					Dimensions: []*cloudwatch.Dimension{
						{Name: aws.String("InstanceId"), Value: aws.String("i-abc123")},
					},
				},
			},
			expectedSkip:     false,
			expectedResource: ec2Instance1,
		},
		{
			name: "should match another instance with InstanceId dimension",
			args: args{
				dimensionRegexps: config.SupportedServices.GetService("AWS/EC2").DimensionRegexps,
				resources:        ec2Resources,
				metric: &cloudwatch.Metric{
					Namespace:  aws.String("AWS/EC2"),
					MetricName: aws.String("CPUUtilization"),
					Dimensions: []*cloudwatch.Dimension{
						{Name: aws.String("InstanceId"), Value: aws.String("i-def456")},
					},
				},
			},
			expectedSkip:     false,
			expectedResource: ec2Instance2,
		},
		{
			name: "should skip with unmatched InstanceId dimension",
			args: args{
				dimensionRegexps: config.SupportedServices.GetService("AWS/EC2").DimensionRegexps,
				resources:        ec2Resources,
				metric: &cloudwatch.Metric{
					Namespace:  aws.String("AWS/EC2"),
					MetricName: aws.String("CPUUtilization"),
					Dimensions: []*cloudwatch.Dimension{
						{Name: aws.String("InstanceId"), Value: aws.String("i-blahblah")},
					},
				},
			},
			expectedSkip:     true,
			expectedResource: nil,
		},
		{
			name: "should not skip when unmatching because of non-ARN dimension",
			args: args{
				dimensionRegexps: config.SupportedServices.GetService("AWS/EC2").DimensionRegexps,
				resources:        ec2Resources,
				metric: &cloudwatch.Metric{
					Namespace:  aws.String("AWS/EC2"),
					MetricName: aws.String("StatusCheckFailed_System"),
					Dimensions: []*cloudwatch.Dimension{
						{Name: aws.String("AutoScalingGroupName"), Value: aws.String("some-asg-name")},
					},
				},
			},
			expectedSkip:     false,
			expectedResource: nil,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			associator := NewAssociator(tc.args.dimensionRegexps, tc.args.resources)
			res, skip := associator.AssociateMetricToResource(tc.args.metric)
			require.Equal(t, tc.expectedSkip, skip)
			require.Equal(t, tc.expectedResource, res)
		})
	}
}
