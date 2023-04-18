package config

import (
	"github.com/grafana/regexp"
)

type ServiceConfig struct {
	Namespace        string
	Alias            string
	ResourceFilters  []string
	DimensionRegexps []*regexp.Regexp
}

type serviceConfigs []ServiceConfig

func (sc serviceConfigs) GetService(serviceType string) *ServiceConfig {
	for _, sf := range sc {
		if sf.Alias == serviceType || sf.Namespace == serviceType {
			return &sf
		}
	}
	return nil
}

var SupportedServices = serviceConfigs{
	{
		Namespace: "AWS/CertificateManager",
		Alias:     "acm",
		ResourceFilters: []string{
			"acm:certificate",
		},
	},
	{
		Namespace: "AWS/ACMPrivateCA",
		Alias:     "acm-pca",
		ResourceFilters: []string{
			"acm-pca:certificate-authority",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("(?P<PrivateCAArn>.*)"),
		},
	},
	{
		Namespace: "AmazonMWAA",
		Alias:     "airflow",
		ResourceFilters: []string{
			"airflow",
		},
	},
	{
		Namespace: "AWS/MWAA",
		Alias:     "mwaa",
		ResourceFilters: []string{
			"mwaa",
		},
	},
	{
		Namespace: "AWS/ApplicationELB",
		Alias:     "alb",
		ResourceFilters: []string{
			"elasticloadbalancing:loadbalancer/app",
			"elasticloadbalancing:targetgroup",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":(?P<TargetGroup>targetgroup/.+)"),
			regexp.MustCompile(":loadbalancer/(?P<LoadBalancer>.+)$"),
		},
	},
	{
		Namespace: "AWS/AppStream",
		Alias:     "appstream",
		ResourceFilters: []string{
			"appstream",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":fleet/(?P<FleetName>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/Backup",
		Alias:     "backup",
		ResourceFilters: []string{
			"backup",
		},
	},
	{
		Namespace: "AWS/ApiGateway",
		Alias:     "apigateway",
		ResourceFilters: []string{
			"apigateway",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("apis/(?P<ApiName>[^/]+)$"),
			regexp.MustCompile("apis/(?P<ApiName>[^/]+)/stages/(?P<Stage>[^/]+)$"),
		},
	},
	{
		Namespace: "AWS/AmazonMQ",
		Alias:     "mq",
		ResourceFilters: []string{
			"mq",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("broker:(?P<Broker>[^:]+)"),
		},
	},
	{
		Namespace: "AWS/AppSync",
		Alias:     "appsync",
		ResourceFilters: []string{
			"appsync",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("apis/(?P<GraphQLAPIId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/Athena",
		Alias:     "athena",
		ResourceFilters: []string{
			"athena",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("workgroup/(?P<WorkGroup>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/AutoScaling",
		Alias:     "asg",
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("autoScalingGroupName/(?P<AutoScalingGroupName>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/ElasticBeanstalk",
		Alias:     "beanstalk",
		ResourceFilters: []string{
			"elasticbeanstalk:environment",
		},
	},
	{
		Namespace: "AWS/Billing",
		Alias:     "billing",
	},
	{
		Namespace: "AWS/Cassandra",
		Alias:     "cassandra",
		ResourceFilters: []string{
			"cassandra",
		},
	},
	{
		Namespace: "AWS/CloudFront",
		Alias:     "cloudfront",
		ResourceFilters: []string{
			"cloudfront:distribution",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("distribution/(?P<DistributionId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/Cognito",
		Alias:     "cognito-idp",
		ResourceFilters: []string{
			"cognito-idp:userpool",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("userpool/(?P<UserPool>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/DMS",
		Alias:     "dms",
		ResourceFilters: []string{
			"dms",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("rep:[^/]+/(?P<ReplicationInstanceIdentifier>[^/]+)"),
			regexp.MustCompile("task:(?P<ReplicationTaskIdentifier>[^/]+)/(?P<ReplicationInstanceIdentifier>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/DDoSProtection",
		Alias:     "shield",
		ResourceFilters: []string{
			"shield:protection",
		},
	},
	{
		Namespace: "AWS/DocDB",
		Alias:     "docdb",
		ResourceFilters: []string{
			"rds:db",
			"rds:cluster",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("cluster:(?P<DBClusterIdentifier>[^/]+)"),
			regexp.MustCompile("db:(?P<DBInstanceIdentifier>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/DX",
		Alias:     "dx",
		ResourceFilters: []string{
			"directconnect",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":dxcon/(?P<ConnectionId>[^/]+)"),
			regexp.MustCompile(":dxlag/(?P<LagId>[^/]+)"),
			regexp.MustCompile(":dxvif/(?P<VirtualInterfaceId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/DynamoDB",
		Alias:     "dynamodb",
		ResourceFilters: []string{
			"dynamodb:table",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":table/(?P<TableName>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/EBS",
		Alias:     "ebs",
		ResourceFilters: []string{
			"ec2:volume",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("volume/(?P<VolumeId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/ElastiCache",
		Alias:     "ec",
		ResourceFilters: []string{
			"elasticache:cluster",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("cluster:(?P<CacheClusterId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/EC2",
		Alias:     "ec2",
		ResourceFilters: []string{
			"ec2:instance",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("instance/(?P<InstanceId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/EC2Spot",
		Alias:     "ec2Spot",
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("(?P<FleetRequestId>.*)"),
		},
	},
	{
		Namespace: "AWS/ECS",
		Alias:     "ecs-svc",
		ResourceFilters: []string{
			"ecs:cluster",
			"ecs:service",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("cluster/(?P<ClusterName>[^/]+)"),
			regexp.MustCompile("service/(?P<ClusterName>[^/]+)/(?P<ServiceName>[^/]+)"),
		},
	},
	{
		Namespace: "ECS/ContainerInsights",
		Alias:     "ecs-containerinsights",
		ResourceFilters: []string{
			"ecs:cluster",
			"ecs:service",
		},
		DimensionRegexps: []*regexp.Regexp{
			// Use "new" long arns as per
			// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-account-settings.html#ecs-resource-ids
			regexp.MustCompile("cluster/(?P<ClusterName>[^/]+)"),
			regexp.MustCompile("service/(?P<ClusterName>[^/]+)/(?P<ServiceName>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/EFS",
		Alias:     "efs",
		ResourceFilters: []string{
			"elasticfilesystem:file-system",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("file-system/(?P<FileSystemId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/ELB",
		Alias:     "elb",
		ResourceFilters: []string{
			"elasticloadbalancing:loadbalancer",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":loadbalancer/(?P<LoadBalancerName>.+)$"),
		},
	},
	{
		Namespace: "AWS/ElasticMapReduce",
		Alias:     "emr",
		ResourceFilters: []string{
			"elasticmapreduce:cluster",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("cluster/(?P<JobFlowId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/EMRServerless",
		Alias:     "emr-serverless",
		ResourceFilters: []string{
			"emr-serverless:applications",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("applications/(?P<ApplicationId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/ES",
		Alias:     "es",
		ResourceFilters: []string{
			"es:domain",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":domain/(?P<DomainName>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/Firehose",
		Alias:     "firehose",
		ResourceFilters: []string{
			"firehose",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":deliverystream/(?P<DeliveryStreamName>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/FSx",
		Alias:     "fsx",
		ResourceFilters: []string{
			"fsx:file-system",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("file-system/(?P<FileSystemId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/GameLift",
		Alias:     "gamelift",
		ResourceFilters: []string{
			"gamelift",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":fleet/(?P<FleetId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/GlobalAccelerator",
		Alias:     "ga",
		ResourceFilters: []string{
			"globalaccelerator",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("destinationEdge/(?P<DestinationEdge>[^/]+)"),
			regexp.MustCompile("accelerator/(?P<Accelerator>[^/]+)"),
			regexp.MustCompile("endpointGroup/(?P<EndpointGroup>[^/]+)"),
			regexp.MustCompile("listener/(?P<Listener>[^/]+)"),
			regexp.MustCompile("transportProtocol/(?P<TransportProtocol>[^/]+)"),
		},
	},
	{
		Namespace: "Glue",
		Alias:     "glue",
		ResourceFilters: []string{
			"glue:job",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":job/(?P<JobName>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/IoT",
		Alias:     "iot",
		ResourceFilters: []string{
			"iot:rule",
			"iot:provisioningtemplate",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":rule/(?P<RuleName>[^/]+)"),
			regexp.MustCompile(":provisioningtemplate/(?P<TemplateName>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/Kafka",
		Alias:     "kafka",
		ResourceFilters: []string{
			"kafka:cluster",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":cluster/(?P<Cluster_Name>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/KafkaConnect",
		Alias:     "kafkaconnect",
		ResourceFilters: []string{
			"kafkaconnect",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":connector/(?P<Connector_Name>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/Kinesis",
		Alias:     "kinesis",
		ResourceFilters: []string{
			"kinesis:stream",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":stream/(?P<StreamName>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/KinesisAnalytics",
		Alias:     "kinesis-analytics",
	},
	{
		Namespace: "AWS/Lambda",
		Alias:     "lambda",
		ResourceFilters: []string{
			"lambda:function",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":function:(?P<FunctionName>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/MediaConnect",
		Alias:     "mediaconnect",
		ResourceFilters: []string{
			"mediaconnect:flow",
			"mediaconnect:source",
			"mediaconnect:output",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("^(?P<FlowARN>.*:flow:.*)$"),
			regexp.MustCompile("^(?P<SourceARN>.*:source:.*)$"),
			regexp.MustCompile("^(?P<OutputARN>.*:output:.*)$"),
		},
	},
	{
		Namespace: "AWS/MediaLive",
		Alias:     "medialive",
		ResourceFilters: []string{
			"medialive:channel",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":channel:(?P<ChannelId>.+)$"),
		},
	},
	{
		Namespace: "AWS/MediaTailor",
		Alias:     "mediatailor",
		ResourceFilters: []string{
			"mediatailor:playbackConfiguration",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("playbackConfiguration/(?P<ConfigurationName>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/Neptune",
		Alias:     "neptune",
		ResourceFilters: []string{
			"rds:db",
			"rds:cluster",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":cluster:(?P<DBClusterIdentifier>[^/]+)"),
			regexp.MustCompile(":db:(?P<DBInstanceIdentifier>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/NetworkFirewall",
		Alias:     "nfw",
		ResourceFilters: []string{
			"network-firewall:firewall",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("firewall/(?P<FirewallName>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/NATGateway",
		Alias:     "ngw",
		ResourceFilters: []string{
			"ec2:natgateway",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("natgateway/(?P<NatGatewayId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/NetworkELB",
		Alias:     "nlb",
		ResourceFilters: []string{
			"elasticloadbalancing:loadbalancer/net",
			"elasticloadbalancing:targetgroup",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":(?P<TargetGroup>targetgroup/.+)"),
			regexp.MustCompile(":loadbalancer/(?P<LoadBalancer>.+)$"),
		},
	},
	{
		Namespace: "AWS/PrivateLinkEndpoints",
		Alias:     "vpc-endpoint",
		ResourceFilters: []string{
			"ec2:vpc-endpoint",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":vpc-endpoint/(?P<VPC_Endpoint_Id>.+)"),
		},
	},
	{
		Namespace: "AWS/PrivateLinkServices",
		Alias:     "vpc-endpoint-service",
		ResourceFilters: []string{
			"ec2:vpc-endpoint-service",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":vpc-endpoint-service:(?P<Service_Id>.+)"),
		},
	},
	{
		Namespace: "AWS/Prometheus",
		Alias:     "amp",
	},
	{
		Namespace: "AWS/RDS",
		Alias:     "rds",
		ResourceFilters: []string{
			"rds:db",
			"rds:cluster",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":cluster:(?P<DBClusterIdentifier>[^/]+)"),
			regexp.MustCompile(":db:(?P<DBInstanceIdentifier>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/Redshift",
		Alias:     "redshift",
		ResourceFilters: []string{
			"redshift:cluster",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":cluster:(?P<ClusterIdentifier>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/Route53Resolver",
		Alias:     "route53-resolver",
		ResourceFilters: []string{
			"route53resolver",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":resolver-endpoint/(?P<EndpointId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/Route53",
		Alias:     "route53",
		ResourceFilters: []string{
			"route53",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":healthcheck/(?P<HealthCheckId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/S3",
		Alias:     "s3",
		ResourceFilters: []string{
			"s3",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("(?P<BucketName>[^:]+)$"),
		},
	},
	{
		Namespace: "AWS/SES",
		Alias:     "ses",
	},
	{
		Namespace: "AWS/States",
		Alias:     "sfn",
		ResourceFilters: []string{
			"states",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("(?P<StateMachineArn>.*)"),
		},
	},
	{
		Namespace: "AWS/SNS",
		Alias:     "sns",
		ResourceFilters: []string{
			"sns",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("(?P<TopicName>[^:]+)$"),
		},
	},
	{
		Namespace: "AWS/SQS",
		Alias:     "sqs",
		ResourceFilters: []string{
			"sqs",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("(?P<QueueName>[^:]+)$"),
		},
	},
	{
		Namespace: "AWS/StorageGateway",
		Alias:     "storagegateway",
		ResourceFilters: []string{
			"storagegateway",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":gateway/(?P<GatewayId>[^:]+)$"),
			regexp.MustCompile(":share/(?P<ShareId>[^:]+)$"),
			regexp.MustCompile("^(?P<GatewayId>[^:/]+)/(?P<GatewayName>[^:]+)$"),
		},
	},
	{
		Namespace: "AWS/TransitGateway",
		Alias:     "tgw",
		ResourceFilters: []string{
			"ec2:transit-gateway",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":transit-gateway/(?P<TransitGateway>[^/]+)"),
			regexp.MustCompile("(?P<TransitGateway>[^/]+)/(?P<TransitGatewayAttachment>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/VPN",
		Alias:     "vpn",
		ResourceFilters: []string{
			"ec2:vpn-connection",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":vpn-connection/(?P<VpnId>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/WAFV2",
		Alias:     "wafv2",
		ResourceFilters: []string{
			"wafv2",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile("/webacl/(?P<WebACL>[^/]+)"),
		},
	},
	{
		Namespace: "AWS/WorkSpaces",
		Alias:     "workspaces",
		ResourceFilters: []string{
			"workspaces:workspace",
			"workspaces:directory",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":workspace/(?P<WorkspaceId>[^/]+)$"),
			regexp.MustCompile(":directory/(?P<DirectoryId>[^/]+)$"),
		},
	},
	{
		Namespace: "AWS/AOSS",
		Alias:     "aoss",
		ResourceFilters: []string{
			"aoss:collection",
		},
		DimensionRegexps: []*regexp.Regexp{
			regexp.MustCompile(":collection/(?P<CollectionId>[^/]+)"),
		},
	},
}
