package main

import (
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/ec2"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/msk"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

func createMskCluster(ctx *pulumi.Context, vpc *ec2.Vpc, subnet *ec2.Subnet, tags pulumi.StringMap) (*msk.Cluster, error) {
	cluster, err := msk.NewCluster(ctx, "KafkaCluster", &msk.ClusterArgs{
		BrokerNodeGroupInfo: &msk.ClusterBrokerNodeGroupInfoArgs{
			InstanceType:   pulumi.String("kafka.m5.large"),
			ClientSubnets:  pulumi.StringArray{subnet.ID()},
			SecurityGroups: pulumi.StringArray{}, // Add SG if needed
		},
		KafkaVersion:        pulumi.String("2.8.1"),
		NumberOfBrokerNodes: pulumi.Int(3),
		Tags:                tags,
	})
	if err != nil {
		return nil, err
	}

	ctx.Export("KafkaClusterArn", cluster.Arn)
	return cluster, nil
}
