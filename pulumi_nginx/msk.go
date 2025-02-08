package main

import (
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/ec2"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/msk"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// Function to create an MSK Cluster with two subnets
func createMskCluster(ctx *pulumi.Context, vpc *ec2.Vpc, subnets []*ec2.Subnet, tags pulumi.StringMap) (*msk.Cluster, error) {
	subnetIds := pulumi.StringArray{}
	for _, subnet := range subnets {
		subnetIds = append(subnetIds, subnet.ID())
	}

	cluster, err := msk.NewCluster(ctx, "KafkaCluster", &msk.ClusterArgs{
		BrokerNodeGroupInfo: &msk.ClusterBrokerNodeGroupInfoArgs{
			InstanceType:   pulumi.String("kafka.m5.large"),
			ClientSubnets:  subnetIds,            // ✅ Now using multiple subnets
			SecurityGroups: pulumi.StringArray{}, // Add security groups as needed
		},
		KafkaVersion:        pulumi.String("2.8.1"),
		NumberOfBrokerNodes: pulumi.Int(6),
		Tags:                tags,
	})
	if err != nil {
		return nil, err
	}

	ctx.Export("KafkaClusterArn", cluster.Arn)
	return cluster, nil
}
