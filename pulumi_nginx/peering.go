package main

import (
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/ec2"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// Function to create a VPC peering connection
func createVpcPeering(ctx *pulumi.Context, ec2Vpc *ec2.Vpc, kafkaVpc *ec2.Vpc, tags pulumi.StringMap) (*ec2.VpcPeeringConnection, error) {
	peering, err := ec2.NewVpcPeeringConnection(ctx, "vpc-peering", &ec2.VpcPeeringConnectionArgs{
		VpcId:      ec2Vpc.ID(),
		PeerVpcId:  kafkaVpc.ID(),
		AutoAccept: pulumi.Bool(true),
		Tags:       tags,
	})
	if err != nil {
		return nil, err
	}

	return peering, nil
}
