package main

import (
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/ec2"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// Function to create a bastion host in public subnet
func createBastion(ctx *pulumi.Context, subnet *ec2.Subnet, tags pulumi.StringMap) (*ec2.Instance, error) {
	ami, err := ec2.LookupAmi(ctx, &ec2.LookupAmiArgs{
		MostRecent: pulumi.BoolRef(true),
		Filters: []ec2.GetAmiFilter{
			{Name: "name", Values: []string{"amzn2-ami-hvm-*-x86_64-gp2"}},
		},
		Owners: []string{"137112412989"},
	})
	if err != nil {
		return nil, err
	}

	bastion, err := ec2.NewInstance(ctx, "bastion-host", &ec2.InstanceArgs{
		InstanceType: pulumi.String("t3.micro"),
		SubnetId:     subnet.ID(),
		Ami:          pulumi.String(ami.Id),
		Tags:         tags,
	})
	if err != nil {
		return nil, err
	}

	return bastion, nil
}
