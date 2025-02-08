package main

import (
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/ec2"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// Function to create 3 Nginx instances in EC2-VPC private subnet
func createNginxInstances(ctx *pulumi.Context, subnet *ec2.Subnet, securityGroup *ec2.SecurityGroup, tags pulumi.StringMap) ([]*ec2.Instance, error) {
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

	instances := []*ec2.Instance{}
	for i := 1; i <= 3; i++ {
		instance, err := ec2.NewInstance(ctx, *pulumi.String("nginx-instance-%d", i), &ec2.InstanceArgs{
			InstanceType:        pulumi.String("t3.micro"),
			VpcSecurityGroupIds: pulumi.StringArray{securityGroup.ID()},
			SubnetId:            subnet.ID(),
			Ami:                 pulumi.String(ami.Id),
			UserData: pulumi.String(`#!/bin/bash
sudo yum update -y
sudo amazon-linux-extras enable nginx1
sudo yum install -y nginx
sudo systemctl start nginx
sudo systemctl enable nginx`),
			Tags: tags,
		})
		if err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}

	return instances, nil
}
