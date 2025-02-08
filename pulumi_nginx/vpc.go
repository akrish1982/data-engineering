package main

import (
	"fmt"

	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/ec2"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// Function to create a VPC
func createVpc(ctx *pulumi.Context, name string, cidr string, tags pulumi.StringMap) (*ec2.Vpc, *ec2.Subnet, error) {
	vpc, err := ec2.NewVpc(ctx, name, &ec2.VpcArgs{
		CidrBlock: pulumi.String(cidr),
		Tags:      tags,
	})
	if err != nil {
		return nil, nil, err
	}

	subnet, err := ec2.NewSubnet(ctx, name+"-subnet", &ec2.SubnetArgs{
		VpcId:     vpc.ID(),
		CidrBlock: pulumi.String(cidr[:len(cidr)-3] + "/24"), // Adjusting subnet CIDR
		Tags:      tags,
	})
	if err != nil {
		return nil, nil, err
	}

	return vpc, subnet, nil
}

// Function to create a VPC with multiple subnets
func createVpcMultipleSubnets(ctx *pulumi.Context, name string, cidr string, tags pulumi.StringMap) (*ec2.Vpc, []*ec2.Subnet, error) {
	vpc, err := ec2.NewVpc(ctx, name, &ec2.VpcArgs{
		CidrBlock: pulumi.String(cidr),
		Tags:      tags,
	})
	if err != nil {
		return nil, nil, err
	}

	// ✅ Define fixed CIDR blocks for subnets
	subnetCIDRs := []string{"10.1.1.0/24", "10.1.2.0/24"} // Replace with actual values based on VPC CIDR

	subnets := []*ec2.Subnet{}
	azs := []string{"us-east-1a", "us-east-1b"} // Ensure different AZs

	for i, subnetCIDR := range subnetCIDRs {
		subnetName := fmt.Sprintf("%s-subnet-%s", name, azs[i]) // ✅ Use `fmt.Sprintf()` for static names

		subnet, err := ec2.NewSubnet(ctx, subnetName, &ec2.SubnetArgs{
			VpcId:            vpc.ID(),
			CidrBlock:        pulumi.String(subnetCIDR),
			AvailabilityZone: pulumi.String(azs[i]),
			Tags:             tags,
		})
		if err != nil {
			return nil, nil, err
		}
		subnets = append(subnets, subnet)
	}

	return vpc, subnets, nil
}

// Function to create a security group for Nginx in EC2-VPC
func createNginxSecurityGroup(ctx *pulumi.Context, vpc *ec2.Vpc, tags pulumi.StringMap) (*ec2.SecurityGroup, error) {
	securityGroup, err := ec2.NewSecurityGroup(ctx, "NginxSecurityGroup", &ec2.SecurityGroupArgs{
		VpcId: vpc.ID(),
		Ingress: ec2.SecurityGroupIngressArray{
			ec2.SecurityGroupIngressArgs{
				Protocol:   pulumi.String("tcp"),
				FromPort:   pulumi.Int(80), // Allow HTTP access
				ToPort:     pulumi.Int(80),
				CidrBlocks: pulumi.StringArray{pulumi.String("10.0.0.0/16")}, // Allow internal access
			},
		},
		Tags: tags,
	})
	if err != nil {
		return nil, err
	}
	return securityGroup, nil
}

// Function to create a security group in Kafka VPC
func createKafkaSecurityGroup(ctx *pulumi.Context, vpc *ec2.Vpc, tags pulumi.StringMap) (*ec2.SecurityGroup, error) {
	securityGroup, err := ec2.NewSecurityGroup(ctx, "KafkaSecurityGroup", &ec2.SecurityGroupArgs{
		VpcId: vpc.ID(),
		Ingress: ec2.SecurityGroupIngressArray{
			ec2.SecurityGroupIngressArgs{
				Protocol:   pulumi.String("tcp"),
				FromPort:   pulumi.Int(9092), // Kafka Port
				ToPort:     pulumi.Int(9092),
				CidrBlocks: pulumi.StringArray{pulumi.String("10.0.0.0/16")}, // Only allow from EC2-VPC
			},
		},
		Egress: ec2.SecurityGroupEgressArray{
			ec2.SecurityGroupEgressArgs{
				Protocol:   pulumi.String("-1"),
				FromPort:   pulumi.Int(0),
				ToPort:     pulumi.Int(0),
				CidrBlocks: pulumi.StringArray{pulumi.String("0.0.0.0/0")},
			},
		},
		Tags: tags,
	})
	if err != nil {
		return nil, err
	}
	return securityGroup, nil
}
