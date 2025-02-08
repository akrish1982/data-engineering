package main

import (
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {
		tags := pulumi.StringMap{
			"caylent:customer": pulumi.String("lgads"),
			"caylent:owner":    pulumi.String("ananth.tirumanur@caylent.com"),
		}

		// Create EC2 VPC
		ec2Vpc, ec2Subnet, err := createVpc(ctx, "EC2-VPC", "10.0.0.0/16", tags)
		if err != nil {
			return err
		}

		// Create Kafka VPC
		kafkaVpc, kafkaSubnet, err := createVpc(ctx, "Kafka-VPC", "10.1.0.0/16", tags)
		if err != nil {
			return err
		}

		// Create VPC Peering
		_, err = createVpcPeering(ctx, ec2Vpc, kafkaVpc, tags)
		if err != nil {
			return err
		}

		// Deploy Bastion Host in EC2 VPC (Public Subnet)
		_, err = createBastion(ctx, ec2Subnet, tags)
		if err != nil {
			return err
		}

		// Create Security Group for MSK Kafka
		kafkaSecurityGroup, err := createKafkaSecurityGroup(ctx, kafkaVpc, tags)
		if err != nil {
			return err
		}
		// Create Security Groups
		nginxSecurityGroup, err := createNginxSecurityGroup(ctx, ec2Vpc, tags)
		if err != nil {
			return err
		}

		// Create Nginx Instances
		_, err = createNginxInstances(ctx, ec2Subnet, nginxSecurityGroup, tags)
		if err != nil {
			return err
		}

		// Create MSK Kafka Cluster in Kafka-VPC (Private Subnet)
		mskCluster, err := createMskCluster(ctx, kafkaVpc, kafkaSubnet, tags)
		if err != nil {
			return err
		}

		// Create MSK Kafka Connect in Kafka-VPC (after MSK Cluster is created)
		_, err = createMskKafkaConnect(ctx, mskCluster, pulumi.StringArray{kafkaSubnet.ID()}, kafkaSecurityGroup.ID(), tags)
		if err != nil {
			return err
		}

		return nil
	})
}
