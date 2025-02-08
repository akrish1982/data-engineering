package main

import (
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/iam"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/msk"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/mskconnect"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

func createMskKafkaConnect(ctx *pulumi.Context, kafkaCluster *msk.Cluster, subnetIds pulumi.StringArray, securityGroupId pulumi.StringInput, tags pulumi.StringMap) (*mskconnect.Connector, error) {
	// ✅ Fetch AWS Account ID
	callerIdentity, err := aws.GetCallerIdentity(ctx, nil)
	if err != nil {
		return nil, err
	}
	accountId := callerIdentity.AccountId // Dynamic Account ID

	// ✅ Fetch AWS Region
	region, err := aws.GetRegion(ctx, nil)
	if err != nil {
		return nil, err
	}

	// ✅ Create IAM Role for Kafka Connect
	role, err := iam.NewRole(ctx, "KafkaConnectRole", &iam.RoleArgs{
		AssumeRolePolicy: pulumi.String(`{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "kafkaconnect.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }`),
		Tags: tags,
	})
	if err != nil {
		return nil, err
	}

	// ✅ Attach IAM Policy for MSK Kafka Connect
	_, err = iam.NewRolePolicyAttachment(ctx, "KafkaConnectPolicy", &iam.RolePolicyAttachmentArgs{
		Role:      role.Name,
		PolicyArn: pulumi.String("arn:aws:iam::aws:policy/AmazonMSKFullAccess"),
	})
	if err != nil {
		return nil, err
	}

	// ✅ Create MSK Kafka Connect Cluster
	kafkaConnect, err := mskconnect.NewConnector(ctx, "KafkaConnect", &mskconnect.ConnectorArgs{
		Name:                pulumi.String("kafka-connect-example"),
		KafkaconnectVersion: pulumi.String("2.7.1"),

		// Kafka Cluster Configuration
		KafkaCluster: &mskconnect.ConnectorKafkaClusterArgs{
			ApacheKafkaCluster: &mskconnect.ConnectorKafkaClusterApacheKafkaClusterArgs{
				BootstrapServers: kafkaCluster.BootstrapBrokersTls,
				Vpc: &mskconnect.ConnectorKafkaClusterApacheKafkaClusterVpcArgs{
					SecurityGroups: pulumi.StringArray{securityGroupId},
					Subnets:        subnetIds,
				},
			},
		},
		KafkaClusterClientAuthentication: &mskconnect.ConnectorKafkaClusterClientAuthenticationArgs{
			AuthenticationType: pulumi.String("NONE"),
		},
		KafkaClusterEncryptionInTransit: &mskconnect.ConnectorKafkaClusterEncryptionInTransitArgs{
			EncryptionType: pulumi.String("TLS"),
		},

		// Capacity Configuration
		Capacity: &mskconnect.ConnectorCapacityArgs{
			Autoscaling: &mskconnect.ConnectorCapacityAutoscalingArgs{
				McuCount:       pulumi.Int(1),
				MinWorkerCount: pulumi.Int(1),
				MaxWorkerCount: pulumi.Int(2),
				ScaleInPolicy: &mskconnect.ConnectorCapacityAutoscalingScaleInPolicyArgs{
					CpuUtilizationPercentage: pulumi.Int(20),
				},
				ScaleOutPolicy: &mskconnect.ConnectorCapacityAutoscalingScaleOutPolicyArgs{
					CpuUtilizationPercentage: pulumi.Int(80),
				},
			},
		},

		// Connector Configuration
		ConnectorConfiguration: pulumi.StringMap{
			"connector.class": pulumi.String("com.github.jcustenborder.kafka.connect.simulator.SimulatorSinkConnector"),
			"tasks.max":       pulumi.String("1"),
			"topics":          pulumi.String("example"),
		},

		// Plugins (✅ Use dynamically fetched ARN)
		Plugins: mskconnect.ConnectorPluginArray{
			&mskconnect.ConnectorPluginArgs{
				CustomPlugin: &mskconnect.ConnectorPluginCustomPluginArgs{
					Arn:      pulumi.Sprintf("arn:aws:kafkaconnect:%s:%s:custom-plugin/example-plugin", region.Name, accountId), // ✅ DYNAMICALLY GENERATED ARN
					Revision: pulumi.Int(1),
				},
			},
		},

		ServiceExecutionRoleArn: role.Arn,
		Tags:                    tags,
	})
	if err != nil {
		return nil, err
	}

	ctx.Export("KafkaConnectArn", kafkaConnect.Arn)
	return kafkaConnect, nil
}
